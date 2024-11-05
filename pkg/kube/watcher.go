package kube

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"

	"github.com/resmoio/kubernetes-event-exporter/pkg/metrics"
)

var startUpTime = time.Now()

type EventHandler func(event *EnhancedEvent)

type EventWatcher struct {
	ctx                context.Context
	cancel             context.CancelFunc
	wg                 sync.WaitGroup
	informer           cache.SharedInformer
	fn                 EventHandler
	maxEventAgeSeconds time.Duration
	metricsStore       *metrics.Store
	clientset          *kubernetes.Clientset
	iw                 *innerWatcher
}

func NewEventWatcher(config *rest.Config, namespace string, MaxEventAgeSeconds int64, fn EventHandler) *EventWatcher {
	clientset := kubernetes.NewForConfigOrDie(config)
	ctx, cancel := context.WithCancel(context.Background())
	return &EventWatcher{
		ctx:                ctx,
		cancel:             cancel,
		iw:                 newInnerWatcher(ctx, namespace, clientset),
		fn:                 fn,
		maxEventAgeSeconds: time.Second * time.Duration(MaxEventAgeSeconds),
		metricsStore:       metrics.Default,
		clientset:          clientset,
	}
}

type innerWatcher struct {
	ctx                 context.Context
	namespace           string
	count               int
	clientset           *kubernetes.Clientset
	ch                  chan innerEvent
	closed              chan struct{}
	lastResourceVersion string
}

type innerEvent struct {
	Type  watch.EventType
	Event *corev1.Event
}

func newInnerWatcher(ctx context.Context, namespace string, clientset *kubernetes.Clientset) *innerWatcher {
	return &innerWatcher{
		ctx:       ctx,
		namespace: namespace,
		clientset: clientset,
		ch:        make(chan innerEvent, 1),
		closed:    make(chan struct{}, 1),
	}
}

func (iw *innerWatcher) Ch() chan innerEvent {
	return iw.ch
}

func (iw *innerWatcher) MustStart() {
	if err := iw.Start(); err != nil {
		panic(err)
	}
}

func (iw *innerWatcher) Start() error {
	defer close(iw.ch)

	rv, err := iw.lastRV()
	if err != nil {
		return err
	}
	iw.lastResourceVersion = rv

	if err := iw.run(); err != nil {
		return err
	}

	for {
		select {
		case <-iw.ctx.Done():
			return nil

		case <-iw.closed:
			log.Warn().Msg("Recv closed signal, try to reconnecting")
			time.Sleep(10 * time.Second)
			metrics.Default.RerunTotal.Add(1)
			if err := iw.run(); err != nil {
				return err
			}
		}
	}
}

func (iw *innerWatcher) lastRV() (string, error) {
	const chunkSize = 500
	var nextToken string
	var lastRv string
	cli := iw.clientset.CoreV1().Events(iw.namespace)

	var round int
	for {
		obj, err := cli.List(iw.ctx, metav1.ListOptions{
			Continue: nextToken,
			Limit:    chunkSize,
		})
		if err != nil {
			return "", err
		}

		for _, item := range obj.Items {
			rv := item.GetResourceVersion()
			if rv != "" {
				lastRv = rv
			}
		}
		round++
		log.Info().Int("round", round).Str("continue", obj.Continue).Str("RV", lastRv).Msg("fetch last rv")

		if obj.Continue == "" {
			break
		}
		nextToken = obj.Continue
	}

	if lastRv == "" {
		return "", errors.New("unknown resource version")
	}

	log.Info().Msgf("last resource version: %v", lastRv)
	return lastRv, nil
}

func (iw *innerWatcher) run() error {
	iw.count++

	timeout := int64(7200)
	log.Info().Str("ResourceVersion", iw.lastResourceVersion).Msgf("run inner-watcher at (%d) times", iw.count)
	w, err := iw.clientset.CoreV1().Events(iw.namespace).Watch(iw.ctx, metav1.ListOptions{
		ResourceVersion: iw.lastResourceVersion,
		TimeoutSeconds:  &timeout,
	})
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case <-iw.ctx.Done():
				return
			case e, ok := <-w.ResultChan():
				if !ok {
					iw.closed <- struct{}{} // notify the watcher conn has broken
					return
				}

				var event *corev1.Event
				switch obj := e.Object.(type) {
				case *corev1.Event:
					event = obj

				case *metav1.Status:
					if obj.Code == http.StatusGone {
						panic("RV too old errors") // lets it crash
					}
					log.Error().Msgf("Recv Status Event: %#v", obj)
					metrics.Default.WatchErrors.Inc()
					continue

				default:
					log.Error().Msgf("Unknown Type (%T), event.Type(%v), event.Obj=(%#v)", e.Object, e.Type, e.Object)
					metrics.Default.WatchErrors.Inc()
					continue
				}

				iw.lastResourceVersion = event.GetResourceVersion()
				iw.ch <- innerEvent{
					Type:  e.Type,
					Event: event,
				}
			}
		}
	}()
	return nil
}

func (e *EventWatcher) loopHandle() {
	for {
		select {
		case evt, ok := <-e.iw.Ch():
			if !ok {
				return
			}
			// only handles Added events
			e.metricsStore.EventsTypeReceived.WithLabelValues(string(evt.Type)).Add(1)
			if evt.Type == watch.Added {
				e.onEvent(evt.Event)
			}

		case <-e.ctx.Done():
			return
		}
	}
}

// Ignore events older than the maxEventAgeSeconds
func (e *EventWatcher) isEventDiscarded(event *corev1.Event) bool {
	timestamp := event.LastTimestamp.Time
	if timestamp.IsZero() {
		timestamp = event.EventTime.Time
	}
	eventAge := time.Since(timestamp)
	if eventAge > e.maxEventAgeSeconds {
		// Log discarded events if they were created after the watcher started
		// (to suppres warnings from initial synchrnization)
		if timestamp.After(startUpTime) {
			log.Warn().
				Str("event age", eventAge.String()).
				Str("event namespace", event.Namespace).
				Str("event name", event.Name).
				Msg("Event discarded as being older then maxEventAgeSeconds")
			e.metricsStore.EventsDiscarded.Inc()
		}
		return true
	}
	return false
}

func (e *EventWatcher) onEvent(event *corev1.Event) {
	if e.isEventDiscarded(event) {
		return
	}

	log.Debug().
		Str("msg", event.Message).
		Str("namespace", event.Namespace).
		Str("reason", event.Reason).
		Str("involvedObject", event.InvolvedObject.Name).
		Msg("Received event")

	e.metricsStore.EventsProcessed.Inc()

	ev := &EnhancedEvent{
		Event: *event.DeepCopy(),
	}
	ev.Event.ManagedFields = nil

	ev.InvolvedObject.ObjectReference = *event.InvolvedObject.DeepCopy()
	e.fn(ev)
}

func (e *EventWatcher) Start() {
	e.wg.Add(2)

	go func() {
		defer e.wg.Done()
		e.iw.MustStart()
	}()

	go func() {
		defer e.wg.Done()
		e.loopHandle()
	}()
}

func (e *EventWatcher) Stop() {
	e.cancel()
	e.wg.Wait()
}

func (e *EventWatcher) setStartUpTime(time time.Time) {
	startUpTime = time
}
