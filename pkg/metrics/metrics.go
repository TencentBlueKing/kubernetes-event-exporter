package metrics

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/exporter-toolkit/web"
	"github.com/rs/zerolog/log"

	"github.com/resmoio/kubernetes-event-exporter/pkg/version"
)

type Store struct {
	EventsProcessed    prometheus.Counter
	EventsDiscarded    prometheus.Counter
	WatchErrors        prometheus.Counter
	SendErrors         prometheus.Counter
	BuildInfo          prometheus.GaugeFunc
	RerunTotal         prometheus.Counter
	EventsTypeReceived *prometheus.CounterVec
}

// promLogger implements promhttp.Logger
type promLogger struct{}

func (pl promLogger) Println(v ...interface{}) {
	log.Logger.Error().Msg(fmt.Sprint(v...))
}

// promLogger implements the Logger interface
func (pl promLogger) Log(v ...interface{}) error {
	log.Logger.Info().Msg(fmt.Sprint(v...))
	return nil
}

func Init(addr string, tlsConf string) {
	// Setup the prometheus metrics machinery
	// Add Go module build info.
	prometheus.MustRegister(collectors.NewBuildInfoCollector())

	promLogger := promLogger{}
	metricsPath := "/metrics"

	// Expose the registered metrics via HTTP.
	http.Handle(metricsPath, promhttp.HandlerFor(
		prometheus.DefaultGatherer,
		promhttp.HandlerOpts{
			// Opt into OpenMetrics to support exemplars.
			EnableOpenMetrics: true,
		},
	))

	landingConfig := web.LandingConfig{
		Name:        "kubernetes-event-exporter",
		Description: "Export Kubernetes Events to multiple destinations with routing and filtering",
		Links: []web.LandingLinks{
			{
				Address: metricsPath,
				Text:    "Metrics",
			},
		},
	}
	landingPage, _ := web.NewLandingPage(landingConfig)
	http.Handle("/", landingPage)

	http.HandleFunc("/-/healthy", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "OK")
	})
	http.HandleFunc("/-/ready", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "OK")
	})

	metricsServer := http.Server{ReadHeaderTimeout: 5 * time.Second}
	metricsFlags := web.FlagConfig{
		WebListenAddresses: &[]string{addr},
		WebSystemdSocket:   new(bool),
		WebConfigFile:      &tlsConf,
	}

	// start up the http listener to expose the metrics
	go web.ListenAndServe(&metricsServer, &metricsFlags, promLogger)
}

const (
	namespace = "kubeevent_exporter"
)

func newMetricsStore() *Store {
	return &Store{
		BuildInfo: promauto.NewGaugeFunc(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "build_info",
				Help:      "A metric with a constant '1' value labeled by version, revision, branch, and goversion from which Kubernetes Event Exporter was built.",
				ConstLabels: prometheus.Labels{
					"version":   version.Version,
					"revision":  version.Revision(),
					"goversion": version.GoVersion,
					"goos":      version.GoOS,
					"goarch":    version.GoArch,
				},
			},
			func() float64 { return 1 },
		),
		EventsProcessed: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "events_sent_total",
			Help:      "The total number of events processed",
		}),
		EventsDiscarded: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "events_discarded_total",
			Help:      "The total number of events discarded because of being older than the maxEventAgeSeconds specified",
		}),
		WatchErrors: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "watch_errors_total",
			Help:      "The total number of errors received from the client",
		}),
		SendErrors: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "send_event_errors_total",
			Help:      "The total number of send event errors",
		}),
		EventsTypeReceived: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "receive_events_total",
			Help:      "The total number of events received from kubernetes",
		}, []string{"type"}),
		RerunTotal: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "rerun_total",
			Help:      "The total number of rerun event",
		}),
	}
}

var Default = newMetricsStore()
