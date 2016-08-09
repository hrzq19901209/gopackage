package server

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/http"

	"github.com/Sirupsen/logrus"
	"github.com/docker/distribution/health"
	"github.com/docker/distribution/registry/auth"
	"github.com/docker/notary"
	"github.com/docker/notary/server/handlers"
	"github.com/docker/notary/tuf/data"
	"github.com/docker/notary/tuf/signed"
	"github.com/docker/notary/utils"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/net/context"
)

func init() {
	data.SetDefaultExpiryTimes(notary.NotaryDefaultExpiries)
}

func prometheusOpts(operation string) prometheus.SummaryOpts {
	return prometheus.SummaryOpts{
		Namespace:   "notary_server",
		Subsystem:   "http",
		ConstLabels: prometheus.Labels{"operation": operation},
	}
}

// Config tells Run how to configure a server
type Config struct {
	Addr                         string
	TLSConfig                    *tls.Config
	Trust                        signed.CryptoService
	AuthMethod                   string
	AuthOpts                     interface{}
	ConsistentCacheControlConfig utils.CacheControlConfig
	CurrentCacheControlConfig    utils.CacheControlConfig
}

// Run sets up and starts a TLS server that can be cancelled using the
// given configuration. The context it is passed is the context it should
// use directly for the TLS server, and generate children off for requests
func Run(ctx context.Context, conf Config) error {
	tcpAddr, err := net.ResolveTCPAddr("tcp", conf.Addr)
	if err != nil {
		return err
	}
	var lsnr net.Listener
	lsnr, err = net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return err
	}

	if conf.TLSConfig != nil {
		logrus.Info("Enabling TLS")
		lsnr = tls.NewListener(lsnr, conf.TLSConfig)
	}

	var ac auth.AccessController
	if conf.AuthMethod == "token" {
		authOptions, ok := conf.AuthOpts.(map[string]interface{})
		if !ok {
			return fmt.Errorf("auth.options must be a map[string]interface{}")
		}
		ac, err = auth.GetAccessController(conf.AuthMethod, authOptions)
		if err != nil {
			return err
		}
	}

	svr := http.Server{
		Addr:    conf.Addr,
		Handler: RootHandler(ac, ctx, conf.Trust, conf.ConsistentCacheControlConfig, conf.CurrentCacheControlConfig),
	}

	logrus.Info("Starting on ", conf.Addr)

	err = svr.Serve(lsnr)

	return err
}

// RootHandler returns the handler that routes all the paths from / for the
// server.
func RootHandler(ac auth.AccessController, ctx context.Context, trust signed.CryptoService,
	consistent, current utils.CacheControlConfig) http.Handler {

	hand := utils.RootHandlerFactory(ac, ctx, trust)

	r := mux.NewRouter()
	r.Methods("GET").Path("/v2/").Handler(hand(handlers.MainHandler))
	r.Methods("POST").Path("/v2/{imageName:.*}/_trust/tuf/").Handler(
		prometheus.InstrumentHandlerWithOpts(
			prometheusOpts("UpdateTuf"),
			hand(handlers.AtomicUpdateHandler, "push", "pull")))
	r.Methods("GET").Path("/v2/{imageName:.*}/_trust/tuf/{tufRole:root|targets(?:/[^/\\s]+)*|snapshot|timestamp}.{checksum:[a-fA-F0-9]{64}|[a-fA-F0-9]{96}|[a-fA-F0-9]{128}}.json").Handler(
		prometheus.InstrumentHandlerWithOpts(
			prometheusOpts("GetRoleByHash"),
			utils.WrapWithCacheHandler(consistent, hand(handlers.GetHandler, "pull"))))
	r.Methods("GET").Path("/v2/{imageName:.*}/_trust/tuf/{tufRole:root|targets(?:/[^/\\s]+)*|snapshot|timestamp}.json").Handler(
		prometheus.InstrumentHandlerWithOpts(
			prometheusOpts("GetRole"),
			utils.WrapWithCacheHandler(current, hand(handlers.GetHandler, "pull"))))
	r.Methods("GET").Path(
		"/v2/{imageName:.*}/_trust/tuf/{tufRole:snapshot|timestamp}.key").Handler(
		prometheus.InstrumentHandlerWithOpts(
			prometheusOpts("GetKey"),
			hand(handlers.GetKeyHandler, "push", "pull")))
	r.Methods("DELETE").Path("/v2/{imageName:.*}/_trust/tuf/").Handler(
		prometheus.InstrumentHandlerWithOpts(
			prometheusOpts("DeleteTuf"),
			hand(handlers.DeleteHandler, "push", "pull")))

	r.Methods("GET").Path("/_notary_server/health").HandlerFunc(health.StatusHandler)
	r.Methods("GET").Path("/metrics").Handler(prometheus.Handler())
	r.Methods("GET", "POST", "PUT", "HEAD", "DELETE").Path("/{other:.*}").Handler(
		hand(handlers.NotFoundHandler))

	return r
}
