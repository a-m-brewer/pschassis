package pschassis

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// Service is the representation of a ps Microservice
type Service struct {
	name   string
	port   string
	server *http.Server
	Logger *log.Logger
}

// NewService create a new ps microservice
func NewService(name string, port string, logger *log.Logger) *Service {
	svc := Service{
		name:   name,
		port:   port,
		Logger: logger,
	}

	return &svc
}

// StartServer start a new http server
func (s *Service) StartServer() {
	s.Logger.Info("starting http server")
	err := s.server.ListenAndServe()
	if err != nil {
		s.Logger.Fatal(err)
	}
}

// WaitForShutdownSignal blocks the thread until either os.Interrupt or os.Kill
func (s *Service) WaitForShutdownSignal() {
	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, os.Interrupt)
	signal.Notify(sigChan, os.Kill)

	sig := <-sigChan

	s.Logger.WithField("sig", sig).Warn("Received Termination Signal")
	tc, _ := context.WithTimeout(context.Background(), 30*time.Second)
	err := s.server.Shutdown(tc)
	if err != nil {
		s.Logger.WithField("err", err).Fatal(err)
	}
}

// InitConfig sets up the config
func (s *Service) InitConfig() {
	// setup where to get the configuration from
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(fmt.Sprintf("$HOME/.config/%s/", s.name))
	viper.AddConfigPath(fmt.Sprintf("/etc/%s/", s.name))
	viper.AddConfigPath(".")
	// all env variables will be prefixed e.g. PSS_FOO
	viper.SetEnvPrefix("PS")
	viper.AutomaticEnv()

	// setup default values
	viper.SetDefault("addr", s.port)
	viper.SetDefault("service_name", s.name)
	viper.SetDefault("docker", false)

	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {
		s.Logger.WithField("err", err).Fatal("Failed to ReadInConfig")
		os.Exit(1)
	}
}

// SetupServer starts a server using gorilla
func (s *Service) SetupServer(router *mux.Router) {
	s.server = &http.Server{
		Addr:         viper.GetString("addr"),
		Handler:      router,
		IdleTimeout:  120 * time.Second,
		ReadTimeout:  1 * time.Second,
		WriteTimeout: 1 * time.Second,
	}
}

// URL gets the URL for the current service
func (s *Service) URL() (string, error) {
	hostname := viper.GetString("hostname")

	if hostname == "" {
		realHostname, err := os.Hostname()
		if err != nil {
			return "", err
		}
		hostname = realHostname
	}

	return fmt.Sprintf("%s%s", hostname, s.port), nil
}

// CreateLogger create a simple logrus logger
func CreateLogger() *log.Logger {
	l := log.New()
	l.SetFormatter(&log.TextFormatter{})
	l.SetOutput(os.Stdout)
	return l
}
