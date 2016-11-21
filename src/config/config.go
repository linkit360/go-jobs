package config

import (
	"flag"
	"os"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/jinzhu/configor"

	"github.com/vostrok/utils/amqp"
	"github.com/vostrok/utils/config"
	"github.com/vostrok/utils/db"
)

type ServerConfig struct {
	Port         string `default:"50304"`
	OperatorName string `yaml:"operator_name"`
	ThreadsCount int    `default:"1" yaml:"threads_count"`
}
type AppConfig struct {
	Name      string                                `yaml:"name"`
	Server    ServerConfig                          `yaml:"server"`
	DbConf    db.DataBaseConfig                     `yaml:"db"`
	Consumer  amqp.ConsumerConfig                   `yaml:"consumer"`
	Notifier  amqp.NotifierConfig                   `yaml:"publisher"`
	Operators map[string]config.OperatorConfig      `yaml:"operators"`
	Queues    map[string]config.OperatorQueueConfig `yaml:"-"`
}

func LoadConfig() AppConfig {
	cfg := flag.String("config", "dev/appconfig.yml", "configuration yml file")
	flag.Parse()
	var appConfig AppConfig

	if *cfg != "" {
		if err := configor.Load(&appConfig, *cfg); err != nil {
			log.WithField("config", err.Error()).Fatal("config load error")
		}
	}

	if appConfig.Name == "" {
		log.Fatal("app name must be defiled as <host>_<name>")
	}
	if strings.Contains(appConfig.Name, "-") {
		log.Fatal("app name must be without '-' : it's not a valid metric name")
	}

	appConfig.Server.Port = envString("PORT", appConfig.Server.Port)
	appConfig.Consumer.Conn.Host = envString("RBMQ_HOST", appConfig.Consumer.Conn.Host)
	appConfig.Notifier.Conn.Host = envString("RBMQ_HOST", appConfig.Notifier.Conn.Host)

	appConfig.Queues = config.GetOperatorsQueue(appConfig.Operators)

	log.WithField("config", appConfig).Info("Config loaded")
	return appConfig
}

func envString(env, fallback string) string {
	e := os.Getenv(env)
	if e == "" {
		return fallback
	}
	return e
}
