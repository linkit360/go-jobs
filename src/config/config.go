package config

import (
	"flag"
	"os"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/jinzhu/configor"

	inmem_client "github.com/vostrok/inmem/rpcclient"
	"github.com/vostrok/utils/amqp"
	queue_config "github.com/vostrok/utils/config"
	"github.com/vostrok/utils/db"
)

type ServerConfig struct {
	Port         string `default:"50304"`
	OperatorName string `yaml:"operator_name"`
	ThreadsCount int    `default:"1" yaml:"threads_count"`
}

type AppConfig struct {
	MetricInstancePrefix string                       `yaml:"metric_instance_prefix"`
	AppName              string                       `yaml:"app_name"`
	Server               ServerConfig                 `yaml:"server"`
	InMemClientConfig    inmem_client.RPCClientConfig `yaml:"inmem_client"`
	DbConf               db.DataBaseConfig            `yaml:"db"`
	Service              ServiceConfig                `yaml:"queues"`
	Consumer             amqp.ConsumerConfig          `yaml:"consumer"`
	Notifier             amqp.NotifierConfig          `yaml:"publisher"`
}

type ServiceConfig struct {
	TransactionLog string              `yaml:"transaction_log" default:"transaction_log"`
	Mobilink       MobilinkQueueConfig `yaml:"mobilink"`
	Yondu          YonduQueueConfig    `yaml:"yondu"`
}

type YonduQueueConfig struct {
	Enabled bool `yaml:"enabled" default:"false"`
	//Periodic        PeriodicConfig                  `yaml:"periodic" `
	NewSubscription queue_config.ConsumeQueueConfig `yaml:"new"`
	SentConsent     string                          `yaml:"sent_consent"`
	MT              string                          `yaml:"mt"`
	Charge          string                          `yaml:"charge"`
	CallBack        queue_config.ConsumeQueueConfig `yaml:"callBack"`
}

type MobilinkQueueConfig struct {
	Enabled         bool                            `yaml:"enabled" default:"false"`
	Periodic        bool                            `yaml:"periodic" default:"false"`
	NewSubscription queue_config.ConsumeQueueConfig `yaml:"new"`
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

	if appConfig.AppName == "" {
		log.Fatal("app name must be defiled as <host>_<name>")
	}
	if strings.Contains(appConfig.AppName, "-") {
		log.Fatal("app name must be without '-' : it's not a valid metric name")
	}
	if appConfig.MetricInstancePrefix == "" {
		log.Fatal("metric_instance_prefix be defiled as <host>_<name>")
	}
	if strings.Contains(appConfig.MetricInstancePrefix, "-") {
		log.Fatal("metric_instance_prefix be without '-' : it's not a valid metric name")
	}

	appConfig.Server.Port = envString("PORT", appConfig.Server.Port)
	appConfig.Consumer.Conn.Host = envString("RBMQ_HOST", appConfig.Consumer.Conn.Host)
	appConfig.Notifier.Conn.Host = envString("RBMQ_HOST", appConfig.Notifier.Conn.Host)

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
