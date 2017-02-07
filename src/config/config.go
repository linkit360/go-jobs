package config

import (
	"flag"
	"fmt"
	"os"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/jinzhu/configor"

	inmem_client "github.com/vostrok/inmem/rpcclient"
	"github.com/vostrok/utils/amqp"
	"github.com/vostrok/utils/db"
)

type ServerConfig struct {
	Port string `default:"50304"`
}

type AppConfig struct {
	AppName           string                       `yaml:"app_name"`
	Server            ServerConfig                 `yaml:"server"`
	Metrics           MetricsConfig                `yaml:"metrics"`
	Jobs              JobsConfig                   `yaml:"jobs"`
	InMemClientConfig inmem_client.RPCClientConfig `yaml:"inmem_client"`
	DbConf            db.DataBaseConfig            `yaml:"db"`
	DbSlaveConf       db.DataBaseConfig            `yaml:"db_slave"`
	Notifier          amqp.NotifierConfig          `yaml:"publisher"`
}

type MetricsConfig struct {
	AllowedDBSizeBytes uint64 `yaml:"allowed_db_size"`
}

type JobsConfig struct {
	PlannedEnabled bool   `yaml:"planned_enabled" default:"false"`
	InjectionsPath string `yaml:"injections_path" default:"/var/www/xmp.linkit360.ru/web/injections"`
	LogPath        string `yaml:"log_path" default:"/var/log/"`
	CheckPrefix    string `yaml:"prefix" default:"92"` // todo: move in settings or in db smth
	CallBackUrl    string `yaml:"callback_url"`
}

func LoadConfig() AppConfig {
	cfg := flag.String("config", "dev/jobs.yml", "configuration yml file")
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
	appConfig.Server.Port = envString("PORT", appConfig.Server.Port)
	appConfig.Notifier.Conn.Host = envString("RBMQ_HOST", appConfig.Notifier.Conn.Host)

	log.WithField("config", fmt.Sprintf("%#v", appConfig)).Info("Config")
	return appConfig
}

func envString(env, fallback string) string {
	e := os.Getenv(env)
	if e == "" {
		return fallback
	}
	return e
}
