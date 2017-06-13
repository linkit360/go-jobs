package config

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/jinzhu/configor"
	log "github.com/sirupsen/logrus"

	mid_client "github.com/linkit360/go-mid/rpcclient"
	"github.com/linkit360/go-utils/amqp"
	"github.com/linkit360/go-utils/db"
)

type ServerConfig struct {
	Port string `default:"50304"`
}

type AppConfig struct {
	AppName     string                  `yaml:"app_name"`
	Server      ServerConfig            `yaml:"server"`
	Metrics     MetricsConfig           `yaml:"metrics"`
	MidConfig   mid_client.ClientConfig `yaml:"mid_client"`
	Jobs        JobsConfig              `yaml:"jobs"`
	DbConf      db.DataBaseConfig       `yaml:"db"`
	DbSlaveConf db.DataBaseConfig       `yaml:"db_slave"`
	Notifier    amqp.NotifierConfig     `yaml:"publisher"`
}

type MetricsConfig struct {
	Period             int      `yaml:"period" default:"600"`
	AllowedDBSizeBytes uint64   `yaml:"allowed_db_size"`
	Databases          []string `yaml:"db"`
}

type JobsConfig struct {
	PlannedEnabled       bool   `yaml:"planned_enabled"`
	PlannedPeriodMinutes int    `yaml:"planned_period_minutes"`
	InjectionsPath       string `yaml:"injections_path" default:"/var/www/xmp.linkit360.ru/web/injections"`
	LogPath              string `yaml:"log_path" default:"/var/log/"`
	CheckPrefix          string `yaml:"prefix" default:"92"` // todo: move in settings or in db smth
	CallBackUrl          string `yaml:"callback_url"`
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
