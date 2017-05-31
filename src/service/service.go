package service

// here happens any initialization of new subscriptions

import (
	"database/sql"
	"os"

	log "github.com/Sirupsen/logrus"

	"github.com/linkit360/go-jobs/src/config"
	mid_client "github.com/linkit360/go-mid/rpcclient"
	"github.com/linkit360/go-utils/amqp"
	"github.com/linkit360/go-utils/db"
)

var svc Service

type Service struct {
	conf                   Config
	publisher              *amqp.Notifier
	dbConn                 *sql.DB
	suspendedSubscriptions *suspendedSubscriptions
	jobs                   *jobs
	exiting                bool
}

type Config struct {
	server    config.ServerConfig
	db        db.DataBaseConfig
	publisher amqp.NotifierConfig
}

func InitService(
	appName string,
	serverConfig config.ServerConfig,
	metricsConfig config.MetricsConfig,
	jobsConfig config.JobsConfig,
	midConfig mid_client.ClientConfig,
	dbConf db.DataBaseConfig,
	dbSlaveConf db.DataBaseConfig,
	notifierConfig amqp.NotifierConfig,
) {
	log.SetLevel(log.DebugLevel)

	svc.dbConn = db.Init(dbConf)
	svc.publisher = amqp.NewNotifier(notifierConfig)
	svc.suspendedSubscriptions = &suspendedSubscriptions{}
	svc.jobs = initJobs(jobsConfig, dbSlaveConf)

	svc.conf = Config{
		server:    serverConfig,
		db:        dbConf,
		publisher: notifierConfig,
	}
	initMetrics(appName, metricsConfig)

	if err := mid_client.Init(midConfig); err != nil {
		log.Fatal("cann't init midory service")
	}
}

func OnExit() {
	log.WithField("pid", os.Getpid()).Info("on exit")
	svc.exiting = true
}
