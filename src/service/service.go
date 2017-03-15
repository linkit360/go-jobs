package service

// here happens any initialization of new subscriptions

import (
	"database/sql"
	"os"

	log "github.com/Sirupsen/logrus"

	inmem_client "github.com/vostrok/inmem/rpcclient"
	"github.com/vostrok/jobs/src/config"
	"github.com/vostrok/utils/amqp"
	"github.com/vostrok/utils/db"
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
	inMemConfig inmem_client.ClientConfig,
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

	if err := inmem_client.Init(inMemConfig); err != nil {
		log.Fatal("cann't init inmemory service")
	}
}

func OnExit() {
	log.WithField("pid", os.Getpid()).Info("on exit")
	svc.exiting = true
}
