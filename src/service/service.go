package service

// here happens any initialization of new subscriptions

import (
	log "github.com/Sirupsen/logrus"

	inmem_client "github.com/vostrok/inmem/rpcclient"
	"github.com/vostrok/mo/src/config"
	"github.com/vostrok/utils/amqp"
	"github.com/vostrok/utils/db"
	"github.com/vostrok/utils/rec"
)

var svc Service

type Service struct {
	conf      Config
	publisher *amqp.Notifier
}

type Config struct {
	server    config.ServerConfig
	db        db.DataBaseConfig
	publisher amqp.NotifierConfig
}

func InitService(
	appName string,
	serverConfig config.ServerConfig,
	inMemConfig inmem_client.RPCClientConfig,
	dbConf db.DataBaseConfig,
	notifierConfig amqp.NotifierConfig,
) {
	log.SetLevel(log.DebugLevel)
	svc.conf = Config{
		server:    serverConfig,
		db:        dbConf,
		publisher: notifierConfig,
	}
	initMetrics(appName)
	rec.Init(dbConf)

	if err := inmem_client.Init(inMemConfig); err != nil {
		log.Fatal("cann't init inmemory service")
	}

	svc.publisher = amqp.NewNotifier(notifierConfig)
}
