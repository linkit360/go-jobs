package service

// here happens any initialization of new subscriptions

import (
	"strconv"
	"time"

	log "github.com/Sirupsen/logrus"
	cache "github.com/patrickmn/go-cache"
	amqp_driver "github.com/streadway/amqp"

	inmem_client "github.com/vostrok/inmem/rpcclient"
	"github.com/vostrok/mo/src/config"
	"github.com/vostrok/utils/amqp"
	"github.com/vostrok/utils/db"
	"github.com/vostrok/utils/rec"
)

var svc Service

type Service struct {
	conf      Config
	consumer  Consumers
	channels  Channels
	publisher *amqp.Notifier
	prevCache *cache.Cache
}

type Config struct {
	server    config.ServerConfig
	db        db.DataBaseConfig
	operators map[string]struct{}
	internal  config.ServiceConfig
	consumer  amqp.ConsumerConfig
	publisher amqp.NotifierConfig
}

type Consumers struct {
	Mobilink *amqp.Consumer
	Yondu    *amqp.Consumer
}

type Channels struct {
	Mobilink <-chan amqp_driver.Delivery
	Yondu    <-chan amqp_driver.Delivery
}

func InitService(
	appName string,
	serverConfig config.ServerConfig,
	inMemConfig inmem_client.RPCClientConfig,
	dbConf db.DataBaseConfig,
	queuesConfig config.ServiceConfig,
	consumerConfig amqp.ConsumerConfig,
	notifierConfig amqp.NotifierConfig,
) {
	log.SetLevel(log.DebugLevel)
	svc.conf = Config{
		server:    serverConfig,
		db:        dbConf,
		internal:  queuesConfig,
		consumer:  consumerConfig,
		publisher: notifierConfig,
	}
	initMetrics(appName)
	rec.Init(dbConf)

	if err := inmem_client.Init(inMemConfig); err != nil {
		log.Fatal("cann't init inmemory service")
	}

	svc.publisher = amqp.NewNotifier(notifierConfig)
}
