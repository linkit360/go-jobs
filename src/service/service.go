package service

// here happens any initialization of new subscriptions

import (
	"database/sql"

	log "github.com/Sirupsen/logrus"
	amqp_driver "github.com/streadway/amqp"

	inmem_client "github.com/vostrok/inmem/rpcclient"
	"github.com/vostrok/mo/src/config"
	"github.com/vostrok/utils/amqp"
	queue_config "github.com/vostrok/utils/config"
	"github.com/vostrok/utils/db"
)

var svc Service

type Service struct {
	conf                 Config
	consumer             map[string]*amqp.Consumer
	publisher            *amqp.Notifier
	newSubscriptionsChan map[string]<-chan amqp_driver.Delivery
	db                   *sql.DB
}

type Config struct {
	server    config.ServerConfig
	db        db.DataBaseConfig
	operators map[string]struct{}
	queues    map[string]queue_config.ConsumeQueueConfig
	consumer  amqp.ConsumerConfig
	publisher amqp.NotifierConfig
}

func InitService(
	serverConfig config.ServerConfig,
	inMemConfig inmem_client.RPCClientConfig,
	dbConf db.DataBaseConfig,
	queuesConfig map[string]queue_config.ConsumeQueueConfig,
	consumerConfig amqp.ConsumerConfig,
	notifierConfig amqp.NotifierConfig,
) {
	log.SetLevel(log.DebugLevel)
	svc.conf = Config{
		server:    serverConfig,
		db:        dbConf,
		queues:    queuesConfig,
		consumer:  consumerConfig,
		publisher: notifierConfig,
	}
	initMetrics()

	svc.db = db.Init(dbConf)
	if err := inmem_client.Init(inMemConfig); err != nil {
		log.Fatal("cann't init inmemory service")
	}

	svc.publisher = amqp.NewNotifier(notifierConfig)

	svc.newSubscriptionsChan = make(map[string]<-chan amqp_driver.Delivery, len(svc.conf.queues))
	svc.consumer = make(map[string]*amqp.Consumer, len(svc.conf.queues))

	for operatorName, queue := range svc.conf.queues {

		svc.consumer[operatorName] = amqp.NewConsumer(
			consumerConfig,
			queue.Name,
			queue.PrefetchCount,
		)

		if err := svc.consumer[operatorName].Connect(); err != nil {
			log.Fatal("rbmq consumer connect:", err.Error())
		}

		amqp.InitQueue(
			svc.consumer[operatorName],
			svc.newSubscriptionsChan[operatorName],
			processNewSubscription,
			serverConfig.ThreadsCount,
			queue.Name,
			queue.Name,
		)
	}
}
