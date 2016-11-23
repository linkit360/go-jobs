package service

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
	consumer             *amqp.Consumer
	publisher            *amqp.Notifier
	newSubscriptionsChan map[string]<-chan amqp_driver.Delivery
	db                   *sql.DB
}

type Config struct {
	server    config.ServerConfig
	db        db.DataBaseConfig
	operators map[string]queue_config.OperatorConfig
	queues    map[string]queue_config.OperatorQueueConfig
	consumer  amqp.ConsumerConfig
	publisher amqp.NotifierConfig
}

func InitService(
	serverConfig config.ServerConfig,
	inMemConfig inmem_client.RPCClientConfig,
	dbConf db.DataBaseConfig,
	operatorsConf map[string]queue_config.OperatorConfig,
	queuesConfig map[string]queue_config.OperatorQueueConfig,
	consumerConfig amqp.ConsumerConfig,
	notifierConfig amqp.NotifierConfig,
) {
	log.SetLevel(log.DebugLevel)
	svc.conf = Config{
		server:    serverConfig,
		db:        dbConf,
		operators: operatorsConf,
		queues:    queuesConfig,
		consumer:  consumerConfig,
		publisher: notifierConfig,
	}
	initMetrics()

	svc.db = db.Init(dbConf)
	inmem_client.Init(inMemConfig)

	svc.publisher = amqp.NewNotifier(notifierConfig)

	// process consumer
	svc.consumer = amqp.NewConsumer(consumerConfig)
	if err := svc.consumer.Connect(); err != nil {
		log.Fatal("rbmq consumer connect:", err.Error())
	}

	svc.newSubscriptionsChan = make(map[string]<-chan amqp_driver.Delivery, len(svc.conf.queues))
	for operatorName, queue := range svc.conf.queues {
		amqp.InitQueue(
			svc.consumer,
			svc.newSubscriptionsChan[operatorName],
			processNewSubscription,
			serverConfig.ThreadsCount,
			queue.NewSubscription,
			queue.NewSubscription,
		)
	}
}
