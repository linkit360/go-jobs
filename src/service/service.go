package service

import (
	"database/sql"

	log "github.com/Sirupsen/logrus"
	amqp_driver "github.com/streadway/amqp"

	"github.com/vostrok/mo/src/config"
	"github.com/vostrok/utils/amqp"
	queue_config "github.com/vostrok/utils/config"
	"github.com/vostrok/utils/db"
)

var svc Service

type Service struct {
	conf                 Config
	consumer             *rabbit.Consumer
	publisher            *rabbit.Notifier
	newSubscriptionsChan map[string]<-chan amqp_driver.Delivery
	db                   *sql.DB
}

type Config struct {
	server    config.ServerConfig
	db        db.DataBaseConfig
	operators map[string]queue_config.OperatorConfig
	queues    map[string]queue_config.OperatorQueueConfig
	consumer  rabbit.ConsumerConfig
	publisher rabbit.NotifierConfig
}

func InitService(
	serverConfig config.ServerConfig,
	dbConf db.DataBaseConfig,
	operatorsConf map[string]queue_config.OperatorConfig,
	queuesConfig map[string]queue_config.OperatorQueueConfig,
	consumerConfig rabbit.ConsumerConfig,
	notifierConfig rabbit.NotifierConfig,
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
	initInMem()

	svc.publisher = rabbit.NewNotifier(notifierConfig)

	// process consumer
	svc.consumer = rabbit.NewConsumer(consumerConfig)
	if err := svc.consumer.Connect(); err != nil {
		log.Fatal("rbmq consumer connect:", err.Error())
	}
	var err error
	svc.newSubscriptionsChan = make(map[string]<-chan amqp_driver.Delivery, len(svc.conf.queues))

	for operatorName, queue := range svc.conf.queues {
		svc.newSubscriptionsChan[operatorName], err = svc.consumer.AnnounceQueue(
			queue.NewSubscription,
			queue.NewSubscription,
		)
		if err != nil {
			log.WithFields(log.Fields{
				"queue": queue.NewSubscription,
				"error": err.Error(),
			}).Fatal("rbmq consumer: AnnounceQueue")
		}
		go svc.consumer.Handle(
			svc.newSubscriptionsChan[operatorName],
			process,
			serverConfig.ThreadsCount,
			queue.NewSubscription,
			queue.NewSubscription,
		)
	}
}
