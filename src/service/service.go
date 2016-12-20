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
	initCache()

	if err := inmem_client.Init(inMemConfig); err != nil {
		log.Fatal("cann't init inmemory service")
	}

	svc.publisher = amqp.NewNotifier(notifierConfig)

	svc.consumer = Consumers{}

	if queuesConfig.Mobilink.Enabled {
		svc.consumer.Mobilink = amqp.NewConsumer(
			consumerConfig,
			queuesConfig.Mobilink.NewSubscription.Name,
			queuesConfig.Mobilink.NewSubscription.PrefetchCount,
		)

		if err := svc.consumer.Mobilink.Connect(); err != nil {
			log.Fatal("rbmq consumer connect:", err.Error())
		}

		amqp.InitQueue(
			svc.consumer.Mobilink,
			svc.channels.Mobilink,
			processNewMobilinkSubscription,
			serverConfig.ThreadsCount,
			queuesConfig.Mobilink.NewSubscription.Name,
			queuesConfig.Mobilink.NewSubscription.Name,
		)
	}

	if queuesConfig.Yondu.NewSubscription.Enabled {
		svc.consumer.Yondu = amqp.NewConsumer(
			consumerConfig,
			queuesConfig.Yondu.NewSubscription.Name,
			queuesConfig.Yondu.NewSubscription.PrefetchCount,
		)

		if err := svc.consumer.Yondu.Connect(); err != nil {
			log.Fatal("rbmq consumer connect:", err.Error())
		}

		amqp.InitQueue(
			svc.consumer.Yondu,
			svc.channels.Yondu,
			processNewYonduSubscription,
			serverConfig.ThreadsCount,
			queuesConfig.Yondu.NewSubscription.Name,
			queuesConfig.Yondu.NewSubscription.Name,
		)
	}

	go func() {
		for {
			time.Sleep(time.Second)
			if queuesConfig.Yondu.Periodic {
				processPeriodic()
			}
		}
	}()

}

func initCache() {
	prev, err := rec.LoadPreviousSubscriptions()
	if err != nil {
		log.WithField("error", err.Error()).Fatal("cannot load previous subscriptions")
	}
	log.WithField("count", len(prev)).Debug("loaded previous subscriptions")
	svc.prevCache = cache.New(24*time.Hour, time.Minute)
	for _, v := range prev {
		key := v.Msisdn + strconv.FormatInt(v.ServiceId, 10)
		svc.prevCache.Set(key, struct{}{}, time.Now().Sub(v.CreatedAt))
	}
}
func getPrevSubscriptionCache(msisdn string, serviceId int64, tid string) bool {
	key := msisdn + strconv.FormatInt(serviceId, 10)
	_, found := svc.prevCache.Get(key)
	log.WithFields(log.Fields{
		"tid":   tid,
		"key":   key,
		"found": found,
	}).Debug("get previous subscription cache")
	return found
}
func setPrevSubscriptionCache(msisdn string, serviceId int64, tid string) {
	key := msisdn + strconv.FormatInt(serviceId, 10)
	_, found := svc.prevCache.Get(key)
	if !found {
		svc.prevCache.Set(key, struct{}{}, 24*time.Hour)
		log.WithFields(log.Fields{
			"tid": tid,
			"key": key,
		}).Debug("set previous subscription cache")
	}
}
