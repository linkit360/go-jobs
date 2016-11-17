package service

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"
	amqp_driver "github.com/streadway/amqp"

	"github.com/vostrok/db"
	m "github.com/vostrok/metrics"
	"github.com/vostrok/mo/src/config"
	rec "github.com/vostrok/mt_manager/src/service/instance"
	"github.com/vostrok/rabbit"
)

var svc Service

type Service struct {
	conf      Config
	consumer  *rabbit.Consumer
	publisher *rabbit.Notifier
	records   <-chan amqp_driver.Delivery
	db        *sql.DB
}
type Config struct {
	server    config.ServerConfig
	db        db.DataBaseConfig
	queues    config.QueueConfig
	consumer  rabbit.ConsumerConfig
	publisher rabbit.NotifierConfig
}

var (
	Dropped        m.Gauge
	Empty          m.Gauge
	DbError        m.Gauge
	AddToDBErrors  m.Gauge
	AddToDbSuccess m.Gauge
)

func initMetrics() {

	Dropped = m.NewGauge("", "", "dropped", "mobilink queue dropped")
	Empty = m.NewGauge("", "", "empty", "mobilink queue empty")
	DbError = m.NewGauge("", "", "db_errors", "db errors overall")
	AddToDBErrors = m.NewGauge("", "", "add_to_db_errors", "subscription add to db errors")
	AddToDbSuccess = m.NewGauge("", "", "add_to_db_success", "subscription add to db success")
	go func() {
		for range time.Tick(time.Minute) {
			Dropped.Update()
			Empty.Update()
			DbError.Update()
			AddToDBErrors.Update()
			AddToDbSuccess.Update()
		}
	}()

}

func InitService(
	serverConfig config.ServerConfig,
	dbConf db.DataBaseConfig,
	queuesConfig config.QueueConfig,
	consumerConfig rabbit.ConsumerConfig,
	notifierConfig rabbit.NotifierConfig,
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

	svc.publisher = rabbit.NewNotifier(notifierConfig)

	// process consumer
	svc.consumer = rabbit.NewConsumer(consumerConfig)
	if err := svc.consumer.Connect(); err != nil {
		log.Fatal("rbmq consumer connect:", err.Error())
	}
	var err error
	svc.records, err = svc.consumer.AnnounceQueue(queuesConfig.NewSubscription, queuesConfig.NewSubscription)
	if err != nil {
		log.WithFields(log.Fields{
			"queue": queuesConfig.NewSubscription,
			"error": err.Error(),
		}).Fatal("rbmq consumer: AnnounceQueue")
	}
	go svc.consumer.Handle(
		svc.records,
		process,
		serverConfig.ThreadsCount,
		queuesConfig.NewSubscription,
		queuesConfig.NewSubscription,
	)
}

func (svc *Service) sendTarifficate(eventName string, r rec.Record) error {
	event := rabbit.EventNotify{
		EventName: eventName,
		EventData: r,
	}
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("json.Marshal: %s", err.Error())
	}
	svc.publisher.Publish(rabbit.AMQPMessage{svc.conf.queues.Tarifficate, body})
	return nil
}
