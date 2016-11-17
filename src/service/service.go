package service

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	amqp_driver "github.com/streadway/amqp"

	"github.com/vostrok/db"
	m "github.com/vostrok/metrics"
	"github.com/vostrok/mo/src/config"
	rec "github.com/vostrok/mt_manager/src/service/instance"
	"github.com/vostrok/rabbit"

	queue_config "github.com/vostrok/utils/config"
)

var svc Service

type Service struct {
	conf                 Config
	consumer             *rabbit.Consumer
	publisher            *rabbit.Notifier
	newSubscriptionsChan map[string]<-chan amqp_driver.Delivery
	db                   *sql.DB
}

// queues:
// in: new_subscriptions
// out: asap_tarifficate
type Config struct {
	server    config.ServerConfig
	db        db.DataBaseConfig
	operators []queue_config.OperatorConfig
	queues    map[string]queue_config.OperatorQueueConfig
	consumer  rabbit.ConsumerConfig
	publisher rabbit.NotifierConfig
}

var (
	Dropped               m.Gauge
	Empty                 m.Gauge
	DbError               m.Gauge
	AddToDBErrors         m.Gauge
	AddToDbSuccess        m.Gauge
	OperatorNotEnabled    m.Gauge
	OperatorNotApplicable m.Gauge
)

func newGaugeOperaor(name, help string) m.Gauge {
	return m.NewGaugeMetric("operator", name, "operator "+help)
}

func initMetrics() {
	Dropped = m.NewGauge("", "", "dropped", "mobilink queue dropped")
	Empty = m.NewGauge("", "", "empty", "mobilink queue empty")
	DbError = m.NewGauge("", "", "db_errors", "db errors overall")
	AddToDBErrors = m.NewGauge("", "", "add_to_db_errors", "subscription add to db errors")
	AddToDbSuccess = m.NewGauge("", "", "add_to_db_success", "subscription add to db success")
	OperatorNotEnabled = newGaugeOperaor("not_enabled", "operator is not enabled in config")
	OperatorNotApplicable = newGaugeOperaor("not_applicable", "there is no such operator in database")

	go func() {
		for range time.Tick(time.Minute) {
			Dropped.Update()
			Empty.Update()
			DbError.Update()
			AddToDBErrors.Update()
			AddToDbSuccess.Update()
			OperatorNotEnabled.Update()
			OperatorNotApplicable.Update()
		}
	}()

}

func InitService(
	serverConfig config.ServerConfig,
	dbConf db.DataBaseConfig,
	operatorsConf []queue_config.OperatorConfig,
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

	svc.publisher = rabbit.NewNotifier(notifierConfig)

	// process consumer
	svc.consumer = rabbit.NewConsumer(consumerConfig)
	if err := svc.consumer.Connect(); err != nil {
		log.Fatal("rbmq consumer connect:", err.Error())
	}
	var err error
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

func (svc *Service) sendTarifficate(r rec.Record) error {

	// todo: rpc service? code is repeated, db in use
	operator, ok := memOperators.ByCode[r.OperatorCode]
	if !ok {
		OperatorNotApplicable.Inc()

		log.WithFields(log.Fields{
			"tid":    r.Tid,
			"msisdn": r.Msisdn,
		}).Debug("send tarifficate: not applicable to any operator")
		return fmt.Errorf("Code %s is not applicable to any operator", r.OperatorCode)
	}
	operatorName := strings.ToLower(operator.Name)
	queue, ok := svc.conf.queues[operatorName]
	if !ok {
		OperatorNotEnabled.Inc()

		log.WithFields(log.Fields{
			"tid":    r.Tid,
			"msisdn": r.Msisdn,
		}).Debug("SMS send: not enabled in mt_manager")
		return fmt.Errorf("Name %s is not enabled", operatorName)
	}

	event := rabbit.EventNotify{
		EventName: "charge",
		EventData: r,
	}
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("json.Marshal: %s", err.Error())
	}
	svc.publisher.Publish(rabbit.AMQPMessage{queue.MOTarifficate, body})
	return nil
}
