package service

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"

	inmem_client "github.com/vostrok/inmem/rpcclient"
	transaction_log_service "github.com/vostrok/qlistener/src/service"
	"github.com/vostrok/utils/amqp"
	"github.com/vostrok/utils/config"
	rec "github.com/vostrok/utils/rec"
)

func (svc *Service) sendTarifficate(r rec.Record) error {
	operator, err := inmem_client.GetOperatorByCode(r.OperatorCode)
	if err != nil {
		OperatorNotApplicable.Inc()
		NotifyErrors.Inc()

		err = fmt.Errorf("Cannot get operator by code: %d, error: %s", r.OperatorCode, err.Error())
		log.WithFields(log.Fields{
			"tid":    r.Tid,
			"msisdn": r.Msisdn,
			"error":  err.Error(),
		}).Error("send tarifficate: cannot get operator by code")
		return err
	}
	operatorName := strings.ToLower(operator.Name)

	queue := config.GetMOQueueName(operatorName)

	event := amqp.EventNotify{
		EventName: "charge",
		EventData: r,
	}
	body, err := json.Marshal(event)
	if err != nil {
		NotifyErrors.Inc()

		err = fmt.Errorf("json.Marshal: %s", err.Error())
		log.WithFields(log.Fields{
			"tid":    r.Tid,
			"msisdn": r.Msisdn,
			"error":  err.Error(),
		}).Error("send tarifficate: cannot marshal charge event")
		return fmt.Errorf("json.Marshal: %s", err.Error())
	}

	log.WithFields(log.Fields{
		"tid":    r.Tid,
		"msisdn": r.Msisdn,
		"queue":  queue,
	}).Info("send")
	svc.publisher.Publish(amqp.AMQPMessage{queue, 0, body})
	return nil
}

func (svc *Service) publishTransactionLog(eventName string,
	transactionMsg transaction_log_service.OperatorTransactionLog) error {
	transactionMsg.SentAt = time.Now().UTC()
	event := amqp.EventNotify{
		EventName: eventName,
		EventData: transactionMsg,
	}
	body, err := json.Marshal(event)
	if err != nil {
		NotifyErrors.Inc()

		return fmt.Errorf("json.Marshal: %s", err.Error())
	}
	svc.publisher.Publish(amqp.AMQPMessage{svc.conf.queues.TransactionLog, 0, body})
	return nil
}

func (svc *Service) publishYonduSentConsent(r rec.Record) error {
	r.SentAt = time.Now().UTC()
	event := amqp.EventNotify{
		EventName: "sent_consent",
		EventData: r,
	}
	body, err := json.Marshal(event)
	if err != nil {
		NotifyErrors.Inc()

		return fmt.Errorf("json.Marshal: %s", err.Error())
	}
	svc.publisher.Publish(amqp.AMQPMessage{svc.conf.queues.Yondu.SentConsent, 0, body})
	return nil
}
func (svc *Service) publishYonduMT(r rec.Record) error {
	r.SentAt = time.Now().UTC()
	event := amqp.EventNotify{
		EventName: "mt",
		EventData: r,
	}
	body, err := json.Marshal(event)
	if err != nil {
		NotifyErrors.Inc()

		return fmt.Errorf("json.Marshal: %s", err.Error())
	}
	svc.publisher.Publish(amqp.AMQPMessage{svc.conf.queues.Yondu.MT, 0, body})
	return nil
}
