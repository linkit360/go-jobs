package service

import (
	"encoding/json"
	"fmt"
	"strings"

	log "github.com/Sirupsen/logrus"

	inmem_client "github.com/vostrok/inmem/rpcclient"
	"github.com/vostrok/utils/amqp"
	rec "github.com/vostrok/utils/rec"
)

func (svc *Service) sendTarifficate(r rec.Record) error {
	operator, err := inmem_client.GetOperatorByCode(r.OperatorCode)
	if err != nil {
		OperatorNotApplicable.Inc()

		err = fmt.Errorf("Cannot get operator by code: %d, error: %s", r.OperatorCode, err.Error())
		log.WithFields(log.Fields{
			"tid":    r.Tid,
			"msisdn": r.Msisdn,
			"error":  err.Error(),
		}).Error("send tarifficate: cannot get operator by code")
		return err
	}
	operatorName := strings.ToLower(operator.Name)
	queue, ok := svc.conf.queues[operatorName]
	if !ok {
		OperatorNotEnabled.Inc()

		err = fmt.Errorf("operator %s is not enabled", operatorName)
		log.WithFields(log.Fields{
			"tid":    r.Tid,
			"msisdn": r.Msisdn,
			"error":  err.Error(),
		}).Error("send tarifficate: not enabled in mo service")
		return err
	}

	event := amqp.EventNotify{
		EventName: "charge",
		EventData: r,
	}
	body, err := json.Marshal(event)
	if err != nil {

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
		"queue":  queue.MOTarifficate,
	}).Info("send")
	svc.publisher.Publish(amqp.AMQPMessage{queue.MOTarifficate, 0, body})
	return nil
}
