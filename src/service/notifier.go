package service

import (
	"encoding/json"
	"fmt"
	"strings"

	log "github.com/Sirupsen/logrus"

	"github.com/vostrok/utils/amqp"
	rec "github.com/vostrok/utils/rec"
)

func (svc *Service) sendTarifficate(r rec.Record) error {

	// todo: rpc service? code is repeated, db in use
	operator, ok := memOperators.ByCode[r.OperatorCode]
	if !ok {
		OperatorNotApplicable.Inc()

		log.WithFields(log.Fields{
			"tid":    r.Tid,
			"msisdn": r.Msisdn,
		}).Debug("send tarifficate: not applicable to any operator")
		return fmt.Errorf("Code %d is not applicable to any operator", r.OperatorCode)
	}
	operatorName := strings.ToLower(operator.Name)
	queue, ok := svc.conf.queues[operatorName]
	if !ok {
		OperatorNotEnabled.Inc()

		log.WithFields(log.Fields{
			"tid":    r.Tid,
			"msisdn": r.Msisdn,
		}).Debug("send tarifficate: not enabled in mo service")
		return fmt.Errorf("operator %s is not enabled", operatorName)
	}

	event := amqp.EventNotify{
		EventName: "charge",
		EventData: r,
	}
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("json.Marshal: %s", err.Error())
	}

	log.WithFields(log.Fields{
		"tid":    r.Tid,
		"msisdn": r.Msisdn,
		"queue":  queue.MOTarifficate,
	}).Info("send tarifficate")
	svc.publisher.Publish(amqp.AMQPMessage{queue.MOTarifficate, body})
	return nil
}
