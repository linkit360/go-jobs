package service

import (
	"encoding/json"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"

	rec "github.com/vostrok/utils/rec"
)

type EventNotifyNewSubscription struct {
	EventName string     `json:"event_name,omitempty"`
	EventData rec.Record `json:"event_data,omitempty"`
}

func processNewMobilinkSubscription(deliveries <-chan amqp.Delivery) {
	for msg := range deliveries {
		var ns EventNotifyNewSubscription
		var r rec.Record

		log.WithField("body", string(msg.Body)).Debug("start process")
		if err := json.Unmarshal(msg.Body, &ns); err != nil {
			Mobilink.Dropped.Inc()

			log.WithFields(log.Fields{
				"error": err.Error(),
				"msg":   "dropped",
				"body":  string(msg.Body),
			}).Error("consume new subscription")
			goto ack
		}

		r = ns.EventData

		if r.Msisdn == "" || r.CampaignId == 0 {
			Mobilink.Dropped.Inc()
			Mobilink.Empty.Inc()

			log.WithFields(log.Fields{
				"error":        "Empty message",
				"msg":          "dropped",
				"subscription": string(msg.Body),
			}).Error("consume new subscritpion")
			goto ack
		}
		if err := rec.AddNewSubscriptionToDB(&r); err != nil {
			Mobilink.AddToDBErrors.Inc()
			msg.Nack(false, true)
			continue
		} else {
			Mobilink.AddToDbSuccess.Inc()
		}

		if err := svc.sendTarifficate(r); err != nil {
			log.WithFields(log.Fields{
				"tid":   r.Tid,
				"error": err.Error(),
				"msg":   "dropped",
			}).Error("charge subscription error")
		}

	ack:
		if err := msg.Ack(false); err != nil {
			log.WithFields(log.Fields{
				"error": err.Error(),
			}).Error("cannot ack")
			time.Sleep(time.Second)
			goto ack
		}
	}
}
