package service

import (
	"encoding/json"
	"fmt"

	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"

	content_service "github.com/vostrok/contentd/service"
	rec "github.com/vostrok/utils/rec"
)

type EventNotifyContentSent struct {
	EventName string                                `json:"event_name,omitempty"`
	EventData content_service.ContentSentProperties `json:"event_data,omitempty"`
}

func process(deliveries <-chan amqp.Delivery) {
	for msg := range deliveries {
		log.WithField("body", string(msg.Body)).Debug("start process")

		var e EventNotifyContentSent
		if err := json.Unmarshal(msg.Body, &e); err != nil {
			Dropped.Inc()

			log.WithFields(log.Fields{
				"error":       err.Error(),
				"msg":         "dropped",
				"contentSent": string(msg.Body),
			}).Error("consume content sent")
			msg.Ack(false)
			continue
		}
		t := e.EventData

		if t.Msisdn == "" ||
			t.CampaignId == 0 ||
			t.ContentId == 0 {
			Dropped.Inc()
			Empty.Inc()

			log.WithFields(log.Fields{
				"error":        "Empty message",
				"msg":          "dropped",
				"subscription": string(msg.Body),
			}).Error("consume new subscritpion")
			msg.Ack(false)
			continue
		}
		// todo: add check for every field
		if len(t.Msisdn) > 32 {
			log.WithFields(log.Fields{
				"tid":    t.Tid,
				"msisdn": t.Msisdn,
				"error":  "too long msisdn",
			}).Error("strange msisdn, truncating")
			t.Msisdn = t.Msisdn[:31]
		}
		if t.SubscriptionId > 0 {
			log.WithFields(log.Fields{
				"tid":    t.Tid,
				"msisdn": t.Msisdn,
				"error":  "already hash subscription id",
				"msg":    "dropped",
			}).Error("consume new subscritpion")
		} else {
			// do not set id_subscriber: msisdn is enough
			query := fmt.Sprintf("INSERT INTO %ssubscriptions ( "+
				"result, "+
				"id_campaign, "+
				"id_service, "+
				"msisdn, "+
				"publisher, "+
				"pixel, "+
				"tid, "+
				"country_code, "+
				"operator_code, "+
				"paid_hours, "+
				"delay_hours, "+
				"price "+
				") values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) "+
				"RETURNING id",
				svc.conf.db.TablePrefix)

			if err := svc.db.QueryRow(query,
				"",
				t.CampaignId,
				t.ServiceId,
				t.Msisdn,
				t.Publisher,
				t.Pixel,
				t.Tid,
				t.CountryCode,
				t.OperatorCode,
				t.PaidHours,
				t.DelayHours,
				t.Price,
			).Scan(&t.SubscriptionId); err != nil {
				DbError.Inc()
				AddToDBErrors.Inc()

				log.WithFields(log.Fields{
					"tid":   t.Tid,
					"error": err.Error(),
					"query": query,
					"msg":   "requeue",
				}).Error("add new subscription")
				msg.Nack(false, true)
				continue
			}
			AddToDbSuccess.Inc()
			log.WithFields(log.Fields{
				"tid": t.Tid,
			}).Info("added new subscription")
		}
		r := rec.Record{
			Msisdn:             t.Msisdn,
			Tid:                t.Tid,
			SubscriptionStatus: "",
			OperatorCode:       t.OperatorCode,
			CountryCode:        t.CountryCode,
			ServiceId:          t.ServiceId,
			SubscriptionId:     t.SubscriptionId,
			CampaignId:         t.CampaignId,
			AttemptsCount:      0,
			DelayHours:         t.DelayHours,
			Pixel:              t.Pixel,
			Publisher:          t.Publisher,
		}

		if err := svc.sendTarifficate(r); err != nil {
			log.WithFields(log.Fields{
				"tid":   t.Tid,
				"error": err.Error(),
				"msg":   "requeue",
			}).Error("charge subscription error")
			// ack, since there are json marshal error possible
		}
		msg.Ack(false)
	}
}
