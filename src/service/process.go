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
type EventNotifyNewSubscription struct {
	EventName string     `json:"event_name,omitempty"`
	EventData rec.Record `json:"event_data,omitempty"`
}

func processNewSubscription(deliveries <-chan amqp.Delivery) {
	for msg := range deliveries {
		log.WithField("body", string(msg.Body)).Debug("start process")

		var e EventNotifyContentSent
		if err := json.Unmarshal(msg.Body, &e); err != nil {
			Dropped.Inc()

			log.WithFields(log.Fields{
				"error": err.Error(),
				"msg":   "dropped",
				"body":  string(msg.Body),
			}).Error("consume new subscription")
			msg.Ack(false)
			continue
		}

		r := rec.Record{
			Msisdn:             e.EventData.Msisdn,
			Tid:                e.EventData.Tid,
			SubscriptionStatus: "",
			OperatorCode:       e.EventData.OperatorCode,
			CountryCode:        e.EventData.CountryCode,
			ServiceId:          e.EventData.ServiceId,
			SubscriptionId:     e.EventData.SubscriptionId,
			CampaignId:         e.EventData.CampaignId,
			AttemptsCount:      0,
			DelayHours:         e.EventData.DelayHours,
			Pixel:              e.EventData.Pixel,
			Publisher:          e.EventData.Publisher,
		}
		if e.EventData.Type == "rec" {
			var ns EventNotifyNewSubscription
			if err := json.Unmarshal(msg.Body, &ns); err != nil {
				Dropped.Inc()

				log.WithFields(log.Fields{
					"error": err.Error(),
					"msg":   "dropped",
					"body":  string(msg.Body),
				}).Error("consume new subscription")
				msg.Ack(false)
				continue
			}
			r = ns.EventData
		} else {
			log.WithFields(log.Fields{
				"tid": e.EventData.Tid,
			}).Debug("use content sent properties")
		}

		if r.Msisdn == "" || r.CampaignId == 0 {
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

		if len(r.Msisdn) > 32 {
			log.WithFields(log.Fields{
				"tid":    r.Tid,
				"msisdn": r.Msisdn,
				"error":  "too long msisdn",
			}).Error("strange msisdn, truncating")
			r.Msisdn = r.Msisdn[:31]
		}

		if r.SubscriptionId > 0 {
			log.WithFields(log.Fields{
				"tid":    r.Tid,
				"msisdn": r.Msisdn,
			}).Debug("already has subscription id")
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
				r.CampaignId,
				r.ServiceId,
				r.Msisdn,
				r.Publisher,
				r.Pixel,
				r.Tid,
				r.CountryCode,
				r.OperatorCode,
				r.PaidHours,
				r.DelayHours,
				r.Price,
			).Scan(&r.SubscriptionId); err != nil {
				DbError.Inc()
				AddToDBErrors.Inc()

				log.WithFields(log.Fields{
					"tid":   r.Tid,
					"error": err.Error(),
					"query": query,
					"msg":   "requeue",
				}).Error("add new subscription")
				msg.Nack(false, true)
				continue
			}
			AddToDbSuccess.Inc()
			log.WithFields(log.Fields{
				"tid": r.Tid,
			}).Info("added new subscription")
		}

		if err := svc.sendTarifficate(r); err != nil {
			NotifyErrors.Inc()
			log.WithFields(log.Fields{
				"tid":   r.Tid,
				"error": err.Error(),
				"msg":   "dropped",
			}).Error("charge subscription error")

		}
		msg.Ack(false)
	}
}
