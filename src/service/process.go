package service

import (
	"encoding/json"
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"

	rec "github.com/vostrok/utils/rec"
)

type EventNotifyNewSubscription struct {
	EventName string     `json:"event_name,omitempty"`
	EventData rec.Record `json:"event_data,omitempty"`
}

func processNewSubscription(deliveries <-chan amqp.Delivery) {
	for msg := range deliveries {
		log.WithField("body", string(msg.Body)).Debug("start process")

		var ns EventNotifyNewSubscription
		var r rec.Record
		if err := json.Unmarshal(msg.Body, &ns); err != nil {
			Dropped.Inc()

			log.WithFields(log.Fields{
				"error": err.Error(),
				"msg":   "dropped",
				"body":  string(msg.Body),
			}).Error("consume new subscription")
			goto ack
		}

		r = ns.EventData

		if r.Msisdn == "" || r.CampaignId == 0 {
			Dropped.Inc()
			Empty.Inc()

			log.WithFields(log.Fields{
				"error":        "Empty message",
				"msg":          "dropped",
				"subscription": string(msg.Body),
			}).Error("consume new subscritpion")
			goto ack
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
				"sent_at, "+
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
				") values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) "+
				"RETURNING id",
				svc.conf.db.TablePrefix)

			if err := svc.db.QueryRow(query,
				r.SentAt,
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
			nack:
				if err := msg.Nack(false, true); err != nil {
					log.WithFields(log.Fields{
						"tid":   r.Tid,
						"error": err.Error(),
					}).Error("cannot nack")
					time.Sleep(time.Second)
					goto nack
				}
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
