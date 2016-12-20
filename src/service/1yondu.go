package service

import (
	"encoding/json"
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"

	inmem_client "github.com/vostrok/inmem/rpcclient"
	yondu_service "github.com/vostrok/operator/ph/yondu/src/service"
	transaction_log_service "github.com/vostrok/qlistener/src/service"
	rec "github.com/vostrok/utils/rec"
)

type EventNotifyMO struct {
	EventName string                     `json:"event_name,omitempty"`
	EventData yondu_service.MOParameters `json:"event_data,omitempty"`
}

func processNewYonduSubscription(deliveries <-chan amqp.Delivery) {
	for msg := range deliveries {
		var r rec.Record
		var periodic rec.PeriodicSubscription

		var err error
		var logCtx *log.Entry
		var blackListed bool
		var hasPrevious bool
		var transactionMsg transaction_log_service.OperatorTransactionLog

		log.WithFields(log.Fields{
			"priority": msg.Priority,
			"body":     string(msg.Body),
		}).Debug("start process")

		var e EventNotifyMO
		if err := json.Unmarshal(msg.Body, &e); err != nil {
			Yondu.Dropped.Inc()

			log.WithFields(log.Fields{
				"error": err.Error(),
				"msg":   "dropped",
				"mo":    string(msg.Body),
			}).Error("consume from " + svc.conf.internal.Yondu.NewSubscription.Name)
			goto ack
		}

		periodic, r, err = getRecordByMO(e.EventData)
		if err != nil {
			msg.Nack(false, true)
			continue
		}
		logCtx = log.WithFields(log.Fields{
			"tid":    r.Tid,
			"msisdn": r.Msisdn,
		})
		logCtx.Debug("blacklist checks..")
		blackListed, err = inmem_client.IsBlackListed(r.Msisdn)
		if err != nil {
			Errors.Inc()

			err := fmt.Errorf("inmem_client.IsBlackListed: %s", err.Error())
			logCtx.WithField("error", err.Error()).Error("cann't get is blacklisted")
		}
		if blackListed {
			Yondu.BlackListed.Inc()

			logCtx.Info("blacklisted")
			r.SubscriptionStatus = "blacklisted"
		} else {
			logCtx.Debug("not blacklisted, start postpaid checks..")
		}

		hasPrevious = getPrevSubscriptionCache(r.Msisdn, r.ServiceId, r.Tid)
		if hasPrevious {
			Yondu.Rejected.Inc()

			logCtx.WithFields(log.Fields{}).Info("paid hours aren't passed")
			r.Result = "rejected"
			r.SubscriptionStatus = "rejected"
		} else {
			logCtx.Debug("no previous subscription found")
		}

		// here get subscription id
		if err := rec.AddPeriodicSubscriptionToDB(&periodic); err != nil {
			Yondu.AddToDBErrors.Inc()
			msg.Nack(false, true)
			continue
		} else {
			Yondu.AddToDbSuccess.Inc()
		}
		setPrevSubscriptionCache(r.Msisdn, r.ServiceId, r.Tid)

		transactionMsg = transaction_log_service.OperatorTransactionLog{
			Tid:              r.Tid,
			Msisdn:           r.Msisdn,
			OperatorToken:    r.OperatorToken,
			OperatorCode:     r.OperatorCode,
			CountryCode:      r.CountryCode,
			Error:            "",
			Price:            r.Price,
			ServiceId:        r.ServiceId,
			SubscriptionId:   r.SubscriptionId,
			CampaignId:       r.CampaignId,
			RequestBody:      string(msg.Body),
			ResponseBody:     "",
			ResponseDecision: "",
			ResponseCode:     200,
			SentAt:           r.SentAt,
			Type:             e.EventName,
		}
		if err := svc.publishTransactionLog("mo", transactionMsg); err != nil {
			logCtx.WithField("error", err.Error()).Error("publishTransactionLog")
		}
		if r.Result == "" {
			if err := svc.publishYonduSentConsent(r); err != nil {
				logCtx.WithField("error", err.Error()).Error("publishYonduSentConsent")
			}
		} else {
			if err := svc.publishYonduMT(r); err != nil {
				logCtx.WithField("error", err.Error()).Error("publishYonduMT")
			}
		}

	ack:
		if err := msg.Ack(false); err != nil {
			log.WithFields(log.Fields{
				"mo":    msg.Body,
				"error": err.Error(),
			}).Error("cannot ack")
			time.Sleep(time.Second)
			goto ack
		}

	}
}

func getRecordByMO(req yondu_service.MOParameters) (rec.PeriodicSubscription, rec.Record, error) {
	r := rec.Record{}
	periodic := rec.PeriodicSubscription{}
	campaign, err := inmem_client.GetCampaignByKeyWord(req.KeyWord)
	if err != nil {
		Yondu.MOCallUnknownCampaign.Inc()

		err = fmt.Errorf("inmem_client.GetCampaignByKeyWord: %s", err.Error())
		log.WithFields(log.Fields{
			"keyword": req.KeyWord,
			"error":   err.Error(),
		}).Error("cannot get campaign by keyword")
		return periodic, r, err
	}
	svc, err := inmem_client.GetServiceById(campaign.ServiceId)
	if err != nil {
		Yondu.MOCallUnknownService.Inc()

		err = fmt.Errorf("inmem_client.GetServiceById: %s", err.Error())
		log.WithFields(log.Fields{
			"keyword":    req.KeyWord,
			"serviceId":  campaign.ServiceId,
			"campaignId": campaign.Id,
			"error":      err.Error(),
		}).Error("cannot get service by id")
		return periodic, r, err
	}
	publisher := ""
	pixelSetting, err := inmem_client.GetPixelSettingByCampaignId(campaign.Id)
	if err != nil {
		Yondu.MOCallUnknownPublisher.Inc()

		err = fmt.Errorf("inmem_client.GetPixelSettingByCampaignId: %s", err.Error())
		log.WithFields(log.Fields{
			"keyword":    req.KeyWord,
			"serviceId":  campaign.ServiceId,
			"campaignId": campaign.Id,
			"error":      err.Error(),
		}).Error("cannot get pixel setting by campaign id")
	} else {
		publisher = pixelSetting.Publisher
	}

	sentAt, err := time.Parse("20060102150405", req.Timestamp)
	if err != nil {
		Yondu.MOCallParseTimeError.Inc()

		err = fmt.Errorf("time.Parse: %s", err.Error())
		log.WithFields(log.Fields{
			"keyword":    req.KeyWord,
			"serviceId":  campaign.ServiceId,
			"campaignId": campaign.Id,
			"error":      err.Error(),
		}).Error("cannot parse operators time")
		sentAt = time.Now().UTC()
	}
	r = rec.Record{
		SentAt:             sentAt,
		Msisdn:             req.Msisdn,
		Tid:                rec.GenerateTID(),
		SubscriptionStatus: "",
		CountryCode:        515, // ph
		OperatorCode:       51500,
		Publisher:          publisher,
		Pixel:              "",
		CampaignId:         campaign.Id,
		ServiceId:          campaign.ServiceId,
		DelayHours:         svc.DelayHours,
		PaidHours:          svc.PaidHours,
		KeepDays:           svc.KeepDays,
		Price:              100 * int(svc.Price),
		OperatorToken:      req.TransID,
	}
	periodic = rec.PeriodicSubscription{
		SentAt:                      r.SentAt,
		Status:                      r.SubscriptionStatus,
		Tid:                         r.Tid,
		Price:                       r.Price,
		ServiceId:                   r.ServiceId,
		CampaignId:                  r.CampaignId,
		CountryCode:                 r.CountryCode,
		OperatorCode:                r.OperatorCode,
		Msisdn:                      r.Msisdn,
		SendContentDay:              svc.SendContentDay,
		SendContentAllowedFromHours: svc.SendContentAllowedTime.From,
		SendContentAllowedToHours:   svc.SendContentAllowedTime.To,
	}
	return periodic, r, nil
}

// get periodic for this day and time
// generate subscriptions tid
// tid := rec.GenerateTID()
// create new subscriptions
// generate send_content_text
// send sms via Yondu API
// create periodic transactions
// update periodic last_request_at
func processPeriodic() {

	begin := time.Now()
	periodics, err := rec.GetPeriodics(
		svc.conf.internal.Yondu.Periodic.OperatorCode,
		svc.conf.internal.Yondu.Periodic.FetchLimit,
	)
	if err != nil {
		err = fmt.Errorf("rec.GetPeriodics: %s", err.Error())
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Error("cannot get periodics")
		return
	}
	Yondu.GetPeriodicsDuration.Observe(time.Since(begin).Seconds())

	for _, p := range periodics {
		sentAt := time.Now().UTC()

		r := rec.Record{
			SentAt:             sentAt,
			Msisdn:             p.Msisdn,
			Tid:                rec.GenerateTID(),
			SubscriptionStatus: "",
			CountryCode:        p.CountryCode,
			OperatorCode:       p.OperatorCode,
			Publisher:          "",
			Pixel:              "",
			CampaignId:         p.CampaignId,
			ServiceId:          p.ServiceId,
			DelayHours:         p.DelayHours,
			KeepDays:           p.KeepDays,
			Price:              p.Price,
			OperatorToken:      p.OperatorToken,
		}

		if err := rec.AddNewSubscriptionToDB(&r); err != nil {
			err = fmt.Errorf("rec.AddNewSubscriptionToDB: %s", err.Error())
			log.WithFields(log.Fields{
				"error": err.Error(),
			}).Error("cannot add new subscription")
			return
		}
		// get content via content service
		// generate send_content_text
		// send sms via Yondu API
		// create periodic transactions
		// update periodic last_request_at
	}

}
