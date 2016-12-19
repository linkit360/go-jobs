package service

import (
	"encoding/json"
	"fmt"
	"strings"
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
			}).Error("consume from " + svc.conf.queues.Yondu.NewSubscription.Name)
			goto ack
		}

		r, err := getRecordByMO(e.EventData)
		if err != nil {
			msg.Nack(false, true)
			continue
		}
		logCtx := log.WithFields(log.Fields{
			"tid":    r.Tid,
			"msisdn": r.Msisdn,
		})
		logCtx.Debug("blacklist checks..")
		blackListed, err := inmem_client.IsBlackListed(r.Msisdn)
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

		hasPrevious := getPrevSubscriptionCache(r.Msisdn, r.ServiceId, r.Tid)
		if hasPrevious {
			Yondu.Rejected.Inc()

			logCtx.WithFields(log.Fields{}).Info("paid hours aren't passed")
			r.Result = "rejected"
			r.SubscriptionStatus = "rejected"
		} else {
			logCtx.Debug("no previous subscription found")
		}
		// here get subscription id
		if err := addNewSubscriptionToDB(r); err != nil {
			msg.Nack(false, true)
			continue
		}
		setPrevSubscriptionCache(r.Msisdn, r.ServiceId, r.Tid)

		transactionMsg := transaction_log_service.OperatorTransactionLog{
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
			RequestBody:      strings.TrimSpace(msg.Body),
			ResponseBody:     "",
			ResponseDecision: "",
			ResponseCode:     "",
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

func getRecordByMO(req yondu_service.MOParameters) (rec.Record, error) {
	r := rec.Record{}
	campaign, err := inmem_client.GetCampaignByKeyWord(req.KeyWord)
	if err != nil {
		Yondu.MOCallUnknownCampaign.Inc()

		err = fmt.Errorf("inmem_client.GetCampaignByKeyWord: %s", err.Error())
		log.WithFields(log.Fields{
			"keyword": req.KeyWord,
			"error":   err.Error(),
		}).Error("cannot get campaign by keyword")
		return r, err
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
		return r, err
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
		CountryCode:        "ph",
		OperatorCode:       "51000",
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
	return r, nil
}
