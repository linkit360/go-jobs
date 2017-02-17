package service

import (
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"

	"database/sql"
	"github.com/vostrok/utils/amqp"
	rec "github.com/vostrok/utils/rec"
)

type suspendedSubscriptions struct {
}

type SuspendedSubscrptionsParams struct {
	Limit int
	Hours int
}

// does simple thing:
// selects all subscriptions form database with result = '' and before hours
// and pushes to queue
func AddSubscriptionsHandler(r *gin.Engine) {
	rg := r.Group("/api")
	rg.GET("", svc.suspendedSubscriptions.Call)
}

func (ss *suspendedSubscriptions) Call(c *gin.Context) {
	limitStr, _ := c.GetQuery("limit")
	limit, err := strconv.Atoi(limitStr)
	if err != nil || limitStr == "" {
		log.WithFields(log.Fields{}).Debug("no required param limit, so use 1000")
		limit = 1000
	}

	hoursStr, _ := c.GetQuery("hours")
	hours, err := strconv.Atoi(hoursStr)
	if err != nil || hoursStr == "" {
		log.WithFields(log.Fields{}).Debug("no required param, so use 1 hour")
		hours = 1
	}

	params := SuspendedSubscrptionsParams{
		Limit: limit,
		Hours: hours,
	}

	count, err := ss.process(params)
	if err != nil {
		c.JSON(500, err.Error())
		return
	}
	c.JSON(200, count)
}

func (ss *suspendedSubscriptions) process(p SuspendedSubscrptionsParams) (count int, err error) {
	begin := time.Now()
	defer func() {
		log.WithFields(log.Fields{
			"took":   time.Since(begin),
			"count":  count,
			"params": p,
		}).Debug("get notpaid subscriptions")
	}()

	records, err := ss.get(41001, p.Hours, p.Limit)
	if err != nil {
		err = fmt.Errorf("rec.GetSuspendedSubscriptions: %s", err.Error())
		return
	}
	count = len(records)

	wg := &sync.WaitGroup{}
	for _, r := range records {
		wg.Add(1)
		go func() {
			if err = ss.sendTarifficate(r); err != nil {
				NotifyErrors.Inc()

				log.WithFields(log.Fields{
					"tid":   r.Tid,
					"error": err.Error(),
					"msg":   "dropped",
				}).Error("sent tarificate  error")
				return
			}
			wg.Done()
		}()
	}
	wg.Wait()
	return
}

func (ss *suspendedSubscriptions) get(operatorCode int64, hours, limit int) (records []rec.Record, err error) {
	query := fmt.Sprintf("SELECT "+
		"id, "+
		"tid, "+
		"msisdn, "+
		"pixel, "+
		"publisher, "+
		"id_service, "+
		"id_campaign, "+
		"operator_code, "+
		"country_code, "+
		"attempts_count, "+
		"delay_hours, "+
		"paid_hours, "+
		"keep_days, "+
		"price "+
		" FROM %ssubscriptions "+
		" WHERE result = '' AND "+
		"operator_code = $1 AND "+
		" (CURRENT_TIMESTAMP - %d * INTERVAL '1 hour' ) > created_at "+
		" ORDER BY id ASC LIMIT %s",
		svc.conf.db.TablePrefix,
		hours,
		strconv.Itoa(limit),
	)
	var rows *sql.Rows
	rows, err = svc.dbConn.Query(query, operatorCode)
	if err != nil {
		DBErrors.Inc()
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return
	}
	defer rows.Close()

	for rows.Next() {
		record := rec.Record{}

		if err = rows.Scan(
			&record.SubscriptionId,
			&record.Tid,
			&record.Msisdn,
			&record.Pixel,
			&record.Publisher,
			&record.ServiceId,
			&record.CampaignId,
			&record.OperatorCode,
			&record.CountryCode,
			&record.AttemptsCount,
			&record.DelayHours,
			&record.PaidHours,
			&record.KeepDays,
			&record.Price,
		); err != nil {
			DBErrors.Inc()
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return
		}
		records = append(records, record)
	}
	if rows.Err() != nil {
		DBErrors.Inc()
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return
	}
	return
}
func (ss *suspendedSubscriptions) sendTarifficate(r rec.Record) error {
	queue := "mobilink_mo_tarifficate"
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
	svc.publisher.Publish(amqp.AMQPMessage{queue, 0, body, event.EventName})
	return nil
}
