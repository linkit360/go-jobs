package service

import (
	"fmt"
	"strconv"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"

	rec "github.com/vostrok/utils/rec"
)

// does simple thing:
// selects all subscriptions form database with result = '' and before hours
// and pushes to queue
func AddSubscriptionsHandler(r *gin.Engine) {
	rg := r.Group("/api")
	rg.GET("", api)
}

type Params struct {
	Limit int
	Hours int
}

func api(c *gin.Context) {
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

	params := Params{
		Limit: limit,
		Hours: hours,
	}

	count, err := processOldNotPaidSubscriptions(params)
	if err != nil {
		c.JSON(500, err.Error())
		return
	}
	c.JSON(200, count)
}

func processOldNotPaidSubscriptions(p Params) (count int, err error) {
	begin := time.Now()
	defer func() {
		log.WithFields(log.Fields{
			"took":   time.Since(begin),
			"count":  count,
			"params": p,
		}).Debug("get notpaid subscriptions")
	}()

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
		" (CURRENT_TIMESTAMP - %d * INTERVAL '1 hour' ) > created_at "+
		" ORDER BY id ASC LIMIT %s",
		svc.conf.db.TablePrefix,
		p.Hours,
		strconv.Itoa(p.Limit),
	)
	rows, err := svc.db.Query(query)
	if err != nil {
		DbError.Inc()
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return
	}
	defer rows.Close()

	count = 0
	for rows.Next() {
		count++
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
			DbError.Inc()
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return
		}

		if err = svc.sendTarifficate(record); err != nil {
			NotifyErrors.Inc()

			log.WithFields(log.Fields{
				"tid":   record.Tid,
				"error": err.Error(),
				"msg":   "dropped",
			}).Error("sent tarificate  error")
			return
		}
	}
	if rows.Err() != nil {
		DbError.Inc()
		err = fmt.Errorf("row.Err: %s", err.Error())
		return
	}

	return count, nil
}
