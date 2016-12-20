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

	records, err := rec.GetSuspendedSubscriptions(p.Hours, p.Limit)
	if err != nil {
		err = fmt.Errorf("rec.GetSuspendedSubscriptions: %s", err.Error())
		return
	}
	count = len(records)
	for _, r := range records {
		if err = svc.sendTarifficate(r); err != nil {
			NotifyErrors.Inc()

			log.WithFields(log.Fields{
				"tid":   r.Tid,
				"error": err.Error(),
				"msg":   "dropped",
			}).Error("sent tarificate  error")
			return
		}
	}
	return
}
