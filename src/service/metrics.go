package service

import (
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus"

	m "github.com/vostrok/utils/metrics"
	"github.com/vostrok/utils/rec"
)

var (
	Errors                    m.Gauge
	NotifyErrors              m.Gauge
	DBErrors                  m.Gauge
	PendingSubscriptionsCount prometheus.Gauge
	PendingRetriesCount       prometheus.Gauge
	RetriesPeriod             prometheus.Gauge
)

func initMetrics(name string) {
	NotifyErrors = m.NewGauge("", "", "notify_errors", "sent to mt manager queue error")
	Errors = m.NewGauge("", "", "errors", "errors")
	DBErrors = m.NewGauge("", "", "db_errors", "db_errors")
	PendingSubscriptionsCount = m.PrometheusGauge("pending", "subscriptions", "count", "pending subscriptions count")
	PendingRetriesCount = m.PrometheusGauge("pending", "retries", "count", "pending retries count")
	RetriesPeriod = m.PrometheusGauge("retries", "period", "seconds", "retries period seconds")

	go func() {
		for range time.Tick(time.Minute) {
			Errors.Update()
			DBErrors.Update()
			NotifyErrors.Update()
		}
	}()

	go func() {
		for range time.Tick(5 * time.Minute) {
			retriesCount, err := rec.GetSuspendedRetriesCount()
			if err != nil {
				err = fmt.Errorf("rec.GetSuspendedRetriesCount: %s", err.Error())
				log.WithFields(log.Fields{
					"error": err.Error(),
				}).Error("get suspended retries")
				PendingRetriesCount.Set(float64(10000000))
			} else {
				PendingRetriesCount.Set(float64(retriesCount))
			}

			moCount, err := rec.GetSuspendedSubscriptionsCount()
			if err != nil {
				err = fmt.Errorf("rec.GetSuspendedSubscriptionsCount: %s", err.Error())
				log.WithFields(log.Fields{
					"error": err.Error(),
				}).Error("get mo")
				PendingSubscriptionsCount.Set(float64(100000000))
			} else {
				PendingSubscriptionsCount.Set(float64(moCount))
			}

			retriesPeriod, err := rec.GetRetriesPeriod()
			if err != nil {
				err = fmt.Errorf("rec.GetRetriesPeriod: %s", err.Error())
				log.WithFields(log.Fields{
					"error": err.Error(),
				}).Error("get retries period")
				RetriesPeriod.Set(float64(100000000))
			} else {
				RetriesPeriod.Set(float64(retriesPeriod))
			}
		}
	}()
}
