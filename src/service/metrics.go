package service

import (
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus"

	m "github.com/vostrok/utils/metrics"
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
			retriesCount, err := getSuspendedRetriesCount()
			if err != nil {
				err = fmt.Errorf("rec.GetSuspendedRetriesCount: %s", err.Error())
				log.WithFields(log.Fields{
					"error": err.Error(),
				}).Error("get suspended retries")
				PendingRetriesCount.Set(float64(0))
			} else {
				PendingRetriesCount.Set(float64(retriesCount))
			}

			moCount, err := getSuspendedSubscriptionsCount()
			if err != nil {
				err = fmt.Errorf("rec.GetSuspendedSubscriptionsCount: %s", err.Error())
				log.WithFields(log.Fields{
					"error": err.Error(),
				}).Error("get mo")
				PendingSubscriptionsCount.Set(float64(0))
			} else {
				PendingSubscriptionsCount.Set(float64(moCount))
			}

			retriesPeriod, err := getRetriesPeriod()
			if err != nil {
				err = fmt.Errorf("rec.GetRetriesPeriod: %s", err.Error())
				log.WithFields(log.Fields{
					"error": err.Error(),
				}).Error("get retries period")
				RetriesPeriod.Set(float64(0))
			} else {
				RetriesPeriod.Set(retriesPeriod)
			}
		}
	}()
}

func getSuspendedRetriesCount() (count int, err error) {
	begin := time.Now()
	defer func() {
		defer func() {
			fields := log.Fields{
				"took": time.Since(begin),
			}
			if err != nil {
				fields["error"] = err.Error()
				log.WithFields(fields).Error("get suspended retries count failed")
			} else {
				fields["count"] = count
				log.WithFields(fields).Debug("get suspended retries")
			}
		}()
	}()

	query := fmt.Sprintf("SELECT count(*) count FROM %sretries "+
		"WHERE status IN ( 'pending', 'script' ) "+
		"AND updated_at < (CURRENT_TIMESTAMP - 4 * INTERVAL '1 hour' ) ",
		svc.conf.db.TablePrefix,
	)
	rows, err := svc.dbConn.Query(query)
	if err != nil {
		DBErrors.Inc()

		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return 0, err
	}
	defer rows.Close()

	for rows.Next() {
		if err := rows.Scan(
			&count,
		); err != nil {
			DBErrors.Inc()

			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return count, err
		}
	}
	if rows.Err() != nil {
		DBErrors.Inc()

		err = fmt.Errorf("get pending retries: rows.Err: %s", err.Error())
		return count, err
	}
	return count, nil
}

func getSuspendedSubscriptionsCount() (count int, err error) {
	begin := time.Now()
	defer func() {
		defer func() {
			fields := log.Fields{
				"took": time.Since(begin),
			}
			if err != nil {
				fields["error"] = err.Error()
				log.WithFields(fields).Error("get mo count failed")
			} else {
				fields["count"] = count
				log.WithFields(fields).Debug("get mo count")
			}
		}()
	}()

	query := fmt.Sprintf("SELECT count(*) count FROM %ssubscriptions "+
		"WHERE result = ''"+
		"AND sent_at < (CURRENT_TIMESTAMP - 2 * INTERVAL '1 hour' ) ",
		svc.conf.db.TablePrefix,
	)
	rows, err := svc.dbConn.Query(query)
	if err != nil {
		DBErrors.Inc()

		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return 0, err
	}
	defer rows.Close()

	for rows.Next() {
		if err := rows.Scan(
			&count,
		); err != nil {
			DBErrors.Inc()

			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return count, err
		}
	}
	if rows.Err() != nil {
		DBErrors.Inc()

		err = fmt.Errorf("get pending subscriptions: rows.Err: %s", err.Error())
		return count, err
	}

	return count, nil
}

func getRetriesPeriod() (seconds float64, err error) {
	begin := time.Now()
	defer func() {
		defer func() {
			fields := log.Fields{
				"took": time.Since(begin),
			}
			if err != nil {
				fields["error"] = err.Error()
				log.WithFields(fields).Error("get retries period failed")
			} else {
				log.WithFields(fields).Debug("get retries period")
			}
		}()
	}()

	query := fmt.Sprintf("SELECT coalesce((SELECT extract (epoch from (now() - "+
		"MIN(last_pay_attempt_at))::interval) seconds from %sretries), 0)",
		svc.conf.db.TablePrefix,
	)
	rows, err := svc.dbConn.Query(query)
	if err != nil {
		DBErrors.Inc()

		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return 0, err
	}
	defer rows.Close()

	for rows.Next() {
		if err = rows.Scan(
			&seconds,
		); err != nil {
			DBErrors.Inc()

			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return
		}
	}
	if rows.Err() != nil {
		DBErrors.Inc()

		err = fmt.Errorf("rows.Err: %s", err.Error())
		return
	}

	return
}
