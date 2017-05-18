package service

import (
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/linkit360/go-jobs/src/config"
	m "github.com/linkit360/go-utils/metrics"
)

var (
	Errors                    m.Gauge
	NotifyErrors              m.Gauge
	DBErrors                  m.Gauge
	PendingSubscriptionsCount prometheus.Gauge
	PendingRetriesCount       prometheus.Gauge
	BlacklistedCount          prometheus.Gauge
	PostpaidCount             prometheus.Gauge
	BufferPixelsCount         prometheus.Gauge
	ActualDBSize              prometheus.Gauge
	AllowedDBSize             prometheus.Gauge
)

func initMetrics(name string, metricsConfig config.MetricsConfig) {
	NotifyErrors = m.NewGauge("", "", "notify_errors", "sent to mt manager queue error")
	Errors = m.NewGauge("", "", "errors", "errors")
	DBErrors = m.NewGauge("", "", "db_errors", "db_errors")
	PendingSubscriptionsCount = m.PrometheusGauge("pending", "subscriptions", "count", "pending subscriptions count")
	BlacklistedCount = m.PrometheusGauge("", "blacklisted", "count", "blacklisted count")
	PostpaidCount = m.PrometheusGauge("", "postpaid", "count", "postpaid count")
	BufferPixelsCount = m.PrometheusGauge("", "buffer_pixels", "count", "buffer pixels count")
	PendingRetriesCount = m.PrometheusGauge("pending", "retries", "count", "pending retries count")
	ActualDBSize = m.PrometheusGauge("actual", "db_size", "bytes", "expired retries count")
	AllowedDBSize = m.PrometheusGauge("capacity", "db_size", "bytes", "expired retries count")

	go func() {
		for range time.Tick(time.Minute) {
			Errors.Update()
			DBErrors.Update()
			NotifyErrors.Update()
		}
	}()

	go func() {
		for range time.Tick(time.Minute) {
			retriesCount, err := getSuspendedRetriesCount()
			if err != nil {
				err = fmt.Errorf("getSuspendedRetriesCount: %s", err.Error())
				log.WithFields(log.Fields{
					"error": err.Error(),
				}).Error("get suspended retries")
				PendingRetriesCount.Set(float64(0))
			} else {
				PendingRetriesCount.Set(float64(retriesCount))
			}

			moCount, err := getSuspendedSubscriptionsCount()
			if err != nil {
				err = fmt.Errorf("getSuspendedSubscriptionsCount: %s", err.Error())
				log.WithFields(log.Fields{
					"error": err.Error(),
				}).Error("get mo")
				PendingSubscriptionsCount.Set(float64(0))
			} else {
				PendingSubscriptionsCount.Set(float64(moCount))
			}

			blacklisted, err := getCount(
				"blacklisted",
				fmt.Sprintf("SELECT count(*) count from %smsisdn_blacklist", svc.conf.db.TablePrefix),
			)
			if err != nil {
				err = fmt.Errorf("get blacklisted: %s", err.Error())
				log.WithFields(log.Fields{
					"error": err.Error(),
				}).Error("failed")
				BlacklistedCount.Set(float64(0))
			} else {
				BlacklistedCount.Set(float64(blacklisted))
			}

			postpaid, err := getCount(
				"blacklisted",
				fmt.Sprintf("SELECT count(*) count from %smsisdn_postpaid", svc.conf.db.TablePrefix),
			)
			if err != nil {
				err = fmt.Errorf("get blacklisted: %s", err.Error())
				log.WithFields(log.Fields{
					"error": err.Error(),
				}).Error("failed")
				PostpaidCount.Set(float64(0))
			} else {
				PostpaidCount.Set(float64(postpaid))
			}

			pixelBufferCount, err := getCount(
				"pixel buffers",
				fmt.Sprintf("SELECT count(*) count from %spixel_buffer", svc.conf.db.TablePrefix),
			)
			if err != nil {
				err = fmt.Errorf("get pixel buffer count: %s", err.Error())
				log.WithFields(log.Fields{
					"error": err.Error(),
				}).Error("failed")
				BufferPixelsCount.Set(float64(0))
			} else {
				BufferPixelsCount.Set(float64(pixelBufferCount))
			}

			dbSumSize := int64(0)
			for _, name := range metricsConfig.Databases {
				dbSize, err := getDBSize(name)
				if err != nil {
					err = fmt.Errorf("%s getDBSize: %s", name, err.Error())
					log.WithFields(log.Fields{
						"error": err.Error(),
					}).Error("get db size")
				} else {
					dbSumSize = dbSumSize + dbSize
				}
			}
			log.WithFields(log.Fields{
				"sum": dbSumSize,
			}).Debug("db size")
			ActualDBSize.Set(float64(dbSumSize))
			AllowedDBSize.Set(float64(metricsConfig.AllowedDBSizeBytes))
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

func getCount(name, query string) (count int, err error) {
	begin := time.Now()
	defer func() {
		defer func() {
			fields := log.Fields{
				"took": time.Since(begin),
				"name": name,
			}
			if err != nil {
				fields["error"] = err.Error()
				log.WithFields(fields).Error("get failed")
			} else {
				fields["count"] = count
				log.WithFields(fields).Debug("get count")
			}
		}()
	}()
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

		err = fmt.Errorf("rows.Err: %s", err.Error())
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

	query := fmt.Sprintf("SELECT count(*) count FROM %ssubscriptions WHERE result = 'pending'",
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

func getDBSize(dbname string) (size int64, err error) {
	begin := time.Now()
	defer func() {
		defer func() {
			fields := log.Fields{
				"name": dbname,
				"took": time.Since(begin),
			}
			if err != nil {
				fields["error"] = err.Error()
				log.WithFields(fields).Error("get db size failed")
			} else {
				fields["size"] = size
				log.WithFields(fields).Debug("get db size")
			}
		}()
	}()

	// pg_size_pretty
	query := fmt.Sprintf("SELECT pg_database_size('%s')", dbname)
	rows, err := svc.dbConn.Query(query)
	if err != nil {
		DBErrors.Inc()

		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return 0, err
	}
	defer rows.Close()

	for rows.Next() {
		if err = rows.Scan(
			&size,
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
