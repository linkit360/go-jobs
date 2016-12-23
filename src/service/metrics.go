package service

import (
	"time"

	m "github.com/vostrok/utils/metrics"
)

var (
	Errors                m.Gauge
	OperatorNotApplicable m.Gauge
	NotifyErrors          m.Gauge
)

func initMetrics(name string) {
	OperatorNotApplicable = m.NewGauge("", "", "operator_not_applicable", "there is no such operator in database")
	NotifyErrors = m.NewGauge("", "", "notify_errors", "sent to mt manager queue error")
	Errors = m.NewGauge("", "", "errors", "errors")

	go func() {
		for range time.Tick(time.Minute) {
			Errors.Update()
			OperatorNotApplicable.Update()
			NotifyErrors.Update()
		}
	}()
}
