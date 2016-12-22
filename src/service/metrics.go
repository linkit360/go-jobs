package service

import (
	"time"

	"github.com/go-kit/kit/metrics/prometheus"
	m "github.com/vostrok/utils/metrics"
)

var (
	Errors                m.Gauge
	OperatorNotApplicable m.Gauge
	NotifyErrors          m.Gauge
)
var appName string

func initMetrics(name string) {
	appName = name

	OperatorNotApplicable = m.NewGauge("", "", "operator_not_applicable", "there is no such operator in database")
	NotifyErrors = m.NewGauge("", "", "notify_errors", "sent to mt manager queue error")

	go func() {
		for range time.Tick(time.Minute) {
			Errors.Update()
			OperatorNotApplicable.Update()
			NotifyErrors.Update()
		}
	}()
}
