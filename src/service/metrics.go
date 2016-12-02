package service

import (
	m "github.com/vostrok/utils/metrics"
	"time"
)

var (
	Dropped               m.Gauge
	Empty                 m.Gauge
	DbError               m.Gauge
	AddToDBErrors         m.Gauge
	AddToDbSuccess        m.Gauge
	OperatorNotEnabled    m.Gauge
	OperatorNotApplicable m.Gauge
	NotifyErrors          m.Gauge
)

func newGaugeOperaor(name, help string) m.Gauge {
	return m.NewGauge("", "operator", name, "operator "+help)
}

func initMetrics() {
	Dropped = m.NewGauge("", "", "dropped", "mobilink queue dropped")
	Empty = m.NewGauge("", "", "empty", "mobilink queue empty")
	DbError = m.NewGauge("", "", "db_errors", "db errors overall")
	AddToDBErrors = m.NewGauge("", "", "add_to_db_errors", "subscription add to db errors")
	AddToDbSuccess = m.NewGauge("", "", "add_to_db_success", "subscription add to db success")
	OperatorNotEnabled = newGaugeOperaor("not_enabled", "operator is not enabled in config")
	OperatorNotApplicable = newGaugeOperaor("not_applicable", "there is no such operator in database")
	NotifyErrors = m.NewGauge("", "", "notify_errors", "sent to mt manager queue error")

	go func() {
		for range time.Tick(time.Minute) {
			Dropped.Update()
			Empty.Update()
			DbError.Update()
			AddToDBErrors.Update()
			AddToDbSuccess.Update()
			OperatorNotEnabled.Update()
			OperatorNotApplicable.Update()
			NotifyErrors.Update()
		}
	}()
}
