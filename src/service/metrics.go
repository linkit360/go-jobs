package service

import (
	m "github.com/vostrok/utils/metrics"
	"time"
)

var (
	Errors                m.Gauge
	Dropped               m.Gauge
	Empty                 m.Gauge
	DbError               m.Gauge
	AddToDBErrors         m.Gauge
	AddToDbSuccess        m.Gauge
	OperatorNotEnabled    m.Gauge
	OperatorNotApplicable m.Gauge
	NotifyErrors          m.Gauge
	Mobilink              MobilinkMetrics
	Yondu                 YonduMetrics
)

type YonduMetrics struct {
	BlackListed            m.Gauge
	Rejected               m.Gauge
	Dropped                m.Gauge
	MOCallUnknownCampaign  m.Gauge
	MOCallUnknownService   m.Gauge
	MOCallUnknownPublisher m.Gauge
	MOCallParseTimeError   m.Gauge
}

type MobilinkMetrics struct {
	Dropped m.Gauge
}

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

	Yondu := YonduMetrics{
		BlackListed:            m.NewGauge("", "yondu", "blacklisted", "yondu blacklisted"),
		Rejected:               m.NewGauge("", "yondu", "rejected", "yondu rejected"),
		Dropped:                m.NewGauge("", "yondu", "dropped", "yondu dropped"),
		MOCallUnknownCampaign:  m.NewGauge("", "api_in", "mo_call_unknown_campaign", "MO unknown campaign"),
		MOCallUnknownService:   m.NewGauge("", "api_in", "mo_call_unknown_service", "MO unknown service"),
		MOCallUnknownPublisher: m.NewGauge("", "api_in", "mo_call_unknown_pixel_setting", "MO unknown pixel setting"),
		MOCallParseTimeError:   m.NewGauge("", "api_in", "mo_call_parse_time_error", "MO parse operators time error"),
	}

	Mobilink := MobilinkMetrics{
		Dropped: m.NewGauge("", "mobilink", "dropped", "mobilink dropped"),
	}

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
			Yondu.BlackListed.Update()
			Yondu.Rejected.Update()
			Yondu.Dropped.Update()
			Yondu.MOCallUnknownCampaign.Update()
			Yondu.MOCallUnknownService.Update()
			Yondu.MOCallUnknownPublisher.Update()
			Yondu.MOCallParseTimeError.Update()

			Mobilink.Dropped.Update()
		}
	}()
}
