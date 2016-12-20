package service

import (
	"time"

	m "github.com/vostrok/utils/metrics"
)

var (
	Errors                m.Gauge
	OperatorNotApplicable m.Gauge
	NotifyErrors          m.Gauge
	Mobilink              *MobilinkMetrics
	Yondu                 *YonduMetrics
)
var appName string

func initMetrics(name string) {
	appName = name

	OperatorNotApplicable = m.NewGauge("", "", "operator_not_applicable", "there is no such operator in database")
	NotifyErrors = m.NewGauge("", "", "notify_errors", "sent to mt manager queue error")

	if svc.conf.queues.Yondu.Enabled {
		Yondu = initYonduMetrics()
		go func() {
			for range time.Tick(time.Minute) {
				Yondu.BlackListed.Update()
				Yondu.Rejected.Update()
				Yondu.Dropped.Update()
				Yondu.MOCallUnknownCampaign.Update()
				Yondu.MOCallUnknownService.Update()
				Yondu.MOCallUnknownPublisher.Update()
				Yondu.MOCallParseTimeError.Update()
				Yondu.AddToDBErrors.Update()
				Yondu.AddToDbSuccess.Update()
			}
		}()
	}
	if svc.conf.queues.Mobilink.Enabled {
		Mobilink = initMobilinkmetrics()
		go func() {
			for range time.Tick(time.Minute) {
				Mobilink.Dropped.Update()
				Mobilink.Empty.Update()
				Mobilink.AddToDBErrors.Update()
				Mobilink.AddToDbSuccess.Update()
			}
		}()
	}

	go func() {
		for range time.Tick(time.Minute) {
			Errors.Update()
			OperatorNotApplicable.Update()
			NotifyErrors.Update()
		}
	}()
}

type YonduMetrics struct {
	BlackListed            m.Gauge
	Rejected               m.Gauge
	Dropped                m.Gauge
	MOCallUnknownCampaign  m.Gauge
	MOCallUnknownService   m.Gauge
	MOCallUnknownPublisher m.Gauge
	MOCallParseTimeError   m.Gauge
	AddToDBErrors          m.Gauge
	AddToDbSuccess         m.Gauge
}

func initYonduMetrics() *YonduMetrics {
	telcoName := "yondu"
	return &YonduMetrics{
		BlackListed:            m.NewGauge(appName, telcoName, "blacklisted", "yondu blacklisted"),
		Rejected:               m.NewGauge(appName, telcoName, "rejected", "yondu rejected"),
		Dropped:                m.NewGauge(appName, telcoName, "dropped", "yondu dropped"),
		MOCallUnknownCampaign:  m.NewGauge(appName, telcoName, "mo_call_unknown_campaign", "yondu MO unknown campaign"),
		MOCallUnknownService:   m.NewGauge(appName, telcoName, "mo_call_unknown_service", "yondu MO unknown service"),
		MOCallUnknownPublisher: m.NewGauge(appName, telcoName, "mo_call_unknown_pixel_setting", "yondu MO unknown pixel setting"),
		MOCallParseTimeError:   m.NewGauge(appName, telcoName, "mo_call_parse_time_error", "yondu MO parse operators time error"),
		AddToDBErrors:          m.NewGauge(appName, telcoName, "add_to_db_errors", "subscription add to db errors"),
		AddToDbSuccess:         m.NewGauge(appName, telcoName, "add_to_db_success", "subscription add to db success"),
	}
}

type MobilinkMetrics struct {
	Dropped        m.Gauge
	Empty          m.Gauge
	AddToDBErrors  m.Gauge
	AddToDbSuccess m.Gauge
}

func initMobilinkmetrics() *MobilinkMetrics {
	telcoName := "mobilink"

	return &MobilinkMetrics{
		Dropped:        m.NewGauge(appName, telcoName, "dropped", "mobilink dropped"),
		Empty:          m.NewGauge(appName, telcoName, "empty", "mobilink queue empty"),
		AddToDBErrors:  m.NewGauge(appName, telcoName, "add_to_db_errors", "subscription add to db errors"),
		AddToDbSuccess: m.NewGauge(appName, telcoName, "add_to_db_success", "subscription add to db success"),
	}
}
