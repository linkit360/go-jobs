//package service
//
// when sthmth wrong with mt manager query
// parse mobilink response and publish result in mt manager response query
// in order to notice paid records and update pending status
//import (
//	"encoding/json"
//	"fmt"
//	"os/exec"
//	"strings"
//	"sync"
//	"time"
//
//	log "github.com/Sirupsen/logrus"
//
//	"github.com/vostrok/utils/amqp"
//	"github.com/vostrok/utils/rec"
//)
//
//type retriesPending struct {
//	conf      PendingRetriesConfig
//	wg        *sync.WaitGroup
//	paidCount int64
//}
//type PendingRetriesConfig struct {
//	Hours          int                              `yaml:"hours" default:"1"`
//	Limit          int                              `yaml:"limit" default:"1"`
//	LogPath        string                           `yaml:"log_path" default:"/var/log/linkit/"`
//	ResponseConfig map[int64]operatorResponseConfig `yaml:"response"`
//}
//
//type operatorResponseConfig struct {
//	ResponseLog   string `yaml:"response_log"`
//	ResponseQueue string `yaml:"response_queue"`
//	PaidMark      string `yaml:"paid_mark"`
//}
//
//func initRetriesPending(conf PendingRetriesConfig) *retriesPending {
//	rp := &retriesPending{
//		conf:      conf,
//		wg:        &sync.WaitGroup{},
//		paidCount: 0,
//	}
//	return rp
//}
//
//func (rp *retriesPending) run(operatorCode int64, hours, limit int) error {
//	records, err := rec.LoadScriptRetries(hours, operatorCode, limit)
//	if err != nil {
//		err = fmt.Errorf("rec.LoadPendingRetries: %s", err.Error())
//		log.WithFields(log.Fields{
//			"error": err.Error(),
//		}).Error("cannot get pending retries")
//		return err
//	}
//	log.WithFields(log.Fields{
//		"count": len(records),
//	}).Debug("got retries")
//
//	retryConfig, ok := rp.conf.ResponseConfig[operatorCode]
//	if !ok {
//		err = fmt.Errorf("No such operator in config: %s", operatorCode)
//		log.WithFields(log.Fields{
//			"error": err.Error(),
//		}).Error("cannt process")
//		return err
//	}
//
//	rp.paidCount = 0
//	for _, v := range records {
//		rp.wg.Add(1)
//		rp.processRetry(v, retryConfig)
//	}
//	rp.wg.Wait()
//	return nil
//}
//
//func (rp *retriesPending) processRetry(v rec.Record, respConfig operatorResponseConfig) {
//
//	cmdOut := getRowResponse(v.Tid, respConfig.ResponseLog)
//	if cmdOut == "" {
//		rec.SetRetryStatus("", v.RetryId)
//		return
//	}
//	v.LastPayAttemptAt = getTime(cmdOut)
//	v.SentAt = v.LastPayAttemptAt
//
//	if strings.Contains(cmdOut, "<value><i4>0</i4></value>") {
//		v.Paid = true
//		rp.paidCount++
//		log.WithFields(log.Fields{
//			"tid": v.Tid,
//		}).Debug("paid")
//	} else {
//		log.WithFields(log.Fields{
//			"tid": v.Tid,
//		}).Debug("not paid")
//	}
//	if err := rp.publishResponse(respConfig.ResponseQueue, "script", v); err != nil {
//		err = fmt.Errorf("notify failed: %s", err.Error())
//
//	}
//	log.WithFields(log.Fields{
//		"tid": v.Tid,
//	}).Debug("sent")
//	rp.wg.Done()
//}
//
//func getTime(row string) time.Time {
//	parseString := row[6:26]
//	t1, err := time.Parse(
//		"2006-01-02T15:04:05Z",
//		parseString,
//	)
//	if err != nil {
//		log.WithFields(log.Fields{
//			"time":  parseString,
//			"error": err.Error(),
//		}).Fatal("cannot parse time")
//	}
//	return t1
//}
//
//func getRowResponse(tid, responseLog string) string {
//	cmdName := "/usr/bin/grep"
//	if strings.Contains(responseLog, ".gz") {
//		cmdName = "/usr/bin/zgrep"
//	}
//	cmdArgs := []string{tid, responseLog}
//
//	cmdOut, err := exec.Command(cmdName, cmdArgs...).Output()
//	if err != nil {
//		log.WithFields(log.Fields{
//			"tid":   tid,
//			"error": err.Error(),
//		}).Error("cannot get row")
//		return ""
//	}
//	return string(cmdOut)
//}
//
//func (rp *retriesPending) publishResponse(queue, eventName string, data interface{}) error {
//	event := amqp.EventNotify{
//		EventName: eventName,
//		EventData: data,
//	}
//	body, err := json.Marshal(event)
//	if err != nil {
//		return fmt.Errorf("json.Marshal: %s", err.Error())
//	}
//	svc.notifier.Publish(amqp.AMQPMessage{queue, 0, body})
//	return nil
//}
