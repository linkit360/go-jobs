package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/jinzhu/configor"
	log "github.com/sirupsen/logrus"

	"github.com/linkit360/go-dispatcherd/src/rbmq"
	"github.com/linkit360/go-utils/db"
	"github.com/linkit360/go-utils/rec"
)

// add transactions by telco log
type conf struct {
	DB       db.DataBaseConfig   `yaml:"db"`
	Notifier rbmq.NotifierConfig `yaml:"notifier"`
}

var appConfig conf
var dbConn *sql.DB

func main() {
	cfg := flag.String("config", "jobs.yml", "configuration yml file")
	filePath := flag.String("file", "sie.log", "log file")
	logPath := flag.String("log", "/var/log/linkit/mobilink.log", "m file")
	limit := flag.Int("limit", 1, "limit")

	flag.Parse()

	if *cfg != "" {
		if err := configor.Load(&appConfig, *cfg); err != nil {
			log.WithField("config", err.Error()).Fatal("config load error")
		}
	}
	dbConn = db.Init(appConfig.DB)

	log.Info(fmt.Sprintf("%#v", appConfig.DB))

	file, err := os.Open(*filePath)
	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	scanner := bufio.NewScanner(file)
	i := 0
	for scanner.Scan() {
		i++
		if i > *limit {
			fmt.Printf("\ni: %d, limit %d", i, *limit)
			goto adas
		}
		tid := scanner.Text()
		tid = strings.TrimSpace(tid)

		cmdOut := getRowResponse(tid, *logPath)
		if cmdOut == "" {
			return
		}
		if strings.Contains(cmdOut, "paid=true") {
			log.WithFields(log.Fields{
				"tid": tid,
			}).Debug("paid")
		} else {
			log.WithFields(log.Fields{
				"tid": tid,
			}).Debug("not paid, continue")
			continue
		}
		msisdn := strings.Split(tid, "-")[0]

		r := rec.Record{
			Tid:            tid,
			SentAt:         time.Now(),
			Msisdn:         msisdn,
			Result:         "injection_paid",
			OperatorCode:   41001,
			CountryCode:    92,
			ServiceCode:    "111",
			SubscriptionId: 0,
			CampaignCode:   "354",
			OperatorToken:  "",
			Price:          600,
		}
		writeTransaction(r)
		log.Printf("\n msisdn: %s, tid: %s", msisdn, tid)
	}

adas:
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	log.Info("done")

	log.Info(i)
}

func writeTransaction(r rec.Record) (err error) {
	begin := time.Now()
	defer func() {
		fields := log.Fields{
			"tid":  r.Tid,
			"took": time.Since(begin),
		}
		if err != nil {
			fields["error"] = err.Error()
			log.WithFields(fields).Error("write transaction error")
		} else {
			log.WithFields(fields).Debug("write transaction")
		}

	}()
	query := fmt.Sprintf("INSERT INTO %stransactions ("+
		"tid, "+
		"sent_at, "+
		"msisdn, "+
		"result, "+
		"operator_code, "+
		"country_code, "+
		"id_service, "+
		"id_subscription, "+
		"id_campaign, "+
		"operator_token, "+
		"price "+
		") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
		appConfig.DB.TablePrefix,
	)
	if _, err = dbConn.Exec(
		query,
		r.Tid,
		r.SentAt,
		r.Msisdn,
		r.Result,
		r.OperatorCode,
		r.CountryCode,
		r.ServiceCode,
		r.SubscriptionId,
		r.CampaignCode,
		r.OperatorToken,
		int(r.Price),
	); err != nil {
		err = fmt.Errorf("db.Exec: %s, Query: %s", err.Error(), query)
		return
	}
	return nil
}

func getRowResponse(tid, responseLog string) string {
	cmdName := "/usr/bin/grep"
	if strings.Contains(responseLog, ".gz") {
		cmdName = "/usr/bin/zgrep"
	}
	cmdArgs := []string{tid, responseLog}

	cmdOut, err := exec.Command(cmdName, cmdArgs...).Output()
	if err != nil {
		log.WithFields(log.Fields{
			"tid":   tid,
			"error": err.Error(),
		}).Debug("cannot get row")
		return ""
	}
	return string(cmdOut)
}
