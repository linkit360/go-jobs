package main

import (
	"database/sql"
	"flag"
	"fmt"
	"time"

	"github.com/jinzhu/configor"
	log "github.com/sirupsen/logrus"

	dbconn "github.com/vostrok/utils/db"
	logger "github.com/vostrok/utils/log"
)

// stat report
// * count of unique users in that day : 140 534
//* billing attempts count total :  2 050 945
//* successfully paid transactions :  10 423
func init() {
	log.SetLevel(log.DebugLevel)
}

type conf struct {
	Db dbconn.DataBaseConfig `yaml:"db_slave"`
}

var dbConn *sql.DB
var logFile *log.Logger

func main() {
	cfg := flag.String("config", "jobs.yml", "configuration yml file")
	from := flag.String("from", "2017-01-01", "date from")
	to := flag.String("to", "2017-01-31", "date to")
	logPath := flag.String("log", "stat_complaints.log", "log path")
	flag.Parse()

	var appConfig conf

	if *cfg != "" {
		if err := configor.Load(&appConfig, *cfg); err != nil {
			log.WithField("config", err.Error()).Fatal("config load error")
		}
	}

	logFile = logger.GetFileLogger(*logPath)

	dbConn = dbconn.Init(appConfig.Db)
	runScript(*from, *to)
}

type RowType struct {
	DateTime              string
	TotalTransactions     int
	TotalPaidTransactions int
	UniqueUsersCount      int
}

func runScript(fromS, toS string) {
	from, err := time.Parse("2006-01-02", fromS)
	if err != nil {
		log.WithField("error", err.Error()).Fatal("time")
	}
	to, err := time.Parse("2006-01-02", toS)
	if err != nil {
		log.WithField("error", err.Error()).Fatal("time")
	}
	if from.Unix() > to.Unix() {
		log.WithField("error", "from date cannot be greater that to date").Fatal("time")
	}
	log.WithFields(log.Fields{
		"from": fromS,
		"to":   toS,
	}).Info("start")
	for from != to {
		log.WithField("from", from.String()[:10]).Info("get script results..")
		begin := time.Now()
		query := fmt.Sprintf(" select stat_func('%s')", from.String()[:10])
		var res string
		if err := dbConn.QueryRow(query).Scan(&res); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			log.WithField("error", err.Error()).Fatal("query")
		}

		log.WithFields(log.Fields{
			"date": from.String()[:10],
			"took": time.Since(begin).Minutes(),
			"res":  res,
		}).Debug("done")
		logFile.WithFields(log.Fields{
			"date": from.String()[:10],
			"took": time.Since(begin).Minutes(),
		}).Println(res)

		from = from.Add(24 * time.Hour)
	}
	log.WithFields(log.Fields{}).Info("done")
	return
}
