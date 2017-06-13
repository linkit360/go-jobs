package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/jinzhu/configor"
	log "github.com/sirupsen/logrus"

	"github.com/linkit360/go-jobs/src/config"
	"github.com/linkit360/go-jobs/src/service"
	"github.com/linkit360/go-utils/db"
)

type AppConfig struct {
	Db   db.DataBaseConfig `yaml:"db"`
	Jobs config.JobsConfig `yaml:"jobs"`
}

func main() {
	cfg := flag.String("config", "../jobs.yml", "path config")
	var appConfig AppConfig

	jobPath := flag.String(
		"path",
		"",
		"path to job files, in /var/www/xmp.linkit360.ru/web/injections")

	params := flag.String(
		"params",
		`{"count": 14000, "never": 1, "dry_run": false, "service_code": 111, "campaign_code": 354}`,
		"params for job (service id, count, dry run, etc)",
	)

	flag.Parse()

	if *cfg != "" {
		if err := configor.Load(&appConfig, *cfg); err != nil {
			log.WithField("config", err.Error()).Fatal("config load error")
		}
	}

	log.WithFields(log.Fields{
		"path":   *jobPath,
		"params": *params,
	}).Info("run")

	dbConn := db.Init(appConfig.Db)

	//fullPath := appConfig.Jobs.InjectionsPath + "/" + *jobPath
	//files, err := ioutil.ReadDir(fullPath)
	//if err != nil {
	//	log.WithField("error", err.Error()).Fatal("cannot read dir")
	//}
	runAt := time.Now().UTC()
	max := 81
	skip := int64(120000)
	for i := 0; i < max; i++ {

		// /for _, f := range files {
		job := service.Job{
			UserId:   0,
			RunAt:    runAt,
			Type:     "injection",
			Status:   "ready",
			FileName: *jobPath + "/" + "injections.csv",
			Params:   *params,
			Skip:     skip,
		}
		skip = skip + 14000

		runAt = runAt.Add(24 * time.Hour)

		query := fmt.Sprintf("INSERT INTO %sjobs ("+
			"id_user, "+
			"run_at, "+
			"status,"+
			"type,"+
			"file_name,"+
			"params,"+
			"skip "+
			") values ($1, $2, $3, $4, $5, $6, $7)",
			appConfig.Db.TablePrefix)

		if _, err := dbConn.Exec(query,
			job.UserId,
			job.RunAt,
			job.Status,
			job.Type,
			job.FileName,
			job.Params,
			job.Skip,
		); err != nil {
			log.WithFields(log.Fields{
				"error": err.Error(),
				"job":   fmt.Sprintf("%#v", job),
				"query": query,
			}).Fatal("cannot add in db")
		} else {
			log.WithFields(log.Fields{
				"run_at": job.RunAt,
				"path":   job.FileName,
			}).Info("added in db")
		}
	}

}
