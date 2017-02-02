package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	log "github.com/Sirupsen/logrus"
	"github.com/jinzhu/configor"

	"github.com/vostrok/dispatcherd/src/rbmq"
	"github.com/vostrok/utils/rec"
	"time"
)

// script parses records in json and let them be added in subscriptions
// usually it's needed when dispatcher couldn;t sent them in mt manager

type conf struct {
	Notifier rbmq.NotifierConfig `yaml:"notifier"`
}

var notifierService rbmq.Notifier

func main() {
	cfg := flag.String("config", "jobs.yml", "configuration yml file")
	filePath := flag.String("file", "log", "log file yml file")
	flag.Parse()

	var appConfig conf

	if *cfg != "" {
		if err := configor.Load(&appConfig, *cfg); err != nil {
			log.WithField("config", err.Error()).Fatal("config load error")
		}
	}
	notifierService = rbmq.NewNotifierService(appConfig.Notifier)
	if *filePath == "" {
		log.WithField("file", filePath).Debug("using default file path")
	}

	log.Info(fmt.Sprintf("%#v", appConfig.Notifier))

	file, err := os.Open(*filePath)
	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	scanner := bufio.NewScanner(file)
	i := 0
	for scanner.Scan() {
		i++
		text := scanner.Text()

		var record rec.Record
		if err := json.Unmarshal([]byte(text), &record); err != nil {
			fmt.Println(text)
			fmt.Println(err.Error())
			continue
		}
		fmt.Println(text)
		if err := notifierService.NewSubscriptionNotify("mobilink_new_subscriptions", record); err != nil {
			fmt.Println(err.Error())
			continue
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	time.Sleep(time.Minute)
	log.Info("done")
	log.Info(i)

}
