package main

import (
	"fmt"

	"flag"
	smpp_client "github.com/fiorix/go-smpp/smpp"
	"github.com/fiorix/go-smpp/smpp/pdu/pdutext"
	log "github.com/sirupsen/logrus"
	"time"
)

var smpp *smpp_client.Transmitter
var conf Conf

type Conf struct {
	Addr     string
	User     string
	Password string
	Timeout  int
}

func init() {
	log.SetLevel(log.DebugLevel)
	conf = Conf{
		Addr:     "182.16.255.46:15019",
		User:     "SLYEPPLA",
		Password: "SLYPEE_1",
		Timeout:  30,
	}
	smpp = &smpp_client.Transmitter{
		Addr:        conf.Addr,
		User:        conf.User,
		Passwd:      conf.Password,
		RespTimeout: time.Duration(conf.Timeout) * time.Second,
		SystemType:  "SMPP",
	}
	log.Info("smpp client init done")

	connStatus := smpp.Bind()
	go func() {
		for c := range connStatus {
			if c.Status().String() != "Connected" {
				log.WithFields(log.Fields{
					"operator": "mobilink",
					"status":   c.Status().String(),
					"error":    "disconnected:" + c.Status().String(),
				}).Error("smpp moblink connect status")
			} else {
				log.WithFields(log.Fields{
					"operator": "mobilink",
					"status":   c.Status().String(),
				}).Info("smpp moblink connect status")
			}
		}
	}()
}

func main() {
	to := flag.String("to", "923009102250", "msisdn")
	msg := flag.String("msg", "Test message", "test mesage")
	sendSMS(*to, *msg)
}

func sendSMS(msisdn, msg string) error {
	time.Sleep(time.Second)

	shortMsg, err := smpp.Submit(&smpp_client.ShortMessage{
		Src:      "4162",
		Dst:      "00" + msisdn[2:],
		Text:     pdutext.Raw(msg),
		Register: smpp_client.NoDeliveryReceipt,
	})

	if err == smpp_client.ErrNotConnected {
		log.WithFields(log.Fields{
			"msisdn": msisdn,
			"msg":    msg,
			"error":  err.Error(),
		}).Error("counldn't send sms: service unavialable")
		return fmt.Errorf("smpp.Submit: %s", err.Error())
	}
	if err != nil {
		log.WithFields(log.Fields{
			"msisdn": msisdn,
			"msg":    msg,
			"error":  err.Error(),
		}).Error("counldn't send sms: bad request")
		return fmt.Errorf("smpp.Submit: %s", err.Error())
	}

	log.WithFields(log.Fields{
		"msisdn": msisdn,
		"msg":    msg,
		"respid": shortMsg.RespID(),
	}).Error("sms sent")

	return nil
}
