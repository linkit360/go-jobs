package main

import (
	"flag"
	"fmt"
	"time"

	smpp_client "github.com/fiorix/go-smpp/smpp"
	"github.com/fiorix/go-smpp/smpp/pdu"
	"github.com/fiorix/go-smpp/smpp/pdu/pdufield"
	"github.com/fiorix/go-smpp/smpp/pdu/pdutext"
	"github.com/jinzhu/configor"
	log "github.com/sirupsen/logrus"
)

var smppTransceiver *smpp_client.Transceiver
var conf Config

type Config struct {
	Smpp        SmppConfig `yaml:"smpp"`
	Msisdn      string     `yaml:"msisdn"`
	Msg         string     `yaml:"msg"`
	ShortNumber string     `yaml:"sn"`
}

type SmppConfig struct {
	Addr     string `yaml:"addr" default:"217.118.84.12:3340"`
	User     string `yaml:"user" default:"1637571"`
	Password string `yaml:"pass" default:"wBy4E2Tz"`
	Timeout  int    `yaml:"timeout"`
}

func init() {
	log.SetLevel(log.DebugLevel)
	cfg := flag.String("config", "config.yml", "configuration yml file")

	flag.Parse()
	if *cfg != "" {
		if err := configor.Load(&conf, *cfg); err != nil {
			log.WithField("config", err.Error()).Fatal("config load error")
		}
	}
	log.Debugf("config %#v", conf)

	smppTransceiver = &smpp_client.Transceiver{
		Addr:        conf.Smpp.Addr,
		User:        conf.Smpp.User,
		Passwd:      conf.Smpp.Password,
		RespTimeout: time.Duration(conf.Smpp.Timeout) * time.Second,
		Handler:     receiverFn,
	}
	connStatus := smppTransceiver.Bind()
	go func() {
		for c := range connStatus {
			if c.Status().String() != "Connected" {
				log.WithFields(log.Fields{
					"status": c.Status().String(),
					"error":  "disconnected:" + c.Status().String(),
				}).Error("smpp connect failed")
			} else {
				log.WithFields(log.Fields{
					"status": c.Status().String(),
				}).Debug("smpp beeline connect ok")
			}
		}
	}()
	log.Info("smpp transceiver init done")
	return
}

func receiverFn(p pdu.Body) {
	f := p.Fields()
	tlv := p.TLVFields()
	fields := log.Fields{
		"seq":           p.Header().Seq,
		"header_id":     p.Header().ID,
		"source_port":   string(tlv[pdufield.SourcePort].Bytes()),
		"source":        f[pdufield.SourceAddr].String(),
		"dst":           f[pdufield.DestinationAddr].String(),
		"short_message": f[pdufield.ShortMessage].String(),
	}
	log.WithFields(fields).Debug("received")
}

func main() {
	sendSMS(conf.Msisdn, conf.Msg, conf.ShortNumber)
}

func sendSMS(msisdn, msg, sn string) error {
	log.WithFields(log.Fields{
		"msisdn": msisdn,
		"msg":    msg,
		"sn":     sn,
	}).Debug("start")
	if msisdn == "" {
		log.WithFields(log.Fields{
			"msisdn": msisdn,
			"msg":    msg,
			"error":  "no msisdn specified",
		}).Error("counldn't send sms")
		return fmt.Errorf("no requreed param: %s", msisdn)
	}
	if msg == "" {
		log.WithFields(log.Fields{
			"msisdn": msisdn,
			"msg":    msg,
			"error":  "no msg specified",
		}).Error("empty")
		return fmt.Errorf("no requreed param: %s", "")
	}

	time.Sleep(time.Second)
	shortMsg, err := smppTransceiver.Submit(&smpp_client.ShortMessage{
		Src:      conf.ShortNumber,
		Dst:      msisdn,
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
	}).Info("sms sent")

	return nil
}
