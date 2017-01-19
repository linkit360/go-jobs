package src

import (
	"runtime"

	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"

	"github.com/vostrok/jobs/src/config"
	"github.com/vostrok/jobs/src/service"
	m "github.com/vostrok/utils/metrics"
)

func RunServer() {
	appConfig := config.LoadConfig()

	service.InitService(
		appConfig.AppName,
		appConfig.Server,
		appConfig.Jobs,
		appConfig.InMemClientConfig,
		appConfig.DbConf,
		appConfig.Notifier,
	)

	nuCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(nuCPU)
	log.WithField("CPUCount", nuCPU)

	r := gin.New()
	m.AddHandler(r)
	service.AddSubscriptionsHandler(r)
	service.AddJobHandlers(r)
	r.Run(":" + appConfig.Server.Port)

	log.WithField("port", appConfig.Server.Port).Info("init")
}

func OnExit() {
	service.OnExit()
}
