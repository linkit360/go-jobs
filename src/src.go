package src

import (
	"runtime"

	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"

	"github.com/linkit360/go-jobs/src/config"
	"github.com/linkit360/go-jobs/src/service"
	m "github.com/linkit360/go-utils/metrics"
)

func RunServer() {
	appConfig := config.LoadConfig()

	service.InitService(
		appConfig.AppName,
		appConfig.Server,
		appConfig.Metrics,
		appConfig.Jobs,
		appConfig.MidConfig,
		appConfig.DbConf,
		appConfig.DbSlaveConf,
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
