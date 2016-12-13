package src

// Former corner for operator service
import (
	"runtime"

	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"

	"github.com/vostrok/mo/src/config"
	"github.com/vostrok/mo/src/service"
	m "github.com/vostrok/utils/metrics"
)

func RunServer() {
	appConfig := config.LoadConfig()
	m.Init(appConfig.Name)

	service.InitService(
		appConfig.Server,
		appConfig.InMemClientConfig,
		appConfig.DbConf,
		appConfig.ConsumeQueues,
		appConfig.Consumer,
		appConfig.Notifier,
	)

	nuCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(nuCPU)
	log.WithField("CPUCount", nuCPU)

	r := gin.New()
	m.AddHandler(r)
	service.AddSubscriptionsHandler(r)
	r.Run(":" + appConfig.Server.Port)

	log.WithField("port", appConfig.Server.Port).Info("mo init")
}
