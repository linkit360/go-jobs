package src

// Former corner for operator service
import (
	"runtime"

	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"

	m "github.com/vostrok/metrics"
	"github.com/vostrok/mo/src/config"
	"github.com/vostrok/mo/src/service"
)

func RunServer() {
	appConfig := config.LoadConfig()
	service.InitService(
		appConfig.Server,
		appConfig.DbConf,
		appConfig.Queues,
		appConfig.Consumer,
		appConfig.Publisher,
	)

	nuCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(nuCPU)
	log.WithField("CPUCount", nuCPU)

	r := gin.New()
	m.AddHandler(r)

	r.Run(":" + appConfig.Server.Port)

	log.WithField("port", appConfig.Server.Port).Info("mobilink init")
}
