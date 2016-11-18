package service

import (
	"database/sql"
	"fmt"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/vostrok/utils/cqr"
)

var cqrConf = []cqr.CQRConfig{{
	Table:      "operator",
	ReloadFunc: memOperators.Reload,
}}

func initInMem() {
	cqr.InitCQR(cqrConf)
}
func AddCQRHandlers(r *gin.Engine) {
	cqr.AddCQRHandler(reloadCQRFunc, r)
}

func reloadCQRFunc(c *gin.Context) {
	cqr.CQRReloadFunc(cqrConf, c)(c)
}

// Tasks:
// Keep in memory all operators names and configuration
// Reload when changes to operators table are done
var memOperators = &Operators{}

type Operators struct {
	sync.RWMutex
	ByCode map[int64]Operator
}

type Operator struct {
	Name     string
	Rps      int
	Settings string
	Code     int64
}

func (ops *Operators) Reload() error {
	ops.Lock()
	defer ops.Unlock()

	query := fmt.Sprintf("SELECT "+
		"name, "+
		"code, "+
		"rps, "+
		"settings "+
		"FROM %soperators",
		svc.conf.db.TablePrefix)
	var rows *sql.Rows
	var err error
	rows, err = svc.db.Query(query)
	if err != nil {
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return err
	}
	defer rows.Close()

	var operators []Operator
	for rows.Next() {
		var operator Operator
		if err = rows.Scan(
			&operator.Name,
			&operator.Code,
			&operator.Rps,
			&operator.Settings,
		); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return err
		}
		operators = append(operators, operator)
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return err
	}
	ops.ByCode = make(map[int64]Operator, len(operators))
	for _, op := range operators {
		ops.ByCode[op.Code] = op
	}
	return nil
}
