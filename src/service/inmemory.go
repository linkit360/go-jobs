package service

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
)

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

	var err error
	log.WithFields(log.Fields{}).Debug("operators reload...")
	begin := time.Now()
	defer func() {
		fields := log.Fields{
			"took": time.Since(begin),
		}
		if err != nil {
			fields["error"] = err.Error()
		}
		log.WithFields(fields).Debug("operators reload")
	}()

	query := fmt.Sprintf("SELECT "+
		"name, "+
		"rps, "+
		"settings "+
		"FROM %soperators",
		svc.conf.db.TablePrefix)
	var rows *sql.Rows
	rows, err = svc.db.Query(query)
	if err != nil {
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return err
	}
	defer rows.Close()

	var operators []Operator
	for rows.Next() {
		var op Operator
		if err = rows.Scan(
			&op.Name,
			&op.Rps,
			&op.Settings,
		); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return err
		}
		operators = append(operators, op)
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
