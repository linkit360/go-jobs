package service

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"

	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"

	"github.com/vostrok/jobs/src/config"
	"github.com/vostrok/utils/amqp"
	"github.com/vostrok/utils/db"
	"github.com/vostrok/utils/rec"
)

func init() {
	log.SetLevel(log.DebugLevel)
}

type jobs struct {
	running map[int64]*Job
	slave   *sql.DB
	conf    config.JobsConfig
	cache   map[int64]map[string]struct{}
}

type Job struct {
	Id            int64          `json:"id"`
	UserId        int64          `json:"user_id"`
	CreatedAt     time.Time      `json:"created_at"`
	RunAt         time.Time      `json:"run_at"`
	Type          string         `json:"type"`
	Status        string         `json:"status"`
	FileName      string         `json:"file_name,omitempty"`
	Params        string         `json:"params,omitempty"`
	Skip          int64          `json:"skip,omitempty"`
	StopRequested bool           `json:"-"`
	ParsedParams  Params         `json:"parsed_params,omitempty"`
	fh            *os.File       `json:"-"`
	scanner       *bufio.Scanner `json:"-"`
	finished      bool           `json:"finished"`
}
type Params struct {
	DateFrom     string `json:"date_from,omitempty"`
	DateTo       string `json:"date_to,omitempty"`
	Count        int64  `json:"count,omitempty"`
	Offset       int64  `json:"offset,omitempty"`
	Order        string `json:"order,omitempty"`
	Never        int    `json:"never,omitempty"`
	ServiceId    int64  `json:"service_id,omitempty"`
	CampaignId   int64  `json:"campaign_id,omitempty"`
	DryRun       bool   `json:"dry_run,omitempty"`
	LastChargeAt string `json:"last_charge_at,omitempty"`
}

func (p Params) ToString() string {
	s, _ := json.Marshal(p)
	return string(s)
}

func initJobs(jConf config.JobsConfig, dbSlaveConf db.DataBaseConfig) *jobs {
	jobs := &jobs{
		running: make(map[int64]*Job),
		cache:   make(map[int64]map[string]struct{}),
		conf:    jConf,
		slave:   db.Init(dbSlaveConf),
	}
	if jConf.PlannedEnabled {
		go jobs.planned()
	} else {
		log.WithFields(log.Fields{
			"planned": jConf.PlannedEnabled,
		}).Info("run planned disabled")
	}

	go jobs.stopJobs()
	return jobs
}
func AddJobHandlers(r *gin.Engine) {
	rg := r.Group("/jobs")
	rg.Group("/start").GET("", svc.jobs.start)
	rg.Group("/stop").GET("", svc.jobs.stop)
	rg.Group("/resume").GET("", svc.jobs.start)
	rg.Group("/status").GET("", svc.jobs.status)
}
func (j *jobs) planned() {
	for range time.Tick(time.Second) {
		jobs, err := j.getList("ready")
		if err != nil {
			log.WithFields(log.Fields{
				"error": err.Error(),
			}).Error("cannt process")
			return
		}

		for _, job := range jobs {
			if time.Now().Sub(job.RunAt) > 10 && job.Status == "ready" {
				if err = svc.jobs.startJob(job.Id, false); err != nil {
					log.WithFields(log.Fields{
						"error": err.Error(),
					}).Error("cannt process")
				}
			}
		}
	}
}

func (j *jobs) start(c *gin.Context) { // start?id=132123
	idStr, ok := c.GetQuery("id")
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "id required",
		})
		return
	}
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		err = fmt.Errorf("strconv.ParseInt: :%s", err.Error())
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}
	resume := false
	if strings.Contains(c.Request.URL.Path, "resume") {
		resume = true
	}
	if err := j.startJob(id, resume); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, struct{}{})
}
func (j *jobs) stop(c *gin.Context) {
	idStr, ok := c.GetQuery("id")
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "id required",
		})
		return
	}
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		err = fmt.Errorf("strconv.ParseInt: :%s", err.Error())
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}
	if err := j.stopJob(id, "canceled"); err != nil {
		err = fmt.Errorf("j.stopJob: %s", err.Error())
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, struct{}{})
}
func (j *jobs) status(c *gin.Context) {
	jobs, err := j.getList("in progress")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, jobs)
}
func (j *jobs) startJob(id int64, resume bool) error {
	log.WithFields(log.Fields{
		"id": id,
	}).Info("start")
	job, err := j.get(id)
	if err != nil {
		return err
	}
	if job.Status != "ready" {
		err = fmt.Errorf("Job status: %s", job.Status)
		log.WithFields(log.Fields{
			"id":    id,
			"error": err.Error(),
		}).Info("failed")
		return err
	}
	if err := json.Unmarshal([]byte(job.Params), &job.ParsedParams); err != nil {
		err = fmt.Errorf("json.Unmarshal: %s, Params: %s", err.Error(), job.Params)
		log.WithFields(log.Fields{
			"id":    id,
			"error": err.Error(),
		}).Info("failed")
		return err
	}

	if err := svc.jobs.setStatus(id, "in progress"); err != nil {
		err = fmt.Errorf("jobs.setStatus: %s", err.Error())
		log.WithFields(log.Fields{
			"id":    id,
			"error": err.Error(),
		}).Info("failed")
		return err
	}

	svc.jobs.cache[id] = make(map[string]struct{})
	svc.jobs.running[id] = &job

	if job.Type == "injection" {
		if err := svc.jobs.running[id].openFile(); err != nil {
			if err := svc.jobs.setStatus(id, "error"); err != nil {
				err = fmt.Errorf("jobs.setStatus: %s", err.Error())
				log.WithFields(log.Fields{
					"id":    id,
					"error": err.Error(),
				}).Info("failed to set job status")
				return err
			}
			err = fmt.Errorf("run job: %s", err.Error())
			log.WithFields(log.Fields{
				"id":    id,
				"error": err.Error(),
			}).Info("failed")
			return err
		}
	}

	svc.jobs.running[id].run(resume)
	return nil
}

func (j *Job) run(resume bool) {
	log.WithFields(log.Fields{
		"id": j.Id,
	}).Info("run")

	go func() {
		for {
			if j.Type == "expired" {
				expired, err := svc.jobs.getExpiredList(j.ParsedParams)
				if err != nil {
					err = fmt.Errorf("svc.jobs.getExpiredList: %s", err.Error())
					svc.jobs.running[j.Id].finished = true
					svc.jobs.running[j.Id].Status = "error"
					log.WithFields(log.Fields{
						"error":    err.Error(),
						"finished": svc.jobs.running[j.Id].finished,
					}).Error("cannt process")
					return
				}
				for idx, r := range expired {
					if j.StopRequested || svc.exiting {
						svc.jobs.running[j.Id].finished = true
						svc.jobs.running[j.Id].Status = "canceled"
						log.WithFields(log.Fields{
							"jobStop":  j.StopRequested,
							"service":  svc.exiting,
							"finished": svc.jobs.running[j.Id].finished,
						}).Info("exiting")
						return
					}
					if resume && r.RetryId < j.Skip {
						log.WithFields(log.Fields{
							"tid": r.Tid,
							"id":  r.RetryId,
						}).Info("skip")
						continue
					} else {
						resume = false
					}
					log.WithFields(log.Fields{
						"tid": r.Tid,
					}).Info("process")

					if j.ParsedParams.ServiceId > 0 {
						r.ServiceId = j.ParsedParams.ServiceId
					}
					if j.ParsedParams.CampaignId > 0 {
						r.CampaignId = j.ParsedParams.CampaignId
					}

					if _, ok := svc.jobs.cache[j.Id][r.Msisdn]; ok {
						log.WithFields(log.Fields{
							"tid": r.Tid,
						}).Info("duplicate")
						continue
					} else {
						svc.jobs.cache[j.Id][r.Msisdn] = struct{}{}
					}

					r.Type = "expired"
					j.Skip = r.RetryId
					if err := j.sendToMobilinkRequests(0, r); err != nil {
						log.WithFields(log.Fields{
							"error": err.Error(),
							"id":    r.RetryId,
						}).Error("cannt process")
						time.Sleep(time.Second)
						idx = idx - 1
						continue
					}
				}
				svc.jobs.running[j.Id].finished = true
				svc.jobs.running[j.Id].Status = "done"
				log.WithFields(log.Fields{
					"id":       j.Id,
					"count":    len(expired),
					"finished": svc.jobs.running[j.Id].finished,
				}).Info("done")
				return
			}
			if j.Type == "injection" {
				defer j.closeJob()

				var i int64
				for {
					if j.StopRequested || svc.exiting {
						log.WithFields(log.Fields{
							"jobStop": j.StopRequested,
							"service": svc.exiting,
						}).Info("exiting")
						svc.jobs.running[j.Id].finished = true
						svc.jobs.running[j.Id].Status = "canceled"
						return
					}
					orig, msisdn, err := j.nextMsisdn()
					if err != nil {
						if err == errPaidInTransactions {
							log.WithFields(log.Fields{
								"msisdn": msisdn,
							}).Warn("already paid")
							continue
						}
						log.WithFields(log.Fields{
							"orig":   orig,
							"msisdn": msisdn,
							"error":  err.Error(),
						}).Error("skip")
						continue
					}
					if resume && i < j.Skip {
						i++
						continue
					} else {
						resume = false
					}

					if _, ok := svc.jobs.cache[j.Id][msisdn]; ok {
						log.WithFields(log.Fields{
							"msisdn": msisdn,
						}).Info("duplicate")
						continue
					} else {
						svc.jobs.cache[j.Id][msisdn] = struct{}{}
					}

					if msisdn == "" && err == nil {
						log.WithFields(log.Fields{"count": i}).Info("done")
						svc.jobs.running[j.Id].finished = true
						svc.jobs.running[j.Id].Status = "done"
						return
					}
					r := rec.Record{
						CampaignId: j.ParsedParams.CampaignId,
						ServiceId:  j.ParsedParams.ServiceId,
						Msisdn:     msisdn,
						Tid:        rec.GenerateTID(),
					}
					log.WithFields(log.Fields{
						"tid":    r.Tid,
						"msisdn": r.Msisdn,
					}).Info("process")
					r.Type = "injection"
					j.Skip = i
				send:
					if err := j.sendToMobilinkRequests(0, r); err != nil {
						log.WithFields(log.Fields{
							"error": err.Error(),
							"id":    r.RetryId,
						}).Error("cannt process")
						time.Sleep(time.Second)
						goto send
					}
					i++
				}
			}
		}
		return
	}()
	return
}
func (j *Job) openFile() error {
	var err error
	if j.FileName == "" {
		return fmt.Errorf("File name is empty: %s", j.FileName)
	}

	path := svc.jobs.conf.InjectionsPath + "/" + j.FileName
	j.fh, err = os.Open(path)
	if err != nil {
		return fmt.Errorf("os.Open: %s, path: %s", err.Error(), path)

	}
	log.WithFields(log.Fields{
		"id":   j.Id,
		"path": path,
	}).Info("opened")
	j.scanner = bufio.NewScanner(j.fh)
	return nil
}
func TrimToNum(r rune) bool {
	return !unicode.IsDigit(r)
}

var errPaidInTransactions = errors.New("Paid in transactions")

func (j *Job) nextMsisdn() (orig, msisdn string, err error) {

	if !j.scanner.Scan() {
		if err = j.scanner.Err(); err != nil {
			err = fmt.Errorf("scanner.Error: %s", err.Error())
			return
		}
		// eof
		return "", "", nil
	}
	orig = j.scanner.Text()
	log.WithFields(log.Fields{
		"original": orig,
	}).Info("got from file")

	msisdn = strings.TrimFunc(orig, TrimToNum)
	if len(msisdn) > 20 {
		err = fmt.Errorf("Too long msisdn, length: %d", len(msisdn))
		return
	}
	if len(msisdn) < 5 {
		err = fmt.Errorf("Too short msisdn, length: %d", len(msisdn))
		return
	}
	if !strings.HasPrefix(msisdn, svc.jobs.conf.CheckPrefix) {
		err = fmt.Errorf("Wrong prefix: %s", msisdn)
		return
	}
	if j.ParsedParams.LastChargeAt != "" {
		var one int
		log.WithFields(log.Fields{
			"last_charge_at": j.ParsedParams.LastChargeAt,
			"msisdn":         msisdn,
		}).Info("check")
		query := fmt.Sprintf("SELECT 1 FROM %stransactions "+
			" WHERE ( result = 'paid' OR result = 'retry_paid') AND "+
			" sent_at > $1 AND msisdn = $2 LIMIT 1", svc.conf.db.TablePrefix,
		)

		if err = svc.jobs.slave.QueryRow(query, j.ParsedParams.LastChargeAt, msisdn).Scan(&one); err != nil {
			if err == sql.ErrNoRows {
				err = nil
				log.WithFields(log.Fields{
					"last_charge_at": j.ParsedParams.LastChargeAt,
					"msisdn":         msisdn,
				}).Info("passed")
				return
			} else {
				err = fmt.Errorf("dbConn.QueryRow.Scan: %s, query %s", err.Error(), query)
				return
			}
			return "", "", errPaidInTransactions
		}
	}
	if j.ParsedParams.Never > 0 {
		var one int
		log.WithFields(log.Fields{
			"msisdn": msisdn,
		}).Info("never?")
		query := fmt.Sprintf("SELECT 1 FROM %stransactions "+
			" WHERE ( result = 'paid' OR result = 'retry_paid') AND msisdn = $1 LIMIT 1",
			svc.conf.db.TablePrefix,
		)

		if err = svc.jobs.slave.QueryRow(query, msisdn).Scan(&one); err != nil {
			if err == sql.ErrNoRows {
				err = nil
				log.WithFields(log.Fields{
					"never":  j.ParsedParams.Never,
					"msisdn": msisdn,
				}).Info("passed")
				return
			} else {
				err = fmt.Errorf("dbConn.QueryRow.Scan: %s, query %s", err.Error(), query)
				return
			}
			return "", "", errPaidInTransactions
		}
	}
	return
}

func (j *Job) closeJob() error {
	return j.fh.Close()
}
func (j *jobs) stopJob(id int64, status string) error {
	log.WithFields(log.Fields{
		"id": id,
	}).Info("stop...")
	_, ok := svc.jobs.running[id]
	if !ok {
		return fmt.Errorf("Not found: %d", id)
	}
	svc.jobs.running[id].StopRequested = true

	if err := j.setStatus(id, status); err != nil {
		return fmt.Errorf("j.setStatus: %s", err.Error())
	}
	if err := j.setSkip(svc.jobs.running[id].Skip, id); err != nil {
		return fmt.Errorf("j.setSkip: %s", err.Error())
	}

	delete(svc.jobs.cache, id)
	delete(svc.jobs.running, id)
	log.WithFields(log.Fields{
		"id": id,
	}).Info("removed from running")
	return nil
}
func (j *jobs) stopJobs() {
	for range time.Tick(time.Second) {
		for k, _ := range j.running {
			if j.running[k].finished {
				status := "done"
				if svc.jobs.running[k].Status != "" {
					status = svc.jobs.running[k].Status
				}
				if err := j.stopJob(k, status); err != nil {
					log.WithFields(log.Fields{
						"id":    k,
						"error": err.Error(),
					}).Error("stop")
				} else {
					log.WithFields(log.Fields{
						"id": k,
					}).Debug("finished")
				}
			}
		}
	}
}
func (j *jobs) getList(status string) (jobs []Job, err error) {
	begin := time.Now()
	query := ""
	defer func() {
		defer func() {
			fields := log.Fields{
				"took": time.Since(begin),
			}
			if err != nil {
				fields["error"] = err.Error()
				fields["query"] = query
				log.WithFields(fields).Error("get job list failed")
			} else {
				fields["count"] = len(jobs)
				log.WithFields(fields).Debug("get job list")
			}
		}()
	}()

	query = fmt.Sprintf("SELECT "+
		"id, "+
		"id_user, "+
		"created_at, "+
		"run_at, "+
		"type, "+
		"status, "+
		"file_name, "+
		"params "+
		" FROM %sjobs "+
		" WHERE status = $1 ",
		svc.conf.db.TablePrefix,
	)

	var rows *sql.Rows
	rows, err = svc.dbConn.Query(query, status)
	if err != nil {
		DBErrors.Inc()
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return
	}
	defer rows.Close()

	for rows.Next() {
		job := Job{}

		if err = rows.Scan(
			&job.Id,
			&job.UserId,
			&job.CreatedAt,
			&job.RunAt,
			&job.Type,
			&job.Status,
			&job.FileName,
			&job.Params,
		); err != nil {
			DBErrors.Inc()
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return
		}
		jobs = append(jobs, job)
	}
	if rows.Err() != nil {
		DBErrors.Inc()
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return
	}
	return
}
func (j *jobs) get(id int64) (job Job, err error) {
	query := fmt.Sprintf("SELECT "+
		"id, "+
		"id_user, "+
		"created_at, "+
		"run_at, "+
		"type, "+
		"status, "+
		"file_name, "+
		"params "+
		" FROM %sjobs "+
		" WHERE id = $1 LIMIT 1",
		svc.conf.db.TablePrefix,
	)

	var rows *sql.Rows
	rows, err = svc.dbConn.Query(query, id)
	if err != nil {
		DBErrors.Inc()
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return
	}
	defer rows.Close()

	for rows.Next() {
		if err = rows.Scan(
			&job.Id,
			&job.UserId,
			&job.CreatedAt,
			&job.RunAt,
			&job.Type,
			&job.Status,
			&job.FileName,
			&job.Params,
		); err != nil {
			DBErrors.Inc()
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return
		}
		return
	}
	if rows.Err() != nil {
		DBErrors.Inc()
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return
	}
	err = fmt.Errorf("Not found: %d", id)
	return
}
func (j *jobs) setStatus(id int64, status string) (err error) {
	query := fmt.Sprintf("UPDATE %sjobs SET status = $1 WHERE id = $2 ",
		svc.conf.db.TablePrefix,
	)
	_, err = svc.dbConn.Exec(query, status, id)
	if err != nil {
		DBErrors.Inc()
		err = fmt.Errorf("db.Exec: %s, query: %s", err.Error(), query)
		return
	}

	if svc.jobs.conf.CallBackUrl != "" {
		url := fmt.Sprintf("%s?id=%d&status=%s", svc.jobs.conf.CallBackUrl, id, status)
		resp, err := http.Get(url)
		fields := log.Fields{}
		fields["url"] = url
		if err != nil {
			fields["error"] = err.Error()
		}
		if resp != nil {
			fields["code"] = resp.Status
		}
		log.WithFields(fields).Info("hook call")
	}
	return
}
func (j *jobs) setSkip(skip int64, id int64) (err error) {

	query := fmt.Sprintf("UPDATE %sjobs SET skip = $1, finished_at =$2 WHERE id = $3",
		svc.conf.db.TablePrefix,
	)
	finishAt := time.Now().UTC()
	_, err = svc.dbConn.Exec(query, skip, finishAt, id)
	if err != nil {
		DBErrors.Inc()
		err = fmt.Errorf("db.Exec: %s, query: %s", err.Error(), query)
		return
	}

	return
}

func (j *jobs) getExpiredList(p Params) (expired []rec.Record, err error) {
	begin := time.Now()
	var query string
	defer func() {
		defer func() {
			fields := log.Fields{
				"took":  time.Since(begin),
				"limit": p.Count,
				"query": query,
			}
			if err != nil {
				fields["error"] = err.Error()
				log.WithFields(fields).Error("load expired retries failed")
			} else {
				fields["count"] = len(expired)
				log.WithFields(fields).Debug("load expired retries")
			}
		}()
	}()

	args := []interface{}{}
	wheres := []string{}
	if p.DateFrom != "" {
		args = append(args, p.DateFrom)
		wheres = append(wheres, "created_at > %s")
	}
	if p.DateTo != "" {
		args = append(args, p.DateTo)
		wheres = append(wheres, "created_at < %s")
	}

	if p.ServiceId > 0 {
		args = append(args, p.ServiceId)
		wheres = append(wheres, "id_service = %s")
	}

	if p.CampaignId > 0 {
		args = append(args, p.CampaignId)
		wheres = append(wheres, "id_campaign = %s")
	}

	if p.Never > 0 {
		wheres = append(wheres, fmt.Sprintf("msisdn NOT IN ("+
			" SELECT DISTINCT msisdn "+
			" FROM %stransactions "+
			" WHERE ( result = 'paid' OR result = 'retry_paid') )",
			svc.conf.db.TablePrefix),
		)
	}

	if p.LastChargeAt != "" {
		args = append(args, p.LastChargeAt)
		wheres = append(wheres, fmt.Sprintf("msisdn NOT IN ("+
			" SELECT DISTINCT msisdn "+
			" FROM %stransactions ", svc.conf.db.TablePrefix)+
			" WHERE ( result = 'paid' OR result = 'retry_paid') AND "+
			"sent_at > %s ) ",
		)
	}

	countWhere := ""
	if p.Count > 0 {
		countWhere = fmt.Sprintf(" LIMIT %d", p.Count)
	}

	whereClauses := []string{}
	for k, v := range wheres {
		if strings.Contains(v, "%s") {
			whereClauses = append(whereClauses, fmt.Sprintf(v, "$"+strconv.Itoa(k+1)))
		} else {
			whereClauses = append(whereClauses, v)
		}
	}

	where := strings.Join(whereClauses, " AND ")
	if where != "" {
		where = "WHERE " + where
	}

	orderTypeWhere := " ORDER BY msisdn, id ASC "
	if p.Order != "" {
		orderTypeWhere = " ORDER BY msisdn, id " + p.Order
	}
	log.WithFields(log.Fields{
		"args":   fmt.Sprintf("%#v", args),
		"where":  where + orderTypeWhere + countWhere,
		"params": p.ToString(),
	}).Debug("run query")

	query = fmt.Sprintf("SELECT "+
		"DISTINCT ON (msisdn), "+
		"id, "+
		"tid, "+
		"created_at, "+
		"last_pay_attempt_at, "+
		"attempts_count, "+
		"keep_days, "+
		"delay_hours, "+
		"price, "+
		"operator_code, "+
		"country_code, "+
		"id_service, "+
		"id_subscription, "+
		"id_campaign "+
		"FROM %sretries_expired "+
		where+
		orderTypeWhere+
		countWhere,
		svc.conf.db.TablePrefix,
	)

	var rows *sql.Rows
	rows, err = svc.jobs.slave.Query(query, args...)
	if err != nil {
		DBErrors.Inc()
		err = fmt.Errorf("db.Query: %s, query: %s, args: %s, params: %s", err.Error(), query, fmt.Sprintf("%#v", args), fmt.Sprintf("%#v", p))
		return
	}
	defer rows.Close()

	for rows.Next() {
		record := rec.Record{}
		if err = rows.Scan(
			&record.Msisdn,
			&record.RetryId,
			&record.Tid,
			&record.CreatedAt,
			&record.LastPayAttemptAt,
			&record.AttemptsCount,
			&record.KeepDays,
			&record.DelayHours,
			&record.Price,
			&record.OperatorCode,
			&record.CountryCode,
			&record.ServiceId,
			&record.SubscriptionId,
			&record.CampaignId,
		); err != nil {
			DBErrors.Inc()
			err = fmt.Errorf("Rows.Next: %s", err.Error())
			return
		}
		expired = append(expired, record)
	}
	if rows.Err() != nil {
		DBErrors.Inc()
		err = fmt.Errorf("rows.Error: %s", err.Error())
		return
	}
	return expired, nil
}

type Rrr struct {
	r      rec.Record
	Count  int
	Msisdn string
}

func (j *Job) sendToMobilinkRequests(priority uint8, r rec.Record) (err error) {
	if j.ParsedParams.DryRun {
		return nil
	}
	event := amqp.EventNotify{
		EventName: "charge",
		EventData: r,
	}
	body, err := json.Marshal(event)
	if err != nil {
		err = fmt.Errorf("json.Marshal: %s", err.Error())
		return
	}
	svc.publisher.Publish(amqp.AMQPMessage{QueueName: "mobilink_requests", Priority: priority, Body: body})
	log.WithFields(log.Fields{
		"tid": r.Tid,
	}).Info("sent")
	return nil
}
