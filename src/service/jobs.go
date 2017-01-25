package service

import (
	"bufio"
	"database/sql"
	"encoding/json"
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
	"github.com/vostrok/utils/rec"
)

func init() {
	log.SetLevel(log.DebugLevel)
}

type jobs struct {
	running map[int64]*Job
	conf    config.JobsConfig
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
	DateFrom   string `json:"date_from,omitempty"`
	DateTo     string `json:"date_to,omitempty"`
	Count      int64  `json:"count,omitempty"`
	Order      string `json:"order,omitempty"`
	Never      int    `json:"never,omitempty"`
	ServiceId  int64  `json:"service_id,omitempty"`
	CampaignId int64  `json:"campaign_id,omitempty"`
	DryRun     bool   `json:"dry_run,omitempty"`
}

func (p Params) ToString() string {
	s, _ := json.Marshal(p)
	return string(s)
}

func initJobs(jConf config.JobsConfig) *jobs {
	jobs := &jobs{
		running: make(map[int64]*Job),
		conf:    jConf,
	}
	if jConf.PlannedEnabled {
		go jobs.planned()
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
	if err := j.stopJob(id, "cancelled"); err != nil {
		err = fmt.Errorf("strconv.ParseInt: :%s", err.Error())
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
		"id":  j.Id,
		"job": fmt.Sprintf("%#v", j),
	}).Info("run")

	go func() {
		for {
			if j.Type == "expired" {
				expired, err := svc.jobs.getExpiredList(j.ParsedParams)
				if err != nil {
					err = fmt.Errorf("svc.jobs.getExpiredList: %s", err.Error())
					svc.jobs.running[j.Id].finished = true
					log.WithFields(log.Fields{
						"error":    err.Error(),
						"finished": svc.jobs.running[j.Id].finished,
					}).Error("cannt process")
					return
				}
				for idx, r := range expired {
					if j.StopRequested || svc.exiting {
						svc.jobs.running[j.Id].finished = true
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
						return
					}
					orig, msisdn, err := j.nextMsisdn()
					if err != nil {
						log.WithFields(log.Fields{
							"orig":   orig,
							"msisdn": msisdn,
							"error":  err.Error(),
						}).Error("skip")
						continue
					}
					if i < j.Skip {
						i++
						continue
					}
					if msisdn == "" && err == nil {
						log.WithFields(log.Fields{"count": i}).Info("done")
						svc.jobs.running[j.Id].finished = true
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
					}).Info("ok")
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
	j.scanner = bufio.NewScanner(j.fh)
	return nil
}
func TrimToNum(r rune) bool {
	return !unicode.IsDigit(r)
}
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
				if err := j.stopJob(k, "done"); err != nil {
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

	return
}
func (j *jobs) setSkip(skip int64, id int64) (err error) {
	query := fmt.Sprintf("UPDATE %sjobs SET skip = $1 WHERE id = $2 ",
		svc.conf.db.TablePrefix,
	)
	_, err = svc.dbConn.Exec(query, skip, id)
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
		wheres = append(wheres, "created_at > ")
	}
	if p.DateTo != "" {
		args = append(args, p.DateTo)
		wheres = append(wheres, "created_at < ")
	}

	if p.ServiceId > 0 {
		args = append(args, p.ServiceId)
		wheres = append(wheres, "id_service = ")
	}

	if p.CampaignId > 0 {
		args = append(args, p.CampaignId)
		wheres = append(wheres, "id_campaign = ")
	}
	if p.Never > 0 {
		notPaidInDays := fmt.Sprintf("msisdn NOT IN ("+
			" SELECT DISTINCT msisdn "+
			" FROM %stransactions "+
			" WHERE sent_at > (CURRENT_TIMESTAMP -  INTERVAL '%d days' ) AND "+
			"       ( result = 'paid' OR result = 'retry_paid') )",
			svc.conf.db.TablePrefix,
			p.Never,
		)
		wheres = append(wheres, notPaidInDays)
	}

	countWhere := ""
	if p.Count > 0 {
		countWhere = fmt.Sprintf(" LIMIT %d", p.Count)
	}

	whereClauses := []string{}
	for k, v := range wheres {
		whereClauses = append(whereClauses, v+"$"+strconv.Itoa(k+1))
	}
	where := strings.Join(whereClauses, " AND ")
	if where != "" {
		where = "WHERE " + where
	}

	orderTypeWhere := " ORDER BY id ASC "
	if p.Order != "" {
		orderTypeWhere = " ORDER BY id " + p.Order
	}
	log.WithFields(log.Fields{
		"args":   fmt.Sprintf("%#v", args),
		"where":  where,
		"params": p.ToString(),
	}).Debug("run query")

	query = fmt.Sprintf("SELECT "+
		"msisdn, "+
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
	rows, err = svc.dbConn.Query(query, args...)
	if err != nil {
		DBErrors.Inc()
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
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
		err = fmt.Errorf("GetRetries RowsError: %s", err.Error())
		return
	}
	return expired, nil
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
