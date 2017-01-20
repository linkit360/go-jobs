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
				if err = svc.jobs.startJob(job.Id); err != nil {
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
	if err := j.startJob(id); err != nil {
		err = fmt.Errorf("strconv.ParseInt: :%s", err.Error())
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
func (j *jobs) startJob(id int64) error {
	log.WithFields(log.Fields{
		"id": id,
	}).Info("start")
	job, err := j.get(id)
	if err != nil {
		return err
	}
	var p Params
	if err := json.Unmarshal([]byte(job.Params), &p); err != nil {
		return fmt.Errorf("json.Unmarshal: %s, Params: %s", err.Error(), job.Params)
	}
	if err := svc.jobs.setStatus(id, "in progress"); err != nil {
		return fmt.Errorf("jobs.setStatus: %s", err.Error())
	}

	svc.jobs.running[id] = &job
	if job.Type == "injection" {
		if err := svc.jobs.running[id].openFile(); err != nil {
			if err := svc.jobs.setStatus(id, "error"); err != nil {
				return fmt.Errorf("jobs.setStatus: %s", err.Error())
			}
			return err
		}
	}

	svc.jobs.running[id].run()
	return nil
}

func (j *Job) run() {
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
					log.WithFields(log.Fields{
						"error": err.Error(),
					}).Error("cannt process")
					svc.jobs.running[j.Id].finished = true
					return
				}
				for idx, r := range expired {
					if j.StopRequested || svc.exiting {
						log.WithFields(log.Fields{
							"jobStop": j.StopRequested,
							"service": svc.exiting,
						}).Info("exiting")
						svc.jobs.running[j.Id].finished = true
						return
					}
					if r.RetryId < j.Skip {
						continue
					}
					r.Type = "expired"
					j.Skip = r.RetryId
					if err := svc.jobs.sendToMobilinkRequests(0, r); err != nil {
						log.WithFields(log.Fields{
							"error": err.Error(),
							"id":    r.RetryId,
						}).Error("cannt process")
						time.Sleep(time.Second)
						idx = idx - 1
						continue
					}
				}
				log.WithFields(log.Fields{
					"count": len(expired),
				}).Info("done")
				svc.jobs.running[j.Id].finished = true
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
					msisdn, err := j.nextMsisdn()
					if err != nil {
						log.WithFields(log.Fields{
							"error": err.Error(),
						}).Error("next msisdn")
						svc.jobs.running[j.Id].finished = true
						return
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
					if len(msisdn) < 5 {
						log.WithFields(log.Fields{
							"error":  "msisdn is too short",
							"msisdn": msisdn,
						}).Error("msisdn")
						continue
					}
					r := rec.Record{
						CampaignId: j.ParsedParams.CampaignId,
						ServiceId:  j.ParsedParams.ServiceId,
						Msisdn:     msisdn,
						Tid:        rec.GenerateTID(),
					}
					r.Type = "injection"
					j.Skip = i
					if err := svc.jobs.sendToMobilinkRequests(0, r); err != nil {
						log.WithFields(log.Fields{
							"error": err.Error(),
							"id":    r.RetryId,
						}).Error("cannt process")
						time.Sleep(time.Second)
						continue
					}
					log.WithFields(log.Fields{
						"tid": r.Tid,
					}).Error("sent")
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
	path := svc.jobs.conf.InjectionsPath + "/" + j.FileName
	j.fh, err = os.Open(path)
	if err != nil {
		return fmt.Errorf("os.Open: %s, path: %s", err.Error(), path)

	}
	j.scanner = bufio.NewScanner(j.fh)
	return nil
}
func (j *Job) nextMsisdn() (msisdn string, err error) {

	if !j.scanner.Scan() {
		if err := j.scanner.Err(); err != nil {
			return "", fmt.Errorf("scanner.Error: %s", err.Error())
		}
		// eof
		return "", nil
	}
	msisdn = j.scanner.Text()
	if len(msisdn) > 35 {
		err = fmt.Errorf("Too long msisdn: %s", msisdn)
		return
	}
	if len(msisdn) < 5 {
		err = fmt.Errorf("Too short msisdn: %s", msisdn)
		return
	}
	return msisdn, nil
}
func (j *Job) closeJob() error {
	return j.fh.Close()
}
func (j *jobs) stopJob(id int64, status string) error {
	log.WithFields(log.Fields{
		"id": id,
	}).Info("stop")
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
	return nil
}
func (j *jobs) stopJobs() {
	for range time.Tick(time.Second) {
		for k, _ := range j.running {
			log.WithFields(log.Fields{
				"id":       k,
				"finished": j.running[k].finished,
			}).Debug("	finish check started")
			if j.running[k].finished {
				if err := j.stopJob(j.running[k].Id, "done"); err != nil {
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
				//"query":         query,
			}
			if err != nil {
				fields["error"] = err.Error()
				log.WithFields(fields).Error("load expired retries failed")
			} else {
				fields["count"] = len(expired)
				log.WithFields(fields).Debug("load expired  retries")
			}
		}()
	}()

	argsCount := 0
	args := []interface{}{}
	wheres := []string{}
	if p.DateFrom != "" {
		argsCount++
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
		whereClauses = v + "$" + strconv.Itoa(k+1)
	}
	where := strings.Join(whereClauses, " AND ")
	if where != "" {
		where = "WHERE " + where
	}

	orderTypeWhere := " ORDER BY id ASC "
	if p.Order != "" {
		orderTypeWhere = " ORDER BY id " + p.Order
	}
	query = fmt.Sprintf("SELECT "+
		"id, "+
		"tid, "+
		"created_at, "+
		"last_pay_attempt_at, "+
		"attempts_count, "+
		"keep_days, "+
		"delay_hours, "+
		"msisdn, "+
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
		p.Count,
	)

	var rows *sql.Rows
	rows, err = svc.dbConn.Query(query, args)
	if err != nil {
		DBErrors.Inc()
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return
	}
	defer rows.Close()

	for rows.Next() {
		record := rec.Record{}
		if err = rows.Scan(
			&record.RetryId,
			&record.Tid,
			&record.CreatedAt,
			&record.LastPayAttemptAt,
			&record.AttemptsCount,
			&record.KeepDays,
			&record.DelayHours,
			&record.Msisdn,
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
func (j *jobs) sendToMobilinkRequests(priority uint8, r rec.Record) (err error) {
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
	return nil
}
