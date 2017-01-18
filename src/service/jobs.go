package service

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"

	"github.com/vostrok/utils/amqp"
	"github.com/vostrok/utils/rec"
)

type jobs struct {
	running map[int64]*Job
}

// rename mo
// exiting check for stop requested
// run at shedulled job
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
	StopRequested bool           `json:"stopping,omitempty"`
	ParsedParams  Params         `json:"parsed_params,omitempty"`
	fh            *os.File       `json:"-"`
	scanner       *bufio.Scanner `json:"-"`
}
type Params struct {
	DateFrom     string `json:"date_from,omitempty"`
	DateTo       string `json:"date_to,omitempty"`
	Count        int64  `json:"count,omitempty"`
	Order        string `json:"order,omitempty"`
	Never        int    `json:"never,omitempty"`
	ServiceId    int64  `json:"service_id,omitempty"`
	CampaignId   int64  `json:"campaign_id,omitempty"`
	OperatorCode int64  `json:"operator_code,omitempty"`
}

func initJobs() *jobs {
	return &jobs{
		running: make(map[int64]*Job),
	}
}
func (j *jobs) AddHandlers(r *gin.Engine) {
	rg := r.Group("/jobs")
	rg.Group("/start").GET("", svc.jobs.start)
	rg.Group("/stop").GET("", svc.jobs.stop)
	//rg.Group("/resume").GET("", svc.jobs.resume)
	rg.Group("/status").GET("", svc.jobs.status)
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
	if err := j.stopJob(id); err != nil {
		err = fmt.Errorf("strconv.ParseInt: :%s", err.Error())
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, struct{}{})
}
func (j *jobs) status(c *gin.Context) {
	jobs, err := j.getList("active")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, jobs)
}
func (j *jobs) startJob(id int64) error {
	job, err := j.get(id)
	if err != nil {
		return err
	}
	var p Params
	if err := json.Unmarshal([]byte(job.Params), &p); err != nil {
		return fmt.Errorf("json.Unmarshal: %s, Params: %s", err.Error(), job.Params)
	}
	svc.jobs.running[id] = &job
	svc.jobs.running[id].run()
	return nil
}
func (j *Job) run() error {
	go func() {
		for {
			if j.StopRequested {
				return nil
			}
			if j.Type == "expired" {
				expired, err := svc.jobs.getExpiredList(j.Params)
				if err != nil {
					return fmt.Errorf("svc.jobs.getExpiredList: %s", err.Error())
				}
				for idx, r := range expired {
					if j.StopRequested {
						return
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

			}
			if j.Type == "injection" {
				if err := j.openFile(); err != nil {
					return err
				}

				var i int64
				for {
					if j.StopRequested {
						return j.close()
					}
					msisdn, err := j.nextMsisdn()
					if err != nil {
						return err
					}
					if i < j.Skip {
						i++
						continue
					}
					if len(msisdn) < 5 {
						return
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
					i++
				}
			}

		}
		return nil
	}()
	return nil
}
func (j *Job) openFile() error {
	var err error
	j.fh, err = os.Open(j.FileName)
	if err != nil {
		err = fmt.Errorf("os.Open: %s, path: %s", err.Error(), j.FileName)
		return
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
func (j *Job) close() error {
	return j.fh.Close()
}
func (j *jobs) stopJob(id int64) error {
	_, ok := svc.jobs.running[id]
	if !ok {
		return fmt.Errorf("Not found: %d", id)
	}
	svc.jobs.running[id].StopRequested = true
	if err := j.setStatus("cancelled", id); err != nil {
		return fmt.Errorf("j.setStatus: %s", err.Error())
	}
	if err := j.setSkip(svc.jobs.running[id].Skip, id); err != nil {
		return fmt.Errorf("j.setSkip: %s", err.Error())
	}
	delete(svc.jobs.running, id)
	return nil
}
func (j *jobs) getList(status string) (jobs []Job, err error) {
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
		return job
	}
	if rows.Err() != nil {
		DBErrors.Inc()
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return
	}
	err = fmt.Errorf("Not found: %d", id)
	return
}
func (j *jobs) setStatus(status string, id int64) (err error) {
	query := fmt.Sprintf("UPDATE %jobs SET status = $1 WHERE id = $2 ",
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
	query := fmt.Sprintf("UPDATE %jobs SET skip = $1 WHERE id = $2 ",
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
	var err error
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
		"WHERE "+
		" operator_code = $1 AND "+
		" status = '' "+
		" ORDER BY id  "+
		" LIMIT %s", // get the last touched
		svc.conf.db.TablePrefix,
		strconv.Itoa(p.Count),
	)

	rows, err := svc.dbConn.Query(query, p.OperatorCode)
	if err != nil {
		DBErrors.Inc()
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return
	}
	defer rows.Close()

	for rows.Next() {
		record := rec.Record{}
		if err := rows.Scan(
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
