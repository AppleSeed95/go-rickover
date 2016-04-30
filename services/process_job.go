package services

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/Shyp/rickover/Godeps/_workspace/src/github.com/Shyp/go-simple-metrics"
	"github.com/Shyp/rickover/downstream"
	"github.com/Shyp/rickover/models"
	"github.com/Shyp/rickover/models/queued_jobs"
	"github.com/Shyp/rickover/rest"
)

// SleepFactor determines how long the application should sleep between runs.
var SleepFactor = 500

// DefaultTimeout is the default amount of time a JobProcessor should wait for
// a job to complete.
var DefaultTimeout = 5 * time.Minute

// JobProcessor implements the Worker interface.
type JobProcessor struct {
	Client *downstream.Client

	// Amount of time we should wait for a response before marking the job as
	// failed.
	Timeout time.Duration
}

// isTimeout returns true if the err was caused by a request timeout.
func isTimeout(err error) bool {
	// This is difficult in Go 1.5: http://stackoverflow.com/a/23497404/329700
	return strings.Contains(err.Error(), "Timeout exceeded")
}

// DoWork sends the given queued job to the downstream service, then waits for
// it to complete.
func (jp *JobProcessor) DoWork(qj *models.QueuedJob) error {
	if err := jp.requestRetry(qj); err != nil {
		if isTimeout(err) {
			// Assume the request made it to Heroku; we see this most often
			// when the downstream server restarts. Heroku receives/queues the
			// requests until the new server is ready, and we see a timeout.
			return waitForJob(qj, jp.Timeout)
		} else {
			return HandleStatusCallback(qj.Id, qj.Name, models.StatusFailed, qj.Attempts)
		}
	}
	return waitForJob(qj, jp.Timeout)
}

func (jp *JobProcessor) requestRetry(qj *models.QueuedJob) error {
	log.Printf("processing job %s (type %s)", qj.Id.String(), qj.Name)
	for i := uint8(0); i < 3; i++ {
		if qj.ExpiresAt.Valid && time.Since(qj.ExpiresAt.Time) >= 0 {
			return createAndDelete(qj.Id, qj.Name, models.StatusExpired, qj.Attempts)
		}
		params := &downstream.JobParams{
			Data:     qj.Data,
			Attempts: qj.Attempts,
		}
		start := time.Now()
		err := jp.Client.Job.Post(qj.Name, &qj.Id, params)
		go metrics.Time("post_job.latency", time.Since(start))
		go metrics.Time(fmt.Sprintf("post_job.%s.latency", qj.Name), time.Since(start))
		if err == nil {
			go metrics.Increment(fmt.Sprintf("post_job.%s.accepted", qj.Name))
			return nil
		} else {
			switch aerr := err.(type) {
			case *rest.Error:
				if aerr.Id == "service_unavailable" {
					go metrics.Increment("post_job.unavailable")
					time.Sleep(time.Duration(1<<i*SleepFactor) * time.Millisecond)
					continue
				}
				go metrics.Increment("dequeue.post_job.error")
				return err
			default:
				go func(err error) {
					if isTimeout(err) {
						metrics.Increment("dequeue.post_job.timeout")
					} else {
						metrics.Increment("dequeue.post_job.error_unknown")
					}
				}(err)
				return err
			}
		}
	}
	return nil
}

func waitForJob(qj *models.QueuedJob, failTimeout time.Duration) error {
	start := time.Now()
	// This is not going to change but we continually overwrite qj
	name := qj.Name
	idStr := qj.Id.String()

	currentAttemptCount := qj.Attempts
	queryCount := int64(0)
	if failTimeout <= 0 {
		failTimeout = DefaultTimeout
	}
	timeoutChan := time.After(failTimeout)
	for {
		select {
		case <-timeoutChan:
			go metrics.Increment(fmt.Sprintf("wait_for_job.%s.timeout", name))
			log.Printf("5 minutes elapsed, marking %s (type %s) as failed", idStr, name)
			err := HandleStatusCallback(qj.Id, name, models.StatusFailed, currentAttemptCount)
			go metrics.Increment(fmt.Sprintf("wait_for_job.%s.failed", name))
			log.Printf("job %s (type %s) timed out after %v", idStr, name, time.Since(start))
			if err == sql.ErrNoRows {
				// Attempted to decrement the failed count, but couldn't do so;
				// we assume another thread got here before we did.
				return nil
			}
			if err != nil {
				log.Printf("error marking job %s as failed: %s\n", idStr, err.Error())
				go metrics.Increment(fmt.Sprintf("wait_for_job.%s.failed.error", name))
			}
			return err
		default:
			getStart := time.Now()
			qj, err := queued_jobs.Get(qj.Id)
			queryCount++
			go metrics.Time("wait_for_job.get.latency", time.Since(getStart))
			if err == queued_jobs.ErrNotFound {
				// inserted this job into archived_jobs. nothing to do!
				go func(name string, start time.Time, idStr string, queryCount int64) {
					metrics.Increment(fmt.Sprintf("wait_for_job.%s.archived", name))
					metrics.Increment("wait_for_job.archived")
					metrics.Time(fmt.Sprintf("wait_for_job.%s.latency", name), time.Since(start))
					metrics.Measure(fmt.Sprintf("wait_for_job.%s.queries", name), queryCount)
					duration := time.Since(start)
					// Default print method has too many decimals
					roundDuration := duration - duration%(time.Millisecond/10)
					log.Printf("job %s (type %s) completed after %s", idStr, name, roundDuration)
				}(name, start, idStr, queryCount)
				return nil
			} else if err != nil {
				continue
			}
			if qj.Attempts < currentAttemptCount {
				// Another thread decremented the attempt count and re-queued
				// the job, we're done.
				go metrics.Time(fmt.Sprintf("wait_for_job.%s.latency", name), time.Since(start))
				go metrics.Increment(fmt.Sprintf("wait_for_job.%s.attempt_count_decremented", name))
				log.Printf("job %s (type %s) failed after %v, retrying", idStr, name, time.Since(start))
				return nil
			}
		}
	}
}