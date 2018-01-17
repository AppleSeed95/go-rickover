package test_queued_jobs

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Shyp/go-dberror"
	"github.com/Shyp/go-types"
	"github.com/Shyp/rickover/models"
	"github.com/Shyp/rickover/models/jobs"
	"github.com/Shyp/rickover/models/queued_jobs"
	"github.com/Shyp/rickover/services"
	"github.com/Shyp/rickover/test"
	"github.com/Shyp/rickover/test/factory"
)

var empty = json.RawMessage([]byte("{}"))

var sampleJob = models.Job{
	Name:             "echo",
	DeliveryStrategy: models.StrategyAtLeastOnce,
	Attempts:         3,
	Concurrency:      1,
}

func TestEnqueue(t *testing.T) {
	defer test.TearDown(t)
	qj := factory.CreateQueuedJob(t, factory.EmptyData)
	test.AssertEquals(t, qj.ID.String(), "job_6740b44e-13b9-475d-af06-979627e0e0d6")
	test.AssertEquals(t, qj.Name, "echo")
	test.AssertEquals(t, qj.Attempts, uint8(7))
	test.AssertEquals(t, qj.Status, models.StatusQueued)

	diff := time.Since(qj.RunAfter)
	test.Assert(t, diff < 20*time.Millisecond, "")

	diff = time.Since(qj.CreatedAt)
	test.Assert(t, diff < 20*time.Millisecond, "")

	diff = time.Since(qj.UpdatedAt)
	test.Assert(t, diff < 20*time.Millisecond, "")
}

func TestEnqueueNoData(t *testing.T) {
	test.SetUp(t)
	defer test.TearDown(t)
	_, err := jobs.Create(sampleJob)
	test.AssertNotError(t, err, "")

	expiresAt := types.NullTime{Valid: false}
	runAfter := time.Now().UTC()

	_, err = queued_jobs.Enqueue(factory.JobId, "echo", runAfter, expiresAt, []byte{})
	test.AssertError(t, err, "")
	switch terr := err.(type) {
	case *dberror.Error:
		test.AssertEquals(t, terr.Message, "Invalid input syntax for type json")
	default:
		t.Fatalf("Expected a dberror, got %#v", terr)
	}
}

func TestEnqueueJobExists(t *testing.T) {
	test.SetUp(t)
	defer test.TearDown(t)
	_, err := jobs.Create(sampleJob)
	test.AssertNotError(t, err, "")

	expiresAt := types.NullTime{Valid: false}
	runAfter := time.Now().UTC()

	_, err = queued_jobs.Enqueue(factory.JobId, "echo", runAfter, expiresAt, empty)
	test.AssertNotError(t, err, "")
	_, err = queued_jobs.Enqueue(factory.JobId, "echo", runAfter, expiresAt, empty)
	test.AssertError(t, err, "")
	switch terr := err.(type) {
	case *dberror.Error:
		test.AssertEquals(t, terr.Code, dberror.CodeUniqueViolation)
		test.AssertEquals(t, terr.Column, "id")
		test.AssertEquals(t, terr.Table, "queued_jobs")
		test.AssertEquals(t, terr.Message,
			fmt.Sprintf("A id already exists with this value (6740b44e-13b9-475d-af06-979627e0e0d6)"))
	default:
		t.Fatalf("Expected a dberror, got %#v", terr)
	}
}

func TestEnqueueUnknownJobTypeErrNoRows(t *testing.T) {
	t.Parallel()
	test.SetUp(t)

	expiresAt := types.NullTime{Valid: false}
	runAfter := time.Now().UTC()
	_, err := queued_jobs.Enqueue(factory.JobId, "unknownJob", runAfter, expiresAt, empty)
	test.AssertError(t, err, "")
	test.AssertEquals(t, err.Error(), "Job type unknownJob does not exist or the job with that id has already been archived")
}

func TestEnqueueWithExistingArchivedJobFails(t *testing.T) {
	qj := factory.CreateQueuedJob(t, factory.EmptyData)
	defer test.TearDown(t)
	err := services.HandleStatusCallback(qj.ID, qj.Name, models.StatusSucceeded, qj.Attempts, true)
	test.AssertNotError(t, err, "")
	expiresAt := types.NullTime{Valid: false}
	runAfter := time.Now().UTC()
	_, err = queued_jobs.Enqueue(qj.ID, "echo", runAfter, expiresAt, empty)
	test.AssertError(t, err, "")
	test.AssertEquals(t, err.Error(), "Job type echo does not exist or the job with that id has already been archived")
}

func TestNonexistentReturnsErrNoRows(t *testing.T) {
	t.Parallel()
	test.SetUp(t)
	id, _ := types.NewPrefixUUID("job_a9173b65-7714-42b4-85f2-8336f6d12180")
	_, err := queued_jobs.Get(id)
	test.AssertEquals(t, err, queued_jobs.ErrNotFound)
}

func TestGetQueuedJob(t *testing.T) {
	defer test.TearDown(t)
	qj := factory.CreateQueuedJob(t, factory.EmptyData)
	gotQj, err := queued_jobs.Get(qj.ID)
	test.AssertNotError(t, err, "")
	test.AssertEquals(t, gotQj.ID.String(), "job_6740b44e-13b9-475d-af06-979627e0e0d6")
}

func TestDeleteQueuedJob(t *testing.T) {
	defer test.TearDown(t)
	qj := factory.CreateQueuedJob(t, factory.EmptyData)
	err := queued_jobs.Delete(qj.ID)
	test.AssertNotError(t, err, "")
}

func TestDeleteNonexistentJobReturnsErrNoRows(t *testing.T) {
	t.Parallel()
	test.SetUp(t)
	defer test.TearDown(t)
	err := queued_jobs.Delete(factory.RandomId("job_"))
	test.AssertEquals(t, err, queued_jobs.ErrNotFound)
}

func TestDataRoundtrip(t *testing.T) {
	test.SetUp(t)
	defer test.TearDown(t)
	_, err := jobs.Create(sampleJob)
	test.AssertNotError(t, err, "")

	type RemoteAccounts struct {
		ID string
	}

	type User struct {
		ID             string
		Balance        uint64
		CreatedAt      time.Time
		RemoteAccounts RemoteAccounts
		Pickups        []string
	}
	user := &User{
		ID:        "usr_123",
		Balance:   uint64(365),
		CreatedAt: time.Now().UTC(),
		RemoteAccounts: RemoteAccounts{
			ID: "rem_123",
		},
		Pickups: []string{"pik_123", "pik_234"},
	}

	expiresAt := types.NullTime{Valid: false}
	runAfter := time.Now().UTC()
	var d json.RawMessage
	d, err = json.Marshal(user)
	test.AssertNotError(t, err, "")
	qj, err := queued_jobs.Enqueue(factory.JobId, "echo", runAfter, expiresAt, d)
	test.AssertNotError(t, err, "")

	gotQj, err := queued_jobs.Get(qj.ID)
	test.AssertNotError(t, err, "")

	var u User
	err = json.Unmarshal(gotQj.Data, &u)
	test.AssertNotError(t, err, "expected to be able to convert data to a User object")
	test.AssertEquals(t, u.ID, "usr_123")
	test.AssertEquals(t, u.Balance, uint64(365))
	test.AssertEquals(t, u.RemoteAccounts.ID, "rem_123")
	test.AssertEquals(t, u.Pickups[0], "pik_123")
	test.AssertEquals(t, u.Pickups[1], "pik_234")
	test.AssertEquals(t, len(u.Pickups), 2)

	diff := time.Since(u.CreatedAt)
	test.Assert(t, diff < 30*time.Millisecond, "")
}

func TestAcquireReturnsCorrectValues(t *testing.T) {
	defer test.TearDown(t)
	factory.CreateQueuedJob(t, factory.EmptyData)

	gotQj, err := queued_jobs.Acquire(sampleJob.Name)
	test.AssertNotError(t, err, "")
	test.AssertEquals(t, gotQj.ID.String(), factory.JobId.String())
	test.AssertEquals(t, gotQj.Status, models.StatusInProgress)
}

func TestAcquireTwoThreads(t *testing.T) {
	var wg sync.WaitGroup
	defer test.TearDown(t)
	factory.CreateQueuedJob(t, factory.EmptyData)

	wg.Add(2)
	var err1, err2 error
	var gotQj1, gotQj2 *models.QueuedJob
	go func() {
		gotQj1, err1 = queued_jobs.Acquire(sampleJob.Name)
		wg.Done()
	}()
	go func() {
		gotQj2, err2 = queued_jobs.Acquire(sampleJob.Name)
		wg.Done()
	}()

	wg.Wait()
	test.Assert(t, err1 == sql.ErrNoRows || err2 == sql.ErrNoRows, "expected one error to be ErrNoRows")
	test.Assert(t, gotQj1 != nil || gotQj2 != nil, "expected one job to be acquired")
}

func TestAcquireDoesntGetFutureJob(t *testing.T) {
	test.SetUp(t)
	defer test.TearDown(t)

	_, err := jobs.Create(sampleJob)
	test.AssertNotError(t, err, "")

	expiresAt := types.NullTime{Valid: false}
	runAfter := time.Now().UTC().Add(20 * time.Millisecond)
	qj, err := queued_jobs.Enqueue(factory.JobId, "echo", runAfter, expiresAt, empty)
	test.AssertNotError(t, err, "")
	_, err = queued_jobs.Acquire(qj.Name)
	test.AssertEquals(t, err, sql.ErrNoRows)
}

func TestAcquireDoesntGetInProgressJob(t *testing.T) {
	test.SetUp(t)
	defer test.TearDown(t)

	_, err := jobs.Create(sampleJob)
	test.AssertNotError(t, err, "")

	expiresAt := types.NullTime{Valid: false}
	runAfter := time.Now().UTC()
	qj, err := queued_jobs.Enqueue(factory.JobId, "echo", runAfter, expiresAt, empty)
	test.AssertNotError(t, err, "")
	qj, err = queued_jobs.Acquire(qj.Name)
	test.AssertNotError(t, err, "")
	test.AssertDeepEquals(t, qj.ID, factory.JobId)

	_, err = queued_jobs.Acquire(qj.Name)
	test.AssertEquals(t, err, sql.ErrNoRows)
}

func TestDecrementDecrements(t *testing.T) {
	defer test.TearDown(t)
	qj := factory.CreateQueuedJob(t, factory.EmptyData)
	qj, err := queued_jobs.Decrement(qj.ID, 7, time.Now().Add(1*time.Minute))
	test.AssertNotError(t, err, "")
	test.AssertEquals(t, qj.Attempts, uint8(6))
	test.AssertBetween(t, int64(qj.RunAfter.Sub(time.Now())), int64(59*time.Second), int64(1*time.Minute))
}

func TestDecrementErrNoRowsWrongAttempts(t *testing.T) {
	defer test.TearDown(t)
	qj := factory.CreateQueuedJob(t, factory.EmptyData)
	_, err := queued_jobs.Decrement(qj.ID, 1, time.Now())
	test.AssertEquals(t, err, sql.ErrNoRows)
}

func TestCountAll(t *testing.T) {
	test.SetUp(t)
	defer test.TearDown(t)
	allCount, readyCount, err := queued_jobs.CountReadyAndAll()
	test.AssertNotError(t, err, "")
	test.AssertEquals(t, allCount, 0)
	test.AssertEquals(t, readyCount, 0)

	factory.CreateRandomQueuedJob(t, factory.EmptyData)
	factory.CreateRandomQueuedJob(t, factory.EmptyData)
	factory.CreateRandomQueuedJob(t, factory.EmptyData)
	allCount, readyCount, err = queued_jobs.CountReadyAndAll()
	test.AssertNotError(t, err, "")
	test.AssertEquals(t, allCount, 3)
	test.AssertEquals(t, readyCount, 3)
}

func TestCountByStatus(t *testing.T) {
	defer test.TearDown(t)
	factory.CreateRandomQueuedJob(t, factory.EmptyData)
	factory.CreateRandomQueuedJob(t, factory.EmptyData)
	factory.CreateRandomQueuedJob(t, factory.EmptyData)
	factory.CreateAtMostOnceJob(t, factory.EmptyData)
	m, err := queued_jobs.GetCountsByStatus(models.StatusQueued)
	test.AssertNotError(t, err, "")
	test.AssertEquals(t, len(m), 2)
	test.AssertEquals(t, m["echo"], int64(3))
	test.AssertEquals(t, m["at-most-once"], int64(1))
}

func TestOldInProgress(t *testing.T) {
	defer test.TearDown(t)
	qj1 := factory.CreateRandomQueuedJob(t, factory.EmptyData)
	qj2 := factory.CreateRandomQueuedJob(t, factory.EmptyData)
	_, err := queued_jobs.Acquire(qj1.Name)
	test.AssertNotError(t, err, "")
	_, err = queued_jobs.Acquire(qj1.Name)
	test.AssertNotError(t, err, "")
	jobs, err := queued_jobs.GetOldInProgressJobs(time.Now().UTC().Add(40 * time.Millisecond))
	test.AssertNotError(t, err, "")
	test.AssertEquals(t, len(jobs), 2)
	if jobs[0].ID.String() == qj1.ID.String() {
		test.AssertEquals(t, jobs[1].ID.String(), qj2.ID.String())
	} else {
		test.AssertEquals(t, jobs[1].ID.String(), qj1.ID.String())
	}
	jobs, err = queued_jobs.GetOldInProgressJobs(time.Now().UTC().Add(-1 * time.Second))
	test.AssertNotError(t, err, "")
	test.AssertEquals(t, len(jobs), 0)
}
