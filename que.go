package que

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Job is a single unit of work for Que to perform.
type Job struct {
	// ID is the unique database ID of the Job. It is ignored on job creation.
	ID int64

	// Queue is the name of the queue. It defaults to the empty queue "".
	Queue string

	// Priority is the priority of the Job. The default priority is 100, and a
	// lower number means a higher priority. A priority of 5 would be very
	// important.
	Priority int16

	// RunAt is the time that this job should be executed. It defaults to now(),
	// meaning the job will execute immediately. Set it to a value in the future
	// to delay a job's execution.
	RunAt time.Time

	// Type corresponds to the Ruby job_class. If you are interoperating with
	// Ruby, you should pick suitable Ruby class names (such as MyJob).
	Type string

	// Args must be the bytes of a valid JSON string
	Args []byte

	// Delay function returns the amount of seconds to wait as a function of
	// the number of retries.
	DelayFunction func(int32) int

	// ErrorCount is the number of times this job has attempted to run, but
	// failed with an error. It is ignored on job creation.
	ErrorCount int32

	// LastError is the error message or stack trace from the last time the job
	// failed. It is ignored on job creation.
	LastError pgtype.Text

	mu      sync.Mutex
	deleted bool

	delayFunction func(int32) int
	pool          *pgxpool.Pool
	conn          *pgxpool.Conn
}

// DelayFunction returns the amount of seconds to wait as a function of
// the number of retries.
var DelayFunction func(int32) int
var defaultDelayFunction = func(errorCount int32) int {
	return intPow(int(errorCount), 4) + 3
}

// Conn returns the pgx connection that this job is locked to. You may initiate
// transactions on this connection or use it as you please until you call
// Done(). At that point, this conn will be returned to the pool and it is
// unsafe to keep using it. This function will return nil if the Job's
// connection has already been released with Done().
func (j *Job) Conn() queryable {
	j.mu.Lock()
	defer j.mu.Unlock()

	return j.conn
}

// Delete marks this job as complete by deleting it form the database.
//
// You must also later call Done() to return this job's database connection to
// the pool.
func (j *Job) Delete(ctx context.Context) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.deleted {
		return nil
	}

	_, err := j.conn.Exec(ctx, "que_destroy_job", j.Queue, j.Priority, j.RunAt, j.ID)
	if err != nil {
		return err
	}

	j.deleted = true
	return nil
}

// Done releases the Postgres advisory lock on the job and returns the database
// connection to the pool.
func (j *Job) Done(ctx context.Context) {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.conn == nil || j.pool == nil {
		// already marked as done
		return
	}

	var ok bool
	// Swallow this error because we don't want an unlock failure to cause work to
	// stop.
	_ = j.conn.QueryRow(ctx, "que_unlock_job", j.ID).Scan(&ok)

	j.conn.Release()
	j.pool = nil
	j.conn = nil
}

// Error marks the job as failed and schedules it to be reworked. An error
// message or backtrace can be provided as msg, which will be saved on the job.
// It will also increase the error count.
//
// You must also later call Done() to return this job's database connection to
// the pool.
func (j *Job) Error(ctx context.Context, msg string) error {
	errorCount := j.ErrorCount + 1

	var delay int
	if j.delayFunction == nil {
		delay = defaultDelayFunction(j.ErrorCount)
	} else {
		delay = j.delayFunction(j.ErrorCount)
	}

	_, err := j.conn.Exec(ctx, "que_set_error", errorCount, delay, msg, j.Queue, j.Priority, j.RunAt, j.ID)
	if err != nil {
		return err
	}
	return nil
}

// Client is a Que client that can add jobs to the queue and remove jobs from
// the queue.
type Client struct {
	pool *pgxpool.Pool

	// TODO: add a way to specify default queueing options
}

// NewClient creates a new Client that uses the pgx pool.
func NewClient(pool *pgxpool.Pool) *Client {
	return &Client{pool: pool}
}

// ErrMissingType is returned when you attempt to enqueue a job with no Type
// specified.
var ErrMissingType = errors.New("job type must be specified")

// Enqueue method appends a job to the queue adhering to the transactional flow of the Talon service.
func (c *Client) Enqueue(j *Job) error {
	return execEnqueue(j, c.pool)
}

// EnqueueInTx adds a job to the queue within the scope of the transaction tx.
// This allows you to guarantee that an enqueued job will either be committed or
// rolled back atomically with other changes in the course of this transaction.
//
// It is the caller's responsibility to Commit or Rollback the transaction after
// this function is called.
func (c *Client) EnqueueInTx(j *Job, txn queryable) error {
	return execEnqueue(j, txn)
}

func execEnqueue(j *Job, txn queryable) error {
	if j.Type == "" {
		return ErrMissingType
	}

	queue := &pgtype.Text{
		String: j.Queue,
		Valid:  j.Queue != "",
	}

	priority := &pgtype.Int2{
		Int16: j.Priority,
		Valid: j.Priority != 0,
	}

	runAt := &pgtype.Timestamptz{
		Time:  j.RunAt,
		Valid: !j.RunAt.IsZero(),
	}

	_, err := txn.Exec(context.Background(), sqlInsertJob, queue, priority, runAt, j.Type, j.Args)
	return err
}

type queryable interface {
	Exec(ctx context.Context, sql string, arguments ...any) (commandTag pgconn.CommandTag, err error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Maximum number of loop iterations in LockJob before giving up.  This is to
// avoid looping forever in case something is wrong.
const maxLockJobAttempts = 10

// Returned by LockJob if a job could not be retrieved from the queue after
// several attempts because of concurrently running transactions.  This error
// should not be returned unless the queue is under extremely heavy
// concurrency.
var ErrAgain = errors.New("maximum number of LockJob attempts reached")

// TODO: consider an alternate Enqueue func that also returns the newly
// enqueued Job struct. The query sqlInsertJobAndReturn was already written for
// this.

// LockJob attempts to retrieve a Job from the database in the specified queue.
// If a job is found, a session-level Postgres advisory lock is created for the
// Job's ID. If no job is found, nil will be returned instead of an error.
//
// Because Que uses session-level advisory locks, we have to hold the
// same connection throughout the process of getting a job, working it,
// deleting it, and removing the lock.
//
// After the Job has been worked, you must call either Done() or Error() on it
// in order to return the database connection to the pool and remove the lock.
func (c *Client) LockJob(ctx context.Context, queue string) (*Job, error) {
	conn, err := c.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}

	j := Job{pool: c.pool, conn: conn, delayFunction: DelayFunction}

	for i := 0; i < maxLockJobAttempts; i++ {
		err = conn.QueryRow(ctx, "que_lock_job", queue).Scan(
			&j.Queue,
			&j.Priority,
			&j.RunAt,
			&j.ID,
			&j.Type,
			&j.Args,
			&j.ErrorCount,
		)
		if err != nil {
			conn.Release()
			if err == pgx.ErrNoRows {
				return nil, nil
			}
			return nil, err
		}

		// Deal with race condition. Explanation from the Ruby Que gem:
		//
		// Edge case: It's possible for the lock_job query to have
		// grabbed a job that's already been worked, if it took its MVCC
		// snapshot while the job was processing, but didn't attempt the
		// advisory lock until it was finished. Since we have the lock, a
		// previous worker would have deleted it by now, so we just
		// double check that it still exists before working it.
		//
		// Note that there is currently no spec for this behavior, since
		// I'm not sure how to reliably commit a transaction that deletes
		// the job in a separate thread between lock_job and check_job.
		var ok bool
		err = conn.QueryRow(ctx, "que_check_job", j.Queue, j.Priority, j.RunAt, j.ID).Scan(&ok)
		if err == nil {
			return &j, nil
		} else if err == pgx.ErrNoRows {
			// Encountered job race condition; start over from the beginning.
			// We're still holding the advisory lock, though, so we need to
			// release it before resuming.  Otherwise we leak the lock,
			// eventually causing the server to run out of locks.
			//
			// Also swallow the possible error, exactly like in Done.
			_ = conn.QueryRow(ctx, "que_unlock_job", j.ID).Scan(&ok)
			continue
		} else {
			conn.Release()
			return nil, err
		}
	}
	conn.Release()
	return nil, ErrAgain
}

var preparedStatements = map[string]string{
	"que_check_job":   sqlCheckJob,
	"que_destroy_job": sqlDeleteJob,
	"que_insert_job":  sqlInsertJob,
	"que_lock_job":    sqlLockJob,
	"que_set_error":   sqlSetError,
	"que_unlock_job":  sqlUnlockJob,
}

func PrepareStatements(ctx context.Context, conn *pgx.Conn) error {
	for name, sql := range preparedStatements {
		if _, err := conn.Prepare(ctx, name, sql); err != nil {
			return err
		}
	}
	return nil
}
