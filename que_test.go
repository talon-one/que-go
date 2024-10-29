package que

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func getConnectionStringFromEnv(t testing.TB) string {
	dbUser := "talon"
	if dbUserEnv, exists := os.LookupEnv("TALON_DB_USER"); exists {
		dbUser = dbUserEnv
	}
	dbPassword := "talon.one.9000"
	if dbPasswordEnv, exists := os.LookupEnv("TALON_DB_PASSWORD"); exists {
		dbPassword = dbPasswordEnv
	}
	dbHost := "localhost"
	if dbHostEnv, exists := os.LookupEnv("TALON_DB_HOST"); exists {
		dbHost = dbHostEnv
	}
	dbPort := "5433"
	if dbPortEnv, exists := os.LookupEnv("TALON_DB_PORT"); exists {
		dbPort = dbPortEnv
	}
	dbName := "talon"
	if dbNameEnv, exists := os.LookupEnv("TALON_DB_NAME"); exists {
		dbName = dbNameEnv
	}

	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s", dbUser, dbPassword, dbHost, dbPort, dbName)
}

func openTestClientMaxConns(t testing.TB, maxConnections int32) *Client {

	connPoolConfig, err := pgxpool.ParseConfig(getConnectionStringFromEnv(t))
	if err != nil {
		t.Fatal(err)
	}
	connPoolConfig.MaxConns = maxConnections
	connPoolConfig.AfterConnect = PrepareStatements

	pool, err := pgxpool.NewWithConfig(context.Background(), connPoolConfig)
	if err != nil {
		t.Fatal(err)
	}
	return NewClient(pool)
}

func openTestClient(t testing.TB) *Client {
	return openTestClientMaxConns(t, 5)
}

func truncateAndClose(pool *pgxpool.Pool) {
	if _, err := pool.Exec(context.Background(), "TRUNCATE TABLE que_jobs"); err != nil {
		panic(err)
	}
	pool.Close()
}

func findOneJob(q queryable) (*Job, error) {
	findSQL := `
	SELECT priority, run_at, job_id, job_class, args, error_count, last_error, queue
	FROM que_jobs LIMIT 1`

	j := &Job{}
	err := q.QueryRow(context.Background(), findSQL).Scan(
		&j.Priority,
		&j.RunAt,
		&j.ID,
		&j.Type,
		&j.Args,
		&j.ErrorCount,
		&j.LastError,
		&j.Queue,
	)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return j, nil
}
