package main

import (
	cryptorand "crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Prometheus metrics
var (
	opsTotalDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "db_ops_duration_seconds_total",
			Help:    "Histogram of the duration of database operations",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"db_type", "query_type"},
	)
	opsDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "db_ops_duration_seconds",
			Help:    "Histogram of the duration of single database operations",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"db_type", "query_type"},
	)
	opsProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "db_ops_processed_total",
			Help: "Total number of database operations",
		},
		[]string{"db_type", "query_type"},
	)
	queryErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "db_query_errors_total",
			Help: "Total number of database query errors",
		},
		[]string{"db_type", "query_type"},
	)
)

func init() {
	prometheus.MustRegister(opsTotalDuration)
	prometheus.MustRegister(opsProcessed)
	prometheus.MustRegister(opsDuration)
	prometheus.MustRegister(queryErrors)
}

func generateRandomString(length int) (string, error) {
	numBytes := length / 2
	bytes := make([]byte, numBytes)
	_, err := cryptorand.Read(bytes)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

func initializeDB(db *sql.DB, dbType string) {
	var query string
	if dbType == "sqlite3" {
		query = `CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT)`
	} else if dbType == "mysql" {
		query = `CREATE TABLE IF NOT EXISTS test (id INT AUTO_INCREMENT PRIMARY KEY, data VARCHAR(255))`
	} else if dbType == "postgres" {
		query = `CREATE TABLE IF NOT EXISTS test (id SERIAL PRIMARY KEY, data TEXT)`
	} else {
		log.Fatalf("Unsupported database type: %s", dbType)
	}

	_, err := db.Exec(query)
	if err != nil {
		log.Fatalf("Error initializing database: %v", err)
	}
	fmt.Printf("[%s] Database initialized successfully.\n", dbType)
}

func performInserts(db *sql.DB, numInserts int, dbType string) time.Duration {
	var query string
	if dbType == "sqlite3" || dbType == "postgres" {
		query = "INSERT INTO test (data) VALUES ($1)"
	} else if dbType == "mysql" {
		query = "INSERT INTO test (data) VALUES (?)"
	} else {
		log.Fatalf("Unsupported database type: %s", dbType)
	}

	randomString, err := generateRandomString(64)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("[%s] Starting %d inserts...\n", dbType, numInserts)
	start := time.Now()
	for i := 0; i < numInserts; i++ {
		timer := prometheus.NewTimer(opsDuration.WithLabelValues(dbType, "insert"))
		r := rand.Intn(10)
		time.Sleep(time.Duration(r) * time.Microsecond)
		_, err := db.Exec(query, randomString)
		if err != nil {
			queryErrors.WithLabelValues(dbType, "insert").Inc()
			log.Fatalf("[%s] Error performing insert: %v", dbType, err)
		}
		timer.ObserveDuration()
		opsProcessed.WithLabelValues(dbType, "insert").Inc()
	}
	duration := time.Since(start)
	opsTotalDuration.WithLabelValues(dbType, "insert").Observe(duration.Seconds())
	fmt.Printf("[%s] Finished inserts in %.4f seconds\n", dbType, duration.Seconds())
	return duration
}

func performReads(db *sql.DB, numReads int, dbType string) time.Duration {
	var query string
	if dbType == "sqlite3" || dbType == "postgres" {
		query = "SELECT * FROM test ORDER BY RANDOM()"
	} else if dbType == "mysql" {
		query = "SELECT * FROM test ORDER BY RAND()"
	} else {
		log.Fatalf("Unsupported database type: %s", dbType)
	}

	fmt.Printf("[%s] Starting %d reads...\n", dbType, numReads)
	start := time.Now()
	for i := 0; i < numReads; i++ {
		timer := prometheus.NewTimer(opsDuration.WithLabelValues(dbType, "read"))
		rows, err := db.Query(query)
		if err != nil {
			queryErrors.WithLabelValues(dbType, "read").Inc()
			log.Fatalf("[%s] Error performing read: %v", dbType, err)
		}
		rows.Close()
		timer.ObserveDuration()
		opsProcessed.WithLabelValues(dbType, "read").Inc()
	}
	duration := time.Since(start)
	opsTotalDuration.WithLabelValues(dbType, "read").Observe(duration.Seconds())
	fmt.Printf("[%s] Finished reads in %.4f seconds\n", dbType, duration.Seconds())
	return duration
}

func cleanupDB(db *sql.DB, dbType string) {
	fmt.Printf("[%s] Cleaning up database...\n", dbType)
	_, err := db.Exec("DROP TABLE IF EXISTS test")
	if err != nil {
		log.Fatalf("[%s] Error cleaning up database: %v", dbType, err)
	}
	fmt.Printf("[%s] Database cleaned up successfully.\n", dbType)
}

func benchmark(dbType, dataSourceName string, numInserts, numReads int) {
	fmt.Printf("[%s] Connecting to database...\n", dbType)
	db, err := sql.Open(dbType, dataSourceName)
	if err != nil {
		log.Fatalf("[%s] Error connecting to database: %v", dbType, err)
	}
	defer db.Close()

	fmt.Printf("[%s] Initializing database...\n", dbType)
	initializeDB(db, dbType)

	fmt.Printf("[%s] Performing inserts...\n", dbType)
	insertTime := performInserts(db, numInserts, dbType)

	fmt.Printf("[%s] Performing reads...\n", dbType)
	readTime := performReads(db, numReads, dbType)

	cleanupDB(db, dbType)

	fmt.Printf("[%s] Total time: %.4fs\n", dbType, insertTime.Seconds()+readTime.Seconds())
	fmt.Printf("[%s] Inserts time: %.4fs\n", dbType, insertTime.Seconds())
	fmt.Printf("[%s] Reads time: %.4fs\n", dbType, readTime.Seconds())
}

func main() {
	// Start the Prometheus metrics server
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Fatal(http.ListenAndServe(":8080", nil))
	}()

	for {
		r := rand.Intn(10000)
		fmt.Println(">>> Starting benchmark for SQLite...")
		benchmark("sqlite3", "file:test.db?cache=shared", r, r)

		fmt.Println(">>> Starting benchmark for MySQL...")
		benchmark("mysql", "user:password@tcp(localhost:3306)/test", r, r)

		fmt.Println(">>> Starting benchmark for PostgreSQL...")
		benchmark("postgres", "user=user password=password dbname=test sslmode=disable", r, r)

		fmt.Printf("\n>>> Waiting %d ms for the next cycle...\n", r)
		time.Sleep(time.Duration(r) * time.Millisecond)
	}
}
