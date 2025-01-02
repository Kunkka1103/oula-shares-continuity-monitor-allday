package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
)

func getMaxEpoch(db *sql.DB, project string) (int, error) {
	query := fmt.Sprintf("SELECT max(max_epoch) FROM distributor WHERE project='%s';", project)
	var maxEpoch sql.NullInt64
	if err := db.QueryRow(query).Scan(&maxEpoch); err != nil {
		return 0, err
	}
	if !maxEpoch.Valid {
		return 0, fmt.Errorf("no data found for project: %s", project)
	}
	return int(maxEpoch.Int64), nil
}

func checkContinuity(db *sql.DB, tableName string, startEpoch int) (bool, error) {
	query := fmt.Sprintf("SELECT DISTINCT(epoch_number) FROM %s WHERE epoch_number >= $1 ORDER BY epoch_number;", tableName)
	rows, err := db.Query(query, startEpoch)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	var previousEpoch *int
	for rows.Next() {
		var currentEpoch int
		if err := rows.Scan(&currentEpoch); err != nil {
			return false, err
		}
		if previousEpoch != nil && currentEpoch != *previousEpoch+1 {
			return false, nil
		}
		previousEpoch = &currentEpoch
	}
	return true, nil
}

func pushMetric(pushgateway, job string, metricName string, value float64) error {
	gauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: metricName,
	})
	gauge.Set(value)
	if err := push.New(pushgateway, job).
		Collector(gauge).
		Push(); err != nil {
		return err
	}
	return nil
}

func main() {
	oulaDSN := flag.String("oula-dsn", "", "DSN for the Oula database")
	aleoDSN := flag.String("aleo-dsn", "", "DSN for the Aleo database")
	quaiDSN := flag.String("quai-dsn", "", "DSN for the Quai database")
	pushgateway := flag.String("push-url", "http://127.0.0.1:9091", "Pushgateway URL")
	interval := flag.Int("interval", 10, "Check interval in minutes")

	flag.Parse()

	oulaDB, err := sql.Open("postgres", *oulaDSN)
	if err != nil {
		log.Fatalf("Failed to connect to Oula database: %v", err)
	}
	defer oulaDB.Close()

	aleoDB, err := sql.Open("postgres", *aleoDSN)
	if err != nil {
		log.Fatalf("Failed to connect to Aleo database: %v", err)
	}
	defer aleoDB.Close()

	quaiDB, err := sql.Open("postgres", *quaiDSN)
	if err != nil {
		log.Fatalf("Failed to connect to Quai database: %v", err)
	}
	defer quaiDB.Close()

	ticker := time.NewTicker(time.Duration(*interval) * time.Minute)
	defer ticker.Stop()

	for {
		aleoStart, err := getMaxEpoch(oulaDB, "ALEO")
		if err != nil {
			log.Printf("Error fetching Aleo max epoch: %v", err)
			time.Sleep(time.Duration(*interval) * time.Minute)
			continue
		}

		quaiStart, err := getMaxEpoch(oulaDB, "Quai_Garden")
		if err != nil {
			log.Printf("Error fetching Quai max epoch: %v", err)
			time.Sleep(time.Duration(*interval) * time.Minute)
			continue
		}

		aleoContinuous, err := checkContinuity(aleoDB, "user_shares", aleoStart)
		if err != nil {
			log.Printf("Error checking Aleo continuity: %v", err)
			time.Sleep(time.Duration(*interval) * time.Minute)
			continue
		}

		quaiContinuous, err := checkContinuity(quaiDB, "shares", quaiStart)
		if err != nil {
			log.Printf("Error checking Quai continuity: %v", err)
			time.Sleep(time.Duration(*interval) * time.Minute)
			continue
		}

		if err := pushMetric(*pushgateway, "aleo", "aleo_shares_continuity_status", boolToFloat64(aleoContinuous)); err != nil {
			log.Printf("Error pushing Aleo metric: %v", err)
		}

		if err := pushMetric(*pushgateway, "quai", "quai_shares_continuity_status", boolToFloat64(quaiContinuous)); err != nil {
			log.Printf("Error pushing Quai metric: %v", err)
		}

		<-ticker.C
	}
}

func boolToFloat64(b bool) float64 {
	if b {
		return 1.0
	}
	return 0.0
}
