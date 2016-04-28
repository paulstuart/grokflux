package grokflux

import (
	"fmt"
	"log"
	"time"

	client "github.com/influxdata/influxdb/client/v2"
)

const (
	// DefaultBatchSize sets how many points to batch to influx
	DefaultBatchSize = 64
	// DefaultQueueSize sets the size of the send queue
	DefaultQueueSize = 8192
	// DefaultPeriod sets how many seconds to flush the queu
	DefaultPeriod = 60

	pingTimeout = 10
)

// Sender maps the arguents needed to write an InfluxDB datapoint
type Sender func(string, map[string]string, map[string]interface{}, time.Time) error

// InfluxConfig defines connection requirements
type InfluxConfig struct {
	Host     string
	Username string
	Password string
	Port     int
	Batch    client.BatchPointsConfig
}

// Make sure the database specified exists
func dbCheck(conn client.Client, database string) error {
	q := client.Query{Command: "show databases"}
	resp, err := conn.Query(q)
	if err != nil {
		return err
	}

	for _, r := range resp.Results {
		for _, s := range r.Series {
			for _, v := range s.Values {
				for _, d := range v {
					if d.(string) == database {
						return nil
					}
				}
			}
		}
	}
	return fmt.Errorf("database %s does not exist", database)
}

// NewSender returns a function that accepts datapoints for sending to influx
func (cfg *InfluxConfig) NewSender(batchSize, queueSize, period int) (Sender, error) {
	url := fmt.Sprintf("http://%s:%d", cfg.Host, cfg.Port)

	conf := client.HTTPConfig{
		Addr:     url,
		Username: cfg.Username,
		Password: cfg.Password,
	}

	conn, err := client.NewHTTPClient(conf)
	if err != nil {
		return nil, err
	}

	_, _, err = conn.Ping(time.Second * pingTimeout)
	if err != nil {
		return nil, fmt.Errorf("can't ping host: %s", cfg.Host)
	}

	// make sure we have a valid database
	if err := dbCheck(conn, cfg.Batch.Database); err != nil {
		return nil, err
	}

	// apply defaults as needed
	if queueSize <= 0 {
		queueSize = DefaultQueueSize
	}
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}
	if period <= 0 {
		period = DefaultPeriod
	}

	pts := make(chan *client.Point, queueSize)

	bp, err := client.NewBatchPoints(cfg.Batch)
	if err != nil {
		return nil, err
	}

	go func() {
		tick := time.Tick(time.Duration(period) * time.Second)
		count := 0
		for {
			select {
			case <-tick:
				if len(bp.Points()) == 0 {
					continue
				}
			case p := <-pts:
				bp.AddPoint(p)
				count++
				if count < batchSize {
					continue
				}
			}
			for {
				if err := conn.Write(bp); err != nil {
					log.Println("influxdb write error:", err)
					continue
				}
				bp, err = client.NewBatchPoints(cfg.Batch)
				if err != nil {
					log.Println("influxdb batchpoints error:", err)
				}
				count = 0
				break
			}
		}
	}()

	return func(key string, tags map[string]string, val map[string]interface{}, ts time.Time) error {
		pt, err := client.NewPoint(key, tags, val, ts)
		if err != nil {
			return err
		}
		pts <- pt
		return nil
	}, nil
}
