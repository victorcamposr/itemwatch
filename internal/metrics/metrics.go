// Package metrics defines the Prometheus collectors.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Metrics groups every collector on its own registry.
type Metrics struct {
	Registry         *prometheus.Registry
	RowsProcessed    prometheus.Counter
	CursorLag        prometheus.Gauge
	BatchDuration    prometheus.Histogram
	Findings         *prometheus.CounterVec
	DupeScanDuration prometheus.Histogram
	DupesTracked     prometheus.Gauge
	Notifications    *prometheus.CounterVec
	Published        prometheus.Counter
	Leader           prometheus.Gauge
}

// New registers the collectors.
func New() *Metrics {
	m := &Metrics{
		Registry: prometheus.NewRegistry(),
		RowsProcessed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "itemwatch_ledger_rows_processed_total", Help: "Ledger rows read and evaluated.",
		}),
		CursorLag: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "itemwatch_ledger_lag_rows", Help: "Ledger rows written but not processed yet.",
		}),
		BatchDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "itemwatch_batch_duration_seconds", Help: "Time to read, evaluate and commit one ledger batch.",
			Buckets: prometheus.ExponentialBuckets(0.005, 2, 12),
		}),
		Findings: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "itemwatch_findings_total", Help: "Anomaly findings by rule and severity.",
		}, []string{"rule", "severity"}),
		DupeScanDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "itemwatch_dupe_scan_duration_seconds", Help: "Time to scan the item tables for duplicated hashes.",
			Buckets: prometheus.ExponentialBuckets(0.01, 2, 12),
		}),
		DupesTracked: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "itemwatch_dupes_tracked", Help: "Hashes currently held in more than one place, confirmed or not.",
		}),
		Notifications: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "itemwatch_notifications_total", Help: "Notification outcomes by channel.",
		}, []string{"channel", "result"}),
		Published: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "itemwatch_alerts_published_total", Help: "Alert generations appended to the stream.",
		}),
		Leader: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "itemwatch_leader", Help: "1 while this replica holds the ledger lock.",
		}),
	}
	m.Registry.MustRegister(m.RowsProcessed, m.CursorLag, m.BatchDuration, m.Findings, m.DupeScanDuration,
		m.DupesTracked, m.Notifications, m.Published, m.Leader,
		collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return m
}

// Delivery implements dispatch.Recorder.
func (m *Metrics) Delivery(channel, result string) {
	m.Notifications.WithLabelValues(channel, result).Inc()
}
