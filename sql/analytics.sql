-- Example analyst queries in Trino (catalog "lake").  trino --server localhost:8080 --catalog lake -f sql/analytics.sql

-- Devices with a low-battery trend in the last 24 hours
SELECT device_id, site, round(avg(avg_value), 1) AS avg_battery, min(min_value) AS lowest
FROM gold.device_metric_hourly
WHERE metric = 'battery_pct' AND hour >= current_timestamp - INTERVAL '24' HOUR
GROUP BY device_id, site
HAVING min(min_value) < 15
ORDER BY lowest;

-- Signal quality by site (p95 RSSI per hour)
SELECT site, hour, round(avg(p95_value), 1) AS p95_rssi_dbm
FROM gold.device_metric_hourly
WHERE metric = 'signal_rssi_dbm'
GROUP BY site, hour
ORDER BY hour DESC, site
LIMIT 50;

-- Data quality: what is being rejected and why
SELECT reason, count(*) AS records, min(ingest_ts) AS first_seen, max(ingest_ts) AS last_seen
FROM quarantine.telemetry_rejects
GROUP BY reason
ORDER BY records DESC;

-- Ingestion lag: event time vs ingest time in silver
SELECT approx_percentile(date_diff('second', event_ts, ingest_ts), ARRAY[0.5, 0.95, 0.99]) AS lag_seconds_p50_p95_p99
FROM silver.telemetry;

-- Time travel: row count of silver as of the previous snapshot
SELECT snapshot_id, committed_at, operation FROM silver."telemetry$snapshots" ORDER BY committed_at DESC LIMIT 5;
