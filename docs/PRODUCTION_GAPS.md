# PRODUCTION_GAPS

## Current Gaps

- Secrets are file-based in local Docker.
  - Production risk: accidental key exposure (for example, credential file leakage) can compromise cloud resources.
- Some dbt paths still run full refresh behavior.
  - Production risk: cost and runtime scale poorly as row counts grow.
- Single local scheduler/executor path.
  - Production risk: limited throughput and slower recovery under failures.
- Manual monitoring for anomalies.
  - Production risk: source outages or data drift can go undetected until users report dashboard issues.

## Production Upgrades

## Gaps, Consequences, and Fixes

| Gap | What breaks in production | Fix |
|---|---|---|
| File-based credentials in repo/runtime | Credential leakage risk and weak rotation controls | GCP Secret Manager + workload identity |
| Full refresh dbt patterns | Long runtimes and high BigQuery scan cost at scale | Incremental models with `is_incremental()` and bounded lookback |
| Single local executor flow | Low concurrency and slower backlog drain after incidents | Celery/Kubernetes executor with worker autoscaling |
| Manual anomaly checks | Missing-city/freshness failures can persist for hours or days unnoticed | Automated alerts on freshness, quarantine spikes, row-volume anomalies |
