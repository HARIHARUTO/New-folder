# LEARNINGS

## 1) Queue state interpretation is an operations skill

Seeing all task states as `None` looked like a broken DAG at first, but it was actually queued-run behavior.  
With `max_active_runs=1`, extra manual triggers stack up and wait.  
I now check `dags list-runs` state first (`queued`/`running`/`success`) before assuming task logic is broken.

## 2) Retries only help if the platform stays stable during execution

I saw `ingest_aqi_data` fail with `SIGTERM`, but root cause was service restart during task runtime, not code failure.  
Airflow marked it `up_for_retry`, and later runs succeeded without code changes.  
This taught me to separate application exceptions from orchestration interruptions.

## 3) Time bucketing is mandatory for multi-source time-series joins

AQI and weather datasets rarely align on exact timestamp values.  
Joining on raw timestamps produced weak match rates; truncating to hour fixed alignment immediately.  
For this class of pipeline, timestamp normalization is not optional.

## 4) Data quality controls must be explicit and upstream

Malformed source values can pass ingestion silently and break analytics later.  
Quarantine plus dbt tests gave immediate visibility into bad payloads and protected marts.  
Without this gate, the dashboard could look healthy while calculations are wrong.

## 5) One-run-at-a-time discipline improves debugging quality

Triggering multiple manual runs created queue noise and made root-cause analysis harder.  
A single trigger, observe, then trigger again pattern gave clean signals from scheduler logs and task states.  
This is the fastest way to debug orchestration issues locally.
