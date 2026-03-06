# ERRORS

## ERROR-001: AQI task terminated with SIGTERM

- Date: 2026-03-06
- Symptom: `ingest_aqi_data` moved to `up_for_retry`.
- Evidence: task log showed `Received SIGTERM. Terminating subprocesses.` while pagination was in progress.
- Root cause: scheduler/webserver restart interrupted running task.
- Fix: avoid restarts during active runs, allow retry flow to continue.
- Result: subsequent runs completed successfully.

## ERROR-002: Tasks showed `None` for all states

- Date: 2026-03-06
- Symptom: `states-for-dag-run` returned `None` on every task.
- Root cause: run was still queued; scheduler had not started task instances.
- Fix: check `dags list-runs` first, wait for scheduler pickup, avoid repeated triggers.

## ERROR-003: Confusing run backlog after repeated manual triggers

- Date: 2026-03-06
- Symptom: many runs stuck queued while one run executed.
- Root cause: DAG has `max_active_runs=1`.
- Fix: trigger one run at a time; let current run finish.

## ERROR-004: Credentials visibility in Docker

- Symptom: BigQuery auth failures in containerized tasks.
- Root cause: host credential file not visible inside container by default.
- Fix: mount `credentials.json` into container and use container path for ADC.

## ERROR-005: AQI-weather join mismatch on timestamps

- Symptom: low join hit rate between AQI and weather.
- Root cause: different timestamp granularity and reporting intervals.
- Fix: truncate to hour before join in dbt intermediate models.
