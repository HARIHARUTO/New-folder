# DECISIONS

## DECISION-001: API-first ingestion over scraping

- Decision: use data.gov.in + weather APIs.
- Why: predictable interfaces, lower breakage, better compliance.
- Tradeoff: less control than custom scraping targets.

## DECISION-002: BigQuery + dbt layered modeling

- Decision: raw -> staging -> intermediate -> marts/features.
- Why: traceability, testability, and clear ownership boundaries.
- Tradeoff: more upfront model structure.

## DECISION-003: Airflow orchestration

- Decision: orchestrate ingestion + dbt in one DAG.
- Why: retries, observability, and reproducible task dependencies.
- Tradeoff: heavier local setup than simple scripts.

## DECISION-004: Quarantine table before transformation

- Decision: reject malformed records from primary raw tables.
- Why: protect downstream models from schema drift and bad values.
- Tradeoff: more ingestion code and error handling paths.

## DECISION-005: Abandoned scraping mid-build

- Decision: switched from BigBasket scraping to government APIs after repeated anti-bot failures.
- Why: reliability over control; a pipeline that breaks every few days is not production-usable.
- Tradeoff: lost a scraping talking point, gained a stronger reliability and architecture decision story.
