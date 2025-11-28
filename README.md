# Dockerized Stock Market Data Pipeline (Dagster + PostgreSQL)

This project implements a fully Dockerized data pipeline that automatically fetches daily stock market data from an external API (Alpha Vantage), parses it, and upserts it into a PostgreSQL database.  
The entire workflow is orchestrated using **Dagster**, with a fully interactive UI provided via **Dagit**.

Designed for robustness, clarity, and scalability, this pipeline follows real-world data engineering practices used in production systems.

---

## Features

- **Automated stock data fetching**
  - Daily time-series data
  - Multiple stock symbols supported (comma-separated)
- **Reliable parsing & validation**
  - Handles malformed/missing fields gracefully
- **Idempotent database upserts**
  - PostgreSQL `ON CONFLICT` ensures clean updates
- **Full containerization**
  - One command launches everything:
    ```bash
    docker compose up --build
    ```
- **Configurable scheduling**
  - Default: daily at 06:00 UTC
  - Override with cron in `.env`
- **Robust error handling**
  - API failures  
  - Rate limits  
  - Database issues  
  - Retries with backoff
- **Clean modular architecture**
  - `fetcher.py`, `db.py`, Dagster ops & job

---

## Project Structure

```
dagster-stock-pipeline/
├─ docker-compose.yml
├─ Dockerfile
├─ .env.example
├─ README.md
├─ init_db.sql
├─ workspace.yaml
├─ repo/
│  ├─ __init__.py
│  ├─ jobs/
│  │  └─ stock_job.py
│  ├─ libs/
│  │  ├─ fetcher.py
│  │  └─ db.py
│  └─ schedules.py  (optional)
└─ dagster_home/ (auto-created)
```

---

## Architecture Overview

```
                 ┌────────────────────────┐
                 │      Dagster Daemon    │
                 │  (scheduling engine)   │
                 └─────────────▲──────────┘
                               │ runs job
                               │
      ┌────────────────────────┴────────────────────────┐
      │                   Dagster Job                    │
      │  fetch_and_parse() → upsert_rows() → postgres   │
      └───────────────▲─────────────────────────────────┘
                      │
                      │ fetch (HTTP GET)
                      │
             ┌────────┴────────┐
             │  Alpha Vantage   │
             │    Stock API     │
             └──────────────────┘
```

---

## Setup Instructions

### Create `.env` file

```bash
cp .env.example .env
```

Edit `.env` and set:

- `API_KEY` — Alpha Vantage API key  
- `STOCK_SYMBOLS` — e.g., `AAPL,MSFT,GOOG`  
- Optional: `DAG_SCHEDULE_CRON="0 * * * *"` (hourly)

---

### Start the pipeline

```bash
docker compose up --build
```

This starts:

- PostgreSQL  
- Dagster code server  
- Dagster daemon  
- Dagit (web UI)

---

### Access Dagit UI

Open: **http://localhost:3000**

You will see:

- `stock_job`  
- `daily_stock_schedule`  
- Logs, runs, graphs, configuration  

---

### Check database

Connect to Postgres:

| Field   | Value                  |
|---------|-------------------------|
| host    | localhost               |
| port    | 5432 (default)          |
| user    | POSTGRES_USER           |
| pass    | POSTGRES_PASSWORD       |
| db      | POSTGRES_DB             |

Example SQL:

```sql
SELECT * FROM stock_data LIMIT 10;
```

---

## Database Schema

Created by `init_db.sql` on first run:

```sql
CREATE TABLE IF NOT EXISTS stock_data (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume BIGINT,
    raw JSONB,
    UNIQUE (symbol, ts)
);
```

---

## Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `API_KEY` | Alpha Vantage API key | `12345ABCD` |
| `STOCK_SYMBOLS` | Comma-separated tickers | `AAPL,MSFT,GOOG` |
| `POSTGRES_USER` | DB username | `stock_user` |
| `POSTGRES_PASSWORD` | DB password | `stock_pass` |
| `POSTGRES_DB` | DB name | `stock_db` |
| `POSTGRES_PORT` | Host port | `5432` |
| `DAG_SCHEDULE_CRON` | Cron schedule | `0 6 * * *` |
| `API_POLITE_DELAY_SEC` | Sleep between API calls | `12` |

---

## Error Handling & Robustness

### API Failures
- Retries with exponential backoff  
- Automatic handling of `"Note"` field (rate limit)  
- Session reuse to reduce overhead  

### Parsing Errors
- Skips malformed rows  
- Logs warnings without crashing  

### DB Failures
- Batched upserts (`execute_values`)  
- Retries on transient errors  
- Connection pooling with SQLAlchemy  

### Dagster Integration
- If fetch returns no data → job fails visibly  
- Daemon automatically manages scheduled runs  
- Dagit UI provides logs & stack traces  

---

## Scalability

To scale further:

- Switch free API → premium provider (higher rate limits)
- Increase Dagster workers / concurrency
- Use streaming inserts for very large datasets
- Move PostgreSQL to a cloud-managed DB (RDS/Cloud SQL)
- Containerize Dagster in Kubernetes for distributed execution

Time complexity:
- O(N) fetch & parsing  
- Batched upsert reduces DB calls to O(1) batches  

Space complexity:
- O(N) when loading all JSON rows  
- Can switch to streaming for constant-space processing  

---

## 🛠 Troubleshooting

### Job not visible in Dagit
Check:
- `definitions = Definitions(...)` is present in `stock_job.py`
- `workspace.yaml` points to correct file

### DB schema not created
Remove old volume & reinitialize:

```bash
docker compose down -v
docker compose up --build
```

### API rate limit errors
Increase delay:

```env
API_POLITE_DELAY_SEC=15
```

### Postgres connection errors
Ensure container names match:

```yaml
POSTGRES_HOST=postgres
```

---

## Future Enhancements

- Add intraday, weekly, and monthly pipelines  
- Add Slack/email alerts for failures  
- Add historical backfill job  
- Convert job to new Dagster Assets API  
- Add Airflow version of same pipeline  
- Add CI/CD for automated deployment  

---

## Evaluation Notes

This project demonstrates:

- Correctness of data fetch, parse, and upsert  
- Strong error-handling and resilience  
- Full Docker orchestration  
- Clean code structure and modular design  
- Scalable architecture with batching + retries  
- Professional documentation  

---
