# repo/jobs/stock_job.py
import os
import time
import json
import requests
from typing import List, Dict
from dagster import job, op, get_dagster_logger, schedule, Definitions

from repo.libs.fetcher import fetch_daily_time_series, parse_daily_response
from repo.libs.db import upsert_stock_rows

logger = get_dagster_logger()

POLITE_DELAY = float(os.getenv("API_POLITE_DELAY_SEC", "12"))


@op
def fetch_and_parse(context) -> List[Dict]:
    """Fetch from API and parse; returns list of rows to upsert."""
    api_key = os.getenv("API_KEY")
    if not api_key:
        context.log.error("API_KEY not set; aborting fetch")
        raise Exception("API_KEY not set")

    symbols = [s.strip() for s in os.getenv("STOCK_SYMBOLS", "AAPL").split(",") if s.strip()]
    if not symbols:
        context.log.error("No STOCK_SYMBOLS configured")
        raise Exception("No STOCK_SYMBOLS configured")

    session = requests.Session()
    all_rows = []
    success_any = False

    for i, sym in enumerate(symbols):
        context.log.info("Fetching symbol %s (%d/%d)", sym, i + 1, len(symbols))
        try:
            json_data = fetch_daily_time_series(sym, api_key, session=session)
            if not json_data:
                context.log.warning("No data returned for %s", sym)
                continue

            rows = parse_daily_response(sym, json_data)
            if rows:
                success_any = True
                all_rows.extend(rows)
                context.log.info("Parsed %d rows for %s", len(rows), sym)
            else:
                context.log.warning("Parsed 0 rows for %s", sym)

        except Exception as e:
            context.log.error("Exception for %s: %s", sym, e)

        if i < len(symbols) - 1:
            time.sleep(POLITE_DELAY)

    context.log.info("Total rows fetched: %d", len(all_rows))

    if not success_any:
        raise Exception("No data fetched for any symbol.")

    return all_rows


@op
def upsert_rows(context, rows: List[dict]) -> int:
    """Upsert rows into Postgres using batched upsert."""
    if not rows:
        context.log.info("No rows to upsert.")
        return 0

    try:
        count = upsert_stock_rows(rows)
        context.log.info("Upserted %d rows.", count)
        return count
    except Exception as e:
        context.log.error("Upsert failed: %s", e)
        raise


@job
def stock_job():
    upsert_rows(fetch_and_parse())



CRON = os.getenv("DAG_SCHEDULE_CRON", "0 6 * * *")

@schedule(cron_schedule=CRON, job=stock_job, execution_timezone="UTC")
def daily_stock_schedule():
    return {}


definitions = Definitions(
    jobs=[stock_job],
    schedules=[daily_stock_schedule]
)
