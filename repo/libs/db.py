# repo/libs/db.py
import os
import time
import json
import logging
from typing import List, Dict, Optional

from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import psycopg2
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

def get_db_url():
    user = os.getenv("POSTGRES_USER", "stock_user")
    pw = os.getenv("POSTGRES_PASSWORD", "stock_pass")
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "stock_db")
    return f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}"

_engine = None
def get_engine():
    global _engine
    if _engine is None:
        url = get_db_url()
        # pool_pre_ping helps with stale connections in long-running containers
        _engine = create_engine(url, pool_pre_ping=True, pool_size=5, max_overflow=10)
    return _engine

# Configurable retry parameters
DB_MAX_RETRIES = int(os.getenv("DB_MAX_RETRIES", "3"))
DB_BACKOFF_BASE = float(os.getenv("DB_BACKOFF_BASE", "1.0"))  # seconds

def _sleep_backoff(attempt: int) -> None:
    time.sleep(DB_BACKOFF_BASE * (2 ** attempt))

def upsert_stock_rows(rows: List[Dict], batch_size: int = 200) -> int:
    """
    Batch upsert rows into stock_data.
    Each row should have: symbol, ts, open, high, low, close, volume, raw (raw can be dict/str)
    Returns total number of rows attempted (sum of batch sizes inserted).
    Raises RuntimeError on repeated failure.
    """
    if not rows:
        return 0

    # normalize raw field to JSON string
    tuples = []
    for r in rows:
        raw_val = r.get("raw")
        if raw_val is None:
            raw_json = None
        elif isinstance(raw_val, (str, bytes)):
            raw_json = raw_val
        else:
            try:
                raw_json = json.dumps(raw_val)
            except Exception:
                raw_json = str(raw_val)
        tuples.append((
            r.get("symbol"),
            r.get("ts"),
            r.get("open"),
            r.get("high"),
            r.get("low"),
            r.get("close"),
            r.get("volume"),
            raw_json
        ))

    insert_sql = """
    INSERT INTO stock_data (symbol, ts, open, high, low, close, volume, raw)
    VALUES %s
    ON CONFLICT (symbol, ts) DO UPDATE
      SET open = EXCLUDED.open,
          high = EXCLUDED.high,
          low = EXCLUDED.low,
          close = EXCLUDED.close,
          volume = EXCLUDED.volume,
          raw = EXCLUDED.raw
    """

    total = 0
    attempt = 0
    # we will iterate batches and attempt each batch with its own retry loop
    for i in range(0, len(tuples), batch_size):
        batch = tuples[i:i + batch_size]
        batch_attempt = 0
        while batch_attempt < DB_MAX_RETRIES:
            batch_attempt += 1
            try:
                engine = get_engine()
                # get raw connection from engine for psycopg2.execute_values
                with engine.raw_connection() as raw_conn:
                    with raw_conn.cursor() as cur:
                        # execute_values handles efficient bulk insert
                        execute_values(cur, insert_sql, batch, template=None, page_size=100)
                    # commit happens when exiting context if no exception
                total += len(batch)
                logger.info("Upserted batch %d (%d rows).", (i // batch_size) + 1, len(batch))
                break
            except psycopg2.OperationalError as e:
                logger.warning("OperationalError on DB batch upsert attempt %d/%d: %s", batch_attempt, DB_MAX_RETRIES, e)
                if batch_attempt >= DB_MAX_RETRIES:
                    logger.exception("Exhausted DB retries for current batch; re-raising.")
                    raise RuntimeError("Failed to upsert batch after retries") from e
                _sleep_backoff(batch_attempt)
                continue
            except Exception as e:
                # Other SQLAlchemy/psycopg2 errors - log and optionally retry
                logger.exception("Error upserting batch %d: %s", (i // batch_size) + 1, e)
                if batch_attempt >= DB_MAX_RETRIES:
                    raise RuntimeError("Failed to upsert batch after retries") from e
                _sleep_backoff(batch_attempt)
                continue

    logger.info("Completed upsert of %d rows (in %d batches)", total, (len(tuples) + batch_size - 1) // batch_size)
    return total
