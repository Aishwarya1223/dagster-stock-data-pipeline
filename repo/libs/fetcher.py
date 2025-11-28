# repo/libs/fetcher.py
import os
import time
import random
import logging
from typing import Dict, Any, List, Optional

import requests

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

ALPHA_VANTAGE_BASE = "https://www.alphavantage.co/query"

DEFAULT_TIMEOUT = int(os.getenv("FETCH_TIMEOUT", "15"))
MAX_RETRIES = int(os.getenv("FETCH_MAX_RETRIES", "5"))
BACKOFF_BASE = float(os.getenv("FETCH_BACKOFF_BASE", "1.0"))
JITTER = True

def _sleep_backoff(attempt: int) -> None:
    backoff = BACKOFF_BASE * (2 ** attempt)
    if JITTER:
        backoff = backoff * (0.8 + 0.4 * random.random())
    time.sleep(backoff)


def fetch_daily_time_series(symbol: str, api_key: str = None, session: Optional[requests.Session] = None) -> Optional[Dict[str, Any]]:
    """
    Robust fetcher for Alpha Vantage TIME_SERIES_DAILY_ADJUSTED.
    Returns JSON dict on success, None on unrecoverable failure.
    Retries transient errors with exponential backoff.
    """
    if api_key is None:
        api_key = os.getenv("API_KEY")

    if not api_key:
        logger.error("No API_KEY provided (env API_KEY missing)")
        return None

    if session is None:
        session = requests.Session()

    params = {
        "function": "TIME_SERIES_DAILY_ADJUSTED",
        "symbol": symbol.strip(),
        "outputsize": "compact",
        "apikey": api_key,
    }

    last_exc = None
    for attempt in range(MAX_RETRIES):
        try:
            logger.debug("Fetching %s attempt=%d", symbol, attempt + 1)
            resp = session.get(ALPHA_VANTAGE_BASE, params=params, timeout=DEFAULT_TIMEOUT)
            # Retry on server errors
            if 500 <= resp.status_code < 600:
                last_exc = RuntimeError(f"Server error {resp.status_code} for {symbol}")
                logger.warning("Server error for %s: %s", symbol, resp.status_code)
                _sleep_backoff(attempt)
                continue

            resp.raise_for_status()
            data = resp.json()

            # AlphaVantage returns rate-limit or informational messages in "Note" key
            if "Note" in data:
                last_exc = RuntimeError(f"Rate limit/Note for {symbol}: {data['Note']}")
                logger.warning("Rate limit/note received for %s: %s", symbol, data["Note"])
                _sleep_backoff(attempt)
                continue

            if "Error Message" in data:
                logger.error("API error for symbol %s: %s", symbol, data["Error Message"])
                return None

            if not any(k.startswith("Time Series") for k in data.keys()):
                last_exc = RuntimeError(f"Unexpected response structure for {symbol}. Keys: {list(data.keys())[:5]}")
                logger.warning("Unexpected JSON structure for %s: %s", symbol, list(data.keys())[:5])
                _sleep_backoff(attempt)
                continue

            return data

        except (requests.Timeout, requests.ConnectionError) as e:
            last_exc = e
            logger.warning("Transient network error for %s on attempt %d: %s", symbol, attempt + 1, e)
            _sleep_backoff(attempt)
            continue
        except ValueError as e:
            logger.exception("JSON decode error for %s: %s", symbol, e)
            return None
        except Exception as e:
            last_exc = e
            logger.exception("Unexpected error fetching %s: %s", symbol, e)
            _sleep_backoff(attempt)
            continue

    logger.error("Exhausted retries fetching %s. Last error: %s", symbol, last_exc)
    return None


def parse_daily_response(symbol: str, json_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Parse Alpha Vantage daily adjusted response into rows:
    list of dicts with keys: symbol, ts, open, high, low, close, volume, raw
    """
    results: List[Dict[str, Any]] = []
    ts_key = None
    for k in json_data.keys():
        if k.startswith("Time Series"):
            ts_key = k
            break
    if not ts_key:
        logger.error("No Time Series key found in response for %s", symbol)
        return results

    ts_map = json_data.get(ts_key, {})
    for ts_str, values in ts_map.items():
        try:
            open_p = float(values.get("1. open") or values.get("open") or 0.0)
            high_p = float(values.get("2. high") or values.get("high") or 0.0)
            low_p = float(values.get("3. low") or values.get("low") or 0.0)
            close_p = float(values.get("4. close") or values.get("close") or 0.0)
            volume = int(values.get("6. volume") or values.get("volume") or 0)
        except Exception as e:
            logger.warning("Failed to parse values for %s %s: %s", symbol, ts_str, e)
            continue

        results.append({
            "symbol": symbol.strip(),
            "ts": ts_str,
            "open": open_p,
            "high": high_p,
            "low": low_p,
            "close": close_p,
            "volume": volume,
            "raw": values
        })
    return results
