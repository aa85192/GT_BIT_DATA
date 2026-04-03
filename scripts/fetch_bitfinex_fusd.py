import csv
import json
import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import requests

BASE_URL = "https://api-pub.bitfinex.com/v2"
SYMBOL = os.getenv("BITFINEX_SYMBOL", "fUSD")
LOOKBACK_MIN = int(os.getenv("LOOKBACK_MIN", "30"))
LIMIT = int(os.getenv("BITFINEX_LIMIT", "1000"))
TIMEOUT = int(os.getenv("HTTP_TIMEOUT_SEC", "20"))

# 融資 K 線必須指定 period；a30:p2:p30 聚合 2~30 天所有 period，資料最完整
# 若聚合格式失敗，fallback 到單一 30 天 period
CANDLE_CANDIDATES = [
    os.getenv("BITFINEX_CANDLE", "trade:1m:fUSD:a30:p2:p30"),
    os.getenv("BITFINEX_CANDLE_FALLBACK", "trade:1m:fUSD:p30"),
]

ROOT = Path(".")
DATA_DIR = ROOT / "data"
META_DIR = ROOT / "meta"
META_DIR.mkdir(parents=True, exist_ok=True)


def utc_now_ms() -> int:
    return int(time.time() * 1000)


def ms_to_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def day_str_from_ms(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def http_get(path: str, params: dict | None = None, attempts: int = 4):
    url = f"{BASE_URL}/{path}"
    last_error = None

    for i in range(attempts):
        try:
            resp = requests.get(url, params=params, timeout=TIMEOUT)
            if resp.status_code == 200:
                return resp.json()

            # Bitfinex 維護/限流/短暫錯誤時做退避
            if resp.status_code in (429, 500, 502, 503, 504):
                last_error = RuntimeError(f"HTTP {resp.status_code} {url}")
                time.sleep(2 ** i)
                continue

            raise RuntimeError(f"HTTP {resp.status_code} {url} -> {resp.text[:500]}")
        except Exception as e:
            last_error = e
            time.sleep(2 ** i)

    raise RuntimeError(f"GET failed: {url}, last_error={last_error}")


def fetch_trades(start_ms: int, end_ms: int):
    return http_get(
        f"trades/{SYMBOL}/hist",
        params={
            "start": start_ms,
            "end": end_ms,
            "sort": 1,   # old -> new
            "limit": LIMIT,
        },
    )


def fetch_candles(start_ms: int, end_ms: int):
    """融資 K 線可能在某些時段沒有資料，所以不 raise — 回傳空代表該時段無成交"""
    last_exc = None
    for candle_key in CANDLE_CANDIDATES:
        try:
            data = http_get(
                f"candles/{candle_key}/hist",
                params={
                    "start": start_ms,
                    "end": end_ms,
                    "sort": 1,
                    "limit": LIMIT,
                },
            )
            if isinstance(data, list) and len(data) > 0:
                return candle_key, data
            last_exc = RuntimeError(f"Empty response for {candle_key}")
        except Exception as e:
            last_exc = e
    print(f"[WARN] All candle candidates returned empty or failed: {last_exc}")
    return CANDLE_CANDIDATES[0], []


def fetch_funding_stats(start_ms: int, end_ms: int):
    """funding/stats 端點不支援 limit 參數，傳了會 500"""
    try:
        return http_get(
            f"funding/stats/{SYMBOL}/hist",
            params={
                "start": start_ms,
                "end": end_ms,
                "sort": 1,
            },
        )
    except Exception:
        return []


def load_existing_jsonl_ids(path: Path) -> set[str]:
    ids = set()
    if not path.exists():
        return ids
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                ids.add(str(obj["id"]))
            except Exception:
                continue
    return ids


def append_trades_jsonl(trades: list):
    """Funding trades 格式: [ID, MTS, AMOUNT, RATE, PERIOD]
    AMOUNT > 0 表示融資提供方成交，< 0 表示借入方成交
    RATE 為日利率，PERIOD 為天數
    """
    grouped = defaultdict(list)
    for row in trades:
        if not isinstance(row, list) or len(row) < 5:
            continue
        trade_id, mts, amount, rate, period = row[:5]
        grouped[day_str_from_ms(mts)].append(
            {
                "id": trade_id,
                "mts": mts,
                "ts_utc": ms_to_iso(mts),
                "amount": amount,
                "rate": rate,
                "period": period,
                "raw": row,
            }
        )

    total_new = 0
    for day, rows in grouped.items():
        out_dir = DATA_DIR / "trades" / SYMBOL
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"{day}.jsonl"

        existing_ids = load_existing_jsonl_ids(out_file)
        with out_file.open("a", encoding="utf-8") as f:
            for row in rows:
                if str(row["id"]) in existing_ids:
                    continue
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
                existing_ids.add(str(row["id"]))
                total_new += 1

    return total_new


def load_existing_csv_keys(path: Path, key_col: str) -> set[str]:
    keys = set()
    if not path.exists():
        return keys
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if key_col in row:
                keys.add(str(row[key_col]))
    return keys


def append_candles_csv(candle_key: str, candles: list):
    grouped = defaultdict(list)
    for row in candles:
        if not isinstance(row, list) or len(row) < 6:
            continue
        mts, open_, close_, high_, low_, volume = row[:6]
        grouped[day_str_from_ms(mts)].append(
            {
                "mts": mts,
                "ts_utc": ms_to_iso(mts),
                "open": open_,
                "close": close_,
                "high": high_,
                "low": low_,
                "volume": volume,
                "candle_key": candle_key,
                "raw": json.dumps(row, ensure_ascii=False),
            }
        )

    total_new = 0
    out_dir = DATA_DIR / "candles" / "fUSD_1m"
    out_dir.mkdir(parents=True, exist_ok=True)

    for day, rows in grouped.items():
        out_file = out_dir / f"{day}.csv"
        exists = out_file.exists()
        existing_keys = load_existing_csv_keys(out_file, "mts")

        with out_file.open("a", encoding="utf-8", newline="") as f:
            fieldnames = ["mts", "ts_utc", "open", "close", "high", "low", "volume", "candle_key", "raw"]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not exists:
                writer.writeheader()

            for row in rows:
                if str(row["mts"]) in existing_keys:
                    continue
                writer.writerow(row)
                existing_keys.add(str(row["mts"]))
                total_new += 1

    return total_new


def append_funding_stats_csv(rows: list):
    """Funding stats 格式 (12 欄位):
    [MTS, _, _, FRR, AVG_PERIOD, _, _, FUNDING_AMOUNT, FUNDING_AMOUNT_USED, _, _, FUNDING_BELOW_THRESHOLD]
    索引:  0   1  2   3      4      5  6       7               8            9  10          11
    """
    grouped = defaultdict(list)
    for row in rows:
        if not isinstance(row, list) or len(row) < 9:
            continue
        mts = row[0]
        grouped[day_str_from_ms(mts)].append(
            {
                "mts": mts,
                "ts_utc": ms_to_iso(mts),
                "frr": row[3],
                "avg_period": row[4],
                "funding_amount": row[7],
                "funding_amount_used": row[8],
                "funding_below_threshold": row[11] if len(row) > 11 else None,
                "raw": json.dumps(row, ensure_ascii=False),
            }
        )

    total_new = 0
    out_dir = DATA_DIR / "funding_stats" / SYMBOL
    out_dir.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "mts", "ts_utc", "frr", "avg_period",
        "funding_amount", "funding_amount_used", "funding_below_threshold", "raw",
    ]

    for day, rows_for_day in grouped.items():
        out_file = out_dir / f"{day}.csv"
        exists = out_file.exists()
        existing_keys = load_existing_csv_keys(out_file, "mts")

        with out_file.open("a", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not exists:
                writer.writeheader()

            for row in rows_for_day:
                if str(row["mts"]) in existing_keys:
                    continue
                writer.writerow(row)
                existing_keys.add(str(row["mts"]))
                total_new += 1

    return total_new


def write_summary(summary: dict):
    with (META_DIR / "last_run_summary.json").open("w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)


def main():
    end_ms = utc_now_ms()
    start_ms = end_ms - LOOKBACK_MIN * 60 * 1000

    trades = fetch_trades(start_ms, end_ms)
    candle_key, candles = fetch_candles(start_ms, end_ms)
    stats = fetch_funding_stats(start_ms, end_ms)

    new_trades = append_trades_jsonl(trades)
    new_candles = append_candles_csv(candle_key, candles)
    new_stats = append_funding_stats_csv(stats)

    summary = {
        "symbol": SYMBOL,
        "start_ms": start_ms,
        "end_ms": end_ms,
        "start_utc": ms_to_iso(start_ms),
        "end_utc": ms_to_iso(end_ms),
        "candle_key_used": candle_key,
        "lookback_min": LOOKBACK_MIN,
        "fetched": {
            "trades_rows": len(trades),
            "candles_rows": len(candles),
            "funding_stats_rows": len(stats),
        },
        "new_written": {
            "trades_rows": new_trades,
            "candles_rows": new_candles,
            "funding_stats_rows": new_stats,
        },
    }
    write_summary(summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
