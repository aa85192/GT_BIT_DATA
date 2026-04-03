"""Bitfinex fUSD 歷史資料回補腳本

用法：
  # 回補最近 7 天
  python scripts/backfill_bitfinex_fusd.py --days 7

  # 回補指定日期範圍
  python scripts/backfill_bitfinex_fusd.py --from 2024-01-01 --to 2024-12-31

  # 從 2016 年開始全部回補（很久，建議分段跑）
  python scripts/backfill_bitfinex_fusd.py --from 2016-07-28

原理：
  把大時間範圍切成小窗口（預設 1 小時），逐段呼叫 API，
  利用和 fetch_bitfinex_fusd.py 相同的去重機制寫入。
  每段之間自動 sleep 避免觸發 rate limit。
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone

# 重用主腳本的所有寫入函式
from fetch_bitfinex_fusd import (
    SYMBOL,
    append_candles_csv,
    append_funding_stats_csv,
    append_trades_jsonl,
    fetch_candles,
    fetch_funding_stats,
    fetch_trades,
    ms_to_iso,
)

WINDOW_MS = int(60 * 60 * 1000)  # 每段 1 小時
SLEEP_SEC = float(1.5)            # 每段間隔，避免 rate limit


def parse_date(s: str) -> int:
    """把 YYYY-MM-DD 或 YYYY-MM-DDTHH:MM:SS 轉成毫秒時間戳"""
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        except ValueError:
            continue
    raise ValueError(f"無法解析日期: {s}，格式須為 YYYY-MM-DD 或 YYYY-MM-DDTHH:MM:SS")


def main():
    parser = argparse.ArgumentParser(description="Bitfinex fUSD 歷史回補")
    parser.add_argument("--from", dest="from_date", help="起始日期 YYYY-MM-DD")
    parser.add_argument("--to", dest="to_date", help="結束日期 YYYY-MM-DD（預設現在）")
    parser.add_argument("--days", type=float, help="回補最近 N 天（與 --from 二擇一，可用小數）")
    parser.add_argument("--window-min", type=int, default=60, help="每段窗口分鐘數（預設 60）")
    parser.add_argument("--sleep", type=float, default=3.0, help="每段間隔秒數（預設 3.0）")
    parser.add_argument("--no-trades", action="store_true", help="跳過 trades")
    parser.add_argument("--no-candles", action="store_true", help="跳過 candles")
    parser.add_argument("--no-stats", action="store_true", help="跳過 funding stats")
    args = parser.parse_args()

    now_ms = int(time.time() * 1000)

    # 決定時間範圍
    if args.days:
        start_ms = int(now_ms - args.days * 24 * 60 * 60 * 1000)
    elif args.from_date:
        start_ms = parse_date(args.from_date)
    else:
        parser.error("需要指定 --days 或 --from")
        return

    end_ms = parse_date(args.to_date) if args.to_date else now_ms
    window_ms = args.window_min * 60 * 1000

    total_windows = (end_ms - start_ms + window_ms - 1) // window_ms
    print(f"回補範圍: {ms_to_iso(start_ms)} → {ms_to_iso(end_ms)}")
    print(f"Symbol: {SYMBOL}")
    print(f"窗口大小: {args.window_min} 分鐘, 共 {total_windows} 段")
    print(f"每段間隔: {args.sleep} 秒")
    print(f"抓取: trades={'OFF' if args.no_trades else 'ON'}, "
          f"candles={'OFF' if args.no_candles else 'ON'}, "
          f"stats={'OFF' if args.no_stats else 'ON'}")
    print("---")

    total_trades = 0
    total_candles = 0
    total_stats = 0
    errors = 0

    cursor = start_ms
    seg = 0

    while cursor < end_ms:
        seg += 1
        seg_end = min(cursor + window_ms, end_ms)

        try:
            # Trades
            new_t = 0
            if not args.no_trades:
                trades = fetch_trades(cursor, seg_end)
                new_t = append_trades_jsonl(trades)
                total_trades += new_t
                time.sleep(0.5)

            # Candles
            new_c = 0
            if not args.no_candles:
                candle_key, candles = fetch_candles(cursor, seg_end)
                new_c = append_candles_csv(candle_key, candles)
                total_candles += new_c
                time.sleep(0.5)

            # Funding Stats
            new_s = 0
            if not args.no_stats:
                stats = fetch_funding_stats(cursor, seg_end)
                new_s = append_funding_stats_csv(stats)
                total_stats += new_s

            print(f"[{seg}/{total_windows}] {ms_to_iso(cursor)[:19]} → {ms_to_iso(seg_end)[:19]}  "
                  f"trades=+{new_t} candles=+{new_c} stats=+{new_s}")

        except Exception as e:
            errors += 1
            print(f"[{seg}/{total_windows}] ERROR: {e} — 等 30 秒後繼續", file=sys.stderr)
            time.sleep(30)

        cursor = seg_end
        if cursor < end_ms:
            time.sleep(args.sleep)

    print("---")
    print(f"完成! trades={total_trades}, candles={total_candles}, stats={total_stats}, errors={errors}")


if __name__ == "__main__":
    main()
