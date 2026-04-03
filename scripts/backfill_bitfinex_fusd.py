"""Bitfinex fUSD 歷史資料回補腳本

用法：
  # 自動模式：偵測現有最早資料，一直往前抓到 2016 年底
  python scripts/backfill_bitfinex_fusd.py --auto

  # 自動模式 + 指定最遠追溯日期
  python scripts/backfill_bitfinex_fusd.py --auto --origin 2020-01-01

  # 回補最近 7 天
  python scripts/backfill_bitfinex_fusd.py --days 7

  # 回補指定��期範圍
  python scripts/backfill_bitfinex_fusd.py --from 2024-01-01 --to 2024-12-31

原理：
  把大時間範圍切成小窗口（預設 1 小時），逐段呼叫 API，
  利用和 fetch_bitfinex_fusd.py 相同的去重機制寫入。
  每段之間自動 sleep 避免觸發 rate limit。
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# 重用主腳本的所有寫入函式
from fetch_bitfinex_fusd import (
    DATA_DIR,
    SYMBOL,
    append_candles_csv,
    append_funding_stats_csv,
    append_trades_jsonl,
    fetch_candles,
    fetch_funding_stats,
    fetch_trades_paginated,
    ms_to_iso,
)

# Bitfinex fUSD 融資市場最早資料: 2016-07-28
ORIGIN_MS = int(datetime(2016, 7, 28, tzinfo=timezone.utc).timestamp() * 1000)


def parse_date(s: str) -> int:
    """把 YYYY-MM-DD 或 YYYY-MM-DDTHH:MM:SS 轉成毫秒時間戳"""
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        except ValueError:
            continue
    raise ValueError(f"無法解析日期: {s}，格式須為 YYYY-MM-DD 或 YYYY-MM-DDTHH:MM:SS")


def find_earliest_data_ms() -> int | None:
    """掃描 data/ 目錄，找出最早的日期分區檔案對應的毫秒時間��"""
    earliest = None

    for subdir in ["trades", "candles", "funding_stats"]:
        search_dir = DATA_DIR / subdir
        if not search_dir.exists():
            continue
        for f in search_dir.rglob("*.jsonl"):
            day = f.stem  # e.g. "2025-06-01"
            try:
                ms = parse_date(day)
                if earliest is None or ms < earliest:
                    earliest = ms
            except ValueError:
                continue
        for f in search_dir.rglob("*.csv"):
            day = f.stem
            try:
                ms = parse_date(day)
                if earliest is None or ms < earliest:
                    earliest = ms
            except ValueError:
                continue

    return earliest


def run_backfill(start_ms: int, end_ms: int, window_min: int, sleep_sec: float,
                 no_trades: bool, no_candles: bool, no_stats: bool):
    """執行回補，回傳 (total_trades, total_candles, total_stats, errors)"""
    window_ms = window_min * 60 * 1000
    total_windows = max(1, (end_ms - start_ms + window_ms - 1) // window_ms)

    print(f"回補範圍: {ms_to_iso(start_ms)} → {ms_to_iso(end_ms)}")
    print(f"Symbol: {SYMBOL}")
    print(f"窗口大小: {window_min} 分鐘, 共 {total_windows} 段")
    print(f"每段間隔: {sleep_sec} 秒")
    print(f"抓取: trades={'OFF' if no_trades else 'ON'}, "
          f"candles={'OFF' if no_candles else 'ON'}, "
          f"stats={'OFF' if no_stats else 'ON'}")
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
            # Trades（用分頁版避免 1000 筆截斷）
            new_t = 0
            if not no_trades:
                trades = fetch_trades_paginated(cursor, seg_end)
                new_t = append_trades_jsonl(trades)
                total_trades += new_t
                time.sleep(0.5)

            # Candles
            new_c = 0
            if not no_candles:
                candle_key, candles = fetch_candles(cursor, seg_end)
                new_c = append_candles_csv(candle_key, candles)
                total_candles += new_c
                time.sleep(0.5)

            # Funding Stats
            new_s = 0
            if not no_stats:
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
            time.sleep(sleep_sec)

    return total_trades, total_candles, total_stats, errors


def main():
    parser = argparse.ArgumentParser(description="Bitfinex fUSD 歷史回補")
    parser.add_argument("--auto", action="store_true",
                        help="自動模式：偵測最早資料，一直往前抓")
    parser.add_argument("--origin", dest="origin_date",
                        help="--auto 的最遠���溯日期（預設 2016-07-28）")
    parser.add_argument("--from", dest="from_date", help="起始日期 YYYY-MM-DD")
    parser.add_argument("--to", dest="to_date", help="結束日期 YYYY-MM-DD（預���現在）")
    parser.add_argument("--days", type=float, help="回補最近 N 天（與 --from 二擇一，可用小數）")
    parser.add_argument("--window-min", type=int, default=60, help="每段窗口分鐘數（預設 60）")
    parser.add_argument("--sleep", type=float, default=3.0, help="每段間隔秒數（預設 3.0）")
    parser.add_argument("--no-trades", action="store_true", help="跳過 trades")
    parser.add_argument("--no-candles", action="store_true", help="跳過 candles")
    parser.add_argument("--no-stats", action="store_true", help="跳過 funding stats")
    args = parser.parse_args()

    now_ms = int(time.time() * 1000)

    if args.auto:
        # 自動模式：找到現有最早資料，從那裡繼續往前
        origin_ms = parse_date(args.origin_date) if args.origin_date else ORIGIN_MS
        earliest = find_earliest_data_ms()

        if earliest is None:
            # 沒有任何資料，從現在開始往前抓
            end_ms = now_ms
            print(f"未找到現有資料，將從現在往前回補到 {ms_to_iso(origin_ms)[:10]}")
        else:
            end_ms = earliest
            print(f"偵測到最早資料: {ms_to_iso(earliest)[:10]}")
            if earliest <= origin_ms:
                print("已達最遠追溯點，無需回補。")
                return

        start_ms = origin_ms
        total_windows = (end_ms - start_ms + args.window_min * 60 * 1000 - 1) // (args.window_min * 60 * 1000)
        hours = (end_ms - start_ms) / 3600000
        est_min = total_windows * (args.sleep + 1.0) / 60
        print(f"待回補: {hours:.0f} 小�� ({total_windows} 段), 預估耗時: ~{est_min:.0f} 分鐘")

        t, c, s, e = run_backfill(
            start_ms, end_ms, args.window_min, args.sleep,
            args.no_trades, args.no_candles, args.no_stats,
        )
        print("---")
        print(f"完成! trades={t}, candles={c}, stats={s}, errors={e}")

    elif args.days:
        start_ms = int(now_ms - args.days * 24 * 60 * 60 * 1000)
        end_ms = parse_date(args.to_date) if args.to_date else now_ms
        t, c, s, e = run_backfill(
            start_ms, end_ms, args.window_min, args.sleep,
            args.no_trades, args.no_candles, args.no_stats,
        )
        print("---")
        print(f"完成! trades={t}, candles={c}, stats={s}, errors={e}")

    elif args.from_date:
        start_ms = parse_date(args.from_date)
        end_ms = parse_date(args.to_date) if args.to_date else now_ms
        t, c, s, e = run_backfill(
            start_ms, end_ms, args.window_min, args.sleep,
            args.no_trades, args.no_candles, args.no_stats,
        )
        print("---")
        print(f"完成! trades={t}, candles={c}, stats={s}, errors={e}")

    else:
        parser.error("需要指��� --auto、--days 或 --from")


if __name__ == "__main__":
    main()
