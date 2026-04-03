"""Bitfinex fUSD 歷史資料回補腳本

用法：
  # 自動模式：偵測現有最早資料，從那裡往前抓到 2016 年
  python scripts/backfill_bitfinex_fusd.py --auto

  # 自動模式 + 指定最遠追溯日期
  python scripts/backfill_bitfinex_fusd.py --auto --origin 2020-01-01

  # 回補最近 7 天
  python scripts/backfill_bitfinex_fusd.py --days 7

  # 回補指定日期範圍
  python scripts/backfill_bitfinex_fusd.py --from 2024-01-01 --to 2024-12-31

原理：
  把大時間範圍切成窗口（預設 6 小時），逐段呼叫 API，
  利用和 fetch_bitfinex_fusd.py 相同的去重機制寫入。
  連續空段時自動加大窗口（最大 24 小時），有資料時恢復原始窗口。
  --auto 模式從最早資料往前抓，中斷後可續跑。
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
    META_DIR,
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

CHECKPOINT_FILE = META_DIR / "backfill_checkpoint.json"

# 自適應跳空設定
EMPTY_STREAK_THRESHOLD = 3    # 連續幾段全空後開始加大窗口
MAX_WINDOW_MULTIPLIER = 4     # 最大窗口倍數 (6h * 4 = 24h)


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
    """掃描 data/ 目錄，找出最早的日期分區檔案對應的毫秒時間戳"""
    earliest = None

    for subdir in ["trades", "candles", "funding_stats"]:
        search_dir = DATA_DIR / subdir
        if not search_dir.exists():
            continue
        for f in search_dir.rglob("*.jsonl"):
            try:
                ms = parse_date(f.stem)
                if earliest is None or ms < earliest:
                    earliest = ms
            except ValueError:
                continue
        for f in search_dir.rglob("*.csv"):
            try:
                ms = parse_date(f.stem)
                if earliest is None or ms < earliest:
                    earliest = ms
            except ValueError:
                continue

    return earliest


def load_checkpoint() -> dict | None:
    """讀取上次回補進度"""
    if not CHECKPOINT_FILE.exists():
        return None
    try:
        with CHECKPOINT_FILE.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def save_checkpoint(origin_ms: int, cursor_ms: int, direction: str):
    """儲存回補進度"""
    META_DIR.mkdir(parents=True, exist_ok=True)
    data = {
        "origin_ms": origin_ms,
        "cursor_ms": cursor_ms,
        "cursor_utc": ms_to_iso(cursor_ms),
        "direction": direction,
        "updated_at": ms_to_iso(int(time.time() * 1000)),
    }
    with CHECKPOINT_FILE.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def run_backfill_forward(start_ms: int, end_ms: int, window_min: int, sleep_sec: float,
                         no_trades: bool, no_candles: bool, no_stats: bool):
    """正向回補（舊→新），用於 --from/--days 模式"""
    window_ms = window_min * 60 * 1000
    total_windows = max(1, (end_ms - start_ms + window_ms - 1) // window_ms)

    print(f"回補範圍: {ms_to_iso(start_ms)} → {ms_to_iso(end_ms)}")
    print(f"Symbol: {SYMBOL}, 方向: 舊→新")
    print(f"窗口大小: {window_min} 分鐘, 約 {total_windows} 段")
    print(f"每段間隔: {sleep_sec} 秒")
    print(f"抓取: trades={'OFF' if no_trades else 'ON'}, "
          f"candles={'OFF' if no_candles else 'ON'}, "
          f"stats={'OFF' if no_stats else 'ON'}")
    print("---")

    total_trades = 0
    total_candles = 0
    total_stats = 0
    errors = 0
    empty_streak = 0

    cursor = start_ms
    seg = 0

    while cursor < end_ms:
        seg += 1

        # 自適應窗口：連續空段時加大
        if empty_streak >= EMPTY_STREAK_THRESHOLD:
            multiplier = min(2 ** (empty_streak - EMPTY_STREAK_THRESHOLD + 1), MAX_WINDOW_MULTIPLIER)
            current_window = window_ms * multiplier
        else:
            current_window = window_ms

        seg_end = min(cursor + current_window, end_ms)

        try:
            new_t, new_c, new_s = fetch_segment(
                cursor, seg_end, no_trades, no_candles, no_stats)
            total_trades += new_t
            total_candles += new_c
            total_stats += new_s

            if new_t == 0 and new_c == 0 and new_s == 0:
                empty_streak += 1
            else:
                empty_streak = 0

            window_label = f" (窗口x{current_window // window_ms})" if current_window != window_ms else ""
            print(f"[{seg}/~{total_windows}] {ms_to_iso(cursor)[:19]} → {ms_to_iso(seg_end)[:19]}  "
                  f"trades=+{new_t} candles=+{new_c} stats=+{new_s}{window_label}")

        except Exception as e:
            errors += 1
            print(f"[{seg}/~{total_windows}] ERROR: {e} — 等 30 秒後繼續", file=sys.stderr)
            time.sleep(30)

        cursor = seg_end
        if cursor < end_ms:
            time.sleep(sleep_sec)

    return total_trades, total_candles, total_stats, errors


def run_backfill_reverse(origin_ms: int, end_ms: int, window_min: int, sleep_sec: float,
                         no_trades: bool, no_candles: bool, no_stats: bool):
    """反向回補（新→舊），用於 --auto 模式。從 end_ms 往前抓到 origin_ms。"""
    window_ms = window_min * 60 * 1000
    total_windows = max(1, (end_ms - origin_ms + window_ms - 1) // window_ms)

    # 檢查 checkpoint，看是否有上次中斷的進度
    ckpt = load_checkpoint()
    if ckpt and ckpt.get("direction") == "reverse" and ckpt.get("origin_ms") == origin_ms:
        resume_cursor = ckpt["cursor_ms"]
        if resume_cursor > origin_ms:
            end_ms = resume_cursor
            total_windows = max(1, (end_ms - origin_ms + window_ms - 1) // window_ms)
            print(f"從 checkpoint 續跑: {ms_to_iso(resume_cursor)[:10]}")

    print(f"回補範圍: {ms_to_iso(end_ms)} → {ms_to_iso(origin_ms)} (新→舊)")
    print(f"Symbol: {SYMBOL}, 方向: 新→舊")
    print(f"窗口大小: {window_min} 分鐘, 約 {total_windows} 段")
    print(f"每段間隔: {sleep_sec} 秒")
    print(f"抓取: trades={'OFF' if no_trades else 'ON'}, "
          f"candles={'OFF' if no_candles else 'ON'}, "
          f"stats={'OFF' if no_stats else 'ON'}")
    print("---")

    total_trades = 0
    total_candles = 0
    total_stats = 0
    errors = 0
    empty_streak = 0

    cursor = end_ms  # 從最新端開始
    seg = 0

    while cursor > origin_ms:
        seg += 1

        # 自適應窗口：連續空段時加大
        if empty_streak >= EMPTY_STREAK_THRESHOLD:
            multiplier = min(2 ** (empty_streak - EMPTY_STREAK_THRESHOLD + 1), MAX_WINDOW_MULTIPLIER)
            current_window = window_ms * multiplier
        else:
            current_window = window_ms

        seg_start = max(cursor - current_window, origin_ms)

        try:
            new_t, new_c, new_s = fetch_segment(
                seg_start, cursor, no_trades, no_candles, no_stats)
            total_trades += new_t
            total_candles += new_c
            total_stats += new_s

            if new_t == 0 and new_c == 0 and new_s == 0:
                empty_streak += 1
            else:
                empty_streak = 0

            window_label = f" (窗口x{current_window // window_ms})" if current_window != window_ms else ""
            print(f"[{seg}/~{total_windows}] {ms_to_iso(seg_start)[:19]} → {ms_to_iso(cursor)[:19]}  "
                  f"trades=+{new_t} candles=+{new_c} stats=+{new_s}{window_label}")

        except Exception as e:
            errors += 1
            print(f"[{seg}/~{total_windows}] ERROR: {e} — 等 30 秒後繼續", file=sys.stderr)
            time.sleep(30)

        cursor = seg_start
        save_checkpoint(origin_ms, cursor, "reverse")

        if cursor > origin_ms:
            time.sleep(sleep_sec)

    return total_trades, total_candles, total_stats, errors


def fetch_segment(start_ms: int, end_ms: int,
                  no_trades: bool, no_candles: bool, no_stats: bool):
    """抓取一個時間段的 trades/candles/stats，回傳 (new_t, new_c, new_s)"""
    new_t = 0
    if not no_trades:
        trades = fetch_trades_paginated(start_ms, end_ms)
        new_t = append_trades_jsonl(trades)
        time.sleep(0.5)

    new_c = 0
    if not no_candles:
        candle_key, candles = fetch_candles(start_ms, end_ms)
        new_c = append_candles_csv(candle_key, candles)
        time.sleep(0.5)

    new_s = 0
    if not no_stats:
        stats = fetch_funding_stats(start_ms, end_ms)
        new_s = append_funding_stats_csv(stats)

    return new_t, new_c, new_s


def main():
    parser = argparse.ArgumentParser(description="Bitfinex fUSD 歷史回補")
    parser.add_argument("--auto", action="store_true",
                        help="自動模式：偵測最早資料，從那裡往前抓（新→舊）")
    parser.add_argument("--origin", dest="origin_date",
                        help="--auto 的最遠追溯日期（預設 2016-07-28）")
    parser.add_argument("--from", dest="from_date", help="起始日期 YYYY-MM-DD")
    parser.add_argument("--to", dest="to_date", help="結束日期 YYYY-MM-DD（預設現在）")
    parser.add_argument("--days", type=float, help="回補最近 N 天（與 --from 二擇一，可用小數）")
    parser.add_argument("--window-min", type=int, default=360,
                        help="每段窗口分鐘數（預設 360 = 6 小時）")
    parser.add_argument("--sleep", type=float, default=3.0, help="每段間隔秒數（預設 3.0）")
    parser.add_argument("--no-trades", action="store_true", help="跳過 trades")
    parser.add_argument("--no-candles", action="store_true", help="跳過 candles")
    parser.add_argument("--no-stats", action="store_true", help="跳過 funding stats")
    args = parser.parse_args()

    now_ms = int(time.time() * 1000)

    if args.auto:
        origin_ms = parse_date(args.origin_date) if args.origin_date else ORIGIN_MS

        # 先檢查 checkpoint
        ckpt = load_checkpoint()
        if ckpt and ckpt.get("direction") == "reverse" and ckpt.get("origin_ms") == origin_ms:
            end_ms = ckpt["cursor_ms"]
            if end_ms <= origin_ms:
                print("Checkpoint 顯示已完成回補。")
                return
            print(f"從 checkpoint 續跑: {ms_to_iso(end_ms)[:10]}")
        else:
            # 偵測現有最早資料
            earliest = find_earliest_data_ms()
            if earliest is None:
                end_ms = now_ms
                print(f"未找到現有資料，將從現在往前回補到 {ms_to_iso(origin_ms)[:10]}")
            else:
                end_ms = earliest
                print(f"偵測到最早資料: {ms_to_iso(earliest)[:10]}")
                if earliest <= origin_ms:
                    print("已達最遠追溯點，無需回補。")
                    return

        hours = (end_ms - origin_ms) / 3600000
        window_ms = args.window_min * 60 * 1000
        est_segments = (end_ms - origin_ms + window_ms - 1) // window_ms
        est_min = est_segments * (args.sleep + 1.5) / 60
        print(f"待回補: {hours:.0f} 小時 ({est_segments} 段), 預估耗時: ~{est_min:.0f} 分鐘")

        t, c, s, e = run_backfill_reverse(
            origin_ms, end_ms, args.window_min, args.sleep,
            args.no_trades, args.no_candles, args.no_stats,
        )
        print("---")
        print(f"完成! trades={t}, candles={c}, stats={s}, errors={e}")

    elif args.days:
        start_ms = int(now_ms - args.days * 24 * 60 * 60 * 1000)
        end_ms = parse_date(args.to_date) if args.to_date else now_ms
        t, c, s, e = run_backfill_forward(
            start_ms, end_ms, args.window_min, args.sleep,
            args.no_trades, args.no_candles, args.no_stats,
        )
        print("---")
        print(f"完成! trades={t}, candles={c}, stats={s}, errors={e}")

    elif args.from_date:
        start_ms = parse_date(args.from_date)
        end_ms = parse_date(args.to_date) if args.to_date else now_ms
        t, c, s, e = run_backfill_forward(
            start_ms, end_ms, args.window_min, args.sleep,
            args.no_trades, args.no_candles, args.no_stats,
        )
        print("---")
        print(f"完成! trades={t}, candles={c}, stats={s}, errors={e}")

    else:
        parser.error("需要指定 --auto、--days 或 --from")


if __name__ == "__main__":
    main()
