#!/usr/bin/env python3
"""
AFEE TRADER BOT - Advanced AI Trading Signal Bot
Strategies: Stop Hunter | Hammer Fibonacci | HB | Trigger Fibonacci
"""

import asyncio
import aiohttp
import json
import time
import logging
import os
import sqlite3
import shutil
from datetime import datetime, timezone
from typing import Optional
import math
import io
import base64

# ─── CONFIG ───────────────────────────────────────────────────────────────────
BOT_VERSION = "3.4.0"
TELEGRAM_TOKEN = "7166787350:AAGFiTzSYxj3f7729czsqTzO3Fiwe1pimQw"
TELEGRAM_CHAT_ID = "-1002532379243"
BINANCE_BASE = "https://api.binance.com/api/v3"

# Timeframe map  (Binance interval string)
TF = {
    "1m": "1m", "3m": "3m", "5m": "5m",
    "15m": "15m", "1h": "1h", "4h": "4h",
}

BLACKLIST_FILE   = "blacklist.json"
TRADES_FILE      = "trades.json"     # فقط برای بکاپ/مهاجرت دیتای قدیمی؛ منبع اصلی دیتابیس است
DB_FILE          = "afee_trader.db"  # دیتابیس SQLite اصلی (سیگنال‌ها، نتایج، آمار)
SIGNAL_COOLDOWN  = 3600   # seconds between signals for same coin/strategy
SCAN_INTERVAL    = 60     # seconds to wait after each full scan
TOP_N_COINS      = 100    # تعداد نهایی نمادهایی که اسکن می‌شوند (بعد از فیلتر کیفیت)
QUALITY_POOL_SIZE = 250   # تعداد نامزدهای اولیه بر اساس حجم، قبل از اعمال فیلتر کیفیت (مورد ۶)
PARALLEL_WORKERS = 15     # coins scanned simultaneously

# ─── BACKTEST ENGINE CONFIG ────────────────────────────────────────────────────
BACKTEST_BATCH_SIZE    = 25   # نمادهای موازی در هر batch بک‌تست
BACKTEST_MAX_QUEUE     = 3    # حداکثر jobهای بک‌تست در صف (از overload جلوگیری می‌کند)
BACKTEST_CACHE_TTL     = 300  # ثانیه — نتایج بک‌تست تکراری از cache تحویل داده می‌شوند

# ─── SESSION/ADX FILTER CONSTANTS ─────────────────────────────────────────────
ADX_THRESHOLD_DEFAULT     = 25.0
ATR_MULTIPLIER_DEFAULT    = 2.2
SESSION_FILTERS_DEFAULT   = {"london": True, "ny": True, "asian": True}

# ─── PROXY (Hiddify Mixed Port) ───────────────────────────────────────────────
PROXY = "http://127.0.0.1:12334"

# ─── LOGGING (UTF-8 safe for Windows CMD) ────────────────────────────────────
import sys
log = logging.getLogger("AFEE")
log.setLevel(logging.INFO)
fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
# File handler — always UTF-8
fh = logging.FileHandler("afee_bot.log", encoding="utf-8")
fh.setFormatter(fmt)
log.addHandler(fh)
# Console handler — safe encoding
sh = logging.StreamHandler(sys.stdout)
sh.setFormatter(fmt)
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass
log.addHandler(sh)

# ─── BLACKLIST ─────────────────────────────────────────────────────────────────
def load_blacklist() -> set:
    if os.path.exists(BLACKLIST_FILE):
        try:
            with open(BLACKLIST_FILE, encoding="utf-8") as f:
                return set(json.load(f))
        except Exception as e:
            log.error(f"⚠️ blacklist.json خراب بود، خالی برگردانده شد: {e}")
            try:
                os.replace(BLACKLIST_FILE, BLACKLIST_FILE + ".corrupted")
            except Exception:
                pass
    return set()

def save_blacklist(bl: set):
    tmp_file = BLACKLIST_FILE + ".tmp"
    try:
        with open(tmp_file, "w", encoding="utf-8") as f:
            json.dump(list(bl), f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_file, BLACKLIST_FILE)
    except Exception as e:
        log.error(f"خطا در ذخیره blacklist.json: {e}")

def add_to_blacklist(symbol: str):
    bl = load_blacklist()
    bl.add(symbol.upper())
    save_blacklist(bl)
    log.info(f"Added {symbol} to blacklist")

# ─── TRADE LOG (برای اتو بک‌تست و گزارش روزانه) ────────────────────────────────
# ─── DATABASE (SQLite) ────────────────────────────────────────────────────────
# جایگزین کامل ذخیره‌سازی JSON برای معاملات با دیتابیس SQLite.
# Schema: signals (سیگنال‌های صادرشده) + results (نتیجه نهایی هر سیگنال، 1-به-1 با signals).
# تمام توابع زیر همان نام و رفتار قبلی (load_trades/save_trades/log_trade/...) را حفظ کرده‌اند
# تا بقیه کد بدون تغییر کار کند؛ فقط لایه ذخیره‌سازی از JSON به SQLite تغییر کرده است.

_db_lock = asyncio.Lock()  # جلوگیری از نوشتن هم‌زمان چند coroutine روی دیتابیس

def get_db_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")  # نوشتن امن‌تر و هم‌زمان‌تر
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn

def init_database():
    """ساخت جدول‌های دیتابیس در صورت عدم وجود. اجرای چندباره این تابع بی‌خطر است (IF NOT EXISTS)."""
    conn = get_db_connection()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS signals (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol      TEXT    NOT NULL,
                direction   TEXT    NOT NULL,
                strategy    TEXT    NOT NULL,
                entry       REAL    NOT NULL,
                stop        REAL    NOT NULL,
                tp1         REAL    NOT NULL,
                tp2         REAL    NOT NULL,
                score       INTEGER,
                opened_at   TEXT    NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS results (
                signal_id    INTEGER PRIMARY KEY REFERENCES signals(id),
                status       TEXT    NOT NULL DEFAULT 'PENDING',
                -- Patch #6 states: PENDING|ENTERED|TP1_HIT|TP2_HIT|SL_HIT|EXPIRED|MISSED
                -- Legacy states kept for compatibility: OPEN|TP1|TP2|SL|EXPIRED
                entered_at   TEXT,
                tp1_hit      INTEGER NOT NULL DEFAULT 0,   -- Patch #4: 0/1 independent flag
                tp2_hit      INTEGER NOT NULL DEFAULT 0,   -- Patch #4: 0/1 independent flag
                tp1_hit_at   TEXT,
                tp2_hit_at   TEXT,
                closed_at    TEXT,
                close_price  REAL,
                pnl_percent  REAL,
                rr_multiple  REAL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS strategy_weights (
                strategy     TEXT    PRIMARY KEY,
                weight       REAL    NOT NULL DEFAULT 1.0,
                updated_at   TEXT
            )
        """)
        # ── جدید: ذخیره نتایج بک‌تست تفصیلی (هر trade یک ردیف) ──
        conn.execute("""
            CREATE TABLE IF NOT EXISTS backtest_trades (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id        TEXT    NOT NULL,
                symbol        TEXT    NOT NULL,
                strategy      TEXT    NOT NULL,
                direction     TEXT    NOT NULL,
                entry         REAL    NOT NULL,
                stop          REAL    NOT NULL,
                tp1           REAL    NOT NULL,
                tp2           REAL    NOT NULL,
                entry_ts      TEXT,
                exit_ts       TEXT,
                outcome       TEXT,   -- TP1|TP2|SL|OPEN
                pnl_percent   REAL,
                rr_multiple   REAL,
                trend_state   TEXT,
                adx_value     REAL,
                ema200_slope  TEXT,
                vol_regime    TEXT,
                session       TEXT,
                entry_reason  TEXT,
                created_at    TEXT    NOT NULL
            )
        """)
        # ── جدید: ذخیره job metadata برای صف بک‌تست ──
        conn.execute("""
            CREATE TABLE IF NOT EXISTS backtest_jobs (
                job_id      TEXT    PRIMARY KEY,
                status      TEXT    NOT NULL DEFAULT 'QUEUED',  -- QUEUED|RUNNING|DONE|FAILED
                requested_by TEXT,
                params      TEXT,   -- JSON: timerange, symbols, strategies
                created_at  TEXT    NOT NULL,
                completed_at TEXT,
                result_summary TEXT  -- JSON: total, wins, losses, pnl
            )
        """)
        # ── جدید: پیکربندی فیلترها (قابل تغییر در Runtime) ──
        conn.execute("""
            CREATE TABLE IF NOT EXISTS filter_config (
                key         TEXT    PRIMARY KEY,
                value       TEXT    NOT NULL,
                updated_at  TEXT
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_symbol_strategy ON signals(symbol, strategy)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_results_status ON results(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_results_closed_at ON results(closed_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_bt_trades_job ON backtest_trades(job_id)")
        conn.commit()

        # ── v3.1 schema migration: add new columns to existing results table (idempotent) ──
        existing_cols = {row[1] for row in conn.execute("PRAGMA table_info(results)").fetchall()}
        migrations = [
            ("tp1_hit",    "ALTER TABLE results ADD COLUMN tp1_hit    INTEGER NOT NULL DEFAULT 0"),
            ("tp2_hit",    "ALTER TABLE results ADD COLUMN tp2_hit    INTEGER NOT NULL DEFAULT 0"),
            ("entered_at", "ALTER TABLE results ADD COLUMN entered_at TEXT"),
            ("tp1_hit_at", "ALTER TABLE results ADD COLUMN tp1_hit_at TEXT"),
            ("tp2_hit_at", "ALTER TABLE results ADD COLUMN tp2_hit_at TEXT"),
        ]
        for col_name, sql in migrations:
            if col_name not in existing_cols:
                try:
                    conn.execute(sql)
                    log.info(f"DB migration: added column results.{col_name}")
                except Exception as e:
                    log.warning(f"DB migration skipped results.{col_name}: {e}")

        # Migrate legacy OPEN statuses to PENDING so state machine works correctly
        try:
            conn.execute("UPDATE results SET status='PENDING' WHERE status='OPEN'")
            # FIX v3.4: اگر سیستم آپگرید شد، مطمئن شو TP1_TOUCHSL در جدول قابل ذخیره است
            # (SQLite text column هر مقداری می‌گیرد — نیاز به ALTER TABLE نیست)
        except Exception:
            pass

        conn.commit()
    finally:
        conn.close()

def _migrate_json_trades_to_db():
    """مهاجرت یک‌باره دیتای قدیمی trades.json (در صورت وجود) به دیتابیس SQLite.
    بعد از مهاجرت موفق، فایل JSON برای امنیت به trades.json.migrated تغییرنام می‌یابد
    تا دیتای قدیمی هیچ‌وقت گم نشود، ولی این تابع دوباره روی آن اجرا نشود."""
    if not os.path.exists(TRADES_FILE):
        return
    try:
        with open(TRADES_FILE, encoding="utf-8") as f:
            old_trades = json.load(f)
        if not old_trades:
            os.replace(TRADES_FILE, TRADES_FILE + ".migrated")
            return

        conn = get_db_connection()
        migrated = 0
        try:
            for t in old_trades:
                cur = conn.execute(
                    "INSERT INTO signals (symbol, direction, strategy, entry, stop, tp1, tp2, score, opened_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (t["symbol"], t["direction"], t["strategy"], t["entry"], t["stop"],
                     t["tp1"], t["tp2"], t.get("score"), t["opened_at"])
                )
                signal_id = cur.lastrowid
                rr = None
                if t.get("pnl_percent") is not None and t.get("entry") and t.get("stop"):
                    risk_pct = abs(t["entry"] - t["stop"]) / t["entry"] * 100
                    rr = (t["pnl_percent"] / risk_pct) if risk_pct > 0 else None
                conn.execute(
                    "INSERT INTO results (signal_id, status, closed_at, close_price, pnl_percent, rr_multiple) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (signal_id, t.get("status", "OPEN"), t.get("closed_at"),
                     t.get("close_price"), t.get("pnl_percent"), rr)
                )
                migrated += 1
            conn.commit()
        finally:
            conn.close()

        os.replace(TRADES_FILE, TRADES_FILE + ".migrated")
        log.info(f"✅ مهاجرت {migrated} رکورد قدیمی از trades.json به دیتابیس SQLite انجام شد.")
    except Exception as e:
        log.error(f"⚠️ خطا در مهاجرت trades.json به دیتابیس: {e}")
        try:
            os.replace(TRADES_FILE, TRADES_FILE + ".migration_failed")
        except Exception:
            pass

def backup_database_to_json():
    """خروجی JSON از کل دیتابیس برای بکاپ (طبق درخواست: JSON فقط برای بکاپ، نه منبع اصلی)."""
    conn = get_db_connection()
    try:
        rows = conn.execute("""
            SELECT s.id, s.symbol, s.direction, s.strategy, s.entry, s.stop, s.tp1, s.tp2,
                   s.score, s.opened_at, r.status, r.closed_at, r.close_price, r.pnl_percent, r.rr_multiple
            FROM signals s LEFT JOIN results r ON r.signal_id = s.id
            ORDER BY s.id
        """).fetchall()
        data = [dict(row) for row in rows]
    finally:
        conn.close()

    tmp_file = TRADES_FILE + ".backup.tmp"
    backup_file = TRADES_FILE + ".backup"
    try:
        with open(tmp_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_file, backup_file)
    except Exception as e:
        log.error(f"خطا در ذخیره بکاپ JSON: {e}")

def _row_to_trade_dict(row: sqlite3.Row) -> dict:
    """یک ردیف join‌شده از signals+results را به همان شکل دیکشنری قدیمی trades.json تبدیل می‌کند
    تا کد بالادست (گزارش روزانه و غیره) بدون تغییر کار کند.
    Patch #4/#6: نگاشت state جدید به compat status قدیمی برای کدهای موجود."""
    raw_status = row["status"] or "PENDING"
    # Normalize new states to legacy-compatible values for existing report code
    _status_compat = {
        "PENDING": "OPEN", "ENTERED": "OPEN",
        "TP1_HIT": "TP1", "TP2_HIT": "TP2",
        "TP1_TOUCHSL": "TP1",  # FIX v3.4: TP1 زده شد سپس Break Even → نتیجه = TP1
        "SL_HIT": "SL", "MISSED": "MISSED",
        "EXPIRED": "EXPIRED", "OPEN": "OPEN",
        "TP1": "TP1", "TP2": "TP2", "SL": "SL",
    }
    compat_status = _status_compat.get(raw_status, raw_status)
    return {
        "symbol": row["symbol"], "direction": row["direction"], "strategy": row["strategy"],
        "entry": row["entry"], "stop": row["stop"], "tp1": row["tp1"], "tp2": row["tp2"],
        "score": row["score"], "opened_at": row["opened_at"],
        "status": compat_status, "raw_status": raw_status,
        "tp1_hit": row["tp1_hit"] if "tp1_hit" in row.keys() else 0,
        "tp2_hit": row["tp2_hit"] if "tp2_hit" in row.keys() else 0,
        "closed_at": row["closed_at"], "close_price": row["close_price"],
        "pnl_percent": row["pnl_percent"], "rr_multiple": row["rr_multiple"], "id": row["id"],
    }

def load_trades() -> list:
    """تمام سیگنال‌ها + نتایج‌شان را از دیتابیس می‌خواند (جایگزین خوانش trades.json قدیمی)."""
    conn = get_db_connection()
    try:
        rows = conn.execute("""
            SELECT s.id, s.symbol, s.direction, s.strategy, s.entry, s.stop, s.tp1, s.tp2,
                   s.score, s.opened_at, r.status, r.tp1_hit, r.tp2_hit,
                   r.closed_at, r.close_price, r.pnl_percent, r.rr_multiple
            FROM signals s LEFT JOIN results r ON r.signal_id = s.id
            ORDER BY s.id
        """).fetchall()
        return [_row_to_trade_dict(row) for row in rows]
    finally:
        conn.close()

def save_trades(trades: list):
    """برای حفظ سازگاری با کدهای قدیمی نگه‌داشته شده؛ امروز دیگر مستقیم استفاده نمی‌شود
    چون نوشتن از طریق log_trade/update_trade_outcomes با دیتابیس انجام می‌شود.
    در صورت صدا زده شدن، فقط یک بکاپ JSON از وضعیت فعلی می‌سازد."""
    backup_database_to_json()

def log_trade(result: dict):
    """هر سیگنال صادر شده را در دیتابیس ثبت می‌کند (signals + ردیف اولیه PENDING در results)."""
    conn = get_db_connection()
    try:
        cur = conn.execute(
            "INSERT INTO signals (symbol, direction, strategy, entry, stop, tp1, tp2, score, opened_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (result["symbol"], result["direction"], result["strategy"], result["entry"],
             result["stop"], result["tp1"], result["tp2"], result.get("score"),
             datetime.now(timezone.utc).isoformat())
        )
        signal_id = cur.lastrowid
        conn.execute(
            "INSERT INTO results (signal_id, status) VALUES (?, 'PENDING')",
            (signal_id,)
        )
        conn.commit()
    finally:
        conn.close()

async def update_trade_outcomes(session: aiohttp.ClientSession):
    """
    Patch #4 + #6: Full trade state machine with independent TP1/TP2 tracking.

    States:
      PENDING  → signal issued, price has not reached Entry yet
      ENTERED  → price touched Entry
      TP1_HIT  → TP1 reached (after ENTERED)
      TP2_HIT  → TP2 reached (implies TP1 also hit)
      SL_HIT   → stop reached (after ENTERED)
      EXPIRED  → 24h passed without Entry being touched (Missed)
      MISSED   → price hit TP/SL before ever touching Entry (not counted in stats)

    Legacy status aliases kept for backward compat: TP1=TP1_HIT, TP2=TP2_HIT, SL=SL_HIT.
    """
    async with _db_lock:
        conn = get_db_connection()
        try:
            open_rows = conn.execute("""
                SELECT s.id, s.symbol, s.direction, s.entry, s.stop, s.tp1, s.tp2, s.opened_at,
                       r.status, r.tp1_hit, r.tp2_hit, r.entered_at
                FROM signals s JOIN results r ON r.signal_id = s.id
                WHERE r.status IN ('PENDING','OPEN','ENTERED','TP1_HIT')
            """).fetchall()
        finally:
            conn.close()

        if not open_rows:
            return load_trades()

        now = datetime.now(timezone.utc)
        updates = []  # (signal_id, new_status, entered_at, tp1_hit, tp2_hit, tp1_hit_at, tp2_hit_at,
                      #  closed_at, close_price, pnl_pct, rr)

        for row in open_rows:
            opened = datetime.fromisoformat(row["opened_at"])
            age_hours = (now - opened).total_seconds() / 3600
            status    = row["status"]
            tp1_hit   = row["tp1_hit"]
            tp2_hit   = row["tp2_hit"]
            entered_at= row["entered_at"]

            try:
                url = f"{BINANCE_BASE}/ticker/price"
                async with session.get(url, params={"symbol": row["symbol"]}, proxy=PROXY,
                                       timeout=aiohttp.ClientTimeout(total=10)) as r:
                    data = await r.json()
                current_price = float(data["price"])
            except Exception as e:
                log.debug(f"Price fetch failed for {row['symbol']}: {e}")
                continue

            entry, direction = row["entry"], row["direction"]
            pnl_pct  = ((current_price - entry) / entry * 100) if direction == "BUY" \
                       else ((entry - current_price) / entry * 100)
            risk_pct = abs(entry - row["stop"]) / entry * 100
            rr       = (pnl_pct / risk_pct) if risk_pct > 0 else 0

            price_at_entry = (direction == "BUY" and current_price >= entry) or \
                             (direction == "SELL" and current_price <= entry)
            hit_tp2 = (direction == "BUY" and current_price >= row["tp2"]) or \
                      (direction == "SELL" and current_price <= row["tp2"])
            hit_tp1 = (direction == "BUY" and current_price >= row["tp1"]) or \
                      (direction == "SELL" and current_price <= row["tp1"])
            hit_sl  = (direction == "BUY" and current_price <= row["stop"]) or \
                      (direction == "SELL" and current_price >= row["stop"])

            now_str = now.isoformat()
            new_status   = None
            new_entered  = entered_at
            new_tp1_hit  = tp1_hit
            new_tp2_hit  = tp2_hit
            new_tp1_at   = None
            new_tp2_at   = None
            closed_at    = None
            close_price  = None
            final_pnl    = None
            final_rr     = None

            if status in ("PENDING", "OPEN"):
                # Has entry been touched?
                if price_at_entry:
                    new_entered = now_str
                    new_status = "ENTERED"
                elif (hit_tp1 or hit_tp2 or hit_sl):
                    # Patch #6: Price reached TP/SL without ever hitting Entry → MISSED
                    new_status = "MISSED"
                    closed_at  = now_str
                elif age_hours >= 24:
                    new_status = "EXPIRED"
                    closed_at  = now_str

            elif status == "ENTERED":
                if hit_tp2:
                    # Patch #4: TP2 hit → TP1 implicitly also hit
                    new_tp1_hit = 1
                    new_tp2_hit = 1
                    new_tp1_at  = now_str
                    new_tp2_at  = now_str
                    new_status  = "TP2_HIT"
                    closed_at   = now_str
                    close_price = current_price
                    tp2_pnl     = ((row["tp2"] - entry) / entry * 100) if direction == "BUY" \
                                  else ((entry - row["tp2"]) / entry * 100)
                    tp2_rr      = (tp2_pnl / risk_pct) if risk_pct > 0 else 0
                    final_pnl   = round(tp2_pnl, 2)
                    final_rr    = round(tp2_rr, 3)
                elif hit_tp1 and not tp1_hit:
                    # Patch #4: TP1 touched independently
                    new_tp1_hit = 1
                    new_tp1_at  = now_str
                    new_status  = "TP1_HIT"
                    # Don't close yet — wait for TP2 or SL
                elif hit_sl:
                    new_status  = "SL_HIT"
                    closed_at   = now_str
                    close_price = current_price
                    final_pnl   = round(pnl_pct, 2)
                    final_rr    = round(rr, 3)
                elif age_hours >= 24:
                    new_status = "EXPIRED"
                    closed_at  = now_str

            elif status == "TP1_HIT":
                if hit_tp2:
                    new_tp2_hit = 1
                    new_tp2_at  = now_str
                    new_status  = "TP2_HIT"
                    closed_at   = now_str
                    close_price = current_price
                    tp2_pnl     = ((row["tp2"] - entry) / entry * 100) if direction == "BUY" \
                                  else ((entry - row["tp2"]) / entry * 100)
                    tp2_rr      = (tp2_pnl / risk_pct) if risk_pct > 0 else 0
                    final_pnl   = round(tp2_pnl, 2)
                    final_rr    = round(tp2_rr, 3)
                elif hit_sl:
                    # FIX v3.4: TP1 زده شد، سپس قیمت به SL (Break Even) برگشت → TP1_TOUCHSL
                    # این یعنی معامله‌گر TP1 گرفت و SL را به Entry آورد (نه ضرر کامل)
                    new_status  = "TP1_TOUCHSL"
                    closed_at   = now_str
                    close_price = current_price
                    tp1_pnl     = ((row["tp1"] - entry) / entry * 100) if direction == "BUY" \
                                  else ((entry - row["tp1"]) / entry * 100)
                    tp1_rr      = (tp1_pnl / risk_pct) if risk_pct > 0 else 0
                    final_pnl   = round(tp1_pnl, 2)  # سود TP1 (نه ضرر — Break Even)
                    final_rr    = round(tp1_rr, 3)
                elif age_hours >= 24:
                    new_status = "EXPIRED"
                    closed_at  = now_str

            if new_status and new_status != status:
                updates.append((
                    row["id"], new_status, new_entered,
                    new_tp1_hit, new_tp2_hit, new_tp1_at, new_tp2_at,
                    closed_at, close_price, final_pnl, final_rr
                ))

        if updates:
            conn = get_db_connection()
            try:
                for u in updates:
                    conn.execute(
                        "UPDATE results SET status=?, entered_at=?, tp1_hit=?, tp2_hit=?, "
                        "tp1_hit_at=?, tp2_hit_at=?, closed_at=?, close_price=?, "
                        "pnl_percent=?, rr_multiple=? WHERE signal_id=?",
                        (u[1], u[2], u[3], u[4], u[5], u[6], u[7], u[8], u[9], u[10], u[0])
                    )
                conn.commit()
            finally:
                conn.close()

        return load_trades()

def get_trades_for_report(trades: list, report_date_str: str) -> list:
    """معاملاتی که در همان روز (UTC) بسته شده‌اند، برای گزارش روزانه.
    FIX v3.2: OPEN / PENDING / ENTERED / TP1_HIT (هنوز باز هستند) را exclude می‌کند.
    MISSED نیز از گزارش روزانه حذف می‌شود چون هرگز Entry نزده است.
    """
    # وضعیت‌هایی که هنوز باز هستند و نباید در گزارش باشند
    # FIX v3.4: TP1_TOUCHSL بسته شده است → در گزارش ظاهر می‌شود (نه OPEN)
    OPEN_STATUSES = {"OPEN", "PENDING", "ENTERED", "TP1_HIT", "TP1"}
    result = []
    for t in trades:
        if t["status"] in OPEN_STATUSES:
            continue
        if t.get("raw_status") in {"PENDING", "ENTERED", "TP1_HIT"}:
            continue  # double-check روی raw_status
        if not t.get("closed_at"):
            continue
        closed_date = t["closed_at"][:10]
        if closed_date == report_date_str:
            result.append(t)
    return result

def build_daily_report(trades: list, report_date_str: str) -> str:
    """متن گزارش روزانه را می‌سازد: درصد سود/ضرر هر ارز، مجموع، و وین‌ریت."""
    todays = get_trades_for_report(trades, report_date_str)

    if not todays:
        return (
            f"📊 <b>گزارش روزانه AFEE TRADER</b>\n"
            f"🗓 تاریخ: {report_date_str}\n\n"
            f"امروز هیچ معامله‌ای بسته نشد."
        )

    lines = [f"📊 <b>گزارش روزانه AFEE TRADER</b>", f"🗓 تاریخ: {report_date_str}", ""]

    total_pnl = 0.0
    win_count = 0
    closed_count = 0  # فقط TP/SL را برای وین‌ریت حساب می‌کنیم؛ EXPIRED را نه

    # FIX v3.2: include both old (TP1/TP2/SL) and new (TP1_HIT/TP2_HIT/SL_HIT) status names
    WIN_STATUSES = {"TP1", "TP2", "TP1_HIT", "TP2_HIT", "TP1_TOUCHSL"}  # FIX v3.4
    LOSS_STATUSES = {"SL", "SL_HIT"}
    CLOSED_STATUSES = WIN_STATUSES | LOSS_STATUSES

    STATUS_FA = {
        "TP1": "TP1 خورد ✅", "TP1_HIT": "TP1 خورد ✅",
        "TP1_TOUCHSL": "TP1 خورد، سپس Break Even (Touch SL) 🔄",  # FIX v3.4
        "TP2": "TP2 خورد ✅✅", "TP2_HIT": "TP2 خورد ✅✅",
        "SL": "استاپ خورد ❌", "SL_HIT": "استاپ خورد ❌",
        "EXPIRED": "بدون نتیجه (۲۴ ساعت گذشت) ⏳",
        "MISSED": "از دست رفت (قیمت Entry نزد) ⚠️",
    }

    for t in todays:
        raw_st = t.get("raw_status", t["status"])
        if raw_st == "MISSED":
            continue  # FIX v3.2: MISSED هرگز وارد آمار نمی‌شود
        pnl = t.get("pnl_percent", 0) or 0
        total_pnl += pnl
        sign = "🟢+" if pnl >= 0 else "🔴"
        status_fa = STATUS_FA.get(raw_st, STATUS_FA.get(t["status"], t["status"]))
        opened_date = t["opened_at"][:10]
        date_note = f" | صدور: {opened_date}" if opened_date != report_date_str else ""
        lines.append(f"💎 {t['symbol']} ({t['strategy']}) — {sign}{abs(pnl):.2f}% — {status_fa}{date_note}")

        if raw_st in CLOSED_STATUSES or t["status"] in CLOSED_STATUSES:
            closed_count += 1
            if raw_st in WIN_STATUSES or t["status"] in WIN_STATUSES:
                win_count += 1

    win_rate = (win_count / closed_count * 100) if closed_count > 0 else 0
    total_sign = "🟢+" if total_pnl >= 0 else "🔴"

    lines.append("")
    lines.append("━━━━━━━━━━━━━━━")
    lines.append(f"📈 <b>مجموع سود/ضرر:</b> {total_sign}{abs(total_pnl):.2f}%")
    lines.append(f"🎯 <b>وین‌ریت (TP/SL):</b> {win_rate:.1f}% ({win_count} از {closed_count})")
    lines.append(f"📦 <b>تعداد کل سیگنال‌های بسته‌شده:</b> {len(todays)}")

    return "\n".join(lines)

# ─── ADVANCED ANALYTICS (/stats) ───────────────────────────────────────────────
def calc_strategy_stats(strategy: str, days: Optional[int] = None) -> dict:
    """
    آمار کامل یک استراتژی را از دیتابیس محاسبه می‌کند:
    winrate, loss rate, TP1 rate, TP2 rate, average RR, expectancy,
    max drawdown, profit factor, total trades.
    اگر days مشخص شود، فقط معاملات بسته‌شده در آن بازه (آخرین N روز) لحاظ می‌شوند.
    """
    conn = get_db_connection()
    try:
        # FIX v3.2: exclude all open/pending states from stats; فقط معاملات واقعاً بسته‌شده
        query = """
            SELECT r.status, r.pnl_percent, r.rr_multiple, r.closed_at
            FROM signals s JOIN results r ON r.signal_id = s.id
            WHERE s.strategy = ?
              AND r.status NOT IN ('OPEN','PENDING','ENTERED','TP1_HIT')
              AND r.closed_at IS NOT NULL
              -- FIX v3.4: TP1_TOUCHSL بسته شده و در آمار حساب می‌شود (به عنوان TP1)
        """
        params = [strategy]
        if days is not None:
            cutoff = (datetime.now(timezone.utc) - __import__('datetime').timedelta(days=days)).isoformat()
            query += " AND r.closed_at >= ?"
            params.append(cutoff)
        rows = conn.execute(query, params).fetchall()
    finally:
        conn.close()

    total = len(rows)
    if total == 0:
        return {
            "strategy": strategy, "total_trades": 0, "winrate": 0, "loss_rate": 0,
            "tp1_rate": 0, "tp2_rate": 0, "avg_rr": 0, "expectancy": 0,
            "max_drawdown": 0, "profit_factor": 0,
        }

    # Patch #6: MISSED trades are excluded from performance analysis
    rows = [r for r in rows if r["status"] != "MISSED"]
    total = len(rows)
    if total == 0:
        return {
            "strategy": strategy, "total_trades": 0, "winrate": 0, "loss_rate": 0,
            "tp1_rate": 0, "tp2_rate": 0, "avg_rr": 0, "expectancy": 0,
            "max_drawdown": 0, "profit_factor": 0,
        }

    # FIX v3.4: TP1_TOUCHSL = win (TP1 زده شد؛ SL بعدی = Break Even، نه ضرر)
    wins = [r for r in rows if r["status"] in ("TP1", "TP2", "TP1_HIT", "TP2_HIT", "TP1_TOUCHSL")]
    losses = [r for r in rows if r["status"] in ("SL", "SL_HIT")]
    tp1_count = len([r for r in rows if r["status"] in ("TP1", "TP1_HIT", "TP1_TOUCHSL")])
    tp2_count = len([r for r in rows if r["status"] in ("TP2", "TP2_HIT")])

    decisive = len(wins) + len(losses)  # EXPIRED را از وین‌ریت/لاس‌ریت کنار می‌گذاریم
    winrate = (len(wins) / decisive * 100) if decisive > 0 else 0
    loss_rate = (len(losses) / decisive * 100) if decisive > 0 else 0
    tp1_rate = (tp1_count / total * 100) if total > 0 else 0
    tp2_rate = (tp2_count / total * 100) if total > 0 else 0

    rr_values = [r["rr_multiple"] for r in rows if r["rr_multiple"] is not None]
    avg_rr = sum(rr_values) / len(rr_values) if rr_values else 0

    # Expectancy = (winrate × avg_win_R) - (lossrate × avg_loss_R)
    win_rr = [r["rr_multiple"] for r in wins if r["rr_multiple"] is not None]
    loss_rr = [abs(r["rr_multiple"]) for r in losses if r["rr_multiple"] is not None]
    avg_win_r = sum(win_rr) / len(win_rr) if win_rr else 0
    avg_loss_r = sum(loss_rr) / len(loss_rr) if loss_rr else 0
    win_prob = (len(wins) / decisive) if decisive > 0 else 0
    loss_prob = (len(losses) / decisive) if decisive > 0 else 0
    expectancy = (win_prob * avg_win_r) - (loss_prob * avg_loss_r)

    # Profit Factor = مجموع سود / مجموع ضرر (بر اساس pnl_percent)
    gross_profit = sum(r["pnl_percent"] for r in rows if r["pnl_percent"] and r["pnl_percent"] > 0)
    gross_loss = abs(sum(r["pnl_percent"] for r in rows if r["pnl_percent"] and r["pnl_percent"] < 0))
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (gross_profit if gross_profit > 0 else 0)

    # Max Drawdown — بر اساس دنباله pnl_percent به ترتیب بسته‌شدن (equity curve فرضی جمعی)
    rows_sorted = sorted(rows, key=lambda r: r["closed_at"] or "")
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for r in rows_sorted:
        pnl = r["pnl_percent"] or 0
        equity += pnl
        peak = max(peak, equity)
        dd = peak - equity
        max_dd = max(max_dd, dd)

    return {
        "strategy": strategy, "total_trades": total,
        "winrate": round(winrate, 1), "loss_rate": round(loss_rate, 1),
        "tp1_rate": round(tp1_rate, 1), "tp2_rate": round(tp2_rate, 1),
        "avg_rr": round(avg_rr, 2), "expectancy": round(expectancy, 2),
        "max_drawdown": round(max_dd, 2), "profit_factor": round(profit_factor, 2),
    }

def build_stats_report() -> str:
    """گزارش کامل آمار پیشرفته همه استراتژی‌ها برای دستور /stats."""
    lines = ["📈 <b>آمار پیشرفته استراتژی‌ها</b>", ""]

    weights = get_all_strategy_weights()

    for strat_name, _ in STRATEGIES:
        all_time = calc_strategy_stats(strat_name)
        last7 = calc_strategy_stats(strat_name, days=7)
        last30 = calc_strategy_stats(strat_name, days=30)
        w = weights.get(strat_name, 1.0)
        w_tag = f" | ⚖️ Weight: {w:.2f}" if w != 1.0 else ""

        if all_time["total_trades"] == 0:
            lines.append(f"▫️ <b>{strat_name}</b> — هنوز معامله بسته‌شده‌ای ندارد{w_tag}")
            lines.append("")
            continue

        lines.append(f"▫️ <b>{strat_name}</b>{w_tag}")
        lines.append(f"   📦 کل معاملات: {all_time['total_trades']}")
        lines.append(f"   🎯 وین‌ریت: {all_time['winrate']}%  |  ضرر: {all_time['loss_rate']}%")
        lines.append(f"   ✅ TP1 Rate: {all_time['tp1_rate']}%  |  ✅✅ TP2 Rate: {all_time['tp2_rate']}%")
        lines.append(f"   📐 میانگین RR: {all_time['avg_rr']}  |  Expectancy: {all_time['expectancy']}")
        lines.append(f"   📉 Max Drawdown: {all_time['max_drawdown']}%  |  Profit Factor: {all_time['profit_factor']}")
        lines.append(f"   🗓 ۷ روز اخیر: {last7['total_trades']} معامله، وین‌ریت {last7['winrate']}%")
        lines.append(f"   🗓 ۳۰ روز اخیر: {last30['total_trades']} معامله، وین‌ریت {last30['winrate']}%")
        lines.append("")

    return "\n".join(lines)

# ─── REPLAY MODE (دستور /replay SYMBOL) ────────────────────────────────────────
def get_signal_history(symbol: str, limit: int = 10) -> list:
    """آخرین N سیگنال صادرشده برای یک نماد را از دیتابیس برمی‌گرداند (جدیدترین اول)."""
    conn = get_db_connection()
    try:
        rows = conn.execute("""
            SELECT s.id, s.symbol, s.direction, s.strategy, s.entry, s.stop, s.tp1, s.tp2,
                   s.score, s.opened_at, r.status, r.closed_at, r.close_price, r.pnl_percent, r.rr_multiple
            FROM signals s LEFT JOIN results r ON r.signal_id = s.id
            WHERE s.symbol = ?
            ORDER BY s.id DESC
            LIMIT ?
        """, (symbol, limit)).fetchall()
        return [_row_to_trade_dict(row) for row in rows]
    finally:
        conn.close()

STRATEGY_ENTRY_REASON = {
    "Stop Hunter":         "شکست فیک یک ناحیه S/R در تایم پایین، اصلاح، و شکست آخرین کف/سقف موج کوچک در جهت مخالف شکست اول",
    "Hammer Fib (4H)":     "تشکیل کندل چکش/شوتینگ‌استار در تایم ۴ساعته، با ورود در سطح فیبوناچی ۰.۶۱۸ موج بعدی",
    "Hammer Fib (1H)":     "تشکیل کندل چکش/شوتینگ‌استار در تایم ۱ساعته، با ورود در سطح فیبوناچی ۰.۶۱۸ موج بعدی",
    "HB":                  "کندل اسپایک قوی (حجم/بدنه بالا) مخالف روند روی یک ناحیه مهم، ورود در سطح ۰.۵ همان کندل",
    "Trigger Fibonacci":   "سه شرط کندلی متوالی در جهت روند اصلی + ورود در سطح فیبوناچی ۰.۶۱۸ اصلاح",
    "Exhaustion":          "ضعیف‌شدن پیوسته بدنه و حجم کندل‌های هم‌جهت روند نزدیک یک ناحیه مهم (نشانه اتمام قدرت روند)",
    "RSI Divergence":      "بازگشت RSI از ناحیه اشباع خرید(>70)/فروش(<30)، همراه با واکینش احتمالی قیمت-RSI",
}

def build_replay_report(symbol: str) -> str:
    """گزارش Replay Mode: آخرین سیگنال‌های یک نماد، دلیل ورود، استراتژی، امتیاز، و نتیجه نهایی."""
    history = get_signal_history(symbol, limit=10)
    if not history:
        return f"🎬 <b>Replay {symbol}</b>\n\nهیچ سیگنالی برای این نماد در تاریخچه ثبت نشده است."

    status_fa = {
        "OPEN": "⏳ هنوز باز (نتیجه مشخص نشده)",
        "TP1": "✅ TP1 خورد",
        "TP1_TOUCHSL": "🔄 TP1 خورد، سپس Break Even (Touch SL)",  # FIX v3.4
        "TP2": "✅✅ TP2 خورد",
        "SL": "❌ استاپ خورد",
        "EXPIRED": "⏳ بدون نتیجه (۲۴ ساعت گذشت)",
        "MISSED": "⚠️ از دست رفت (قیمت Entry نزد)",  # FIX v3.4
        "PENDING": "⏳ در انتظار Entry",
        "ENTERED": "⏳ وارد شد — در انتظار TP/SL",
    }

    lines = [f"🎬 <b>Replay {symbol}</b>", f"آخرین {len(history)} سیگنال ثبت‌شده:", ""]

    for t in history:
        decimals = 8 if t["entry"] < 0.01 else (4 if t["entry"] < 10 else 2)
        emoji = "🟢" if t["direction"] == "BUY" else "🔴"
        opened_dt = t["opened_at"][:16].replace("T", " ")
        reason = STRATEGY_ENTRY_REASON.get(t["strategy"], "دلیل ورود ثبت نشده")

        lines.append(f"{emoji} <b>{t['strategy']}</b> — {t['direction']}  |  🕒 {opened_dt}")
        lines.append(f"   📝 دلیل ورود: {reason}")
        lines.append(f"   💰 Entry: {t['entry']:.{decimals}f}  |  🛑 Stop: {t['stop']:.{decimals}f}")
        lines.append(f"   🎯 TP1: {t['tp1']:.{decimals}f}  |  🎯 TP2: {t['tp2']:.{decimals}f}")
        lines.append(f"   ⭐️ Score: {t.get('score', '-')}/100")
        lines.append(f"   📌 نتیجه: {status_fa.get(t['status'], t['status'])}")
        if t.get("pnl_percent") is not None:
            sign = "🟢+" if t["pnl_percent"] >= 0 else "🔴"
            lines.append(f"   📊 نتیجه نهایی: {sign}{abs(t['pnl_percent']):.2f}%")
        lines.append("")

    return "\n".join(lines)


MIN_TRADES_FOR_WEIGHTING = 10   # حداقل تعداد معامله بسته‌شده برای اینکه وزن واقعی بگیرد (وگرنه neutral=1.0)
WEIGHT_MIN = 0.5                # حداقل وزن ممکن (پنالتی شدید برای استراتژی ضعیف)
WEIGHT_MAX = 1.5                # حداکثر وزن ممکن (بوست برای استراتژی قوی)

def get_all_strategy_weights() -> dict:
    """تمام وزن‌های ذخیره‌شده را از دیتابیس می‌خواند؛ برای استراتژی‌های بدون رکورد، 1.0 (نوترال) برمی‌گرداند."""
    conn = get_db_connection()
    try:
        rows = conn.execute("SELECT strategy, weight FROM strategy_weights").fetchall()
        weights = {row["strategy"]: row["weight"] for row in rows}
    finally:
        conn.close()
    return {name: weights.get(name, 1.0) for name, _ in STRATEGIES}

def get_strategy_weight(strategy: str) -> float:
    """وزن فعلی یک استراتژی خاص (برای استفاده لحظه‌ای هنگام محاسبه Score)."""
    conn = get_db_connection()
    try:
        row = conn.execute("SELECT weight FROM strategy_weights WHERE strategy = ?", (strategy,)).fetchone()
    finally:
        conn.close()
    return row["weight"] if row else 1.0

# ─── CONFIGURABLE FILTER PANEL ────────────────────────────────────────────────
def get_filter_config(key: str, default=None):
    """خواندن یک پارامتر فیلتر از دیتابیس. مقدار برگشتی رشته است — تبدیل نوع در محل استفاده."""
    conn = get_db_connection()
    try:
        row = conn.execute("SELECT value FROM filter_config WHERE key=?", (key,)).fetchone()
    finally:
        conn.close()
    return row["value"] if row else default

def set_filter_config(key: str, value):
    """ذخیره یک پارامتر فیلتر به صورت runtime (بدون نیاز به ری‌استارت ربات)."""
    now = datetime.now(timezone.utc).isoformat()
    conn = get_db_connection()
    try:
        conn.execute(
            "INSERT INTO filter_config (key, value, updated_at) VALUES (?, ?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
            (key, str(value), now)
        )
        conn.commit()
    finally:
        conn.close()

def get_adx_threshold() -> float:
    return float(get_filter_config("adx_threshold", ADX_THRESHOLD_DEFAULT))

def get_atr_multiplier() -> float:
    return float(get_filter_config("atr_multiplier", ATR_MULTIPLIER_DEFAULT))

def get_session_filters() -> dict:
    raw = get_filter_config("session_filters", json.dumps(SESSION_FILTERS_DEFAULT))
    try:
        return json.loads(raw)
    except Exception:
        return SESSION_FILTERS_DEFAULT.copy()

# ─── SESSION DETECTION ────────────────────────────────────────────────────────
def get_current_session() -> str:
    """تشخیص session فعلی بر اساس UTC. سشن‌ها: London 07-16 UTC، NY 13-22 UTC، Asian 00-09 UTC."""
    h = datetime.now(timezone.utc).hour
    if 7 <= h < 16:
        return "london"
    elif 13 <= h < 22:
        return "ny"
    elif h < 9 or h >= 22:
        return "asian"
    return "ny"  # overlap

def is_session_allowed(state: dict) -> bool:
    """بررسی اینکه آیا session فعلی توسط کاربر مجاز دانسته شده."""
    session_filters = state.get("session_filters", SESSION_FILTERS_DEFAULT)
    current = get_current_session()
    return session_filters.get(current, True)

def _normalize_metric(value: float, worst: float, best: float) -> float:
    """مقدار یک معیار را به بازه ۰ تا ۱ نرمال می‌کند (برای ترکیب چند معیار با واحد متفاوت)."""
    if best == worst:
        return 0.5
    v = (value - worst) / (best - worst)
    return max(0.0, min(1.0, v))

def calc_strategy_weight(strategy: str) -> float:
    """
    وزن جدید یک استراتژی را بر اساس عملکرد ۳۰ روز اخیر محاسبه می‌کند:
    ترکیبی وزن‌دار از Win Rate + Expectancy + Profit Factor.
    اگر تعداد معاملات کافی نباشد (آماری غیرقابل‌اعتماد)، وزن نوترال 1.0 برمی‌گردد.
    خروجی همیشه بین WEIGHT_MIN و WEIGHT_MAX محدود می‌شود.
    """
    stats = calc_strategy_stats(strategy, days=30)
    if stats["total_trades"] < MIN_TRADES_FOR_WEIGHTING:
        return 1.0

    # نرمال‌سازی هر معیار به بازه ۰-۱ با بازه‌های واقع‌گرایانه برای این نوع استراتژی‌ها
    norm_winrate = _normalize_metric(stats["winrate"], worst=30, best=70)        # 30%→0 , 70%→1
    norm_expectancy = _normalize_metric(stats["expectancy"], worst=-1.0, best=1.5)  # -1R→0 , +1.5R→1
    norm_pf = _normalize_metric(stats["profit_factor"], worst=0.5, best=2.5)     # 0.5→0 , 2.5→1

    # ترکیب وزن‌دار: Win Rate و Expectancy اهمیت بیشتری دارند تا Profit Factor (که می‌تواند نویزی باشد)
    composite = (norm_winrate * 0.4) + (norm_expectancy * 0.4) + (norm_pf * 0.2)

    # تبدیل امتیاز ترکیبی (۰ تا ۱) به بازه وزن (WEIGHT_MIN تا WEIGHT_MAX)
    weight = WEIGHT_MIN + composite * (WEIGHT_MAX - WEIGHT_MIN)
    return round(weight, 2)

def update_all_strategy_weights() -> dict:
    """وزن همه استراتژی‌ها را بازمحاسبه و در دیتابیس ذخیره می‌کند. خروجی: دیکشنری وزن‌های جدید."""
    now = datetime.now(timezone.utc).isoformat()
    new_weights = {}
    conn = get_db_connection()
    try:
        for strat_name, _ in STRATEGIES:
            w = calc_strategy_weight(strat_name)
            new_weights[strat_name] = w
            conn.execute(
                "INSERT INTO strategy_weights (strategy, weight, updated_at) VALUES (?, ?, ?) "
                "ON CONFLICT(strategy) DO UPDATE SET weight=excluded.weight, updated_at=excluded.updated_at",
                (strat_name, w, now)
            )
        conn.commit()
    finally:
        conn.close()
    return new_weights

def build_weight_update_report(old_weights: dict, new_weights: dict) -> str:
    """متن گزارش تغییر وزن استراتژی‌ها بعد از بازمحاسبه هفتگی، برای ارسال به ادمین‌ها."""
    lines = ["⚖️ <b>به‌روزرسانی هفتگی وزن استراتژی‌ها</b>", ""]
    any_change = False
    for strat_name, _ in STRATEGIES:
        old_w = old_weights.get(strat_name, 1.0)
        new_w = new_weights.get(strat_name, 1.0)
        if abs(old_w - new_w) < 0.01:
            continue
        any_change = True
        arrow = "📈" if new_w > old_w else "📉"
        tag = "تقویت شد (Boost)" if new_w > 1.0 else ("تضعیف شد (Penalty)" if new_w < 1.0 else "نوترال")
        lines.append(f"{arrow} <b>{strat_name}</b>: {old_w:.2f} → {new_w:.2f}  ({tag})")
    if not any_change:
        lines.append("هیچ تغییر معناداری در وزن استراتژی‌ها رخ نداد (یا داده کافی برای محاسبه نبود).")
    return "\n".join(lines)

# ─── TELEGRAM ─────────────────────────────────────────────────────────────────
async def send_telegram(session: aiohttp.ClientSession, text: str, chat_id: str = None):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id or TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": False,
    }
    try:
        async with session.post(url, json=payload, proxy=PROXY, timeout=aiohttp.ClientTimeout(total=10)) as r:
            data = await r.json()
            if not data.get("ok"):
                log.error(f"Telegram error: {data}")
    except Exception as e:
        log.error(f"Telegram send failed: {e}")

async def broadcast_signal(session: aiohttp.ClientSession, text: str, state: dict):
    """سیگنال رو به چت اصلی + همه کانال/گروه‌های فعال ارسال میکند.
    اگر چت اصلی (TELEGRAM_CHAT_ID) خودش هم در لیست channels ثبت شده باشد (یعنی با /stop
    داخل همان چت غیرفعال شده باشد)، دیگر به آن ارسال نمی‌شود — نه به‌صورت پیش‌فرض هاردکد."""
    channels = state.get("channels", [])
    main_chat_entry = next((c for c in channels if str(c["id"]) == str(TELEGRAM_CHAT_ID)), None)

    # چت اصلی را فقط در صورتی بفرست که یا اصلاً در لیست channels ثبت نشده (یعنی هنوز
    # کنترل نشده و به‌صورت پیش‌فرض فعال است)، یا ثبت شده ولی active=True باشد.
    if main_chat_entry is None or main_chat_entry.get("active", True):
        await send_telegram(session, text, TELEGRAM_CHAT_ID)

    for ch in channels:
        if str(ch["id"]) == str(TELEGRAM_CHAT_ID):
            continue  # همین الان بالاتر مدیریت شد، دوباره نفرستیم
        if ch.get("active", True):
            await send_telegram(session, text, str(ch["id"]))
            await asyncio.sleep(0.1)

def build_message(
    symbol: str,
    direction: str,          # "BUY" or "SELL"
    strategy: str,
    price: float,
    entry: float,
    stop: float,
    tp1: float,
    tp2: float,
    timeframe: str,
    score: int,
    rsi: Optional[float] = None,
    market_regime: Optional[str] = None,
) -> str:
    now_utc = datetime.now(timezone.utc)
    time_str = now_utc.strftime("%Y-%m-%d %H:%M UTC")

    signal_emoji = "🟢" if direction == "BUY" else "🔴"
    signal_text  = "𝗕𝗨𝗬 𝗦𝗜𝗚𝗡𝗔𝗟" if direction == "BUY" else "𝗦𝗘𝗟𝗟 𝗦𝗜𝗚𝗡𝗔𝗟"
    tv_link = f"https://www.tradingview.com/chart/?symbol=BINANCE:{symbol}"

    decimals = 8 if price < 0.01 else (4 if price < 10 else 2)

    def fmt(v): return f"{v:.{decimals}f}"

    rsi_line = f"📐 <b>RSI:</b> {rsi}\n" if rsi is not None else ""
    regime_line = f"🌐 <b>Market Regime:</b> {market_regime}\n" if market_regime else ""

    msg = (
        f"📡 <b>Auto Trading Signal | AI Analysis 🤖</b>\n"
        f"🐍 <b>𝐀𝐈 𝐀𝐅𝐄𝐄 𝐓𝐑𝐀𝐃𝐄𝐑</b> 🐍\n\n"
        f"💎 <b>Symbol:</b> {symbol}\n"
        f"{signal_emoji} <b>Signal:</b> {signal_text}\n"
        f"📊 <b>Strategy:</b> {strategy}\n"
        f"🔰 <b>Price:</b> {fmt(price)} USDT\n"
        f"{rsi_line}"
        f"{regime_line}"
        f"📈 <b>Timeframe:</b> {timeframe}\n"
        f"💰 <b>Entry:</b> {fmt(entry)}\n"
        f"🛑 <b>Stop:</b> {fmt(stop)}\n"
        f"🎯 <b>TP1:</b> {fmt(tp1)} (1.5R)\n"
        f"🎯 <b>TP2:</b> {fmt(tp2)} (3R)\n"
        f"⭐️ <b>Signal Score:</b> {score}/100\n"
        f"🕒 <b>Time:</b> {time_str}\n"
        f"━━━━━━━━━━━━━━━\n"
        f"<blockquote>🤖 This signal is automatically generated by the advanced AI trading robot "
        f"𝐀𝐅𝐄𝐄 𝐓𝐑𝐀𝐃𝐄𝐑 based on real-time data analysis.</blockquote>\n\n"
        f'<a href="{tv_link}">Chart Analysis {symbol}</a>\n\n'
        f"@AFEETRADER"
    )
    return msg

# ─── BINANCE DATA ─────────────────────────────────────────────────────────────
async def get_top_symbols(session: aiohttp.ClientSession, n: int = 100) -> list[str]:
    """Return top N USDT pairs by 24h quote volume, excluding stablecoins."""
    STABLE = {"USDC", "BUSD", "TUSD", "DAI", "FDUSD", "USDP", "USDD"}
    url = f"{BINANCE_BASE}/ticker/24hr"
    async with session.get(url, proxy=PROXY, timeout=aiohttp.ClientTimeout(total=15)) as r:
        data = await r.json()
    usdt = [
        d for d in data
        if d["symbol"].endswith("USDT")
        and not any(d["symbol"].startswith(s) for s in STABLE)
        and float(d["quoteVolume"]) > 0
    ]
    usdt.sort(key=lambda x: float(x["quoteVolume"]), reverse=True)
    return [d["symbol"] for d in usdt[:n]]

# ─── SYMBOL QUALITY RANKING (مورد ۶) ───────────────────────────────────────────
async def get_symbol_spread_quality(session: aiohttp.ClientSession, symbol: str) -> float:
    """
    کیفیت اسپرد: فاصله bid/ask نسبت به قیمت میانی. هرچه اسپرد کمتر (نسبت به قیمت)،
    کیفیت بالاتر. خروجی بین ۰ تا ۱ (۱ = بهترین، اسپرد نزدیک صفر).
    """
    try:
        url = f"{BINANCE_BASE}/ticker/bookTicker"
        async with session.get(url, params={"symbol": symbol}, proxy=PROXY,
                               timeout=aiohttp.ClientTimeout(total=8)) as r:
            data = await r.json()
        bid, ask = float(data["bidPrice"]), float(data["askPrice"])
        mid = (bid + ask) / 2
        if mid == 0:
            return 0.5
        spread_pct = (ask - bid) / mid * 100
        # نرمال‌سازی: اسپرد ۰٪ → امتیاز ۱.۰  |  اسپرد ۰.5٪ یا بیشتر → امتیاز ۰.۰
        return max(0.0, min(1.0, 1.0 - (spread_pct / 0.5)))
    except Exception:
        return 0.3  # در صورت خطا، امتیاز محافظه‌کارانه پایین (نه صفر، نه کامل)

def calc_structure_cleanliness(candles: list[dict]) -> float:
    """
    نظم ساختاری بازار: تعداد سطوح S/R معتبر (با حداقل ۲ برخورد) که در داده تشخیص داده می‌شود.
    بازاری با چند سطح S/R واضح، «تمیزتر» و قابل تحلیل‌تر از بازاری با نویز کامل یا بدون هیچ سطحی است.
    خروجی بین ۰ تا ۱.
    """
    if len(candles) < 30:
        return 0.5
    levels = find_sr_levels(candles, lookback=min(50, len(candles)))
    # ۲ تا ۶ سطح S/R معتبر ایده‌آل است؛ صفر سطح (بی‌ساختار) یا تعداد بیش‌ازحد (نویزی) امتیاز کمتر می‌گیرد
    count = len(levels)
    if count == 0:
        return 0.2
    elif 2 <= count <= 6:
        return 1.0
    elif count == 1 or count == 7:
        return 0.7
    else:
        return 0.4

def calc_trend_clarity(candles: list[dict]) -> float:
    """
    وضوح روند: بر اساس ADX. روند خیلی واضح (قوی) یا رنج خیلی واضح (بدون روند) هر دو قابل تحلیل‌اند؛
    ابهام‌برانگیزترین حالت، ADX میانه (نه قوی نه ضعیف، حدود ۲۰-۲۵) است که نه استراتژی ترند نه ریورسال
    مطمئن کار می‌کند. خروجی بین ۰ تا ۱.
    """
    if len(candles) < 30:
        return 0.5
    adx = calc_adx(candles, 14)
    if adx >= 25 or adx <= 15:
        return 1.0   # روند واضح یا رنج واضح — هر دو قابل تحلیل
    elif 15 < adx < 20 or 25 > adx >= 22:
        return 0.7
    else:
        return 0.4    # ناحیه خاکستری ۲۰-۲۲ — مبهم‌ترین حالت

def calc_volatility_quality(candles: list[dict]) -> float:
    """
    کیفیت نوسان: نه بازار خیلی بی‌حرکت (سیگنال‌های کم‌سود)، نه خیلی پرنوسان (SL راحت می‌خورد).
    بر اساس ATR نسبت به قیمت (ATR درصدی). خروجی بین ۰ تا ۱.
    """
    if len(candles) < 20:
        return 0.5
    atr = calc_atr(candles, 14)
    price = candles[-1]["close"]
    if price == 0:
        return 0.5
    atr_pct = (atr / price) * 100
    # محدوده ایده‌آل نوسان: ۰.۵٪ تا ۳٪ ATR نسبت به قیمت (بسته به نوع کوین، نسبی است)
    if 0.5 <= atr_pct <= 3.0:
        return 1.0
    elif 0.2 <= atr_pct < 0.5 or 3.0 < atr_pct <= 5.0:
        return 0.6
    else:
        return 0.25   # خیلی بی‌حرکت یا خیلی پرنوسان

async def calc_symbol_quality_score(session: aiohttp.ClientSession, symbol: str) -> dict:
    """
    امتیاز کیفیت کلی یک نماد، ترکیبی از ۴ معیار:
    spread quality, volatility quality, structure cleanliness, trend clarity.
    خروجی: دیکشنری شامل امتیاز نهایی (۰ تا ۱۰۰) و جزئیات هر معیار.
    """
    candles = await get_candles(session, symbol, "1h", 100)
    if len(candles) < 30:
        return {"symbol": symbol, "quality_score": 0, "valid": False}

    spread_q = await get_symbol_spread_quality(session, symbol)
    structure_q = calc_structure_cleanliness(candles)
    trend_q = calc_trend_clarity(candles)
    volatility_q = calc_volatility_quality(candles)

    # ترکیب وزن‌دار: ساختار و وضوح روند برای استراتژی‌های تکنیکال این ربات اهمیت بیشتری دارند
    composite = (spread_q * 0.20) + (volatility_q * 0.25) + (structure_q * 0.30) + (trend_q * 0.25)
    score = round(composite * 100)

    return {
        "symbol": symbol, "quality_score": score, "valid": True,
        "spread_quality": round(spread_q, 2), "volatility_quality": round(volatility_q, 2),
        "structure_cleanliness": round(structure_q, 2), "trend_clarity": round(trend_q, 2),
    }

async def get_quality_ranked_symbols(session: aiohttp.ClientSession, final_n: int = 100,
                                      pool_size: int = 250) -> list[str]:
    """
    انتخاب نمادها در دو مرحله (مورد ۶ - Symbol Quality Ranking):
    ۱. ابتدا `pool_size` نماد برتر بر اساس حجم معاملات ۲۴ ساعته انتخاب می‌شوند (نامزدهای اولیه).
    ۲. سپس برای همین نامزدها، امتیاز کیفیت (spread/volatility/structure/trend) محاسبه می‌شود
       و فقط `final_n` نماد با بالاترین کیفیت برای اسکن نهایی انتخاب می‌شوند.
    """
    candidates = await get_top_symbols(session, pool_size)

    semaphore = asyncio.Semaphore(PARALLEL_WORKERS)
    async def scored(symbol):
        async with semaphore:
            return await calc_symbol_quality_score(session, symbol)

    results = await asyncio.gather(*[scored(s) for s in candidates], return_exceptions=True)
    valid_results = [r for r in results if isinstance(r, dict) and r.get("valid")]
    valid_results.sort(key=lambda r: r["quality_score"], reverse=True)

    return [r["symbol"] for r in valid_results[:final_n]]

async def get_candles(
    session: aiohttp.ClientSession,
    symbol: str,
    interval: str,
    limit: int = 200,
) -> list[dict]:
    """Fetch klines and return list of OHLCV dicts."""
    url = f"{BINANCE_BASE}/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    try:
        async with session.get(url, params=params, proxy=PROXY, timeout=aiohttp.ClientTimeout(total=10)) as r:
            raw = await r.json()
        candles = []
        for k in raw:
            candles.append({
                "open_time": k[0],
                "open":   float(k[1]),
                "high":   float(k[2]),
                "low":    float(k[3]),
                "close":  float(k[4]),
                "volume": float(k[5]),
            })
        return candles
    except Exception as e:
        log.debug(f"Candle fetch error {symbol} {interval}: {e}")
        return []

# ─── TECHNICAL HELPERS ────────────────────────────────────────────────────────
def find_sr_levels(candles: list[dict], lookback: int = 50, tolerance: float = 0.003) -> list[float]:
    """
    Find S&R levels from highs/lows with at least 2 touches.
    Also detects simple Order Block regions.
    """
    highs = [c["high"] for c in candles[-lookback:]]
    lows  = [c["low"]  for c in candles[-lookback:]]
    levels: list[float] = []

    def is_pivot_high(i):
        return highs[i] == max(highs[max(0,i-5):i+6])

    def is_pivot_low(i):
        return lows[i] == min(lows[max(0,i-5):i+6])

    pivots: list[float] = []
    for i in range(5, len(highs)-5):
        if is_pivot_high(i): pivots.append(highs[i])
        if is_pivot_low(i):  pivots.append(lows[i])

    # cluster pivots that are within tolerance
    used = [False]*len(pivots)
    for i, p in enumerate(pivots):
        if used[i]: continue
        cluster = [p]
        for j in range(i+1, len(pivots)):
            if not used[j] and abs(pivots[j]-p)/p < tolerance:
                cluster.append(pivots[j])
                used[j] = True
        if len(cluster) >= 2:
            levels.append(sum(cluster)/len(cluster))
        used[i] = True

    return sorted(levels)

# ─── S&R WEIGHT SYSTEM (Patch #2) ─────────────────────────────────────────────
SR_SCORE_3_TOUCHES   = 90
SR_SCORE_ORDER_BLOCK = 85
SR_SCORE_SUPPLY_DEMAND = 80
SR_SCORE_2_TOUCHES   = 65

def find_sr_levels_scored(candles: list[dict], lookback: int = 50, tolerance: float = 0.003) -> list[dict]:
    """
    Enhanced S&R detection that returns scored levels.
    Each level dict: {"price": float, "sr_score": int, "type": str, "touches": int}
    Score: 3+ touches=90, Order Block=85, Supply/Demand=80, 2 touches=65
    """
    highs = [c["high"] for c in candles[-lookback:]]
    lows  = [c["low"]  for c in candles[-lookback:]]
    closes= [c["close"] for c in candles[-lookback:]]
    opens = [c["open"]  for c in candles[-lookback:]]
    scored_levels: list[dict] = []

    def is_pivot_high(i):
        return highs[i] == max(highs[max(0,i-5):i+6])

    def is_pivot_low(i):
        return lows[i] == min(lows[max(0,i-5):i+6])

    pivots: list[float] = []
    for i in range(5, len(highs)-5):
        if is_pivot_high(i): pivots.append(highs[i])
        if is_pivot_low(i):  pivots.append(lows[i])

    used = [False]*len(pivots)
    for i, p in enumerate(pivots):
        if used[i]: continue
        cluster = [p]
        for j in range(i+1, len(pivots)):
            if not used[j] and abs(pivots[j]-p)/p < tolerance:
                cluster.append(pivots[j])
                used[j] = True
        if len(cluster) >= 2:
            level_price = sum(cluster)/len(cluster)
            touches = len(cluster)
            # Detect order block: strong opposite candle before the level
            is_ob = False
            for k in range(5, len(highs)-5):
                if abs(highs[k] - level_price)/level_price < tolerance or abs(lows[k] - level_price)/level_price < tolerance:
                    body = abs(closes[k] - opens[k])
                    avg_b = sum(abs(closes[m]-opens[m]) for m in range(max(0,k-10),k)) / max(1, min(k,10))
                    if avg_b > 0 and body >= 2 * avg_b:
                        is_ob = True
                        break
            if is_ob:
                sr_score = SR_SCORE_ORDER_BLOCK
                sr_type  = "Order Block"
            elif touches >= 3:
                sr_score = SR_SCORE_3_TOUCHES
                sr_type  = "3+ Touches"
            else:
                sr_score = SR_SCORE_2_TOUCHES
                sr_type  = "2 Touches"
            scored_levels.append({"price": level_price, "sr_score": sr_score,
                                   "type": sr_type, "touches": touches})
        used[i] = True

    return sorted(scored_levels, key=lambda x: x["price"])

def get_nearest_sr_scored(price: float, scored_levels: list[dict], tol: float = 0.008) -> Optional[dict]:
    """Return the nearest scored S&R level within tolerance, or None."""
    for sl in scored_levels:
        if abs(price - sl["price"]) / sl["price"] < tol:
            return sl
    return None

def is_near_level(price: float, levels: list[float], tol: float = 0.005) -> Optional[float]:
    for lv in levels:
        if abs(price - lv) / lv < tol:
            return lv
    return None

def swing_high(candles: list[dict], n: int = 5) -> float:
    return max(c["high"] for c in candles[-n:])

def swing_low(candles: list[dict], n: int = 5) -> float:
    return min(c["low"] for c in candles[-n:])

def avg_body(candles: list[dict], n: int = 20) -> float:
    return sum(abs(c["close"]-c["open"]) for c in candles[-n:]) / n

def avg_volume(candles: list[dict], n: int = 20) -> float:
    return sum(c["volume"] for c in candles[-n:]) / n

def has_sufficient_entry_volume(candles: list[dict], ma_period: int = 20, multiplier: float = 1.5) -> bool:
    """
    فیلتر حجم کندل ورود (مورد ۱۰ — مستقل از فیلتر حجم مشکوک):
    فقط زمانی True برمی‌گرداند که حجم آخرین کندل (کندل ورود) حداقل `multiplier` برابر
    میانگین حجم `ma_period` کندل اخیر باشد. شرط پیش‌فرض: Volume >= 1.5 × SMA(Volume, 20).
    اگر داده کافی نباشد، محافظه‌کارانه True برمی‌گرداند (فیلتر اعمال نمی‌شود).
    """
    if len(candles) < ma_period + 1:
        return True
    baseline = candles[-(ma_period + 1):-1]  # ma_period کندل قبل از کندل آخر، بدون خود کندل آخر
    avg_vol = avg_volume(baseline, ma_period)
    if avg_vol == 0:
        return True
    entry_volume = candles[-1]["volume"]
    return entry_volume >= (multiplier * avg_vol)

def entry_volume_ratio(candles: list[dict], ma_period: int = 20) -> float:
    """نسبت حجم کندل ورود به میانگین — برای استفاده در امتیازدهی (Score) سیگنال."""
    if len(candles) < ma_period + 1:
        return 1.0
    baseline = candles[-(ma_period + 1):-1]
    avg_vol = avg_volume(baseline, ma_period)
    if avg_vol == 0:
        return 1.0
    return candles[-1]["volume"] / avg_vol

def is_strong_candle(c: dict, prev_candles: list[dict]) -> bool:
    """
    Strong candle: body >= 2x avg body of last 20 OR volume >= 1.5x avg volume.
    Shadow should be small relative to body.
    """
    body  = abs(c["close"] - c["open"])
    ab    = avg_body(prev_candles)
    av    = avg_volume(prev_candles)
    shadow = (c["high"] - c["low"]) - body
    if ab == 0: return False
    big_body   = body >= 2 * ab
    big_volume = c["volume"] >= 1.5 * av
    small_shadow = shadow <= body * 1.5
    # engulfs previous candle
    prev = prev_candles[-1] if prev_candles else None
    engulfs = False
    if prev:
        prev_range = prev["high"] - prev["low"]
        engulfs = (c["high"] >= prev["high"] and c["low"] <= prev["low"]) if prev_range > 0 else False
    return (big_body or big_volume) and small_shadow

def has_suspicious_opposing_volume(candles: list[dict], direction: str, lookback: int = 5) -> bool:
    """
    فیلتر «حجم مشکوک»: بررسی می‌کند آیا در چند کندل آخر، یک حرکت قوی با حجم بالا
    دقیقاً در جهت مخالف سیگنال اتفاق افتاده است (نشانه ورود نقدینگی بزرگ مخالف ما،
    که معمولاً به استاپ خوردن سیگنال منجر می‌شود).
    اگر چنین حرکتی پیدا شود، True برمی‌گرداند یعنی سیگنال باید رد (فیلتر) شود.
    """
    if len(candles) < lookback + 20:
        return False
    recent = candles[-lookback:]
    baseline = candles[-(lookback + 20):-lookback]
    avg_vol = avg_volume(baseline, 20)
    if avg_vol == 0:
        return False

    for c in recent:
        body = abs(c["close"] - c["open"])
        is_bearish = c["close"] < c["open"]
        is_bullish = c["close"] > c["open"]
        high_volume = c["volume"] >= 1.8 * avg_vol  # حجم به‌طور غیرعادی بالا

        if not high_volume or body == 0:
            continue

        # سیگنال BUY است ولی حرکت قوی نزولی با حجم بالا دیده شده → مشکوک
        if direction == "BUY" and is_bearish:
            return True
        # سیگنال SELL است ولی حرکت قوی صعودی با حجم بالا دیده شده → مشکوک
        if direction == "SELL" and is_bullish:
            return True

    return False

def is_hammer(c: dict) -> bool:
    """Bullish hammer: small body at top, long lower shadow >= 2x body."""
    body   = abs(c["close"] - c["open"])
    total  = c["high"] - c["low"]
    if total == 0 or body == 0: return False
    lower_shadow = min(c["open"], c["close"]) - c["low"]
    upper_shadow = c["high"] - max(c["open"], c["close"])
    return (lower_shadow >= 2 * body and upper_shadow <= body * 0.5
            and c["close"] > c["open"])

def is_shooting_star(c: dict) -> bool:
    """Bearish shooting star: small body at bottom, long upper shadow >= 2x body."""
    body   = abs(c["close"] - c["open"])
    if body == 0: return False
    upper_shadow = c["high"] - max(c["open"], c["close"])
    lower_shadow = min(c["open"], c["close"]) - c["low"]
    return (upper_shadow >= 2 * body and lower_shadow <= body * 0.5
            and c["close"] < c["open"])

def calc_rsi(closes: list[float], period: int = 14) -> list[float]:
    """محاسبه RSI استاندارد (Wilder's smoothing) - برمیگرداند لیست RSI هم‌طول closes (مقادیر اول None)."""
    if len(closes) < period + 1:
        return [None] * len(closes)

    rsis: list[Optional[float]] = [None] * len(closes)
    gains, losses = [], []
    for i in range(1, period + 1):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    rs = avg_gain / avg_loss if avg_loss != 0 else float("inf")
    rsis[period] = 100 - (100 / (1 + rs)) if avg_loss != 0 else 100.0

    for i in range(period + 1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gain = max(diff, 0)
        loss = max(-diff, 0)
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
        rs = avg_gain / avg_loss if avg_loss != 0 else float("inf")
        rsis[i] = 100 - (100 / (1 + rs)) if avg_loss != 0 else 100.0

    return rsis

def fibonacci_level(high: float, low: float, ratio: float) -> float:
    return high - (high - low) * ratio

def calc_score(factors: list[bool], sr_score: int = None) -> int:
    """
    Score based on confirmation factors + optional S&R zone weight (Patch #2).
    If sr_score provided, it influences the final score proportionally.

    FIX v3.2: sr_score is now properly propagated from find_sr_levels_scored()
    into every strategy call via get_nearest_sr_scored(). Previously sr_score
    was never passed so the S&R weight had zero effect on the final score.
    """
    base = 50
    per_factor = 50 // max(len(factors), 1)
    raw = min(100, base + sum(factors) * per_factor)
    if sr_score is not None:
        # Blend raw score with sr_score weight (60/40 split)
        raw = round(raw * 0.6 + sr_score * 0.4)
    return min(100, max(1, raw))

# ─── MIN SIGNAL SCORE (runtime configurable via /setscore) ─────────────────────
_MIN_SIGNAL_SCORE_DEFAULT = 65

def get_min_signal_score() -> int:
    """خواندن حداقل score سیگنال از filter_config دیتابیس (در صورت تنظیم /setscore)."""
    raw = get_filter_config("min_signal_score", str(_MIN_SIGNAL_SCORE_DEFAULT))
    try:
        return max(1, min(100, int(raw)))
    except Exception:
        return _MIN_SIGNAL_SCORE_DEFAULT

# نگه‌داشتن MIN_SIGNAL_SCORE به عنوان property پویا — کد قدیمی که مستقیم از این نام استفاده می‌کند
# به‌جای مقدار ثابت، هر بار get_min_signal_score() را صدا می‌زند
MIN_SIGNAL_SCORE = _MIN_SIGNAL_SCORE_DEFAULT  # مقدار پیش‌فرض ثابت؛ در run_live_filters پویا خوانده می‌شود

# ─── SIGNAL VALIDATION (Patch #5) ─────────────────────────────────────────────
MIN_RISK_THRESHOLD = 0.0001  # حداقل فاصله نسبی Entry-Stop (0.01%)

def validate_signal_rr(result: dict) -> tuple[bool, str]:
    """
    Patch #5: Validate signal math before sending.
    Rules:
      - Entry ≠ Stop
      - Risk > minimum threshold
      - Reward (to TP1) > Risk
      - RR must be mathematically valid (no division by zero, no negative reward)
    Returns (is_valid, reason_if_invalid).
    """
    entry = result.get("entry", 0)
    stop  = result.get("stop", 0)
    tp1   = result.get("tp1", 0)
    tp2   = result.get("tp2", 0)
    direction = result.get("direction", "BUY")

    if entry == 0 or stop == 0 or tp1 == 0:
        return False, "zero_price_values"

    if abs(entry - stop) < 1e-12:
        return False, "entry_equals_stop"

    risk = abs(entry - stop)
    risk_pct = risk / entry if entry > 0 else 0

    if risk_pct < MIN_RISK_THRESHOLD:
        return False, f"risk_too_small:{risk_pct:.6f}"

    # Check reward direction and magnitude
    if direction == "BUY":
        if tp1 <= entry:
            return False, "tp1_below_entry_for_buy"
        reward = tp1 - entry
    else:
        if tp1 >= entry:
            return False, "tp1_above_entry_for_sell"
        reward = entry - tp1

    if reward <= 0:
        return False, "zero_reward"

    rr = reward / risk
    if rr < 0.5:
        return False, f"rr_too_low:{rr:.2f}"

    return True, ""

def calc_atr(candles: list[dict], period: int = 14) -> float:
    """میانگین برد واقعی (ATR) — برای تشخیص نوسان غیرعادی بازار."""
    if len(candles) < period + 1:
        return 0.0
    trs = []
    for i in range(1, len(candles)):
        h, l, prev_close = candles[i]["high"], candles[i]["low"], candles[i-1]["close"]
        tr = max(h - l, abs(h - prev_close), abs(l - prev_close))
        trs.append(tr)
    return sum(trs[-period:]) / period

def is_volatility_abnormal(candles: list[dict], period: int = 14, threshold: float = 2.2) -> bool:
    """
    اگر ATR لحظه‌ای (۳ کندل آخر) نسبت به ATR پایه (۱۴ کندل) بیش از حد بالا باشد،
    یعنی بازار غیرعادی پرنوسان شده — در این حالت SL راحت می‌خورد و بهتر است سیگنال رد شود.
    """
    if len(candles) < period + 5:
        return False
    baseline_atr = calc_atr(candles[:-3], period)
    if baseline_atr == 0:
        return False
    recent_ranges = [c["high"] - c["low"] for c in candles[-3:]]
    recent_avg_range = sum(recent_ranges) / len(recent_ranges)
    return recent_avg_range >= baseline_atr * threshold

async def is_aligned_with_higher_trend(session, symbol: str, direction: str) -> bool:
    """
    بررسی همسویی جهت سیگنال با روند تایم ۱ ساعته (میانگین ساده ۲۰ کندل آخر در مقابل قیمت فعلی).
    اگر سیگنال BUY باشد ولی روند ۱ساعته نزولی باشد (یا برعکس)، سیگنال ضدروند تشخیص داده و رد می‌شود.
    """
    c1h = await get_candles(session, symbol, "1h", 25)
    if len(c1h) < 20:
        return True  # داده کافی نیست، فیلتر را اعمال نمی‌کنیم (محافظه‌کارانه عبور می‌دهیم)
    closes = [c["close"] for c in c1h]
    sma20 = sum(closes[-20:]) / 20
    price = closes[-1]
    trend_up = price > sma20
    if direction == "BUY":
        return trend_up
    else:
        return not trend_up

# ─── MARKET REGIME ENGINE (موتور رژیم بازار) ──────────────────────────────────
# طبقه‌بندی استراتژی‌ها بر اساس نوع منطق‌شان — برای انتخاب استراتژی متناسب با رژیم فعلی بازار.
STRATEGY_REGIME_TYPE = {
    "Stop Hunter":         "HYBRID",    # هم در روند قوی هم در بازار رنج کاربرد دارد (شکست فیک)
    "Hammer Fib (4H)":     "REVERSAL",
    "Hammer Fib (1H)":     "REVERSAL",
    "HB":                  "REVERSAL",
    "Trigger Fibonacci":   "TREND",     # دنباله‌روی موج در جهت روند اصلی
    "Exhaustion":          "REVERSAL",
    "RSI Divergence":      "REVERSAL",
}

def calc_ema(closes: list[float], period: int) -> list[float]:
    """میانگین متحرک نمایی (EMA) — برمی‌گرداند لیست هم‌طول closes (مقادیر اول None)."""
    if len(closes) < period:
        return [None] * len(closes)
    emas: list[Optional[float]] = [None] * len(closes)
    multiplier = 2 / (period + 1)
    sma_seed = sum(closes[:period]) / period
    emas[period - 1] = sma_seed
    for i in range(period, len(closes)):
        emas[i] = (closes[i] - emas[i-1]) * multiplier + emas[i-1]
    return emas

def calc_adx(candles: list[dict], period: int = 14) -> float:
    """
    محاسبه استاندارد ADX (Average Directional Index) — معیار قدرت روند، فارغ از جهت آن.
    ADX > 25 یعنی روند قوی (مناسب استراتژی‌های Trend)؛ ADX < 20 یعنی بازار رنج/بدون روند
    (مناسب استراتژی‌های Reversal). بین ۲۰ تا ۲۵ ناحیه خاکستری/گذار است.
    """
    if len(candles) < period * 2:
        return 0.0

    plus_dm, minus_dm, trs = [], [], []
    for i in range(1, len(candles)):
        up_move = candles[i]["high"] - candles[i-1]["high"]
        down_move = candles[i-1]["low"] - candles[i]["low"]
        plus_dm.append(up_move if (up_move > down_move and up_move > 0) else 0)
        minus_dm.append(down_move if (down_move > up_move and down_move > 0) else 0)
        tr = max(
            candles[i]["high"] - candles[i]["low"],
            abs(candles[i]["high"] - candles[i-1]["close"]),
            abs(candles[i]["low"] - candles[i-1]["close"])
        )
        trs.append(tr)

    def smooth(values, period):
        if len(values) < period:
            return []
        smoothed = [sum(values[:period])]
        for v in values[period:]:
            smoothed.append(smoothed[-1] - (smoothed[-1] / period) + v)
        return smoothed

    smoothed_tr = smooth(trs, period)
    smoothed_plus_dm = smooth(plus_dm, period)
    smoothed_minus_dm = smooth(minus_dm, period)

    if not smoothed_tr or len(smoothed_tr) != len(smoothed_plus_dm):
        return 0.0

    dx_values = []
    for i in range(len(smoothed_tr)):
        if smoothed_tr[i] == 0:
            continue
        plus_di = 100 * (smoothed_plus_dm[i] / smoothed_tr[i])
        minus_di = 100 * (smoothed_minus_dm[i] / smoothed_tr[i])
        di_sum = plus_di + minus_di
        if di_sum == 0:
            dx_values.append(0)
            continue
        dx = 100 * abs(plus_di - minus_di) / di_sum
        dx_values.append(dx)

    if len(dx_values) < period:
        return sum(dx_values) / len(dx_values) if dx_values else 0.0
    return sum(dx_values[-period:]) / period

def calc_ema200_slope(closes: list[float], lookback: int = 5) -> str:
    """
    شیب EMA200 طی چند کندل اخیر — جهت کلی روند بلندمدت را نشان می‌دهد.
    خروجی: "UP" (صعودی)، "DOWN" (نزولی)، یا "FLAT" (بدون شیب واضح).
    """
    emas = calc_ema(closes, 200)
    valid_emas = [e for e in emas[-lookback-1:] if e is not None]
    if len(valid_emas) < 2:
        return "FLAT"
    slope_pct = (valid_emas[-1] - valid_emas[0]) / valid_emas[0] * 100
    if slope_pct > 0.15:
        return "UP"
    elif slope_pct < -0.15:
        return "DOWN"
    return "FLAT"

def calc_atr_regime(candles: list[dict], period: int = 14, lookback_avg: int = 50) -> str:
    """
    رژیم ATR: ATR فعلی را با میانگین بلندمدت‌تر آن (مثلاً ۵۰ کندل) مقایسه می‌کند.
    خروجی: "HIGH" (نوسان بالاتر از معمول)، "LOW" (نوسان پایین‌تر از معمول)، یا "NORMAL".
    """
    if len(candles) < lookback_avg + period:
        return "NORMAL"
    current_atr = calc_atr(candles, period)
    historical_atrs = []
    step = max(1, (len(candles) - period) // lookback_avg)
    for i in range(period, len(candles) - period, step):
        historical_atrs.append(calc_atr(candles[:i+period], period))
    if not historical_atrs:
        return "NORMAL"
    avg_historical_atr = sum(historical_atrs) / len(historical_atrs)
    if avg_historical_atr == 0:
        return "NORMAL"
    ratio = current_atr / avg_historical_atr
    if ratio >= 1.4:
        return "HIGH"
    elif ratio <= 0.7:
        return "LOW"
    return "NORMAL"

def calc_volatility_regime(candles: list[dict], period: int = 20) -> str:
    """
    رژیم نوسان قیمتی بر اساس انحراف معیار بازده‌های لگاریتمی — مستقل از ATR (که بر مبنای High/Low است).
    خروجی: "HIGH", "LOW", یا "NORMAL".
    """
    if len(candles) < period + 1:
        return "NORMAL"
    closes = [c["close"] for c in candles[-(period+1):]]
    returns = []
    for i in range(1, len(closes)):
        if closes[i-1] > 0:
            returns.append(math.log(closes[i] / closes[i-1]))
    if len(returns) < 2:
        return "NORMAL"
    mean_r = sum(returns) / len(returns)
    variance = sum((r - mean_r) ** 2 for r in returns) / len(returns)
    std = math.sqrt(variance)
    annualized_like = std * 100  # درصد ساده برای مقایسه نسبی، نه annualized واقعی

    if annualized_like >= 3.0:
        return "HIGH"
    elif annualized_like <= 0.8:
        return "LOW"
    return "NORMAL"

def calc_volume_regime(candles: list[dict], period: int = 20) -> str:
    """
    رژیم حجم معاملات: حجم میانگین چند کندل اخیر را نسبت به میانگین بلندمدت‌تر می‌سنجد.
    خروجی: "HIGH" (ورود نقدینگی بیشتر از معمول)، "LOW" (بازار بی‌رمق)، یا "NORMAL".
    """
    if len(candles) < period + 5:
        return "NORMAL"
    recent_avg = avg_volume(candles, 5)
    baseline_avg = avg_volume(candles[:-5], period)
    if baseline_avg == 0:
        return "NORMAL"
    ratio = recent_avg / baseline_avg
    if ratio >= 1.5:
        return "HIGH"
    elif ratio <= 0.6:
        return "LOW"
    return "NORMAL"

async def get_market_regime(session, symbol: str) -> dict:
    """
    تحلیل کامل رژیم بازار یک نماد، بر مبنای تایم ۱ ساعته (برای دیدگاه میان‌مدت، نه نویز کوتاه‌مدت).
    خروجی شامل تمام معیارها به‌همراه تصمیم نهایی «کدام دسته از استراتژی‌ها مجاز است».
    """
    candles = await get_candles(session, symbol, "1h", 250)
    if len(candles) < 60:
        # داده کافی نیست — محافظه‌کارانه همه نوع استراتژی را مجاز می‌کنیم
        return {
            "adx": 0, "ema200_slope": "FLAT", "atr_regime": "NORMAL",
            "volatility_regime": "NORMAL", "volume_regime": "NORMAL",
            "allowed_types": {"TREND", "REVERSAL", "HYBRID"},
            "regime_label": "UNKNOWN (insufficient data)",
        }

    closes = [c["close"] for c in candles]
    adx = calc_adx(candles, 14)
    ema_slope = calc_ema200_slope(closes)
    atr_regime = calc_atr_regime(candles)
    vol_regime = calc_volatility_regime(candles)
    volume_regime = calc_volume_regime(candles)

    # ── منطق اصلی: تعیین دسته‌های مجاز استراتژی بر اساس ADX ──
    # ADX > 25  → فقط استراتژی‌های Trend (+ Hybrid)
    # ADX < 20  → فقط استراتژی‌های Reversal (+ Hybrid)
    # 20≤ADX≤25 → ناحیه خاکستری/گذار؛ همه دسته‌ها مجاز (نه قوی‌روند، نه کاملاً رنج)
    if adx > 25:
        allowed_types = {"TREND", "HYBRID"}
        regime_label = f"TRENDING (ADX={adx:.1f})"
    elif adx < 20:
        allowed_types = {"REVERSAL", "HYBRID"}
        regime_label = f"RANGING (ADX={adx:.1f})"
    else:
        allowed_types = {"TREND", "REVERSAL", "HYBRID"}
        regime_label = f"TRANSITIONAL (ADX={adx:.1f})"

    return {
        "adx": round(adx, 1), "ema200_slope": ema_slope, "atr_regime": atr_regime,
        "volatility_regime": vol_regime, "volume_regime": volume_regime,
        "allowed_types": allowed_types, "regime_label": regime_label,
    }

def calc_targets(entry: float, stop: float, direction: str):
    risk = abs(entry - stop)
    if direction == "BUY":
        tp1 = entry + risk * 1.5
        tp2 = entry + risk * 3.0
    else:
        tp1 = entry - risk * 1.5
        tp2 = entry - risk * 3.0
    return tp1, tp2

def correction_size(candles: list[dict], n: int = 10) -> float:
    """Check if last move is a correction (20% of prior wave or ~38.2% fib)."""
    if len(candles) < n+5: return 0
    wave_high = max(c["high"] for c in candles[-(n+5):-5])
    wave_low  = min(c["low"]  for c in candles[-(n+5):-5])
    wave = wave_high - wave_low
    recent_high = max(c["high"] for c in candles[-5:])
    recent_low  = min(c["low"]  for c in candles[-5:])
    recent_move = abs(recent_high - recent_low)
    if wave == 0: return 0
    return recent_move / wave

# ─── CORRECTION SCORING (Patch #3) ────────────────────────────────────────────
CORRECTION_MIN   = 0.20    # minimum valid correction ratio (20% of wave)
CORRECTION_OPT   = 0.382   # optimal correction (38.2% Fibonacci)

def correction_score(ratio: float) -> tuple[bool, float]:
    """
    Returns (is_valid, score_multiplier) for a given pullback ratio.
    - below 20%  → invalid (False, 0)
    - 20%-38.2%  → valid but reduced score (True, 0.7)
    - 38.2%+     → full score (True, 1.0)
    """
    if ratio < CORRECTION_MIN:
        return False, 0.0
    elif ratio < CORRECTION_OPT:
        return True, 0.7
    else:
        return True, 1.0

def measure_pullback(candles: list[dict], direction: str, wave_lookback: int = 20) -> float:
    """
    After a mini-trend completes, measure how much price has pulled back.
    direction='SELL' → trend was up; pullback = how far price dropped from peak.
    direction='BUY'  → trend was down; pullback = how far price rose from trough.
    Returns ratio of pullback vs wave size (0.0 if insufficient data).
    """
    if len(candles) < wave_lookback + 3:
        return 0.0
    wave_candles = candles[-(wave_lookback + 3):-3]
    recent_candles = candles[-3:]
    if direction == "SELL":
        wave_high = max(c["high"] for c in wave_candles)
        wave_low  = min(c["low"]  for c in wave_candles)
        wave      = wave_high - wave_low
        if wave == 0: return 0.0
        recent_low  = min(c["low"] for c in recent_candles)
        pullback    = wave_high - recent_low
        return pullback / wave
    else:
        wave_high = max(c["high"] for c in wave_candles)
        wave_low  = min(c["low"]  for c in wave_candles)
        wave      = wave_high - wave_low
        if wave == 0: return 0.0
        recent_high = max(c["high"] for c in recent_candles)
        pullback    = recent_high - wave_low
        return pullback / wave

# ─── STRATEGY 1: STOP HUNTER ──────────────────────────────────────────────────
async def strategy_stop_hunter(
    session: aiohttp.ClientSession,
    symbol: str,
) -> Optional[dict]:
    """
    1. Find S&R levels on 1H and 4H
    2. On 1m/3m/5m: look for sweep of the level then reversal
       - SELL: price swept above resistance, formed lower high, broke last low
       - BUY:  price swept below support,    formed higher low, broke last high
    """
    # Get higher TF S&R
    c1h = await get_candles(session, symbol, "1h", 100)
    c4h = await get_candles(session, symbol, "4h", 100)
    if not c1h or not c4h: return None

    # FIX v3.2: use scored S&R so sr_score actually reaches calc_score
    scored_1h = find_sr_levels_scored(c1h)
    scored_4h = find_sr_levels_scored(c4h)
    levels_1h = [sl["price"] for sl in scored_1h]
    levels_4h = [sl["price"] for sl in scored_4h]
    all_scored = scored_1h + scored_4h
    all_levels = sorted(set(levels_1h + levels_4h))

    price = c1h[-1]["close"]

    # Find nearest level
    near = is_near_level(price, all_levels, tol=0.015)
    if near is None:
        # Check if price just broke a level (within last 3 candles on 1h)
        for lv in all_levels:
            recent_highs = [c["high"] for c in c1h[-3:]]
            recent_lows  = [c["low"]  for c in c1h[-3:]]
            if any(h > lv * 1.001 for h in recent_highs) or any(l < lv * 0.999 for l in recent_lows):
                near = lv
                break

    if near is None: return None

    # Check each lower TF for the sweep + reversal pattern
    for tf in ["1m", "3m", "5m"]:
        c = await get_candles(session, symbol, tf, 50)
        if len(c) < 20: continue

        highs  = [x["high"]  for x in c]
        lows   = [x["low"]   for x in c]
        closes = [x["close"] for x in c]

        # ── SELL setup ──
        # Swept above resistance (candle high > near level)
        swept_up = any(highs[-10:-1] > near * 1.001 for _ in [0])
        swept_up = max(highs[-10:-1]) > near * 1.001
        if swept_up:
            # Lower high formed after sweep
            local_max_idx = highs[-10:-1].index(max(highs[-10:-1]))
            after_highs = highs[-10+local_max_idx+1:-1] if local_max_idx < 8 else []
            lower_high  = len(after_highs) > 0 and max(after_highs) < max(highs[-10:-1])
            # Last low broken
            recent_low  = min(lows[-10:-2])
            broke_low   = closes[-1] < recent_low
            if lower_high and broke_low:
                shadow_candle_high = max(highs[-10:-1])
                stop  = shadow_candle_high * 1.002
                entry = closes[-1]
                tp1, tp2 = calc_targets(entry, stop, "SELL")
                # FIX v3.2: pass sr_score from nearest scored S&R level
                nearest_sr = get_nearest_sr_scored(near, all_scored) if near else None
                sr_sc = nearest_sr["sr_score"] if nearest_sr else None
                score = calc_score([swept_up, lower_high, broke_low, near in levels_4h], sr_score=sr_sc)
                return {
                    "symbol": symbol, "direction": "SELL",
                    "strategy": "Stop Hunter", "price": price,
                    "entry": entry, "stop": stop, "tp1": tp1, "tp2": tp2,
                    "timeframe": f"S/R on 1H/4H | Entry {tf}", "score": score,
                }

        # ── BUY setup ──
        swept_down = min(lows[-10:-1]) < near * 0.999
        if swept_down:
            local_min_idx = lows[-10:-1].index(min(lows[-10:-1]))
            after_lows  = lows[-10+local_min_idx+1:-1] if local_min_idx < 8 else []
            higher_low  = len(after_lows) > 0 and min(after_lows) > min(lows[-10:-1])
            recent_high = max(highs[-10:-2])
            broke_high  = closes[-1] > recent_high
            if higher_low and broke_high:
                shadow_candle_low = min(lows[-10:-1])
                stop  = shadow_candle_low * 0.998
                entry = closes[-1]
                tp1, tp2 = calc_targets(entry, stop, "BUY")
                # FIX v3.2: pass sr_score from nearest scored S&R level
                nearest_sr = get_nearest_sr_scored(near, all_scored) if near else None
                sr_sc = nearest_sr["sr_score"] if nearest_sr else None
                score = calc_score([swept_down, higher_low, broke_high, near in levels_4h], sr_score=sr_sc)
                return {
                    "symbol": symbol, "direction": "BUY",
                    "strategy": "Stop Hunter", "price": price,
                    "entry": entry, "stop": stop, "tp1": tp1, "tp2": tp2,
                    "timeframe": f"S/R on 1H/4H | Entry {tf}", "score": score,
                }
    return None

# ─── STRATEGY 2: HAMMER FIBONACCI (1H + 4H) ───────────────────────────────────
async def _hammer_fib_on_tf(
    session: aiohttp.ClientSession,
    symbol: str,
    pattern_tf: str,          # "4h" or "1h"
) -> Optional[dict]:
    """
    وقتی کندل pattern_tf بسته شد چک میکنه آیا چکش یا شوتینگ‌استار هست.
    اگر بود، در تایم‌فریم‌های پایین‌تر فیبو 0.618 رو پیدا میکنه.
    """
    c = await get_candles(session, symbol, pattern_tf, 30)
    if len(c) < 5: return None

    last  = c[-1]
    price = last["close"]
    hammer = is_hammer(last)
    star   = is_shooting_star(last)
    if not hammer and not star: return None

    direction   = "BUY" if hammer else "SELL"
    pattern_name = "Hammer" if hammer else "Shooting Star"

    # تایم‌فریم‌های ورود بر اساس pattern_tf
    entry_tfs = ["1m", "3m", "5m"] if pattern_tf == "4h" else ["1m", "3m", "5m"]

    for tf in entry_tfs:
        lc = await get_candles(session, symbol, tf, 80)
        if len(lc) < 20: continue

        highs  = [x["high"]  for x in lc]
        lows   = [x["low"]   for x in lc]

        if direction == "SELL":
            prev_high = max(highs[-40:-20])
            broke = any(h > prev_high for h in highs[-20:])
            if not broke: continue
            wave_high = max(highs[-20:])
            wave_low  = min(lows[-30:-20])
            fib_618   = fibonacci_level(wave_high, wave_low, 0.618)
            stop      = wave_high * 1.002
            entry     = fib_618
            tp1, tp2  = calc_targets(entry, stop, "SELL")
            score = calc_score([star, broke, True])
            return {
                "symbol": symbol, "direction": "SELL",
                "strategy": f"Hammer Fib ({pattern_tf.upper()})",
                "price": price, "entry": entry, "stop": stop,
                "tp1": tp1, "tp2": tp2,
                "timeframe": f"{pattern_tf.upper()} {pattern_name} | Fib {tf}",
                "score": score,
            }
        else:
            prev_low = min(lows[-40:-20])
            broke = any(l < prev_low for l in lows[-20:])
            if not broke: continue
            wave_low  = min(lows[-20:])
            wave_high = max(highs[-30:-20])
            fib_618   = wave_low + (wave_high - wave_low) * 0.618
            stop      = wave_low * 0.998
            entry     = fib_618
            tp1, tp2  = calc_targets(entry, stop, "BUY")
            score = calc_score([hammer, broke, True])
            return {
                "symbol": symbol, "direction": "BUY",
                "strategy": f"Hammer Fib ({pattern_tf.upper()})",
                "price": price, "entry": entry, "stop": stop,
                "tp1": tp1, "tp2": tp2,
                "timeframe": f"{pattern_tf.upper()} {pattern_name} | Fib {tf}",
                "score": score,
            }
    return None

async def strategy_hammer_fib(session, symbol):
    return await _hammer_fib_on_tf(session, symbol, "4h")

async def strategy_hammer_fib_1h(session, symbol):
    return await _hammer_fib_on_tf(session, symbol, "1h")

# ─── STRATEGY 3: HB ───────────────────────────────────────────────────────────
async def strategy_hb(
    session: aiohttp.ClientSession,
    symbol: str,
) -> Optional[dict]:
    """
    On important S&R level: strong spike candle opposite to trend →
    place order at 0.5 fib of that spike candle. Stop behind candle. TP 3R.
    """
    c1h = await get_candles(session, symbol, "1h", 100)
    c4h = await get_candles(session, symbol, "4h", 60)
    if not c1h or not c4h: return None

    # FIX v3.2: use scored S&R so sr_score propagates to calc_score
    scored_1h = find_sr_levels_scored(c1h)
    scored_4h = find_sr_levels_scored(c4h)
    all_scored = scored_1h + scored_4h
    levels = [sl["price"] for sl in all_scored]
    price  = c1h[-1]["close"]

    for tf in ["5m", "15m"]:
        c = await get_candles(session, symbol, tf, 80)
        if len(c) < 25: continue

        last = c[-1]
        prev = c[:-1]

        # Check near S&R
        near = is_near_level(price, levels, tol=0.008)
        if near is None: continue

        strong = is_strong_candle(last, prev[-20:])
        if not strong: continue

        # Determine trend direction from last 20 candles
        trend_up = c[-20]["close"] < c[-2]["close"]

        # FIX v3.2: get sr_score for the nearest level
        nearest_sr = get_nearest_sr_scored(near, all_scored) if near else None
        sr_sc = nearest_sr["sr_score"] if nearest_sr else None

        if trend_up:
            # At resistance, strong bearish spike → SELL at 0.5 of candle
            if last["close"] < last["open"]:  # bearish candle
                mid   = (last["high"] + last["low"]) / 2
                stop  = last["high"] * 1.002
                entry = mid
                tp1, tp2 = calc_targets(entry, stop, "SELL")
                score = calc_score([strong, near is not None, True], sr_score=sr_sc)
                return {
                    "symbol": symbol, "direction": "SELL",
                    "strategy": "HB", "price": price,
                    "entry": entry, "stop": stop, "tp1": tp1, "tp2": tp2,
                    "timeframe": tf, "score": score,
                }
        else:
            # At support, strong bullish spike → BUY at 0.5 of candle
            if last["close"] > last["open"]:  # bullish candle
                mid   = (last["high"] + last["low"]) / 2
                stop  = last["low"] * 0.998
                entry = mid
                tp1, tp2 = calc_targets(entry, stop, "BUY")
                score = calc_score([strong, near is not None, True], sr_score=sr_sc)
                return {
                    "symbol": symbol, "direction": "BUY",
                    "strategy": "HB", "price": price,
                    "entry": entry, "stop": stop, "tp1": tp1, "tp2": tp2,
                    "timeframe": tf, "score": score,
                }
    return None

# ─── STRATEGY 4: TRIGGER FIBONACCI ────────────────────────────────────────────
async def strategy_trigger_fib(
    session: aiohttp.ClientSession,
    symbol: str,
) -> Optional[dict]:
    """
    1H/15m structure → find important zone → wait for price to touch
    3m/1m: 3-condition trigger candle sequence → fib 0.618 entry after correction
    Condition 1: strong opposite candle
    Condition 2: next candle same color, close beyond shadow of prev
    Condition 3: close beyond shadow of last strong trend candle
    """
    c1h  = await get_candles(session, symbol, "1h",  60)
    c15m = await get_candles(session, symbol, "15m", 80)
    if not c1h or not c15m: return None

    # FIX v3.2: use scored S&R so sr_score propagates to calc_score
    scored_1h_tf = find_sr_levels_scored(c1h)
    scored_15m_tf = find_sr_levels_scored(c15m)
    all_scored_tf = scored_1h_tf + scored_15m_tf
    levels = [sl["price"] for sl in all_scored_tf]
    price  = c1h[-1]["close"]
    near   = is_near_level(price, levels, tol=0.008)
    if near is None: return None
    nearest_sr_tf = get_nearest_sr_scored(near, all_scored_tf) if near else None
    sr_sc_tf = nearest_sr_tf["sr_score"] if nearest_sr_tf else None

    # Detect structure trend from 1h
    trend_up = c1h[-10]["close"] < c1h[-1]["close"]

    for tf in ["3m", "1m"]:
        c = await get_candles(session, symbol, tf, 50)
        if len(c) < 10: continue

        # Need at least 3 candles for condition check
        # Candle indices: c[-3]=trigger_1, c[-2]=trigger_2, c[-1]=current
        t1 = c[-3]
        t2 = c[-2]
        t3 = c[-1]

        if trend_up:
            # Looking for bearish reversal at resistance
            # Cond 1: t1 is strong bearish candle
            cond1 = t1["close"] < t1["open"] and is_strong_candle(t1, c[-23:-3])
            # Cond 2: t2 is bearish, close below shadow (low) of t1
            cond2 = (t2["close"] < t2["open"] and
                     t2["close"] < t1["low"])
            # Cond 3: t3 close below shadow (low) of last strong bullish trend candle
            trend_candles_bull = [x for x in c[-20:-3] if x["close"] > x["open"]]
            if trend_candles_bull:
                last_bull_low = min(x["low"] for x in trend_candles_bull[-3:])
                cond3 = t3["close"] < last_bull_low
            else:
                cond3 = False

            if cond1 and cond2 and cond3:
                # ── Patch #1: Require minimum pullback before drawing Fibonacci ──
                pullback_ratio = measure_pullback(c, "SELL", wave_lookback=20)
                pb_valid, pb_score_mult = correction_score(pullback_ratio)
                if not pb_valid:
                    # Less than 20% pullback → early entry risk, skip
                    continue
                # Fib from t1 open to where mini-trend ended (t3 low)
                fib_high  = t1["open"]
                fib_low   = min(t2["low"], t3["low"])
                fib_618   = fibonacci_level(fib_high, fib_low, 0.618)
                stop  = t1["high"] * 1.002
                entry = fib_618
                tp1, tp2 = calc_targets(entry, stop, "SELL")
                base_score = calc_score([cond1, cond2, cond3, near in levels], sr_score=sr_sc_tf)
                score = max(1, min(100, round(base_score * pb_score_mult)))
                return {
                    "symbol": symbol, "direction": "SELL",
                    "strategy": "Trigger Fibonacci", "price": price,
                    "entry": entry, "stop": stop, "tp1": tp1, "tp2": tp2,
                    "timeframe": f"1H/15m zone | Entry {tf}", "score": score,
                    "pullback_ratio": round(pullback_ratio, 3),
                }

        else:
            # Looking for bullish reversal at support
            # Cond 1: t1 is strong bullish candle
            cond1 = t1["close"] > t1["open"] and is_strong_candle(t1, c[-23:-3])
            # Cond 2: t2 bullish, close above shadow (high) of t1
            cond2 = (t2["close"] > t2["open"] and
                     t2["close"] > t1["high"])
            # Cond 3: t3 close above shadow (high) of last strong bearish trend candle
            trend_candles_bear = [x for x in c[-20:-3] if x["close"] < x["open"]]
            if trend_candles_bear:
                last_bear_high = max(x["high"] for x in trend_candles_bear[-3:])
                cond3 = t3["close"] > last_bear_high
            else:
                cond3 = False

            if cond1 and cond2 and cond3:
                # ── Patch #1: Require minimum pullback before drawing Fibonacci ──
                pullback_ratio = measure_pullback(c, "BUY", wave_lookback=20)
                pb_valid, pb_score_mult = correction_score(pullback_ratio)
                if not pb_valid:
                    # Less than 20% pullback → early entry risk, skip
                    continue
                fib_low   = t1["open"]
                fib_high  = max(t2["high"], t3["high"])
                fib_618   = fib_low + (fib_high - fib_low) * 0.618
                stop  = t1["low"] * 0.998
                entry = fib_618
                tp1, tp2 = calc_targets(entry, stop, "BUY")
                base_score = calc_score([cond1, cond2, cond3, near in levels], sr_score=sr_sc_tf)
                score = max(1, min(100, round(base_score * pb_score_mult)))
                return {
                    "symbol": symbol, "direction": "BUY",
                    "strategy": "Trigger Fibonacci", "price": price,
                    "entry": entry, "stop": stop, "tp1": tp1, "tp2": tp2,
                    "timeframe": f"1H/15m zone | Entry {tf}", "score": score,
                    "pullback_ratio": round(pullback_ratio, 3),
                }
    return None

# ─── COOLDOWN TRACKER ─────────────────────────────────────────────────────────
_last_signal: dict[str, float] = {}   # key: "symbol_strategy_direction" → timestamp
_filter_cooldown: dict[str, float] = {}  # Patch #8/#13: symbol → time when filter cooldown expires

# Patch #9: Duplicate guard uses (symbol + strategy + direction)
SIGNAL_COOLDOWN_WINDOW = SIGNAL_COOLDOWN       # 3600s for confirmed signals
FILTER_RESCAN_COOLDOWN = 10 * 60               # Patch #8/#13: 10 minutes after filter rejection

def can_signal(symbol: str, strategy: str, open_keys: set = None, direction: str = "") -> bool:
    """Patch #9: Duplicate guard on (symbol + strategy + direction) within cooldown window."""
    key = f"{symbol}_{strategy}_{direction}"
    now = time.time()
    if key in _last_signal and now - _last_signal[key] < SIGNAL_COOLDOWN_WINDOW:
        return False
    if open_keys is not None and f"{symbol}_{strategy}" in open_keys:
        return False
    _last_signal[key] = now
    return True

def mark_filtered_for_rescan(symbol: str):
    """Patch #8/#13: Mark a symbol for re-scan after filter rejection cooldown."""
    _filter_cooldown[symbol] = time.time() + FILTER_RESCAN_COOLDOWN

def is_in_filter_cooldown(symbol: str) -> bool:
    """Returns True if symbol is still in filter cooldown (not yet ready for rescan)."""
    expiry = _filter_cooldown.get(symbol)
    if expiry is None:
        return False
    if time.time() >= expiry:
        del _filter_cooldown[symbol]
        return False
    return True

def get_open_trade_keys() -> set:
    """مجموعه‌ای از کلیدهای 'symbol_strategy' که هنوز معامله فعال دارند.
    Patch #6: شامل PENDING، ENTERED، و TP1_HIT (در انتظار TP2 یا SL) می‌شود."""
    conn = get_db_connection()
    try:
        rows = conn.execute("""
            SELECT s.symbol, s.strategy FROM signals s
            JOIN results r ON r.signal_id = s.id
            WHERE r.status IN ('OPEN','PENDING','ENTERED','TP1_HIT')
        """).fetchall()
        return {f"{row['symbol']}_{row['strategy']}" for row in rows}
    finally:
        conn.close()

# ─── STRATEGY 5: EXHAUSTION (ضعیف شدن روند در ناحیه مهم) ──────────────────────
async def strategy_exhaustion(
    session: aiohttp.ClientSession,
    symbol: str,
) -> Optional[dict]:
    """
    روند صعودی به ناحیه مقاومتی میرسه:
      - هرچه نزدیک‌تر، کندل‌های صعودی ضعیف‌تر و حجم کمتر → SELL
    روند نزولی به ناحیه حمایتی میرسه:
      - هرچه نزدیک‌تر، کندل‌های نزولی ضعیف‌تر و حجم کمتر → BUY
    """
    c1h = await get_candles(session, symbol, "1h", 120)
    c4h = await get_candles(session, symbol, "4h",  80)
    if not c1h or not c4h: return None

    # FIX v3.2: use scored S&R so sr_score propagates to calc_score
    scored_1h = find_sr_levels_scored(c1h)
    scored_4h = find_sr_levels_scored(c4h)
    all_scored_ex = scored_1h + scored_4h
    levels = [sl["price"] for sl in all_scored_ex]
    if not levels: return None

    price = c1h[-1]["close"]

    # پیدا کردن ناحیه نزدیک (tolerance بیشتر چون داریم نزدیک شدن رو چک میکنیم)
    near = is_near_level(price, levels, tol=0.012)
    if near is None: return None

    # کندل‌های ۱۵ دقیقه برای بررسی ضعیف شدن
    c15 = await get_candles(session, symbol, "15m", 60)
    if len(c15) < 20: return None

    # تعیین جهت روند اصلی از ۱ ساعته
    trend_up = c1h[-15]["close"] < c1h[-1]["close"]

    # آخرین ۱۰ کندل نزدیک به ناحیه
    recent = c15[-10:]

    if trend_up and price < near * 1.001:
        # بررسی ضعیف شدن کندل‌های صعودی
        bull_candles = [c for c in recent if c["close"] > c["open"]]
        if len(bull_candles) < 3: return None

        # بدنه کندل‌های صعودی باید کوچک‌تر بشن
        bodies = [abs(c["close"] - c["open"]) for c in bull_candles]
        body_weakening = bodies[-1] < bodies[0] * 0.7  # آخری حداقل ۳۰٪ کوچکتر از اولی

        # حجم کندل‌های صعودی باید کم بشه
        vols = [c["volume"] for c in bull_candles]
        vol_weakening = vols[-1] < vols[0] * 0.75

        # کندل آخر باید صعودی ضعیف یا نزولی باشه
        last_weak = abs(recent[-1]["close"] - recent[-1]["open"]) < avg_body(c15[-20:]) * 0.8

        if (body_weakening or vol_weakening) and last_weak:
            stop  = max(c["high"] for c in recent[-3:]) * 1.002
            entry = price
            tp1, tp2 = calc_targets(entry, stop, "SELL")
            # FIX v3.2: use sr_score from nearest scored S&R
            nearest_sr_ex = get_nearest_sr_scored(near, all_scored_ex) if near else None
            sr_sc_ex = nearest_sr_ex["sr_score"] if nearest_sr_ex else None
            in_4h = any(abs(near - sl["price"]) / sl["price"] < 0.012 for sl in scored_4h)
            score = calc_score([body_weakening, vol_weakening, last_weak, in_4h], sr_score=sr_sc_ex)
            return {
                "symbol": symbol, "direction": "SELL",
                "strategy": "Exhaustion", "price": price,
                "entry": entry, "stop": stop, "tp1": tp1, "tp2": tp2,
                "timeframe": "1H/4H zone | 15m exhaustion", "score": score,
            }

    elif not trend_up and price > near * 0.999:
        # بررسی ضعیف شدن کندل‌های نزولی
        bear_candles = [c for c in recent if c["close"] < c["open"]]
        if len(bear_candles) < 3: return None

        bodies = [abs(c["close"] - c["open"]) for c in bear_candles]
        body_weakening = bodies[-1] < bodies[0] * 0.7

        vols = [c["volume"] for c in bear_candles]
        vol_weakening = vols[-1] < vols[0] * 0.75

        last_weak = abs(recent[-1]["close"] - recent[-1]["open"]) < avg_body(c15[-20:]) * 0.8

        if (body_weakening or vol_weakening) and last_weak:
            stop  = min(c["low"] for c in recent[-3:]) * 0.998
            entry = price
            tp1, tp2 = calc_targets(entry, stop, "BUY")
            # FIX v3.2: use sr_score from nearest scored S&R
            nearest_sr_ex = get_nearest_sr_scored(near, all_scored_ex) if near else None
            sr_sc_ex = nearest_sr_ex["sr_score"] if nearest_sr_ex else None
            in_4h = any(abs(near - sl["price"]) / sl["price"] < 0.012 for sl in scored_4h)
            score = calc_score([body_weakening, vol_weakening, last_weak, in_4h], sr_score=sr_sc_ex)
            return {
                "symbol": symbol, "direction": "BUY",
                "strategy": "Exhaustion", "price": price,
                "entry": entry, "stop": stop, "tp1": tp1, "tp2": tp2,
                "timeframe": "1H/4H zone | 15m exhaustion", "score": score,
            }

    return None

# ─── STRATEGY 6: RSI DIVERGENCE / OVERBOUGHT-OVERSOLD ────────────────────────
async def strategy_rsi_divergence(
    session: aiohttp.ClientSession,
    symbol: str,
) -> Optional[dict]:
    """
    وقتی RSI (تایم 15 دقیقه) به بالای ۷۰ میرسد و دوباره به زیر ۷۰ برمیگردد → SELL
    وقتی RSI به زیر ۳۰ میرسد و دوباره به بالای ۳۰ برمیگردد → BUY
    همچنین واکینش‌های ساده (Divergence) بین قیمت و RSI روی سقف/کف‌های اخیر بررسی میشود.
    """
    c = await get_candles(session, symbol, "15m", 100)
    if len(c) < 30: return None

    closes = [x["close"] for x in c]
    highs  = [x["high"]  for x in c]
    lows   = [x["low"]   for x in c]
    rsis   = calc_rsi(closes, 14)

    if rsis[-1] is None or rsis[-2] is None:
        return None

    price       = closes[-1]
    rsi_now     = rsis[-1]
    rsi_prev    = rsis[-2]

    # ── خروج از ناحیه اشباع خرید (Overbought) → SELL ──
    if rsi_prev >= 70 and rsi_now < 70:
        # بررسی Bearish Divergence: قیمت سقف بالاتر زده ولی RSI سقف پایین‌تر زده
        recent_rsi_vals  = [r for r in rsis[-20:] if r is not None]
        divergence = False
        if len(recent_rsi_vals) >= 10:
            past_high_idx  = highs[-20:-5].index(max(highs[-20:-5])) if len(highs) >= 20 else 0
            recent_high    = max(highs[-5:])
            past_high      = max(highs[-20:-5]) if len(highs) >= 20 else 0
            if recent_high > past_high and rsi_now < max(recent_rsi_vals[:10] or [0]):
                divergence = True

        stop  = max(highs[-5:]) * 1.003
        entry = price
        tp1, tp2 = calc_targets(entry, stop, "SELL")
        score = calc_score([rsi_prev >= 70, rsi_now < 70, divergence, rsi_now < 65])
        return {
            "symbol": symbol, "direction": "SELL",
            "strategy": "RSI Divergence", "price": price,
            "entry": entry, "stop": stop, "tp1": tp1, "tp2": tp2,
            "timeframe": f"15m | RSI: {rsi_now:.1f} (از بالای ۷۰ برگشت)" + (" | Divergence" if divergence else ""),
            "score": score,
            "rsi": round(rsi_now, 1),
        }

    # ── خروج از ناحیه اشباع فروش (Oversold) → BUY ──
    if rsi_prev <= 30 and rsi_now > 30:
        recent_rsi_vals = [r for r in rsis[-20:] if r is not None]
        divergence = False
        if len(recent_rsi_vals) >= 10:
            recent_low = min(lows[-5:])
            past_low   = min(lows[-20:-5]) if len(lows) >= 20 else 0
            if recent_low < past_low and rsi_now > min(recent_rsi_vals[:10] or [100]):
                divergence = True

        stop  = min(lows[-5:]) * 0.997
        entry = price
        tp1, tp2 = calc_targets(entry, stop, "BUY")
        score = calc_score([rsi_prev <= 30, rsi_now > 30, divergence, rsi_now > 35])
        return {
            "symbol": symbol, "direction": "BUY",
            "strategy": "RSI Divergence", "price": price,
            "entry": entry, "stop": stop, "tp1": tp1, "tp2": tp2,
            "timeframe": f"15m | RSI: {rsi_now:.1f} (از زیر ۳۰ برگشت)" + (" | Divergence" if divergence else ""),
            "score": score,
            "rsi": round(rsi_now, 1),
        }

    return None

# ─── MAIN SCANNER ─────────────────────────────────────────────────────────────
STRATEGIES = [
    ("Stop Hunter",         strategy_stop_hunter),
    ("Hammer Fib (4H)",     strategy_hammer_fib),
    ("Hammer Fib (1H)",     strategy_hammer_fib_1h),
    ("HB",                  strategy_hb),
    ("Trigger Fibonacci",   strategy_trigger_fib),
    ("Exhaustion",          strategy_exhaustion),
    ("RSI Divergence",      strategy_rsi_divergence),
]

# ═══════════════════════════════════════════════════════════════════════════════
# 🟢 LAYER A — LIVE SIGNAL ENGINE  (all filters MUST pass, else signal blocked)
# ═══════════════════════════════════════════════════════════════════════════════
async def run_live_filters(session, symbol: str, result: dict, state: dict) -> tuple[bool, list[str]]:
    """
    زنجیره کامل فیلترهای Live Engine.
    خروجی: (True, []) اگر همه فیلترها پاس شوند — (False, [reasons]) اگر هر فیلتری بلاک کند.
    هر فیلتر شکست‌خورده دلیلش را در لیست reasons می‌گذارد.
    Patch #7:  high-score signals (85+) get relaxed strictness.
    Patch #10: reversal strategies — HTF alignment is soft (score penalty) not hard reject.
    Patch #11: volume spikes with structure context are confirmations, not rejections.
    Patch #12: 3+ confluences soften weak filters.
    """
    reasons = []
    direction  = result["direction"]
    score      = result.get("score", 0)
    strategy   = result.get("strategy", "")
    strat_type = STRATEGY_REGIME_TYPE.get(strategy, "HYBRID")

    # ── Patch #7: high-score relaxation ──
    high_score = score >= 85
    # FIX v3.2: very_high_score (>=86) relaxes ADX + score gate further
    very_high_score = score >= 86

    # ── Patch #12: Count confluences for override system ──
    confluence_count = 0
    confluence_flags = result.get("confluence_flags", [])
    # We'll increment as we detect them below

    # ── 1. Session filter ──
    if not is_session_allowed(state):
        reasons.append(f"session_blocked:{get_current_session()}")
        return False, reasons

    # ── 2. Volume (entry candle) filter ──
    entry_vol_filter_on = state.get("entry_volume_filter_enabled", True)
    ma_period    = state.get("entry_volume_ma_period", 20)
    vol_multiplier = state.get("entry_volume_multiplier", 1.5)
    if entry_vol_filter_on:
        try:
            c5m_vol = await get_candles(session, symbol, "5m", ma_period + 5)
            if not has_sufficient_entry_volume(c5m_vol, ma_period, vol_multiplier):
                if not high_score:
                    reasons.append("low_entry_volume")
                    return False, reasons
                # Patch #7: high-score → warn but don't block
        except Exception:
            pass

    # ── 3. ADX regime filter (FIX v3.2: use runtime-configurable adx_threshold, not hardcoded 15) ──
    try:
        c1h_adx = await get_candles(session, symbol, "1h", 100)
        adx_val = calc_adx(c1h_adx, 14)
        adx_min = get_adx_threshold()  # پویا از DB — با /adx_inc/dec یا آینده قابل تغییر است
        # اگر score خیلی بالا نیست، فیلتر سخت؛ اگر score >= 86 فقط آستانه مطلق ۱۰ را چک می‌کن
        effective_adx_min = max(10.0, adx_min - 8) if very_high_score else adx_min
        if adx_val < effective_adx_min:
            if not high_score:
                reasons.append(f"adx_too_low:{adx_val:.1f}")
                return False, reasons
            # high_score اما ADX ضعیف → فقط warn، بلاک نکن
    except Exception:
        pass

    # ── 4. EMA200 slope / trend filter — SOFT for Reversal strategies (Patch #10) ──
    htf_conflict = False
    try:
        c1h_ema  = await get_candles(session, symbol, "1h", 250)
        closes_1h = [c["close"] for c in c1h_ema]
        ema_slope = calc_ema200_slope(closes_1h)
        ema_conflict = (direction == "BUY" and ema_slope == "DOWN") or \
                       (direction == "SELL" and ema_slope == "UP")
        if ema_conflict:
            if strat_type == "TREND":
                # Patch #10: Trend strategies → hard reject
                reasons.append("ema200_slope_conflict_trend")
                return False, reasons
            elif strat_type in ("REVERSAL", "HYBRID"):
                # Patch #10: Reversal/Hybrid → soft filter, score penalty applied later
                htf_conflict = True
                result["htf_score_penalty"] = 15  # will be applied below
    except Exception:
        pass

    # ── 5. ATR volatility filter ──
    atr_mult = get_atr_multiplier()
    try:
        c5m_atr = await get_candles(session, symbol, "5m", 30)
        if is_volatility_abnormal(c5m_atr, threshold=atr_mult):
            if not high_score:
                reasons.append("abnormal_volatility")
                return False, reasons
    except Exception:
        pass

    # ── 6. Suspicious volume filter (Patch #11: context-aware) ──
    filter_on = state.get("volume_filter_enabled", True)
    if filter_on:
        try:
            c5m = await get_candles(session, symbol, "5m", 30)
            if has_suspicious_opposing_volume(c5m, direction):
                # Patch #11: Check for structural context that validates the spike
                has_structure_context = _has_volume_structure_context(c5m, direction)
                if has_structure_context:
                    # Volume spike with structure = confirmation, not rejection
                    confluence_count += 1
                elif not high_score:
                    reasons.append("suspicious_opposing_volume")
                    return False, reasons
        except Exception:
            pass

    # ── 7. Higher-trend alignment filter (Patch #10 already handled above via htf_conflict) ──
    if not htf_conflict:
        try:
            if not await is_aligned_with_higher_trend(session, symbol, direction):
                if strat_type == "TREND":
                    reasons.append("against_higher_tf_trend")
                    return False, reasons
                elif strat_type in ("REVERSAL", "HYBRID"):
                    htf_conflict = True
                    result["htf_score_penalty"] = result.get("htf_score_penalty", 0) + 10
        except Exception:
            pass

    # ── Patch #10: Apply HTF penalty for reversals ──
    if htf_conflict:
        penalty = result.get("htf_score_penalty", 15)
        result["score"] = max(1, result.get("score", 0) - penalty)
        score = result["score"]

    # ── Patch #12: Confluence override — soften remaining filters if 3+ confluences ──
    total_confluences = confluence_count + len(confluence_flags)
    high_confluence = total_confluences >= 3 or (high_score and total_confluences >= 2)

    # ── 8. Score gate (FIX v3.2: use runtime-configurable min score) ──
    # اگر score >= 86 باشد، آستانه به‌صورت خودکار کاهش می‌یابد (فیلترها سبک‌تر می‌شوند)
    # FIX v3.3: اگر کاربر آستانه‌ای بالاتر از ۹۴ تنظیم کرده، relaxation اعمال نمی‌شود
    # تا min_score واقعی کاربر نادیده گرفته نشود.
    runtime_min = get_min_signal_score()
    USER_STRICT_THRESHOLD = 95  # اگر کاربر >= این مقدار تنظیم کرده، relaxation غیرفعال است
    user_is_strict = runtime_min >= USER_STRICT_THRESHOLD
    effective_min_score = runtime_min
    if not user_is_strict:
        if high_confluence:
            effective_min_score = max(50, runtime_min - 10)  # soften by 10 pts
        # اگر score بالای 86 است، فیلترهای ضعیف را نادیده بگیر (طبق نیاز #6)
        if very_high_score:
            effective_min_score = max(50, effective_min_score - 8)
    if score < effective_min_score:
        reasons.append(f"low_score:{score}")
        return False, reasons

    # ── 9. Signal RR/math validation (Patch #5) ──
    rr_ok, rr_reason = validate_signal_rr(result)
    if not rr_ok:
        reasons.append(f"invalid_rr:{rr_reason}")
        return False, reasons

    return True, []


def _has_volume_structure_context(candles: list[dict], direction: str) -> bool:
    """
    Patch #11: Returns True if high volume candle appears with structural context
    (liquidity sweep, divergence signal, exhaustion candle, or strong reversal candle).
    """
    if len(candles) < 10:
        return False
    avg_vol = avg_volume(candles[:-3], 20) if len(candles) > 23 else avg_volume(candles, len(candles)-1)
    if avg_vol == 0:
        return False
    recent = candles[-5:]
    for c in recent:
        body = abs(c["close"] - c["open"])
        high_vol = c["volume"] >= 1.8 * avg_vol
        if not high_vol:
            continue
        # Strong reversal candle: body >= 60% of range
        total_range = c["high"] - c["low"]
        strong_candle = total_range > 0 and body / total_range >= 0.6
        # Wick sweep: long shadow toward opposite direction (liquidity sweep)
        if direction == "BUY":
            lower_shadow = min(c["open"], c["close"]) - c["low"]
            sweep = lower_shadow >= body * 1.5 and c["close"] > c["open"]
        else:
            upper_shadow = c["high"] - max(c["open"], c["close"])
            sweep = upper_shadow >= body * 1.5 and c["close"] < c["open"]
        if strong_candle or sweep:
            return True
    return False


# ═══════════════════════════════════════════════════════════════════════════════
# 📊 LAYER B — BACKTEST ENGINE  (async, batched, queued, cached)
# ═══════════════════════════════════════════════════════════════════════════════
_backtest_queue: asyncio.Queue = None          # init در main()
_backtest_cache: dict = {}                     # job_id → result dict
_backtest_running = False

def _init_backtest_queue():
    global _backtest_queue
    _backtest_queue = asyncio.Queue(maxsize=BACKTEST_MAX_QUEUE)

def _bt_job_id(params: dict) -> str:
    import hashlib
    return hashlib.md5(json.dumps(params, sort_keys=True).encode()).hexdigest()[:12]

async def _fetch_candles_for_backtest(session, symbol: str, interval: str, timerange_hours: int) -> list:
    """تعداد کندل‌های مناسب برای بازه زمانی خواسته‌شده (با احتساب تبدیل ساعت به تعداد کندل)."""
    interval_minutes = {"1h": 60, "4h": 240, "1m": 1, "5m": 5, "15m": 15}
    mins = interval_minutes.get(interval, 60)
    limit = min(1000, max(50, (timerange_hours * 60) // mins + 50))
    return await get_candles(session, symbol, interval, limit)

def _ms_to_iso(ts) -> str:
    """
    تبدیل timestamp به رشته ISO 8601 UTC.
    - اگر رشته ISO باشد (از دیتابیس واقعی مثل '2024-01-15T10:30:00'): همان را برمی‌گرداند.
    - اگر عدد باشد: از millisecond یا second به ISO تبدیل می‌کند.
    - اگر None باشد: None برمی‌گرداند.
    """
    try:
        if ts is None:
            return None
        # رشته ISO از دیتابیس: فقط 16 کاراکتر اول برای نمایش YY-MM-DDTHH:MM
        if isinstance(ts, str):
            return ts[:16] if len(ts) >= 16 else ts
        # عدد: millisecond یا second
        ts_int = int(ts)
        if ts_int > 32503680000:  # بیشتر از سال 3000 → millisecond
            ts_int = ts_int // 1000
        return datetime.fromtimestamp(ts_int, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M")
    except Exception:
        return str(ts) if ts is not None else None

async def _simulate_trade_outcome(session, symbol: str, result: dict, timerange_hours: int) -> dict:
    """
    شبیه‌سازی نتیجه یک سیگنال روی کندل‌های آینده (نسبت به لحظه سیگنال).
    منطق:
      - کندل‌های 1h را می‌گیریم (از timerange_hours ساعت قبل تا الان).
      - کندل آخر = کندل سیگنال (همین لحظه). از کندل بعد از آن شروع می‌کنیم.
      - برای هر کندل بعدی: ابتدا Entry باید لمس شود، سپس TP/SL چک می‌شود.
      - اگر در بازه timerange_hours نتیجه مشخص نشد → OPEN.
    """
    try:
        # یک بافر اضافی (50 کندل) برای اینکه مطمئن شویم کندل‌های "بعد از سیگنال" داریم
        c = await _fetch_candles_for_backtest(session, symbol, "1h", timerange_hours + 5)
        if not c or len(c) < 2:
            return {**result, "outcome": "OPEN", "pnl_percent": None, "rr_multiple": None,
                    "entry_ts": None, "exit_ts": None}

        entry     = result["entry"]
        stop      = result["stop"]
        tp1       = result["tp1"]
        tp2       = result["tp2"]
        direction = result["direction"]
        risk_pct  = abs(entry - stop) / entry * 100 if entry else 1

        # FIX v3.4: منطق صحیح future_candles برای بک‌تست
        # بک‌تست می‌گوید: "اگر این سیگنال ~timerange_hours ساعت پیش صادر می‌شد، چه نتیجه‌ای داشت؟"
        # c = کندل‌های 1H از گذشته تا الان (آخرین = کندل جاری)
        # نقطه فرضی سیگنال = اولین کندل (قدیمی‌ترین)
        # future_candles = همه کندل‌های بعد از نقطه فرضی سیگنال = c[1:] (همه به جز اولین)
        # entry تقریباً = close اولین کندل که باید در محدوده کندل‌های بعدی باشد

        if len(c) < 3:
            return {**result, "outcome": "OPEN", "pnl_percent": None, "rr_multiple": None,
                    "entry_ts": None, "exit_ts": None}

        # کندل‌های "آینده" = همه کندل‌ها به جز اولین (که نماینده لحظه سیگنال است)
        # محدود به timerange_hours کندل تا timeframe بک‌تست رعایت شود
        max_candles = min(len(c) - 1, max(timerange_hours, 1))
        future_candles = c[1: 1 + max_candles]

        if not future_candles:
            return {**result, "outcome": "OPEN", "pnl_percent": None, "rr_multiple": None,
                    "entry_ts": None, "exit_ts": None}

        entry_candle_ts = None
        entered = False
        tp1_reached = False   # FIX v3.4: track TP1 independently

        for candle in future_candles:
            h, l = candle["high"], candle["low"]
            ts_iso = _ms_to_iso(candle.get("open_time"))

            # ── مرحله ۱: آیا Entry لمس شده؟ ──
            if not entered:
                entry_touched = (direction == "BUY" and l <= entry <= h) or \
                                (direction == "SELL" and l <= entry <= h)
                if entry_touched:
                    entered = True
                    entry_candle_ts = ts_iso
                    # FIX v3.4: چک فوری TP/SL در همان کندل ورود
                    _ht2 = (direction == "BUY" and h >= tp2) or (direction == "SELL" and l <= tp2)
                    _ht1 = (direction == "BUY" and h >= tp1) or (direction == "SELL" and l <= tp1)
                    _hsl = (direction == "BUY" and l <= stop) or (direction == "SELL" and h >= stop)
                    if _ht2:
                        pnl = ((tp2 - entry) / entry * 100) if direction == "BUY" else ((entry - tp2) / entry * 100)
                        rr  = pnl / risk_pct if risk_pct else 0
                        return {**result, "outcome": "TP2", "pnl_percent": round(pnl, 2),
                                "rr_multiple": round(rr, 3), "entry_ts": entry_candle_ts, "exit_ts": ts_iso}
                    elif _ht1:
                        tp1_reached = True  # TP1 در همان کندل ورود — ادامه دهیم
                    elif _hsl:
                        pnl = ((stop - entry) / entry * 100) if direction == "BUY" else ((entry - stop) / entry * 100)
                        rr  = -abs(pnl / risk_pct) if risk_pct else -1.0
                        return {**result, "outcome": "SL", "pnl_percent": round(-abs(pnl), 2),
                                "rr_multiple": round(rr, 3), "entry_ts": entry_candle_ts, "exit_ts": ts_iso}
                # FIX v3.4: اگر قیمت مستقیم به TP1/TP2/SL رفت بدون لمس Entry → MISSED (No Trade)
                # یعنی معامله اصلاً باز نشد — نه سود، نه ضرر
                elif (direction == "BUY" and (h >= tp1 or h >= tp2 or l <= stop)) or \
                     (direction == "SELL" and (l <= tp1 or l <= tp2 or h >= stop)):
                    return {**result, "outcome": "MISSED", "pnl_percent": None, "rr_multiple": None,
                            "entry_ts": None, "exit_ts": ts_iso}
                continue

            # ── مرحله ۲: بعد از Entry → چک TP/SL ──
            hit_tp2 = (direction == "BUY" and h >= tp2) or (direction == "SELL" and l <= tp2)
            hit_tp1 = (direction == "BUY" and h >= tp1) or (direction == "SELL" and l <= tp1)
            hit_sl  = (direction == "BUY" and l <= stop) or (direction == "SELL" and h >= stop)

            if hit_tp2:
                # FIX v3.4: TP2 زده شد — نتیجه نهایی TP2 (چه TP1 قبلاً زده شده باشد چه نه)
                pnl = ((tp2 - entry) / entry * 100) if direction == "BUY" else ((entry - tp2) / entry * 100)
                rr  = pnl / risk_pct if risk_pct else 0
                return {**result, "outcome": "TP2", "pnl_percent": round(pnl, 2),
                        "rr_multiple": round(rr, 3), "entry_ts": entry_candle_ts, "exit_ts": ts_iso}
            elif hit_tp1 and not tp1_reached:
                # FIX v3.4: TP1 زده شد — loop ادامه می‌دهد برای TP2 یا SL (break even)
                tp1_reached = True
                # ادامه می‌دهیم — منتظر TP2 یا SL می‌مانیم
            elif hit_sl:
                if tp1_reached:
                    # FIX v3.4: TP1 زده شد سپس SL لمس شد → TP1 (TOUCH SL) = Break Even
                    pnl = ((tp1 - entry) / entry * 100) if direction == "BUY" else ((entry - tp1) / entry * 100)
                    rr  = pnl / risk_pct if risk_pct else 0
                    return {**result, "outcome": "TP1_TOUCHSL", "pnl_percent": round(pnl, 2),
                            "rr_multiple": round(rr, 3), "entry_ts": entry_candle_ts, "exit_ts": ts_iso}
                else:
                    # SL مستقیم بدون TP1 → ضرر
                    pnl = ((stop - entry) / entry * 100) if direction == "BUY" else ((entry - stop) / entry * 100)
                    rr  = -abs(pnl / risk_pct) if risk_pct else -1.0
                    return {**result, "outcome": "SL", "pnl_percent": round(-abs(pnl), 2),
                            "rr_multiple": round(rr, 3), "entry_ts": entry_candle_ts, "exit_ts": ts_iso}

        # بازه تمام شد بدون نتیجه — اگر TP1 زده شده بود آن را برگردان
        if tp1_reached:
            pnl = ((tp1 - entry) / entry * 100) if direction == "BUY" else ((entry - tp1) / entry * 100)
            rr  = pnl / risk_pct if risk_pct else 0
            return {**result, "outcome": "TP1", "pnl_percent": round(pnl, 2),
                    "rr_multiple": round(rr, 3), "entry_ts": entry_candle_ts, "exit_ts": None}
        return {**result, "outcome": "OPEN", "pnl_percent": None, "rr_multiple": None,
                "entry_ts": entry_candle_ts, "exit_ts": None}
    except Exception as e:
        log.debug(f"Backtest simulate error {symbol}: {e}")
        return {**result, "outcome": "OPEN", "pnl_percent": None, "rr_multiple": None,
                "entry_ts": None, "exit_ts": None}

def _load_closed_trades_from_db(timerange_hours: int) -> list:
    """
    معاملات واقعی بسته‌شده را از دیتابیس می‌خواند.
    فقط سیگنال‌هایی که در بازه timerange_hours گذشته بسته شده‌اند برمی‌گردند.

    نکات مهم:
    - فیلتر زمانی بر اساس closed_at (UTC ISO string) انجام می‌شود.
    - PENDING/ENTERED/TP1_HIT (هنوز باز) را حذف می‌کنیم.
    - opened_at برای سیگنال‌هایی که هنوز بسته نشده‌اند به عنوان fallback استفاده می‌شود.
    - timestamp مقایسه: هر دو طرف UTC هستند → مشکل timezone وجود ندارد.
    """
    cutoff_dt = datetime.now(timezone.utc) - __import__('datetime').timedelta(hours=timerange_hours)
    # SQLite ISO strings: "2024-01-15T10:30:00+00:00" یا "2024-01-15T10:30:00"
    # هر دو فرمت با مقایسه string کار می‌کنند چون ISO 8601 lexicographically مرتب است
    # برای اطمینان از سازگاری، cutoff را به فرمت ساده ISO بدون timezone تبدیل می‌کنیم
    cutoff_str = cutoff_dt.strftime("%Y-%m-%dT%H:%M:%S")

    conn = get_db_connection()
    try:
        rows = conn.execute("""
            SELECT s.id, s.symbol, s.direction, s.strategy,
                   s.entry, s.stop, s.tp1, s.tp2, s.score,
                   s.opened_at,
                   r.status, r.entered_at, r.tp1_hit, r.tp2_hit,
                   r.tp1_hit_at, r.tp2_hit_at,
                   r.closed_at, r.close_price, r.pnl_percent, r.rr_multiple
            FROM signals s
            JOIN results r ON r.signal_id = s.id
            WHERE r.status NOT IN ('PENDING', 'ENTERED', 'TP1_HIT', 'OPEN')
              AND r.closed_at IS NOT NULL
              AND substr(r.closed_at, 1, 19) >= ?
            ORDER BY r.closed_at ASC
        """, (cutoff_str,)).fetchall()
    finally:
        conn.close()

    trades = []
    # نگاشت status دیتابیس به outcome بک‌تست
    _status_to_outcome = {
        "TP1": "TP1", "TP1_HIT": "TP1",
        "TP2": "TP2", "TP2_HIT": "TP2",
        "TP1_TOUCHSL": "TP1_TOUCHSL",
        "SL": "SL", "SL_HIT": "SL",
        "MISSED": "MISSED",
        "EXPIRED": "OPEN",
    }

    for row in rows:
        raw_status = row["status"] or "OPEN"
        outcome = _status_to_outcome.get(raw_status, "OPEN")

        # pnl محاسبه می‌شود اگر در DB موجود است، وگرنه از entry/close_price محاسبه می‌کنیم
        pnl = row["pnl_percent"]
        if pnl is None and row["close_price"] and row["entry"]:
            direction = row["direction"]
            if direction == "BUY":
                pnl = (row["close_price"] - row["entry"]) / row["entry"] * 100
            else:
                pnl = (row["entry"] - row["close_price"]) / row["entry"] * 100
            pnl = round(pnl, 2)

        rr = row["rr_multiple"]
        if rr is None and pnl is not None and row["entry"] and row["stop"]:
            risk_pct = abs(row["entry"] - row["stop"]) / row["entry"] * 100
            if risk_pct > 0:
                rr = round(pnl / risk_pct, 3)

        trades.append({
            "symbol":      row["symbol"],
            "strategy":    row["strategy"],
            "direction":   row["direction"],
            "entry":       row["entry"],
            "stop":        row["stop"],
            "tp1":         row["tp1"],
            "tp2":         row["tp2"],
            "score":       row["score"],
            "outcome":     outcome,
            "pnl_percent": pnl,
            "rr_multiple": rr,
            # entry_ts: از entered_at یا opened_at
            "entry_ts":    row["entered_at"] or row["opened_at"],
            # exit_ts: از closed_at
            "exit_ts":     row["closed_at"],
            "trend_state": "",
            "adx_value":   0,
            "ema200_slope": "",
            "vol_regime":  "",
            "session":     "",
            "entry_reason": "",
        })

    return trades


async def _run_backtest_job(session, job: dict) -> dict:
    """
    اجرای یک job بک‌تست با خواندن معاملات واقعی بسته‌شده از دیتابیس.

    رویکرد جدید (v3.4 fix):
    - به جای اجرای مجدد استراتژی‌ها و شبیه‌سازی، معاملاتی که واقعاً در دیتابیس
      ثبت و بسته شده‌اند را می‌خوانیم.
    - فیلتر زمانی: فقط معاملاتی که closed_at آن‌ها در بازه timerange_hours گذشته
      باشد وارد گزارش می‌شود.
    - هیچ معامله‌ای به دلیل مشکل timestamp، UTC/Local یا query اشتباه حذف نمی‌شود.
    """
    global _backtest_running
    _backtest_running = True

    job_id    = job["job_id"]
    timerange = job["timerange_hours"]

    # ── به‌روزرسانی وضعیت job در DB ──
    conn = get_db_connection()
    try:
        conn.execute("UPDATE backtest_jobs SET status='RUNNING' WHERE job_id=?", (job_id,))
        conn.commit()
    finally:
        conn.close()

    # ── خواندن معاملات واقعی بسته‌شده از دیتابیس ──
    try:
        all_trades = _load_closed_trades_from_db(timerange)
    except Exception as e:
        log.error(f"BT DB read error: {e}")
        all_trades = []

    log.info(f"BT job {job_id}: {len(all_trades)} closed trades found in last {timerange}h")

    # ── Summary ──
    wins      = [t for t in all_trades if t.get("outcome") in ("TP1", "TP1_TOUCHSL", "TP2")]
    losses    = [t for t in all_trades if t.get("outcome") == "SL"]
    total_pnl = sum(t.get("pnl_percent", 0) or 0 for t in all_trades)
    summary = {
        "total":     len(all_trades),
        "wins":      len(wins),
        "losses":    len(losses),
        "win_rate":  round(len(wins) / len(all_trades) * 100, 1) if all_trades else 0,
        "total_pnl": round(total_pnl, 2),
    }

    # ── به‌روزرسانی DB ──
    conn = get_db_connection()
    try:
        conn.execute(
            "UPDATE backtest_jobs SET status='DONE', completed_at=?, result_summary=? WHERE job_id=?",
            (datetime.now(timezone.utc).isoformat(), json.dumps(summary), job_id)
        )
        conn.commit()
    finally:
        conn.close()

    _backtest_cache[job_id] = {"trades": all_trades, "summary": summary}
    _backtest_running = False
    return {"job_id": job_id, "trades": all_trades, "summary": summary}

async def enqueue_backtest(session, job_params: dict) -> str:
    """
    یک job بک‌تست را در صف قرار می‌دهد (یا از cache برمی‌گرداند اگر تکراری باشد).
    خروجی: job_id
    """
    job_id = _bt_job_id(job_params)

    # ── cache hit ──
    if job_id in _backtest_cache:
        cached = _backtest_cache[job_id]
        cached_time = _backtest_cache.get(f"{job_id}_ts", 0)
        if time.time() - cached_time < BACKTEST_CACHE_TTL:
            return job_id

    # ── ثبت در DB ──
    now_str = datetime.now(timezone.utc).isoformat()
    conn = get_db_connection()
    try:
        conn.execute(
            "INSERT OR IGNORE INTO backtest_jobs (job_id,status,requested_by,params,created_at) VALUES (?,?,?,?,?)",
            (job_id, "QUEUED", str(job_params.get("requested_by","")), json.dumps(job_params), now_str)
        )
        conn.commit()
    finally:
        conn.close()

    # ── queue ──
    if _backtest_queue and not _backtest_queue.full():
        job_params["job_id"] = job_id
        await _backtest_queue.put(job_params)

    return job_id

async def backtest_worker_loop(session, state):
    """حلقه background که jobهای صف بک‌تست را یکی‌یکی اجرا می‌کند (بدون block کردن Live Engine)."""
    while True:
        try:
            if _backtest_queue is None:
                await asyncio.sleep(5)
                continue
            job = await asyncio.wait_for(_backtest_queue.get(), timeout=10)
            job["state"] = state
            log.info(f"BT job starting: {job['job_id']}")
            await _run_backtest_job(session, job)
            log.info(f"BT job done: {job['job_id']}")
        except asyncio.TimeoutError:
            pass
        except Exception as e:
            log.error(f"BT worker error: {e}")
        await asyncio.sleep(1)


# ─── CHART GENERATION (simple ASCII/text chart for Telegram) ──────────────────
def build_trade_chart(symbol: str, entry: float, stop: float, tp1: float, tp2: float,
                      direction: str, outcome: str = None) -> str:
    """
    ساخت یک نمودار متنی ساده برای نمایش Entry/SL/TP در تلگرام.
    (برای chart تصویری واقعی، نیاز به matplotlib دارد که در محیط production ممکن است موجود نباشد)
    """
    decimals = 8 if entry < 0.01 else (4 if entry < 10 else 2)
    f = lambda v: f"{v:.{decimals}f}"
    arrow = "↑" if direction == "BUY" else "↓"
    outcome_str = f" ➜ {outcome}" if outcome else ""
    lines = [
        f"📊 <b>{symbol} {direction} {arrow}</b>{outcome_str}",
        f"",
        f"   🎯 TP2: {f(tp2)}",
        f"   ─────────────────",
        f"   🎯 TP1: {f(tp1)}",
        f"   ─────────────────",
        f"   💰 Entry: {f(entry)}  ◄",
        f"   ─────────────────",
        f"   🛑 SL: {f(stop)}",
    ]
    if direction == "SELL":
        lines = [
            f"📊 <b>{symbol} {direction} {arrow}</b>{outcome_str}",
            f"",
            f"   🛑 SL: {f(stop)}",
            f"   ─────────────────",
            f"   💰 Entry: {f(entry)}  ◄",
            f"   ─────────────────",
            f"   🎯 TP1: {f(tp1)}",
            f"   ─────────────────",
            f"   🎯 TP2: {f(tp2)}",
        ]
    return "\n".join(lines)


# ─── BACKTEST REPORT BUILDER ──────────────────────────────────────────────────
def build_backtest_report(trades: list, summary: dict, timerange_hours: int) -> list[str]:
    """
    ساخت گزارش بک‌تست به صورت لیستی از پیام‌ها (هر پیام حداکثر ۳۸۰۰ کاراکتر — chunked).
    برای هر trade: symbol، strategy، direction، entry/SL/TP، outcome، PnL، R-multiple، context.
    """
    messages = []
    header = (
        f"📊 <b>گزارش بک‌تست AFEE TRADER</b>\n"
        f"⏱ بازه: {timerange_hours} ساعت\n"
        f"📦 کل سیگنال‌ها: {summary['total']} | ✅ Wins: {summary['wins']} | ❌ Losses: {summary['losses']}\n"
        f"🎯 Win Rate: {summary['win_rate']}% | 💵 جمع PnL: {summary['total_pnl']:+.2f}%\n"
        f"━━━━━━━━━━━━━━━\n"
    )
    messages.append(header)

    chunk = ""
    for t in trades:
        decimals = 8 if t.get("entry", 1) < 0.01 else (4 if t.get("entry", 1) < 10 else 2)
        f = lambda v: f"{v:.{decimals}f}" if v else "—"
        outcome_emoji = {"TP1": "✅", "TP1_TOUCHSL": "🔄", "TP2": "✅✅", "SL": "❌", "OPEN": "⏳", "MISSED": "⚠️"}.get(t.get("outcome",""), "")
        pnl_str = f"{t['pnl_percent']:+.2f}%" if t.get("pnl_percent") is not None else "—"
        rr_str  = f"{t['rr_multiple']:+.2f}R" if t.get("rr_multiple") is not None else "—"
        trade_txt = (
            f"{outcome_emoji} <b>{t.get('symbol','')} | {t.get('strategy','')} | {t.get('direction','')}</b>\n"
            f"  Entry: {f(t.get('entry'))} | SL: {f(t.get('stop'))} | TP1: {f(t.get('tp1'))} | TP2: {f(t.get('tp2'))}\n"
            f"  نتیجه: {t.get('outcome','—')} | PnL: {pnl_str} | RR: {rr_str}\n"
            f"  ورود: {_ms_to_iso(t.get('entry_ts')) or '—'} | خروج: {_ms_to_iso(t.get('exit_ts')) or '—'}\n"
        )
        if t.get("trend_state"):
            trade_txt += f"  🌐 Trend: {t['trend_state']}\n"
        if t.get("session"):
            trade_txt += f"  🕐 Session: {t['session']}\n"
        if t.get("entry_reason"):
            trade_txt += f"  📝 {t['entry_reason']}\n"
        trade_txt += "─────\n"

        if len(chunk) + len(trade_txt) > 3800:
            messages.append(chunk)
            chunk = trade_txt
        else:
            chunk += trade_txt

    if chunk:
        messages.append(chunk)

    return messages


# ═══════════════════════════════════════════════════════════════════════════════
# 🧠 LAYER C — PERSONAL ANALYZER  (unfiltered — filters become warnings only)
# ═══════════════════════════════════════════════════════════════════════════════
async def _build_analyzer_warnings(session, symbol: str, result: dict, state: dict) -> list[str]:
    """
    فیلترها را روی سیگنال اجرا می‌کند ولی هیچ‌کدام را بلاک نمی‌کند.
    فقط لیستی از هشدارها برمی‌گرداند.
    """
    warnings = []
    direction = result["direction"]

    # Session warning
    if not is_session_allowed(state):
        warnings.append(f"⚠️ Session mismatch ({get_current_session().upper()})")

    # Volume warning
    try:
        ma_period = state.get("entry_volume_ma_period", 20)
        vol_multiplier = state.get("entry_volume_multiplier", 1.5)
        c5m_vol = await get_candles(session, symbol, "5m", ma_period + 5)
        if not has_sufficient_entry_volume(c5m_vol, ma_period, vol_multiplier):
            warnings.append("⚠️ Volume anomaly detected (entry candle volume below threshold)")
    except Exception:
        pass

    # ATR volatility warning
    try:
        atr_mult = get_atr_multiplier()
        c5m_atr = await get_candles(session, symbol, "5m", 30)
        if is_volatility_abnormal(c5m_atr, threshold=atr_mult):
            warnings.append("⚠️ High volatility regime")
    except Exception:
        pass

    # Opposing volume warning
    try:
        c5m = await get_candles(session, symbol, "5m", 30)
        if has_suspicious_opposing_volume(c5m, direction):
            warnings.append("⚠️ News / macro risk zone (suspicious opposing volume)")
    except Exception:
        pass

    # Trend conflict warning
    try:
        if not await is_aligned_with_higher_trend(session, symbol, direction):
            warnings.append("⚠️ Trend conflict (signal opposes 1H trend)")
    except Exception:
        pass

    # Regime mismatch warning
    if result.get("regime_mismatch"):
        warnings.append("⚠️ Strategy type mismatches current market regime (ADX zone)")

    return warnings

async def normalize_and_validate_symbol(session, raw_text: str) -> Optional[str]:
    """اسم ارز ورودی کاربر را به فرمت نماد بایننس (مثل BTCUSDT) تبدیل و اعتبارسنجی می‌کند."""
    sym = raw_text.strip().upper().replace(" ", "")
    if not sym:
        return None
    if not sym.endswith("USDT"):
        sym += "USDT"
    try:
        url = f"{BINANCE_BASE}/ticker/price"
        async with session.get(url, params={"symbol": sym}, proxy=PROXY,
                               timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200:
                return None
            data = await r.json()
            if "price" not in data:
                return None
    except Exception:
        return None
    return sym

async def analyze_symbol_manually(session, symbol: str, state: dict = None) -> str:
    """
    🧠 LAYER C — PERSONAL ANALYZER
    تحلیل دستی یک ارز با تمام استراتژی‌ها.
    فیلترها هیچ‌گاه سیگنال را بلاک نمی‌کنند — فقط هشدار به خروجی اضافه می‌شود.
    """
    state = state or {}
    found_signals = []

    try:
        regime = await get_market_regime(session, symbol)
    except Exception:
        regime = None

    for strat_name, strat_fn in STRATEGIES:
        try:
            result = await strat_fn(session, symbol)
            if not result:
                continue

            # ── Regime mismatch: فقط flag می‌گذاریم، بلاک نمی‌کنیم ──
            if regime:
                strat_type = STRATEGY_REGIME_TYPE.get(strat_name, "HYBRID")
                if strat_type not in regime["allowed_types"]:
                    result["regime_mismatch"] = True

            # ── اعمال وزن آداپتیو روی Score ──
            try:
                weight = get_strategy_weight(strat_name)
                if weight != 1.0:
                    result["score"] = max(1, min(100, round(result.get("score", 0) * weight)))
                    result["strategy_weight"] = weight
            except Exception:
                pass

            # ── جمع‌آوری هشدارها (بدون بلاک) ──
            result["analyzer_warnings"] = await _build_analyzer_warnings(session, symbol, result, state)

            found_signals.append(result)
        except Exception as e:
            log.debug(f"Analyzer error {strat_name} {symbol}: {e}")

    # قیمت فعلی
    try:
        url = f"{BINANCE_BASE}/ticker/price"
        async with session.get(url, params={"symbol": symbol}, proxy=PROXY,
                               timeout=aiohttp.ClientTimeout(total=10)) as r:
            price_data = await r.json()
        current_price = float(price_data["price"])
    except Exception:
        current_price = None

    decimals = 8 if (current_price and current_price < 0.01) else (4 if (current_price and current_price < 10) else 2)
    price_str = f"{current_price:.{decimals}f}" if current_price is not None else "نامشخص"
    regime_str = f"🌐 رژیم بازار: {regime['regime_label']}\n" if regime else ""

    if not found_signals:
        return (
            f"🔍 <b>تحلیلگر شخصی — {symbol}</b>\n\n"
            f"🔰 قیمت فعلی: {price_str} USDT\n"
            f"{regime_str}"
            f"❌ هیچ ست‌آپی توسط هیچ استراتژی‌ای شناسایی نشد."
        )

    lines = [f"🔍 <b>تحلیلگر شخصی — {symbol}</b>", f"🔰 قیمت فعلی: {price_str} USDT"]
    if regime_str:
        lines.append(regime_str.strip())
    lines.append("")

    for result in found_signals:
        emoji = "🟢" if result["direction"] == "BUY" else "🔴"
        lines.append(f"{emoji} <b>{result['strategy']}</b> — {result['direction']}")
        lines.append(f"   💰 Entry: {result['entry']:.{decimals}f}")
        lines.append(f"   🛑 Stop: {result['stop']:.{decimals}f}")
        lines.append(f"   🎯 TP1: {result['tp1']:.{decimals}f} (1.5R)")
        lines.append(f"   🎯 TP2: {result['tp2']:.{decimals}f} (3R)")
        lines.append(f"   ⭐️ Score: {result['score']}/100")
        if result.get("rsi") is not None:
            lines.append(f"   📐 RSI: {result['rsi']}")
        lines.append(f"   📈 {result['timeframe']}")
        # ── هشدارها (فقط نمایش، هیچ سیگنالی بلاک نمی‌شود) ──
        for w in result.get("analyzer_warnings", []):
            lines.append(f"   {w}")
        lines.append("")

    return "\n".join(lines)

# ─── PERMISSIONS ──────────────────────────────────────────────────────────────
PERMISSIONS = {
    "scan_toggle":   "روشن/خاموش کردن اسکن",
    "strategies":    "مدیریت استراتژی‌ها",
    "blacklist":     "مدیریت بلک‌لیست",
    "logs":          "مشاهده لاگ‌ها",
    "channels":      "مدیریت کانال/گروه‌ها",
    "admins":        "مدیریت ادمین‌ها",
}
ALL_PERMS = list(PERMISSIONS.keys())

# ─── BOT STATE ────────────────────────────────────────────────────────────────
BOT_STATE_FILE = "bot_state.json"

def load_state() -> dict:
    default = {
        "scanning": True,
        "disabled_strategies": [],
        "admins": {},
        "channels": [],
        "pending_admin_add": None,
        "pending_analysis": None,
        "pending_replay": None,
        "pending_backtest": None,       # uid منتظر تنظیمات بک‌تست
        "backtest_enabled": True,
        "last_report_date": None,
        "volume_filter_enabled": True,
        "entry_volume_filter_enabled": True,
        "entry_volume_ma_period": 20,
        "entry_volume_multiplier": 1.5,
        "adaptive_ranking_enabled": True,
        "last_reweight_date": None,
        "regime_engine_enabled": True,
        "quality_ranking_enabled": True,
        "session_filters": SESSION_FILTERS_DEFAULT.copy(),   # جدید: فیلتر session
        "adx_threshold": ADX_THRESHOLD_DEFAULT,              # جدید: آستانه ADX (قابل تنظیم)
        "atr_multiplier": ATR_MULTIPLIER_DEFAULT,            # جدید: ضریب ATR
    }
    loaded = None
    if os.path.exists(BOT_STATE_FILE):
        try:
            with open(BOT_STATE_FILE, encoding="utf-8") as f:
                loaded = json.load(f)
        except Exception as e:
            log.error(f"⚠️ bot_state.json خراب یا ناقص بود: {e}")
            # نسخه خراب رو برای بررسی بعدی نگه می‌داریم به جای از دست دادنش
            try:
                os.replace(BOT_STATE_FILE, BOT_STATE_FILE + ".corrupted")
            except Exception:
                pass
            # تلاش برای ریکاوری از آخرین بکاپ سالم، به جای ریست کامل
            backup_file = BOT_STATE_FILE + ".bak"
            if os.path.exists(backup_file):
                try:
                    with open(backup_file, encoding="utf-8") as f:
                        loaded = json.load(f)
                    log.info("✅ اطلاعات از روی فایل بکاپ (bot_state.json.bak) با موفقیت بازیابی شد.")
                except Exception as e2:
                    log.error(f"بکاپ هم خراب بود، با تنظیمات پیش‌فرض شروع شد: {e2}")
    if loaded:
        default.update(loaded)
    return default

def save_state(state: dict):
    """نوشتن امن (Atomic) + بکاپ: اول از فایل فعلی (سالم) یک نسخه .bak می‌سازد،
    بعد روی فایل موقت می‌نویسد و در آخر جایگزین فایل اصلی می‌کند.
    این یعنی حتی اگر فایل اصلی به هر دلیلی (قطع برنامه، قفل‌شدن توسط برنامه دیگر مثل Notepad، و غیره)
    خراب شود، همیشه یک نسخه قبلی سالم برای ریکاوری وجود دارد."""
    tmp_file = BOT_STATE_FILE + ".tmp"
    backup_file = BOT_STATE_FILE + ".bak"
    try:
        # بکاپ از نسخه فعلی (در صورت سالم بودن) قبل از رونویسی
        if os.path.exists(BOT_STATE_FILE):
            try:
                with open(BOT_STATE_FILE, encoding="utf-8") as f:
                    json.load(f)  # فقط برای تست سالم بودن JSON فعلی
                import shutil
                shutil.copyfile(BOT_STATE_FILE, backup_file)
            except Exception:
                pass  # اگر فایل فعلی خودش خراب بود، بکاپ قدیمی‌تر دست‌نخورده می‌ماند

        with open(tmp_file, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_file, BOT_STATE_FILE)
    except Exception as e:
        log.error(f"خطا در ذخیره bot_state.json: {e}")

def is_super_admin(uid: int, state: dict) -> bool:
    """اولین ادمین (سازنده ربات) همیشه دسترسی کامل دارد و قابل حذف نیست."""
    admins = state.get("admins", {})
    if not admins:
        return False
    first_id = list(admins.keys())[0]
    return str(uid) == first_id

def has_permission(uid: int, perm: str, state: dict) -> bool:
    if is_super_admin(uid, state):
        return True
    admin = state.get("admins", {}).get(str(uid))
    if not admin:
        return False
    return perm in admin.get("perms", [])

def is_admin(uid: int, state: dict) -> bool:
    return str(uid) in state.get("admins", {})

# ─── TELEGRAM CONTROL PANEL ───────────────────────────────────────────────────

def iran_time_str() -> str:
    from datetime import timedelta
    iran_tz = timezone(timedelta(hours=3, minutes=30))
    now = datetime.now(iran_tz)
    try:
        import jdatetime
        jdt = jdatetime.datetime.fromgregorian(datetime=now.replace(tzinfo=None))
        return jdt.strftime("%Y/%-m/%-d %H:%M:%S")
    except ImportError:
        return now.strftime("%Y-%m-%d %H:%M:%S")

def iran_date_str() -> str:
    """تاریخ امروز به وقت ایران، برای نمایش در عنوان گزارش (شمسی اگر jdatetime موجود باشد)."""
    from datetime import timedelta
    iran_tz = timezone(timedelta(hours=3, minutes=30))
    now = datetime.now(iran_tz)
    try:
        import jdatetime
        jdt = jdatetime.datetime.fromgregorian(datetime=now.replace(tzinfo=None))
        return jdt.strftime("%Y/%-m/%-d")
    except ImportError:
        return now.strftime("%Y-%m-%d")

def utc_date_str() -> str:
    """تاریخ امروز به وقت UTC — همان مبنایی که opened_at/closed_at معاملات با آن ذخیره می‌شوند."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

async def send_msg(session, chat_id, text, reply_markup=None):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    if reply_markup:
        payload["reply_markup"] = json.dumps(reply_markup)
    try:
        async with session.post(url, json=payload, proxy=PROXY, timeout=aiohttp.ClientTimeout(total=10)) as r:
            return await r.json()
    except Exception as e:
        log.error(f"send_msg error: {e}")

async def edit_msg(session, chat_id, message_id, text, reply_markup=None):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/editMessageText"
    payload = {"chat_id": chat_id, "message_id": message_id, "text": text, "parse_mode": "HTML"}
    if reply_markup:
        payload["reply_markup"] = json.dumps(reply_markup)
    try:
        async with session.post(url, json=payload, proxy=PROXY, timeout=aiohttp.ClientTimeout(total=10)) as r:
            return await r.json()
    except Exception:
        pass

async def answer_callback(session, callback_id, text=""):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/answerCallbackQuery"
    try:
        async with session.post(url, json={"callback_query_id": callback_id, "text": text},
                                proxy=PROXY, timeout=aiohttp.ClientTimeout(total=5)) as r:
            pass
    except Exception:
        pass

async def react_to_message(session, chat_id, message_id, emoji="❤️"):
    """با ایموجی به یک پیام ری‌اکشن میزند (تأیید دریافت دستور)."""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/setMessageReaction"
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "reaction": json.dumps([{"type": "emoji", "emoji": emoji}]),
        "is_big": False,
    }
    try:
        async with session.post(url, json=payload, proxy=PROXY, timeout=aiohttp.ClientTimeout(total=10)) as r:
            data = await r.json()
            if not data.get("ok"):
                log.debug(f"Reaction failed: {data}")
    except Exception as e:
        log.debug(f"react_to_message error: {e}")

async def get_chat_info(session, chat_id):
    """اطلاعات یک چت (کانال/گروه) را میگیرد — برای چک کردن عضویت/ادمین بودن ربات."""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getChat"
    try:
        async with session.get(url, params={"chat_id": chat_id}, proxy=PROXY,
                               timeout=aiohttp.ClientTimeout(total=10)) as r:
            return await r.json()
    except Exception:
        return None

async def get_chat_member(session, chat_id, user_id):
    """بررسی نقش ربات (یا کاربر) در یک چت."""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getChatMember"
    try:
        async with session.get(url, params={"chat_id": chat_id, "user_id": user_id}, proxy=PROXY,
                               timeout=aiohttp.ClientTimeout(total=10)) as r:
            return await r.json()
    except Exception:
        return None

def main_menu_kb(uid, state):
    icon = "🟢" if state["scanning"] else "🔴"
    bt_icon = "🟢" if state.get("backtest_enabled", True) else "🔴"
    vf_icon = "🟢" if state.get("volume_filter_enabled", True) else "🔴"
    evf_icon = "🟢" if state.get("entry_volume_filter_enabled", True) else "🔴"
    ar_icon = "🟢" if state.get("adaptive_ranking_enabled", True) else "🔴"
    re_icon = "🟢" if state.get("regime_engine_enabled", True) else "🔴"
    qr_icon = "🟢" if state.get("quality_ranking_enabled", True) else "🔴"
    rows = []
    rows.append([{"text": "🔍 تحلیلگر شخصی", "callback_data": "analyzer_start"}])
    rows.append([{"text": "📊 بک‌تست پیشرفته", "callback_data": "backtest_menu"}])
    rows.append([{"text": "🎬 Replay Mode (تاریخچه سیگنال)", "callback_data": "replay_start"}])
    if has_permission(uid, "scan_toggle", state):
        rows.append([{"text": f"{icon} اسکن: {'روشن' if state['scanning'] else 'خاموش'}", "callback_data": "toggle_scan"}])
    if has_permission(uid, "strategies", state):
        rows.append([{"text": "📊 استراتژی‌ها", "callback_data": "strategies"}])
    if has_permission(uid, "scan_toggle", state):
        rows.append([{"text": f"{bt_icon} اتو بک‌تست: {'روشن' if state.get('backtest_enabled', True) else 'خاموش'}", "callback_data": "toggle_backtest"}])
        rows.append([{"text": f"{vf_icon} فیلتر حجم مشکوک: {'روشن' if state.get('volume_filter_enabled', True) else 'خاموش'}", "callback_data": "toggle_volume_filter"}])
        rows.append([{"text": f"{evf_icon} فیلتر حجم کندل ورود", "callback_data": "entry_vol_menu"}])
        rows.append([{"text": f"{ar_icon} رتبه‌بندی آداپتیو استراتژی‌ها", "callback_data": "adaptive_menu"}])
        rows.append([{"text": f"{re_icon} موتور رژیم بازار", "callback_data": "regime_menu"}])
        rows.append([{"text": f"{qr_icon} رتبه‌بندی کیفیت نمادها", "callback_data": "quality_menu"}])
        rows.append([{"text": "🕐 فیلترهای پیشرفته (ADX/ATR/Session)", "callback_data": "advanced_filters_menu"}])
    if has_permission(uid, "blacklist", state):
        rows.append([{"text": "🚫 بلک‌لیست", "callback_data": "bl_menu"}])
    if has_permission(uid, "channels", state):
        rows.append([{"text": "📢 کانال‌ها و گروه‌ها", "callback_data": "channels_menu"}])
    if has_permission(uid, "admins", state):
        rows.append([{"text": "👥 مدیریت ادمین‌ها", "callback_data": "admins_menu"}])
    if has_permission(uid, "logs", state):
        rows.append([{"text": "📋 لاگ‌های اخیر", "callback_data": "show_logs"}])
        rows.append([{"text": "📊 گزارش امروز (دستی)", "callback_data": "manual_report"}])
        rows.append([{"text": "📈 آمار پیشرفته استراتژی‌ها", "callback_data": "show_stats"}])
    rows.append([{"text": "⚙️ تنظیمات", "callback_data": "settings"}])
    return {"inline_keyboard": rows}

def backtest_menu_kb(state):
    sf = state.get("session_filters", SESSION_FILTERS_DEFAULT)
    rows = [
        [{"text": "⏱ بازه: ۱ ساعت", "callback_data": "bt_range_1"}],
        [{"text": "⏱ بازه: ۴ ساعت", "callback_data": "bt_range_4"}],
        [{"text": "⏱ بازه: ۱۲ ساعت", "callback_data": "bt_range_12"}],
        [{"text": "⏱ بازه: ۲۴ ساعت", "callback_data": "bt_range_24"}],
        [{"text": "⏱ بازه: ۷ روز", "callback_data": "bt_range_168"}],
        [{"text": "📦 scope: همه USDT pairs", "callback_data": "bt_scope_all"}],
        [{"text": "◀️ برگشت", "callback_data": "main_menu"}],
    ]
    return {"inline_keyboard": rows}

def advanced_filters_kb(state):
    sf = state.get("session_filters", SESSION_FILTERS_DEFAULT)
    adx = state.get("adx_threshold", ADX_THRESHOLD_DEFAULT)
    atr = state.get("atr_multiplier", ATR_MULTIPLIER_DEFAULT)
    lon = "🟢" if sf.get("london", True) else "🔴"
    ny  = "🟢" if sf.get("ny", True) else "🔴"
    asi = "🟢" if sf.get("asian", True) else "🔴"
    rows = [
        [{"text": f"ADX Threshold: {adx}", "callback_data": "noop"}],
        [{"text": "➖ ADX", "callback_data": "adx_dec"}, {"text": "➕ ADX", "callback_data": "adx_inc"}],
        [{"text": f"ATR Multiplier: {atr}", "callback_data": "noop"}],
        [{"text": "➖ ATR", "callback_data": "atr_dec"}, {"text": "➕ ATR", "callback_data": "atr_inc"}],
        [{"text": f"{lon} London Session", "callback_data": "sess_london"}],
        [{"text": f"{ny} NY Session",      "callback_data": "sess_ny"}],
        [{"text": f"{asi} Asian Session",  "callback_data": "sess_asian"}],
        [{"text": "◀️ برگشت", "callback_data": "main_menu"}],
    ]
    return {"inline_keyboard": rows}

def strategies_kb(state):
    disabled = state.get("disabled_strategies", [])
    rows = []
    for name, _ in STRATEGIES:
        icon = "🔴" if name in disabled else "🟢"
        rows.append([{"text": f"{icon} {name}", "callback_data": f"ts:{name}"}])
    rows.append([{"text": "◀️ برگشت", "callback_data": "main_menu"}])
    return {"inline_keyboard": rows}

def entry_vol_kb(state):
    enabled = state.get("entry_volume_filter_enabled", True)
    icon = "🟢" if enabled else "🔴"
    ma = state.get("entry_volume_ma_period", 20)
    mult = state.get("entry_volume_multiplier", 1.5)
    rows = [
        [{"text": f"{icon} فیلتر: {'روشن' if enabled else 'خاموش'}", "callback_data": "evf_toggle"}],
        [{"text": f"دوره میانگین حجم: {ma}", "callback_data": "noop"}],
        [{"text": "➖", "callback_data": "evf_ma_dec"}, {"text": "➕", "callback_data": "evf_ma_inc"}],
        [{"text": f"ضریب حجم: {mult}", "callback_data": "noop"}],
        [{"text": "➖", "callback_data": "evf_mult_dec"}, {"text": "➕", "callback_data": "evf_mult_inc"}],
        [{"text": "◀️ برگشت", "callback_data": "main_menu"}],
    ]
    return {"inline_keyboard": rows}

def adaptive_kb(state):
    enabled = state.get("adaptive_ranking_enabled", True)
    icon = "🟢" if enabled else "🔴"
    rows = [
        [{"text": f"{icon} رتبه‌بندی آداپتیو: {'روشن' if enabled else 'خاموش'}", "callback_data": "ar_toggle"}],
        [{"text": "🔄 بازمحاسبه فوری وزن‌ها", "callback_data": "ar_reweight_now"}],
        [{"text": "📋 مشاهده وزن‌های فعلی", "callback_data": "ar_show_weights"}],
        [{"text": "◀️ برگشت", "callback_data": "main_menu"}],
    ]
    return {"inline_keyboard": rows}

def regime_kb(state):
    enabled = state.get("regime_engine_enabled", True)
    icon = "🟢" if enabled else "🔴"
    rows = [
        [{"text": f"{icon} موتور رژیم بازار: {'روشن' if enabled else 'خاموش'}", "callback_data": "re_toggle"}],
        [{"text": "📋 طبقه‌بندی استراتژی‌ها", "callback_data": "re_show_types"}],
        [{"text": "◀️ برگشت", "callback_data": "main_menu"}],
    ]
    return {"inline_keyboard": rows}

def quality_kb(state):
    enabled = state.get("quality_ranking_enabled", True)
    icon = "🟢" if enabled else "🔴"
    rows = [
        [{"text": f"{icon} رتبه‌بندی کیفیت: {'روشن' if enabled else 'خاموش'}", "callback_data": "qr_toggle"}],
        [{"text": "🏆 مشاهده ۲۰ نماد برتر فعلی", "callback_data": "qr_show_top"}],
        [{"text": "◀️ برگشت", "callback_data": "main_menu"}],
    ]
    return {"inline_keyboard": rows}

def bl_kb(bl):
    rows = [[{"text": f"❌ {s}", "callback_data": f"blr:{s}"}] for s in sorted(bl)]
    rows.append([{"text": "◀️ برگشت", "callback_data": "main_menu"}])
    return {"inline_keyboard": rows}

def channels_kb(state):
    rows = []
    for ch in state.get("channels", []):
        icon = "🟢" if ch.get("active", True) else "🔴"
        title = ch.get("title", str(ch["id"]))
        rows.append([{"text": f"{icon} {title}", "callback_data": f"ch_toggle:{ch['id']}"},
                     {"text": "🗑", "callback_data": f"ch_remove:{ch['id']}"}])
    rows.append([{"text": "➕ افزودن کانال/گروه جدید", "callback_data": "ch_add"}])
    rows.append([{"text": "◀️ برگشت", "callback_data": "main_menu"}])
    return {"inline_keyboard": rows}

def admins_kb(state):
    rows = []
    admins = state.get("admins", {})
    first_id = list(admins.keys())[0] if admins else None
    for aid, info in admins.items():
        tag = " 👑" if aid == first_id else ""
        rows.append([{"text": f"👤 {info.get('name', aid)}{tag}", "callback_data": f"admin_view:{aid}"}])
    rows.append([{"text": "➕ افزودن ادمین جدید", "callback_data": "admin_add"}])
    rows.append([{"text": "◀️ برگشت", "callback_data": "main_menu"}])
    return {"inline_keyboard": rows}

def admin_detail_kb(state, aid):
    admins = state.get("admins", {})
    first_id = list(admins.keys())[0] if admins else None
    info = admins.get(aid, {})
    perms = info.get("perms", [])
    rows = []
    for pkey, pname in PERMISSIONS.items():
        icon = "🟢" if pkey in perms else "🔴"
        rows.append([{"text": f"{icon} {pname}", "callback_data": f"admin_perm:{aid}:{pkey}"}])
    if aid != first_id:
        rows.append([{"text": "🗑 حذف این ادمین", "callback_data": f"admin_remove:{aid}"}])
    rows.append([{"text": "◀️ برگشت", "callback_data": "admins_menu"}])
    return {"inline_keyboard": rows}

async def handle_update(session, update, state):

    # ── ربات به کانال/گروه اضافه یا حذف شد ──
    if "my_chat_member" in update:
        cm = update["my_chat_member"]
        chat = cm["chat"]
        new_status = cm["new_chat_member"]["status"]  # member/administrator/left/kicked
        chat_id = chat["id"]
        title = chat.get("title", str(chat_id))
        channels = state.get("channels", [])
        existing = next((c for c in channels if c["id"] == chat_id), None)

        if new_status in ("administrator", "member"):
            if not existing:
                channels.append({"id": chat_id, "title": title, "active": True})
                state["channels"] = channels
                save_state(state)
                log.info(f"Channel registered: {title} ({chat_id}) status={new_status}")
                # به همه ادمین‌ها اطلاع بده
                for aid in state.get("admins", {}):
                    await send_msg(session, aid,
                        f"✅ ربات به <b>{title}</b> اضافه شد و سیگنال‌ها از این پس اونجا هم ارسال میشن.\n"
                        f"اگه ربات هنوز ادمین نشده، حتماً بهش دسترسی ادمین (ارسال پیام) بده.")
            elif existing and new_status == "member":
                existing["title"] = title
                save_state(state)
        elif new_status in ("left", "kicked"):
            state["channels"] = [c for c in channels if c["id"] != chat_id]
            save_state(state)
            log.info(f"Channel removed: {title} ({chat_id})")
        return

    # ── نرمال‌سازی: در کانال‌ها، تلگرام پیام را زیر کلید "channel_post" می‌فرستد،
    # نه "message". با کپی کردن آن زیر همان کلید "message"، کل منطق پایین
    # (که خودش chat.type == "channel" را هم تشخیص می‌دهد) بدون تغییر کار می‌کند. ──
    if "channel_post" in update and "message" not in update:
        update = dict(update)
        update["message"] = update["channel_post"]

    if "message" in update:
        msg  = update["message"]
        uid  = msg.get("from", {}).get("id")
        cid  = msg["chat"]["id"]
        chat_type = msg["chat"].get("type", "private")
        text = msg.get("text", "")

        # ── پیام در کانال/گروه (نه چت خصوصی) ──
        if chat_type in ("group", "supergroup", "channel"):
            channels = state.get("channels", [])
            existing = next((c for c in channels if c["id"] == cid), None)
            msg_id = msg.get("message_id")
            title = msg["chat"].get("title", str(cid))

            # ── ثبت خودکار اگر هنوز ثبت نشده (پشتیبان برای my_chat_member که ممکنه از دست رفته باشه) ──
            if text in ("/start", "/stop") and not existing:
                existing = {"id": cid, "title": title, "active": True}
                channels.append(existing)
                state["channels"] = channels
                save_state(state)
                log.info(f"Channel auto-registered via command: {title} ({cid})")

            if text == "/stop" and existing:
                existing["active"] = False
                save_state(state)
                await react_to_message(session, cid, msg_id, "❤️")
                await send_msg(session, cid,
                    f"🔴 ارسال سیگنال در این کانال/گروه متوقف شد.\n"
                    f"برای روشن کردن دوباره /start بزنید.\n\n"
                    f"🐍 AFEE TRADER — نسخه {BOT_VERSION}")
                return
            if text == "/start" and existing:
                existing["active"] = True
                save_state(state)
                await react_to_message(session, cid, msg_id, "❤️")
                await send_msg(session, cid,
                    f"🟢 ارسال سیگنال در این کانال/گروه فعال شد.\n\n"
                    f"🐍 AFEE TRADER — نسخه {BOT_VERSION}")
                return
            return  # سایر پیام‌های گروه/کانال نادیده گرفته میشه

        # ── چت خصوصی ──
        if text == "/start":
            admins = state.get("admins", {})
            if not admins:
                # اولین نفر = سوپر ادمین
                name = msg.get("from", {}).get("first_name", "Admin")
                admins[str(uid)] = {"name": name, "perms": ALL_PERMS.copy()}
                state["admins"] = admins
                save_state(state)
                log.info(f"Super admin registered: {uid}")
            if not is_admin(uid, state):
                await send_msg(session, cid, "⛔️ دسترسی ندارید."); return
            await send_msg(session, cid,
                f"🐍 <b>AFEE TRADER - کنترل پنل</b>\n\n"
                f"🔖 نسخه: {BOT_VERSION}\n"
                f"🕒 {iran_time_str()}\n"
                f"📡 اسکن: {'روشن ✅' if state['scanning'] else 'خاموش ❌'}",
                reply_markup=main_menu_kb(uid, state)); return

        if not is_admin(uid, state): return

        # ── منتظر اسم ارز برای تحلیلگر شخصی ──
        if state.get("pending_analysis") == uid and text and not text.startswith("/"):
            state["pending_analysis"] = None
            save_state(state)
            wait_msg = await send_msg(session, cid, f"⏳ در حال تحلیل {text.strip().upper()}...")
            wait_msg_id = wait_msg.get("result", {}).get("message_id") if wait_msg else None

            symbol = await normalize_and_validate_symbol(session, text)
            if not symbol:
                err_text = f"❌ نماد «{text.strip()}» در بایننس پیدا نشد. اسم را بدون اشتباه تایپی دوباره وارد کن (مثلاً BTC یا BTCUSDT)."
                if wait_msg_id:
                    await edit_msg(session, cid, wait_msg_id, err_text)
                else:
                    await send_msg(session, cid, err_text)
                return

            report = await analyze_symbol_manually(session, symbol, state)
            if wait_msg_id:
                await edit_msg(session, cid, wait_msg_id, report,
                    reply_markup={"inline_keyboard": [
                        [{"text": "🔍 تحلیل ارز دیگر", "callback_data": "analyzer_start"}],
                        [{"text": "◀️ برگشت به منو", "callback_data": "main_menu"}],
                    ]})
            else:
                await send_msg(session, cid, report)
            return

        # ── منتظر اسم ارز برای Replay Mode ──
        if state.get("pending_replay") == uid and text and not text.startswith("/"):
            state["pending_replay"] = None
            save_state(state)
            raw_sym = text.strip().upper().replace(" ", "")
            sym = raw_sym if raw_sym.endswith("USDT") else raw_sym + "USDT"
            wait_msg = await send_msg(session, cid, f"⏳ در حال بازیابی تاریخچه {sym}...")
            wait_msg_id = wait_msg.get("result", {}).get("message_id") if wait_msg else None

            try:
                report = build_replay_report(sym)
            except Exception as e:
                log.error(f"Replay (pending) error: {e}")
                report = "خطا در بازیابی تاریخچه."

            if wait_msg_id:
                await edit_msg(session, cid, wait_msg_id, report,
                    reply_markup={"inline_keyboard": [
                        [{"text": "🎬 بررسی ارز دیگر", "callback_data": "replay_start"}],
                        [{"text": "◀️ برگشت به منو", "callback_data": "main_menu"}],
                    ]})
            else:
                await send_msg(session, cid, report)
            return

        # ── منتظر فوروارد برای افزودن ادمین ──
        if state.get("pending_admin_add") == uid and msg.get("forward_from"):
            fwd = msg["forward_from"]
            new_id = str(fwd["id"])
            name = fwd.get("first_name", new_id)
            admins = state.get("admins", {})
            if new_id not in admins:
                admins[new_id] = {"name": name, "perms": []}
                state["admins"] = admins
            state["pending_admin_add"] = None
            save_state(state)
            await send_msg(session, cid, f"✅ <b>{name}</b> به عنوان ادمین اضافه شد (بدون دسترسی).\nاز منوی ادمین‌ها دسترسی‌هاش رو تنظیم کن.")
            return

        if text.startswith("/bl ") and has_permission(uid, "blacklist", state):
            sym = text.split()[1].upper()
            if not sym.endswith("USDT"): sym += "USDT"
            add_to_blacklist(sym)
            await send_msg(session, cid, f"✅ <b>{sym}</b> به بلک‌لیست اضافه شد.")
        elif text.startswith("/unbl ") and has_permission(uid, "blacklist", state):
            sym = text.split()[1].upper()
            if not sym.endswith("USDT"): sym += "USDT"
            bl = load_blacklist(); bl.discard(sym); save_blacklist(bl)
            await send_msg(session, cid, f"✅ <b>{sym}</b> از بلک‌لیست حذف شد.")
        elif text == "/logs" and has_permission(uid, "logs", state):
            try:
                with open("afee_bot.log", encoding="utf-8") as f:
                    lines = f.readlines()
                await send_msg(session, cid, f"<pre>{''.join(lines[-20:])[-3500:]}</pre>")
            except Exception:
                await send_msg(session, cid, "فایل لاگ پیدا نشد.")
        elif text == "/stats" and has_permission(uid, "logs", state):
            await send_msg(session, cid, "⏳ در حال محاسبه آمار پیشرفته...")
            try:
                report = build_stats_report()
                await send_msg(session, cid, report)
            except Exception as e:
                log.error(f"/stats error: {e}")
                await send_msg(session, cid, "خطا در محاسبه آمار. لاگ را بررسی کنید.")
        elif text == "/weights" and has_permission(uid, "scan_toggle", state):
            weights = get_all_strategy_weights()
            current_min_sc = get_min_signal_score()
            lines = ["📋 <b>وزن فعلی استراتژی‌ها</b>", ""]
            for strat_name, _ in STRATEGIES:
                w = weights.get(strat_name, 1.0)
                tag = "📈 بوست" if w > 1.0 else ("📉 پنالتی" if w < 1.0 else "➖ نوترال")
                lines.append(f"▫️ {strat_name}: <b>{w:.2f}</b> ({tag})")
            lines.append("")
            lines.append(f"⭐️ حداقل Score فعلی: <b>{current_min_sc}/100</b>  (برای تغییر: /setscore [عدد])")
            await send_msg(session, cid, "\n".join(lines))
        elif text.startswith("/replay") and has_permission(uid, "logs", state):
            parts = text.split()
            if len(parts) < 2:
                await send_msg(session, cid, "فرمت درست: /replay BTCUSDT")
            else:
                raw_sym = parts[1].strip().upper()
                sym = raw_sym if raw_sym.endswith("USDT") else raw_sym + "USDT"
                await send_msg(session, cid, f"⏳ در حال بازیابی تاریخچه {sym}...")
                try:
                    report = build_replay_report(sym)
                except Exception as e:
                    log.error(f"/replay error: {e}")
                    report = "خطا در بازیابی تاریخچه. لاگ را بررسی کنید."
                await send_msg(session, cid, report)

        elif text.startswith("/setscore") and has_permission(uid, "scan_toggle", state):
            # FIX v3.2 / Feature #6: تنظیم حداقل امتیاز سیگنال در runtime
            # فرمت: /setscore 86
            parts_sc = text.split()
            if len(parts_sc) < 2:
                current_sc = get_min_signal_score()
                await send_msg(session, cid,
                    f"⭐️ <b>تنظیم حداقل Score سیگنال</b>\n\n"
                    f"مقدار فعلی: <b>{current_sc}/100</b>\n\n"
                    f"فرمت: /setscore [عدد بین ۱ تا ۱۰۰]\n"
                    f"مثال: /setscore 86\n\n"
                    f"💡 هرچه score بالاتر تنظیم شود، فقط سیگنال‌های با کیفیت بالاتر ارسال می‌شوند.\n"
                    f"اگر score >= 86 باشد، فیلترهای سخت‌گیرانه تا ۸ امتیاز نرم‌تر می‌شوند (کمتر reject).")
            else:
                try:
                    new_sc = int(parts_sc[1])
                    if not (1 <= new_sc <= 100):
                        raise ValueError("out of range")
                    set_filter_config("min_signal_score", new_sc)
                    relaxed = 86 <= new_sc < 95
                    strict = new_sc >= 95
                    await send_msg(session, cid,
                        f"✅ حداقل Score سیگنال روی <b>{new_sc}/100</b> تنظیم شد.\n"
                        + (f"🔓 چون score >= 86 است، فیلترهای ADX/EMA/Volume تا ۸ امتیاز نرم‌تر اعمال می‌شوند."
                           if relaxed else "")
                        + (f"\n🔒 حالت سخت‌گیر فعال: چون score >= 95 است، هیچ relaxation روی آستانه اعمال نمی‌شود. فقط سیگنال‌هایی با score واقعی >= {new_sc} ارسال خواهند شد."
                           if strict else ""))
                    log.info(f"MIN_SIGNAL_SCORE set to {new_sc} by admin {uid}")
                except ValueError:
                    await send_msg(session, cid, "❌ عدد نامعتبر. باید بین ۱ تا ۱۰۰ باشد. مثال: /setscore 75")

        elif text.startswith("/backtest") and has_permission(uid, "logs", state):
            # /backtest [hours] [symbol1,symbol2,...]
            parts = text.split()
            hours = 24
            symbols_override = None
            if len(parts) >= 2:
                try:
                    hours = int(parts[1])
                except ValueError:
                    pass
            if len(parts) >= 3:
                symbols_override = [s.strip().upper() for s in parts[2].split(",")]
                symbols_override = [s if s.endswith("USDT") else s + "USDT" for s in symbols_override]

            await send_msg(session, cid, f"⏳ شروع بک‌تست برای {hours} ساعت گذشته...")
            try:
                if symbols_override:
                    symbols = symbols_override
                else:
                    symbols = await get_quality_ranked_symbols(session, TOP_N_COINS, QUALITY_POOL_SIZE) \
                        if state.get("quality_ranking_enabled", True) \
                        else await get_top_symbols(session, TOP_N_COINS)
                    symbols = symbols[:50]

                job_params = {
                    "symbols": symbols,
                    "timerange_hours": hours,
                    "strategies": [s for s, _ in STRATEGIES],
                    "requested_by": str(uid),
                }
                job_id = await enqueue_backtest(session, job_params)
                # انتظار با timeout ۲۴۰ ثانیه
                for _ in range(120):
                    await asyncio.sleep(2)
                    if job_id in _backtest_cache:
                        break
                cached = _backtest_cache.get(job_id)
                if cached:
                    parts_msgs = build_backtest_report(cached["trades"], cached["summary"], hours)
                    for part in parts_msgs:
                        await send_msg(session, cid, part)
                        await asyncio.sleep(1)
                else:
                    await send_msg(session, cid, "⏳ بک‌تست در صف است. نتایج به زودی ارسال می‌شوند.")
            except Exception as e:
                log.error(f"/backtest error: {e}")
                await send_msg(session, cid, f"❌ خطا: {e}")

    elif "callback_query" in update:
        cb   = update["callback_query"]
        uid  = cb["from"]["id"]
        cid  = cb["message"]["chat"]["id"]
        mid  = cb["message"]["message_id"]
        data = cb.get("data", "")

        if not is_admin(uid, state):
            await answer_callback(session, cb["id"], "⛔️ دسترسی ندارید"); return

        await answer_callback(session, cb["id"])

        if data == "main_menu":
            await edit_msg(session, cid, mid,
                f"🐍 <b>AFEE TRADER - کنترل پنل</b>\n🔖 نسخه: {BOT_VERSION}\n🕒 {iran_time_str()}",
                reply_markup=main_menu_kb(uid, state))

        elif data == "toggle_scan" and has_permission(uid, "scan_toggle", state):
            state["scanning"] = not state["scanning"]
            save_state(state)
            await edit_msg(session, cid, mid,
                f"📡 اسکن: <b>{'روشن ✅' if state['scanning'] else 'خاموش ❌'}</b>",
                reply_markup=main_menu_kb(uid, state))

        elif data == "toggle_backtest" and has_permission(uid, "scan_toggle", state):
            state["backtest_enabled"] = not state.get("backtest_enabled", True)
            save_state(state)
            await edit_msg(session, cid, mid,
                f"🧪 اتو بک‌تست: <b>{'روشن ✅' if state['backtest_enabled'] else 'خاموش ❌'}</b>\n\n"
                f"وقتی روشنه، هر سیگنال صادرشده ثبت می‌شود و در گزارش روزانه ساعت ۰۰:۰۰ نتیجه‌اش بررسی می‌شود.",
                reply_markup=main_menu_kb(uid, state))

        elif data == "toggle_volume_filter" and has_permission(uid, "scan_toggle", state):
            state["volume_filter_enabled"] = not state.get("volume_filter_enabled", True)
            save_state(state)
            await edit_msg(session, cid, mid,
                f"🛡 فیلتر حجم مشکوک: <b>{'روشن ✅' if state['volume_filter_enabled'] else 'خاموش ❌'}</b>\n\n"
                f"وقتی روشنه، اگه قبل از صدور سیگنال یک حرکت قوی با حجم بالا دقیقاً در جهت مخالف دیده شود، آن سیگنال رد می‌شود (برای کاهش استاپ‌خوردن).",
                reply_markup=main_menu_kb(uid, state))

        elif data == "entry_vol_menu" and has_permission(uid, "scan_toggle", state):
            await edit_msg(session, cid, mid,
                "📊 <b>فیلتر حجم کندل ورود</b>\n\n"
                "فقط زمانی سیگنال صادر می‌شود که حجم کندل ورود حداقل «ضریب حجم» برابر میانگین حجم «دوره میانگین» کندل اخیر باشد.\n"
                "شرط: Volume ≥ Multiplier × SMA(Volume, Period)",
                reply_markup=entry_vol_kb(state))

        elif data == "evf_toggle" and has_permission(uid, "scan_toggle", state):
            state["entry_volume_filter_enabled"] = not state.get("entry_volume_filter_enabled", True)
            save_state(state)
            await edit_msg(session, cid, mid,
                "📊 <b>فیلتر حجم کندل ورود</b>",
                reply_markup=entry_vol_kb(state))

        elif data == "evf_ma_inc" and has_permission(uid, "scan_toggle", state):
            state["entry_volume_ma_period"] = min(100, state.get("entry_volume_ma_period", 20) + 5)
            save_state(state)
            await edit_msg(session, cid, mid, "📊 <b>فیلتر حجم کندل ورود</b>", reply_markup=entry_vol_kb(state))

        elif data == "evf_ma_dec" and has_permission(uid, "scan_toggle", state):
            state["entry_volume_ma_period"] = max(5, state.get("entry_volume_ma_period", 20) - 5)
            save_state(state)
            await edit_msg(session, cid, mid, "📊 <b>فیلتر حجم کندل ورود</b>", reply_markup=entry_vol_kb(state))

        elif data == "evf_mult_inc" and has_permission(uid, "scan_toggle", state):
            state["entry_volume_multiplier"] = round(min(5.0, state.get("entry_volume_multiplier", 1.5) + 0.1), 2)
            save_state(state)
            await edit_msg(session, cid, mid, "📊 <b>فیلتر حجم کندل ورود</b>", reply_markup=entry_vol_kb(state))

        elif data == "evf_mult_dec" and has_permission(uid, "scan_toggle", state):
            state["entry_volume_multiplier"] = round(max(1.0, state.get("entry_volume_multiplier", 1.5) - 0.1), 2)
            save_state(state)
            await edit_msg(session, cid, mid, "📊 <b>فیلتر حجم کندل ورود</b>", reply_markup=entry_vol_kb(state))

        elif data == "noop":
            pass

        elif data == "adaptive_menu" and has_permission(uid, "scan_toggle", state):
            await edit_msg(session, cid, mid,
                "⚖️ <b>رتبه‌بندی آداپتیو استراتژی‌ها</b>\n\n"
                "هر ۷ روز، عملکرد ۳۰ روز اخیر هر استراتژی (Win Rate + Expectancy + Profit Factor) بررسی می‌شود "
                "و یک وزن بین 0.5 (پنالتی) تا 1.5 (بوست) به آن اختصاص می‌یابد. این وزن مستقیماً روی Score نهایی سیگنال‌های همان استراتژی ضرب می‌شود.\n\n"
                f"حداقل معامله لازم برای وزن‌دهی واقعی: {MIN_TRADES_FOR_WEIGHTING} معامله بسته‌شده (وگرنه وزن نوترال 1.0 می‌ماند).",
                reply_markup=adaptive_kb(state))

        elif data == "ar_toggle" and has_permission(uid, "scan_toggle", state):
            state["adaptive_ranking_enabled"] = not state.get("adaptive_ranking_enabled", True)
            save_state(state)
            await edit_msg(session, cid, mid, "⚖️ <b>رتبه‌بندی آداپتیو استراتژی‌ها</b>", reply_markup=adaptive_kb(state))

        elif data == "ar_reweight_now" and has_permission(uid, "scan_toggle", state):
            await edit_msg(session, cid, mid, "⏳ در حال بازمحاسبه وزن‌ها...")
            try:
                old_weights = get_all_strategy_weights()
                new_weights = update_all_strategy_weights()
                state["last_reweight_date"] = datetime.now(timezone.utc).isoformat()
                save_state(state)
                report = build_weight_update_report(old_weights, new_weights)
            except Exception as e:
                log.error(f"Manual reweight error: {e}")
                report = "خطا در بازمحاسبه وزن‌ها."
            await edit_msg(session, cid, mid, report,
                reply_markup={"inline_keyboard": [[{"text": "◀️ برگشت", "callback_data": "adaptive_menu"}]]})

        elif data == "ar_show_weights" and has_permission(uid, "scan_toggle", state):
            weights = get_all_strategy_weights()
            lines = ["📋 <b>وزن فعلی استراتژی‌ها</b>", ""]
            for strat_name, _ in STRATEGIES:
                w = weights.get(strat_name, 1.0)
                tag = "📈 بوست" if w > 1.0 else ("📉 پنالتی" if w < 1.0 else "➖ نوترال")
                lines.append(f"▫️ {strat_name}: <b>{w:.2f}</b> ({tag})")
            await edit_msg(session, cid, mid, "\n".join(lines),
                reply_markup={"inline_keyboard": [[{"text": "◀️ برگشت", "callback_data": "adaptive_menu"}]]})

        elif data == "regime_menu" and has_permission(uid, "scan_toggle", state):
            await edit_msg(session, cid, mid,
                "🌐 <b>موتور رژیم بازار</b>\n\n"
                "قبل از اجرای هر استراتژی، رژیم کلی بازار (ADX, EMA200 Slope, ATR Regime, Volatility Regime, Volume Regime) "
                "بررسی می‌شود:\n\n"
                "• ADX بالای ۲۵ → فقط استراتژی‌های روندی (TREND) و هیبریدی اجرا می‌شوند\n"
                "• ADX زیر ۲۰ → فقط استراتژی‌های برگشتی (REVERSAL) و هیبریدی اجرا می‌شوند\n"
                "• بین ۲۰ تا ۲۵ → همه استراتژی‌ها مجاز هستند (ناحیه گذار)",
                reply_markup=regime_kb(state))

        elif data == "re_toggle" and has_permission(uid, "scan_toggle", state):
            state["regime_engine_enabled"] = not state.get("regime_engine_enabled", True)
            save_state(state)
            await edit_msg(session, cid, mid, "🌐 <b>موتور رژیم بازار</b>", reply_markup=regime_kb(state))

        elif data == "re_show_types" and has_permission(uid, "scan_toggle", state):
            lines = ["📋 <b>طبقه‌بندی استراتژی‌ها</b>", ""]
            type_fa = {"TREND": "روندی 📈", "REVERSAL": "برگشتی 🔄", "HYBRID": "هیبریدی ⚖️"}
            for strat_name, _ in STRATEGIES:
                t = STRATEGY_REGIME_TYPE.get(strat_name, "HYBRID")
                lines.append(f"▫️ {strat_name}: <b>{type_fa.get(t, t)}</b>")
            await edit_msg(session, cid, mid, "\n".join(lines),
                reply_markup={"inline_keyboard": [[{"text": "◀️ برگشت", "callback_data": "regime_menu"}]]})

        elif data == "quality_menu" and has_permission(uid, "scan_toggle", state):
            await edit_msg(session, cid, mid,
                "🏆 <b>رتبه‌بندی کیفیت نمادها</b>\n\n"
                f"به‌جای انتخاب صرف بر اساس حجم معاملات، ابتدا {QUALITY_POOL_SIZE} نماد برتر بر اساس حجم انتخاب می‌شوند؛ "
                f"سپس برای همان نامزدها امتیاز کیفیت محاسبه و فقط {TOP_N_COINS} نماد با بالاترین کیفیت برای اسکن نهایی انتخاب می‌شوند.\n\n"
                "معیارهای امتیاز کیفیت:\n"
                "• Spread Quality (۲۰٪) — فاصله bid/ask\n"
                "• Volatility Quality (۲۵٪) — نه خیلی بی‌حرکت، نه خیلی پرنوسان\n"
                "• Structure Cleanliness (۳۰٪) — تعداد سطوح S/R معتبر\n"
                "• Trend Clarity (۲۵٪) — وضوح روند یا رنج بر اساس ADX",
                reply_markup=quality_kb(state))

        elif data == "qr_toggle" and has_permission(uid, "scan_toggle", state):
            state["quality_ranking_enabled"] = not state.get("quality_ranking_enabled", True)
            save_state(state)
            await edit_msg(session, cid, mid, "🏆 <b>رتبه‌بندی کیفیت نمادها</b>", reply_markup=quality_kb(state))

        elif data == "qr_show_top" and has_permission(uid, "scan_toggle", state):
            await edit_msg(session, cid, mid, f"⏳ در حال محاسبه کیفیت {QUALITY_POOL_SIZE} نماد... (ممکن است کمی طول بکشد)")
            try:
                candidates = await get_top_symbols(session, QUALITY_POOL_SIZE)
                semaphore_local = asyncio.Semaphore(PARALLEL_WORKERS)
                async def scored(symbol):
                    async with semaphore_local:
                        return await calc_symbol_quality_score(session, symbol)
                results = await asyncio.gather(*[scored(s) for s in candidates], return_exceptions=True)
                valid = [r for r in results if isinstance(r, dict) and r.get("valid")]
                valid.sort(key=lambda r: r["quality_score"], reverse=True)
                lines = ["🏆 <b>۲۰ نماد برتر بر اساس کیفیت</b>", ""]
                for i, r in enumerate(valid[:20], 1):
                    lines.append(f"{i}. {r['symbol']}: <b>{r['quality_score']}/100</b>")
                report = "\n".join(lines)
            except Exception as e:
                log.error(f"qr_show_top error: {e}")
                report = "خطا در محاسبه رتبه‌بندی کیفیت."
            await edit_msg(session, cid, mid, report,
                reply_markup={"inline_keyboard": [[{"text": "◀️ برگشت", "callback_data": "quality_menu"}]]})

        elif data == "manual_report" and has_permission(uid, "logs", state):
            await edit_msg(session, cid, mid, "⏳ در حال آماده‌سازی گزارش امروز...")
            trades = await update_trade_outcomes(session)
            today_str = utc_date_str()
            report_text = build_daily_report(trades, today_str)
            await edit_msg(session, cid, mid, report_text,
                reply_markup={"inline_keyboard": [[{"text": "◀️ برگشت", "callback_data": "main_menu"}]]})

        elif data == "show_stats" and has_permission(uid, "logs", state):
            await edit_msg(session, cid, mid, "⏳ در حال محاسبه آمار پیشرفته...")
            try:
                report = build_stats_report()
            except Exception as e:
                log.error(f"show_stats error: {e}")
                report = "خطا در محاسبه آمار."
            await edit_msg(session, cid, mid, report,
                reply_markup={"inline_keyboard": [[{"text": "◀️ برگشت", "callback_data": "main_menu"}]]})

        elif data == "analyzer_start":
            state["pending_analysis"] = uid
            save_state(state)
            await edit_msg(session, cid, mid,
                "🔍 <b>تحلیلگر شخصی</b>\n\n"
                "اسم ارز رو بفرست (مثلاً BTC یا BTCUSDT) تا با همه استراتژی‌ها و فیلترهای کیفیت تحلیلش کنم.",
                reply_markup={"inline_keyboard": [[{"text": "◀️ انصراف", "callback_data": "main_menu"}]]})

        elif data == "replay_start" and has_permission(uid, "logs", state):
            state["pending_replay"] = uid
            save_state(state)
            await edit_msg(session, cid, mid,
                "🎬 <b>Replay Mode</b>\n\n"
                "اسم ارز رو بفرست (مثلاً BTC یا BTCUSDT) تا آخرین سیگنال‌های ثبت‌شده‌اش رو با دلیل ورود، استراتژی، امتیاز و نتیجه نهایی نشونت بدم.",
                reply_markup={"inline_keyboard": [[{"text": "◀️ انصراف", "callback_data": "main_menu"}]]})

        elif data == "strategies" and has_permission(uid, "strategies", state):
            await edit_msg(session, cid, mid,
                "📊 <b>استراتژی‌ها</b> — برای روشن/خاموش کردن کلیک کن:",
                reply_markup=strategies_kb(state))

        elif data.startswith("ts:") and has_permission(uid, "strategies", state):
            name = data[3:]
            disabled = state.get("disabled_strategies", [])
            if name in disabled: disabled.remove(name); icon = "🟢 فعال"
            else: disabled.append(name); icon = "🔴 غیرفعال"
            state["disabled_strategies"] = disabled
            save_state(state)
            await edit_msg(session, cid, mid,
                f"<b>{name}</b>: {icon}", reply_markup=strategies_kb(state))

        elif data == "bl_menu" and has_permission(uid, "blacklist", state):
            bl = load_blacklist()
            txt = "\n".join(sorted(bl)) if bl else "بلک‌لیست خالیه"
            await edit_msg(session, cid, mid,
                f"🚫 <b>بلک‌لیست</b>\n\n{txt}\n\n/bl SYMBOL — اضافه\n/unbl SYMBOL — حذف",
                reply_markup=bl_kb(bl))

        elif data.startswith("blr:") and has_permission(uid, "blacklist", state):
            sym = data[4:]
            bl = load_blacklist(); bl.discard(sym); save_blacklist(bl)
            await edit_msg(session, cid, mid, f"✅ {sym} حذف شد.", reply_markup=bl_kb(bl))

        # ── کانال‌ها و گروه‌ها ──
        elif data == "channels_menu" and has_permission(uid, "channels", state):
            channels = state.get("channels", [])
            if channels:
                lines = "\n".join(f"• {c.get('title', c['id'])} — {'فعال ✅' if c.get('active', True) else 'غیرفعال ❌'}" for c in channels)
            else:
                lines = "هنوز هیچ کانال یا گروهی ثبت نشده."
            await edit_msg(session, cid, mid,
                f"📢 <b>کانال‌ها و گروه‌ها</b>\n\n{lines}",
                reply_markup=channels_kb(state))

        elif data == "ch_add" and has_permission(uid, "channels", state):
            await edit_msg(session, cid, mid,
                "➕ <b>افزودن کانال یا گروه جدید</b>\n\n"
                "۱. ربات رو با یوزرنیمش (@یوزرنیم_ربات) به کانال یا گروه مورد نظر اضافه کن.\n"
                "۲. حتماً به ربات دسترسی <b>ادمین</b> (Admin) بده تا بتونه پیام ارسال کنه.\n"
                "۳. به محض اضافه شدن، ربات خودش این کانال رو شناسایی و ثبت میکنه.\n"
                "۴. بعد از ثبت، از همین منو میتونی فعال/غیرفعالش کنی.\n\n"
                "💡 همچنین داخل خود کانال یا گروه، با دستورات /start و /stop میتونی ارسال سیگنال رو روشن یا خاموش کنی.",
                reply_markup={"inline_keyboard": [[{"text": "◀️ برگشت", "callback_data": "channels_menu"}]]})

        elif data.startswith("ch_toggle:") and has_permission(uid, "channels", state):
            ch_id = int(data.split(":", 1)[1])
            for c in state.get("channels", []):
                if c["id"] == ch_id:
                    c["active"] = not c.get("active", True)
            save_state(state)
            await edit_msg(session, cid, mid, "📢 <b>کانال‌ها و گروه‌ها</b>", reply_markup=channels_kb(state))

        elif data.startswith("ch_remove:") and has_permission(uid, "channels", state):
            ch_id = int(data.split(":", 1)[1])
            state["channels"] = [c for c in state.get("channels", []) if c["id"] != ch_id]
            save_state(state)
            await edit_msg(session, cid, mid, "✅ حذف شد.", reply_markup=channels_kb(state))

        # ── مدیریت ادمین‌ها ──
        elif data == "admins_menu" and has_permission(uid, "admins", state):
            await edit_msg(session, cid, mid,
                "👥 <b>مدیریت ادمین‌ها</b>\n\nروی هر ادمین کلیک کن تا دسترسی‌هاشو ببینی و تغییر بدی.",
                reply_markup=admins_kb(state))

        elif data == "admin_add" and has_permission(uid, "admins", state):
            state["pending_admin_add"] = uid
            save_state(state)
            await edit_msg(session, cid, mid,
                "➕ <b>افزودن ادمین جدید</b>\n\n"
                "یک پیام از فردی که میخوای ادمین کنی رو برام فوروارد کن.\n"
                "(باید قبلاً یه پیام به این ربات داده باشه یا پیامی ازش داشته باشی که بتونی فوروارد کنی)",
                reply_markup={"inline_keyboard": [[{"text": "◀️ انصراف", "callback_data": "admins_menu"}]]})

        elif data.startswith("admin_view:") and has_permission(uid, "admins", state):
            aid = data.split(":", 1)[1]
            info = state.get("admins", {}).get(aid, {})
            perms = info.get("perms", [])
            perm_txt = "\n".join(f"• {PERMISSIONS[p]}" for p in perms) if perms else "بدون دسترسی"
            await edit_msg(session, cid, mid,
                f"👤 <b>{info.get('name', aid)}</b>\n\nدسترسی‌های فعلی:\n{perm_txt}\n\nبرای تغییر روی هرکدوم کلیک کن:",
                reply_markup=admin_detail_kb(state, aid))

        elif data.startswith("admin_perm:") and has_permission(uid, "admins", state):
            _, aid, pkey = data.split(":", 2)
            admins = state.get("admins", {})
            if aid in admins:
                perms = admins[aid].get("perms", [])
                if pkey in perms: perms.remove(pkey)
                else: perms.append(pkey)
                admins[aid]["perms"] = perms
                state["admins"] = admins
                save_state(state)
            info = admins.get(aid, {})
            perms = info.get("perms", [])
            perm_txt = "\n".join(f"• {PERMISSIONS[p]}" for p in perms) if perms else "بدون دسترسی"
            await edit_msg(session, cid, mid,
                f"👤 <b>{info.get('name', aid)}</b>\n\nدسترسی‌های فعلی:\n{perm_txt}",
                reply_markup=admin_detail_kb(state, aid))

        elif data.startswith("admin_remove:") and has_permission(uid, "admins", state):
            aid = data.split(":", 1)[1]
            admins = state.get("admins", {})
            admins.pop(aid, None)
            state["admins"] = admins
            save_state(state)
            await edit_msg(session, cid, mid, "✅ ادمین حذف شد.", reply_markup=admins_kb(state))

        elif data == "show_logs" and has_permission(uid, "logs", state):
            try:
                with open("afee_bot.log", encoding="utf-8") as f:
                    lines = f.readlines()
                txt = "".join(lines[-15:])[-3000:]
                await edit_msg(session, cid, mid, f"📋 <b>لاگ‌ها:</b>\n<pre>{txt}</pre>",
                    reply_markup={"inline_keyboard": [[{"text": "◀️ برگشت", "callback_data": "main_menu"}]]})
            except Exception:
                await edit_msg(session, cid, mid, "لاگ پیدا نشد.",
                    reply_markup={"inline_keyboard": [[{"text": "◀️ برگشت", "callback_data": "main_menu"}]]})

        elif data == "settings":
            await edit_msg(session, cid, mid,
                f"⚙️ <b>تنظیمات</b>\n\n"
                f"🔍 ارزها: <b>{TOP_N_COINS}</b>\n"
                f"⏱ Cooldown: <b>{SIGNAL_COOLDOWN//60} دقیقه</b>\n"
                f"👷 Workers: <b>{PARALLEL_WORKERS}</b>\n"
                f"⏰ فاصله اسکن: <b>{SCAN_INTERVAL}s</b>",
                reply_markup={"inline_keyboard": [[{"text": "◀️ برگشت", "callback_data": "main_menu"}]]})

        # ─── BACKTEST MENU ─────────────────────────────────────────────────────
        elif data == "backtest_menu":
            await edit_msg(session, cid, mid,
                "📊 <b>بک‌تست پیشرفته</b>\n\n"
                "بازه زمانی و scope را انتخاب کنید. فیلترها دقیقاً مثل Live Engine اعمال می‌شوند.\n"
                "نتایج به صورت گزارش تفصیلی (chunked) ارسال می‌شوند.",
                reply_markup=backtest_menu_kb(state))

        elif data.startswith("bt_range_"):
            hours = int(data.split("_")[-1])
            await edit_msg(session, cid, mid, f"⏳ در حال اجرای بک‌تست برای {hours} ساعت گذشته...")
            try:
                symbols = await get_quality_ranked_symbols(session, TOP_N_COINS, QUALITY_POOL_SIZE) \
                    if state.get("quality_ranking_enabled", True) \
                    else await get_top_symbols(session, TOP_N_COINS)
                job_params = {
                    "symbols": symbols[:50],  # محدودیت ۵۰ نماد در هر job
                    "timerange_hours": hours,
                    "strategies": [s for s, _ in STRATEGIES],
                    "requested_by": str(uid),
                }
                job_id = await enqueue_backtest(session, job_params)
                # صبر می‌کنیم تا job تمام شود (با timeout)
                for _ in range(120):
                    await asyncio.sleep(2)
                    if job_id in _backtest_cache:
                        break
                cached = _backtest_cache.get(job_id)
                if cached:
                    parts = build_backtest_report(cached["trades"], cached["summary"], hours)
                    for part in parts:
                        await send_msg(session, cid, part)
                    # Chart برای اولین trade
                    if cached["trades"]:
                        t = cached["trades"][0]
                        chart_txt = build_trade_chart(
                            t.get("symbol",""), t.get("entry",0), t.get("stop",0),
                            t.get("tp1",0), t.get("tp2",0),
                            t.get("direction","BUY"), t.get("outcome"))
                        await send_msg(session, cid, chart_txt)
                else:
                    await send_msg(session, cid, "⏳ بک‌تست در صف است. نتایج بعداً ارسال می‌شوند.")
            except Exception as e:
                log.error(f"Backtest handler error: {e}")
                await send_msg(session, cid, f"❌ خطا در اجرای بک‌تست: {e}")

        elif data == "bt_scope_all":
            await edit_msg(session, cid, mid,
                "📦 scope: همه USDT pairs (پیش‌فرض)\nبرای شروع یک بازه زمانی انتخاب کنید.",
                reply_markup=backtest_menu_kb(state))

        # ─── ADVANCED FILTERS ──────────────────────────────────────────────────
        elif data == "advanced_filters_menu" and has_permission(uid, "scan_toggle", state):
            await edit_msg(session, cid, mid,
                "🛡 <b>فیلترهای پیشرفته</b>\n\n"
                "ADX Threshold: حداقل ADX برای تأیید وضوح روند\n"
                "ATR Multiplier: ضریب نوسان‌سنج (هر چه بالاتر، سخت‌گیرانه‌تر)\n"
                "Session Filters: کدام session‌ها مجاز هستند\n\n"
                "تمام این فیلترها در Live + Backtest Engine اعمال می‌شوند.",
                reply_markup=advanced_filters_kb(state))

        elif data == "adx_inc" and has_permission(uid, "scan_toggle", state):
            state["adx_threshold"] = round(min(50.0, state.get("adx_threshold", ADX_THRESHOLD_DEFAULT) + 1.0), 1)
            set_filter_config("adx_threshold", state["adx_threshold"])
            save_state(state)
            await edit_msg(session, cid, mid, "🛡 <b>فیلترهای پیشرفته</b>", reply_markup=advanced_filters_kb(state))

        elif data == "adx_dec" and has_permission(uid, "scan_toggle", state):
            state["adx_threshold"] = round(max(10.0, state.get("adx_threshold", ADX_THRESHOLD_DEFAULT) - 1.0), 1)
            set_filter_config("adx_threshold", state["adx_threshold"])
            save_state(state)
            await edit_msg(session, cid, mid, "🛡 <b>فیلترهای پیشرفته</b>", reply_markup=advanced_filters_kb(state))

        elif data == "atr_inc" and has_permission(uid, "scan_toggle", state):
            state["atr_multiplier"] = round(min(5.0, state.get("atr_multiplier", ATR_MULTIPLIER_DEFAULT) + 0.1), 1)
            set_filter_config("atr_multiplier", state["atr_multiplier"])
            save_state(state)
            await edit_msg(session, cid, mid, "🛡 <b>فیلترهای پیشرفته</b>", reply_markup=advanced_filters_kb(state))

        elif data == "atr_dec" and has_permission(uid, "scan_toggle", state):
            state["atr_multiplier"] = round(max(1.0, state.get("atr_multiplier", ATR_MULTIPLIER_DEFAULT) - 0.1), 1)
            set_filter_config("atr_multiplier", state["atr_multiplier"])
            save_state(state)
            await edit_msg(session, cid, mid, "🛡 <b>فیلترهای پیشرفته</b>", reply_markup=advanced_filters_kb(state))

        elif data.startswith("sess_") and has_permission(uid, "scan_toggle", state):
            sess_key = data[5:]  # london | ny | asian
            sf = state.get("session_filters", SESSION_FILTERS_DEFAULT.copy())
            sf[sess_key] = not sf.get(sess_key, True)
            state["session_filters"] = sf
            save_state(state)
            await edit_msg(session, cid, mid, "🛡 <b>فیلترهای پیشرفته</b>", reply_markup=advanced_filters_kb(state))

async def poll_updates(session, state):
    offset = 0
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
    while True:
        try:
            params = {"timeout": 5, "offset": offset,
                      "allowed_updates": ["message", "channel_post", "callback_query", "my_chat_member"]}
            async with session.get(url, params=params, proxy=PROXY,
                                   timeout=aiohttp.ClientTimeout(total=15)) as r:
                data = await r.json()
            if data.get("ok"):
                for upd in data.get("result", []):
                    offset = upd["update_id"] + 1
                    await handle_update(session, upd, state)
        except Exception as e:
            log.debug(f"Poll error: {e}")
        await asyncio.sleep(1)

async def scan_symbol(session, symbol, semaphore, state, open_keys):
    """
    🟢 LAYER A — LIVE SIGNAL ENGINE
    تمام فیلترها باید پاس شوند؛ هر شکستی سیگنال را کاملاً بلاک می‌کند.
    Patch #8/#13: فیلتر شده‌ها کولدوان ۱۰-۱۵ دقیقه می‌خورند، سپس دوباره اسکن می‌شوند.
    """
    async with semaphore:
        results = []
        disabled = state.get("disabled_strategies", [])
        regime_engine_on = state.get("regime_engine_enabled", True)

        # ── Session Gate (اگر session غیرفعال باشد، کل symbol را skip می‌کنیم) ──
        if not is_session_allowed(state):
            return results

        # ── Market Regime Engine ──
        if regime_engine_on:
            try:
                regime = await get_market_regime(session, symbol)
                allowed_types = regime["allowed_types"]
            except Exception as e:
                log.debug(f"Regime detection failed for {symbol}: {e}")
                allowed_types = {"TREND", "REVERSAL", "HYBRID"}
                regime = None
        else:
            allowed_types = {"TREND", "REVERSAL", "HYBRID"}
            regime = None

        for strat_name, strat_fn in STRATEGIES:
            if strat_name in disabled:
                continue

            strat_type = STRATEGY_REGIME_TYPE.get(strat_name, "HYBRID")
            if strat_type not in allowed_types:
                continue

            try:
                result = await strat_fn(session, symbol)
                if not result:
                    continue

                direction = result.get("direction", "")

                # Patch #9: Duplicate guard on (symbol + strategy + direction)
                if not can_signal(symbol, strat_name, open_keys, direction):
                    continue

                if regime:
                    result["market_regime"] = regime["regime_label"]

                # ── Layer A: همه فیلترها باید پاس شوند ──
                passed, block_reasons = await run_live_filters(session, symbol, result, state)
                if not passed:
                    log.info(f"LIVE FILTERED: {symbol} | {strat_name} | {block_reasons}")
                    # Patch #8/#13: mark for rescan after cooldown
                    mark_filtered_for_rescan(symbol)
                    continue

                # ── Entry volume bonus on Score ──
                try:
                    ma_period = state.get("entry_volume_ma_period", 20)
                    vol_multiplier = state.get("entry_volume_multiplier", 1.5)
                    c5m_vol = await get_candles(session, symbol, "5m", ma_period + 5)
                    vol_ratio = entry_volume_ratio(c5m_vol, ma_period)
                    bonus = min(10, int((vol_ratio - vol_multiplier) * 4)) if vol_ratio > vol_multiplier else 0
                    result["score"] = min(100, result.get("score", 0) + max(0, bonus))
                except Exception:
                    pass

                # ── Adaptive weight ──
                try:
                    weight = get_strategy_weight(strat_name)
                    if weight != 1.0:
                        result["score"] = max(1, min(100, round(result.get("score", 0) * weight)))
                        result["strategy_weight"] = weight
                except Exception as e:
                    log.debug(f"Weight lookup failed for {strat_name}: {e}")

                # ── Final score gate: re-check after volume bonus + weight adjustments ──
                # تضمین می‌کند هیچ سیگنالی با score کمتر از min_score ارسال نشود،
                # حتی اگر بعد از run_live_filters تغییراتی روی score اعمال شده باشد.
                _final_score = result.get("score", 0)
                _min_required = get_min_signal_score()
                if _final_score < _min_required:
                    log.info(f"POST-FILTER SCORE DROP: {symbol} | {strat_name} | score={_final_score} < min={_min_required} → blocked")
                    continue

                results.append(result)
            except Exception as e:
                log.debug(f"Error {strat_name} {symbol}: {e}")
        return results

async def scan_once(session, state):
    if not state.get("scanning", True):
        log.info("Scanning paused."); return
    blacklist = load_blacklist()
    log.info("Starting scan...")

    # بررسی معاملات بازِ قبلی برای TP/SL — مستقل از اسکن جدید، تا نتیجه‌ها سریع ثبت شوند
    if state.get("backtest_enabled", True):
        try:
            await update_trade_outcomes(session)
        except Exception as e:
            log.error(f"update_trade_outcomes error: {e}")

    try:
        if state.get("quality_ranking_enabled", True):
            symbols = await get_quality_ranked_symbols(session, TOP_N_COINS, QUALITY_POOL_SIZE)
            log.info(f"Quality ranking selected {len(symbols)} symbols from pool of {QUALITY_POOL_SIZE}")
        else:
            symbols = await get_top_symbols(session, TOP_N_COINS)
    except Exception as e:
        log.error(f"Failed to get symbols: {e}"); return

    symbols = [s for s in symbols if s not in blacklist]

    # Patch #8/#13: Also include symbols whose filter cooldown has expired (ready for rescan)
    rescan_symbols = [s for s in list(_filter_cooldown.keys())
                      if not is_in_filter_cooldown(s) and s not in blacklist and s not in symbols]
    if rescan_symbols:
        log.info(f"Re-scanning {len(rescan_symbols)} previously-filtered symbols: {rescan_symbols[:5]}...")
        symbols = symbols + rescan_symbols

    log.info(f"Scanning {len(symbols)} symbols | workers={PARALLEL_WORKERS}")
    semaphore = asyncio.Semaphore(PARALLEL_WORKERS)

    # یک‌بار در ابتدای چرخه، معاملات بازِ فعلی را می‌خوانیم تا برای همان ارز+استراتژی
    # تا وقتی نتیجه قبلی مشخص نشده، سیگنال تکراری صادر نشود.
    open_keys = get_open_trade_keys() if state.get("backtest_enabled", True) else set()

    start = time.time()
    all_results = await asyncio.gather(
        *[scan_symbol(session, s, semaphore, state, open_keys) for s in symbols],
        return_exceptions=True
    )
    log.info(f"Scan done in {time.time()-start:.1f}s")

    backtest_on = state.get("backtest_enabled", True)
    signals_sent = 0
    for res in all_results:
        if isinstance(res, Exception) or not res: continue
        for result in res:
            log.info(f"SIGNAL: {result['symbol']} | {result['strategy']} | {result['direction']} | score={result['score']}")
            if backtest_on:
                try:
                    log_trade(result)
                except Exception as e:
                    log.error(f"log_trade error: {e}")
            await broadcast_signal(session, build_message(
                symbol=result["symbol"], direction=result["direction"],
                strategy=result["strategy"], price=result["price"],
                entry=result["entry"], stop=result["stop"],
                tp1=result["tp1"], tp2=result["tp2"],
                timeframe=result["timeframe"], score=result["score"],
                rsi=result.get("rsi"), market_regime=result.get("market_regime"),
            ), state)
            signals_sent += 1
            await asyncio.sleep(5)  # جلوگیری از Rate Limit تلگرام: حداقل ۵ ثانیه فاصله بین هر سیگنال
    log.info(f"Signals sent: {signals_sent}")

async def main():
    log.info("AFEE TRADER BOT starting up...")
    init_database()
    _migrate_json_trades_to_db()
    _init_backtest_queue()
    state = load_state()
    connector = aiohttp.TCPConnector(limit=30)
    async with aiohttp.ClientSession(connector=connector) as session:
        from datetime import timedelta
        iran_tz = timezone(timedelta(hours=3, minutes=30))
        now_iran = datetime.now(iran_tz)
        try:
            import jdatetime
            jdt = jdatetime.datetime.fromgregorian(datetime=now_iran.replace(tzinfo=None))
            started_str = jdt.strftime("%Y/%-m/%-d, %H:%M:%S")
        except ImportError:
            started_str = now_iran.strftime("%Y-%m-%d, %H:%M:%S")

        await send_telegram(session,
            "📡 <b>Auto Trading Signal | AI Analysis 🤖</b>\n"
            "🐍 <b>AI AFEE TRADER</b> 🐍\n\n"
            "💎 <b>Symbol:</b> BOT\n"
            "🟢 <b>Signal:</b> START AFEE BOT\n"
            f"⏰ <b>Started:</b> {started_str}\n"
            f"🔖 <b>Version:</b> {BOT_VERSION}\n"
            "━━━━━━━━━━━━━━━\n"
            "<blockquote>🤖 This signal is automatically generated by the advanced AI trading robot "
            "AFEE TRADER based on real-time data analysis.</blockquote>\n\n"
            "@AFEETRADER\n\n"
            "برای کنترل پنل: /start بزن"
        )

        async def scan_loop():
            while True:
                try:
                    await scan_once(session, state)
                except Exception as e:
                    log.error(f"Scan error: {e}")
                log.info(f"Sleeping {SCAN_INTERVAL}s...")
                await asyncio.sleep(SCAN_INTERVAL)

        async def daily_report_loop():
            """هر شب ساعت ۰۰:۰۰ به وقت ایران، نتایج معاملات ۲۴ ساعت اخیر را بررسی و گزارش می‌فرستد."""
            from datetime import timedelta
            iran_tz = timezone(timedelta(hours=3, minutes=30))
            while True:
                try:
                    now_iran = datetime.now(iran_tz)
                    today_iran_str = now_iran.strftime("%Y-%m-%d")
                    if state.get("last_report_date") != today_iran_str and now_iran.hour == 0:
                        log.info("Generating daily report...")
                        trades = await update_trade_outcomes(session)
                        report_date_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                        report_text = build_daily_report(trades, report_date_utc)
                        await broadcast_signal(session, report_text, state)
                        state["last_report_date"] = today_iran_str
                        save_state(state)
                        log.info("Daily report sent.")
                except Exception as e:
                    log.error(f"Daily report error: {e}")
                await asyncio.sleep(60)

        async def weekly_reweight_loop():
            """هر ۷ روز وزن همه استراتژی‌ها را بازمحاسبه می‌کند."""
            while True:
                try:
                    if state.get("adaptive_ranking_enabled", True):
                        last_str = state.get("last_reweight_date")
                        now_utc = datetime.now(timezone.utc)
                        should_run = False
                        if last_str is None:
                            should_run = True
                        else:
                            try:
                                last_dt = datetime.fromisoformat(last_str)
                                should_run = (now_utc - last_dt).total_seconds() >= 7 * 24 * 3600
                            except Exception:
                                should_run = True

                        if should_run:
                            log.info("Running weekly adaptive strategy reweighting...")
                            old_weights = get_all_strategy_weights()
                            new_weights = update_all_strategy_weights()
                            report = build_weight_update_report(old_weights, new_weights)
                            for aid in state.get("admins", {}):
                                await send_msg(session, aid, report)
                            state["last_reweight_date"] = now_utc.isoformat()
                            save_state(state)
                            log.info("Weekly reweighting done.")
                except Exception as e:
                    log.error(f"Weekly reweight error: {e}")
                await asyncio.sleep(3600)

        await asyncio.gather(
            poll_updates(session, state),
            scan_loop(),
            daily_report_loop(),
            weekly_reweight_loop(),
            backtest_worker_loop(session, state),   # جدید: background backtest worker
        )

# ─── CLI ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        cmd = sys.argv[1].lower()
        if cmd == "blacklist" and len(sys.argv) > 2:
            for sym in sys.argv[2:]:
                add_to_blacklist(sym.upper() + ("USDT" if not sym.upper().endswith("USDT") else ""))
            print("Blacklist:", load_blacklist())
        elif cmd == "show-blacklist":
            print("Blacklist:", load_blacklist())
        elif cmd == "remove-blacklist" and len(sys.argv) > 2:
            bl = load_blacklist()
            for sym in sys.argv[2:]:
                bl.discard(sym.upper() + ("USDT" if not sym.upper().endswith("USDT") else ""))
            save_blacklist(bl)
            print("Blacklist:", bl)
        else:
            print("Commands: blacklist <SYM> | show-blacklist | remove-blacklist <SYM>")
    else:
        asyncio.run(main())

