"""
SteelPulse Learning Engine
===========================
Walk-Forward Validation + Adaptive Correction Factors

HOW IT WORKS:
  1. BOOTSTRAP: On first upload, train on 2021-2024, validate on 2025
     → Store correction factor per item in SQLite
  2. MONTHLY UPDATE: Each new upload compares last forecast vs new actuals
     → Update correction factors using exponential smoothing
     → Model gets smarter every month
  3. APPLY: Corrected forecast = Raw forecast × Item correction factor
     → Each item has its own personalised multiplier

CORRECTION FACTOR:
  CF = Actual / Predicted  (capped 0.05x to 20x)
  CF = 1.0 means perfect prediction
  CF = 2.0 means we under-predicted by 2x → boost future forecast
  CF = 0.5 means we over-predicted by 2x → reduce future forecast

SMOOTHING:
  new_CF = 0.7 × old_CF + 0.3 × new_observation
  → Prevents wild swings from one outlier month
  → Gradually adapts to changing patterns
"""

import sqlite3
import os
import json
import math
from datetime import datetime, date
import numpy as np
import pandas as pd

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "steelpulse_learning.db")

TRAIN_YEARS  = [2021, 2022, 2023, 2024]
VALID_YEAR   = 2025
WEIGHTS      = {2021: 0.40, 2022: 0.55, 2023: 0.70, 2024: 1.00}
SMOOTHING    = 0.30   # How fast to adopt new evidence (0=never, 1=immediately)
CF_MIN, CF_MAX = 0.05, 20.0   # Correction factor bounds


# ─────────────────────────────────────────────────────────────────
# DATABASE SETUP
# ─────────────────────────────────────────────────────────────────
def init_db():
    """Create all tables if they don't exist."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Item learning table — one row per item
    c.execute("""
        CREATE TABLE IF NOT EXISTS item_learning (
            item_code           TEXT PRIMARY KEY,
            item_class          TEXT,
            correction_factor   REAL DEFAULT 1.0,
            pred_2025           REAL DEFAULT 0,
            actual_2025         REAL DEFAULT 0,
            error_pct_2025      REAL DEFAULT 0,
            pred_2026           REAL DEFAULT 0,
            actual_2026         REAL DEFAULT 0,
            months_tracked      INTEGER DEFAULT 0,
            avg_error_pct       REAL DEFAULT 0,
            last_updated        TEXT,
            is_bootstrapped     INTEGER DEFAULT 0,
            notes               TEXT DEFAULT ''
        )
    """)

    # Monthly forecast snapshots
    c.execute("""
        CREATE TABLE IF NOT EXISTS forecast_history (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            upload_id       TEXT,
            item_code       TEXT,
            forecast_month  TEXT,
            predicted_qty   REAL,
            corrected_qty   REAL,
            signal          TEXT,
            score           REAL,
            net_stock       REAL,
            uploaded_at     TEXT
        )
    """)

    # Monthly actuals (extracted from each upload)
    c.execute("""
        CREATE TABLE IF NOT EXISTS monthly_actuals (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            upload_id       TEXT,
            item_code       TEXT,
            year            INTEGER,
            actual_sales    REAL,
            actual_purchase REAL,
            closing_stock   REAL,
            recorded_at     TEXT
        )
    """)

    # Upload log
    c.execute("""
        CREATE TABLE IF NOT EXISTS upload_log (
            upload_id           TEXT PRIMARY KEY,
            filename            TEXT,
            uploaded_at         TEXT,
            items_processed     INTEGER,
            buy_signals         INTEGER,
            stockout_risk       INTEGER,
            total_proposed_qty  REAL,
            model_version       INTEGER DEFAULT 1,
            is_bootstrapped     INTEGER DEFAULT 0
        )
    """)

    # Model performance over time
    c.execute("""
        CREATE TABLE IF NOT EXISTS model_performance (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            upload_id       TEXT,
            upload_date     TEXT,
            items_evaluated INTEGER,
            mean_error_pct  REAL,
            median_error_pct REAL,
            items_within_20pct INTEGER,
            items_within_50pct INTEGER,
            items_within_100pct INTEGER,
            model_version   INTEGER
        )
    """)

    conn.commit()
    conn.close()


def get_connection():
    return sqlite3.connect(DB_PATH)


def is_bootstrapped():
    """Check if the learning model has been initialised."""
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM item_learning WHERE is_bootstrapped=1")
    count = c.fetchone()[0]
    conn.close()
    return count > 0


def get_upload_count():
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM upload_log")
    count = c.fetchone()[0]
    conn.close()
    return count


# ─────────────────────────────────────────────────────────────────
# BOOTSTRAP — Learn from 6 years of historical data
# ─────────────────────────────────────────────────────────────────
def bootstrap_from_history(result_df):
    """
    One-time learning from 2021-2025 data.
    Train on 2021-2024, validate on 2025.
    Store correction factor per item.
    """
    conn = get_connection()
    c = conn.cursor()

    items_learned   = 0
    items_skipped   = 0
    errors_tracked  = []

    for _, row in result_df.iterrows():
        item_code  = row["ItemCode"]
        item_class = row.get("ItemClass", "UNKNOWN")

        # Get training sales (2021-2024)
        train_sales = {y: float(row.get(f"Sales_{y}", 0) or 0) for y in TRAIN_YEARS}
        actual_2025 = float(row.get("Sales_2025", 0) or 0)
        actual_2026 = float(row.get("Sales_2026", 0) or 0)  # partial year

        # Predict 2025 using weighted average of training years
        active_train = [y for y in TRAIN_YEARS if train_sales[y] > 0]

        if active_train:
            pred_2025 = (
                sum(train_sales[y] * WEIGHTS[y] for y in active_train) /
                sum(WEIGHTS[y] for y in active_train)
            )
        else:
            pred_2025 = 0

        # Compute correction factor
        if actual_2025 > 0 and pred_2025 > 0:
            raw_cf    = actual_2025 / pred_2025
            cf        = max(CF_MIN, min(CF_MAX, raw_cf))
            error_pct = abs(pred_2025 - actual_2025) / actual_2025 * 100
            errors_tracked.append(min(error_pct, 500))
        elif actual_2025 == 0 and pred_2025 == 0:
            cf        = 1.0
            error_pct = 0
        elif actual_2025 == 0:
            # We predicted sales but nothing happened → over-predicted
            cf        = CF_MIN
            error_pct = 100
        else:
            # We predicted 0 but they sold → under-predicted
            cf        = CF_MAX
            error_pct = 100

        # Insert or update item_learning
        c.execute("""
            INSERT OR REPLACE INTO item_learning
            (item_code, item_class, correction_factor,
             pred_2025, actual_2025, error_pct_2025,
             pred_2026, actual_2026,
             months_tracked, avg_error_pct,
             last_updated, is_bootstrapped, notes)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,1,?)
        """, (
            item_code, item_class, cf,
            round(pred_2025, 2), actual_2025, round(error_pct, 1),
            0, actual_2026,
            1, round(error_pct, 1),
            datetime.now().isoformat(),
            f"Bootstrapped: train 2021-2024, validate 2025"
        ))

        items_learned += 1

    conn.commit()

    # Record model performance
    if errors_tracked:
        errors_arr = np.array(errors_tracked)
        upload_id  = f"BOOTSTRAP_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        c.execute("""
            INSERT INTO model_performance
            (upload_id, upload_date, items_evaluated,
             mean_error_pct, median_error_pct,
             items_within_20pct, items_within_50pct, items_within_100pct,
             model_version)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (
            upload_id,
            datetime.now().strftime("%Y-%m-%d"),
            len(errors_tracked),
            round(float(errors_arr.mean()), 1),
            round(float(np.median(errors_arr)), 1),
            int((errors_arr < 20).sum()),
            int((errors_arr < 50).sum()),
            int((errors_arr < 100).sum()),
            1
        ))
        conn.commit()

    conn.close()
    return items_learned


# ─────────────────────────────────────────────────────────────────
# APPLY LEARNING — Get corrected forecast
# ─────────────────────────────────────────────────────────────────
def get_correction_factors():
    """Load all correction factors from DB as a dict."""
    conn = get_connection()
    df = pd.read_sql("SELECT item_code, correction_factor, months_tracked, avg_error_pct FROM item_learning", conn)
    conn.close()
    if df.empty:
        return {}
    return df.set_index("item_code").to_dict(orient="index")


def apply_corrections(result_df, corrections):
    """
    Apply learned correction factors to the forecast.
    Returns df with new columns: CF, CorrectedQty_6M, LearningApplied
    """
    df = result_df.copy()

    MONTH_LABELS = _get_month_labels()

    cfs, corrected_qtys, applied = [], [], []

    for _, row in df.iterrows():
        item = row["ItemCode"]
        if item in corrections:
            cf      = corrections[item]["correction_factor"]
            tracked = corrections[item]["months_tracked"]
        else:
            cf      = 1.0
            tracked = 0

        # Confidence in correction: more months = more trust
        # Blend: CF weight grows with months tracked (max weight at 6+ months)
        blend = min(1.0, tracked / 6.0)

        # Blended correction (starts at 1.0, grows toward learned CF)
        blended_cf = 1.0 * (1 - blend) + cf * blend

        cfs.append(round(blended_cf, 3))

        # Apply to 6M proposed qty
        raw_qty = float(row.get("ProposedQty_6M", 0) or 0)
        corrected = max(0, round(raw_qty * blended_cf, 0))
        corrected_qtys.append(corrected)

        applied.append(tracked > 0)

    df["CorrectionFactor"]  = cfs
    df["CorrectedQty_6M"]   = corrected_qtys
    df["LearningApplied"]   = applied
    df["CorrectedCostUSD"]  = (df["CorrectedQty_6M"] * df["PricePerLength"]).round(2)

    return df


def _get_month_labels():
    import calendar
    today = date.today()
    result = []
    for i in range(1, 7):
        m = today.month + i
        y = today.year + (m - 1) // 12
        m = ((m - 1) % 12) + 1
        result.append(f"{calendar.month_abbr[m]}'{str(y)[2:]}")
    return result


# ─────────────────────────────────────────────────────────────────
# MONTHLY UPDATE — Learn from new upload
# ─────────────────────────────────────────────────────────────────
def update_from_new_upload(result_df, upload_id):
    """
    Called every time a new file is uploaded.
    Compares new actuals vs last stored forecast.
    Updates correction factors using exponential smoothing.
    """
    conn = get_connection()
    c = conn.cursor()

    updates = 0
    errors  = []

    for _, row in result_df.iterrows():
        item  = row["ItemCode"]
        iclass = row.get("ItemClass", "UNKNOWN")

        # Get 2026 actuals from new upload (partial year)
        actual_2026 = float(row.get("Sales_2026", 0) or 0)

        # Get stored prediction for 2026 if exists
        c.execute("SELECT pred_2026, correction_factor, months_tracked, avg_error_pct FROM item_learning WHERE item_code=?", (item,))
        existing = c.fetchone()

        if existing is None:
            # New item not seen before — insert with neutral CF
            c.execute("""
                INSERT OR IGNORE INTO item_learning
                (item_code, item_class, correction_factor, months_tracked,
                 actual_2026, last_updated, is_bootstrapped)
                VALUES (?,?,1.0,0,?,?,1)
            """, (item, iclass, actual_2026, datetime.now().isoformat()))
            continue

        old_pred, old_cf, months, avg_err = existing

        # If we have a stored prediction for 2026, compute new CF
        if old_pred and old_pred > 0 and actual_2026 > 0:
            new_raw_cf = actual_2026 / old_pred
            new_cf     = max(CF_MIN, min(CF_MAX, new_raw_cf))
            # Exponential smoothing
            blended_cf = (1 - SMOOTHING) * old_cf + SMOOTHING * new_cf
            blended_cf = round(max(CF_MIN, min(CF_MAX, blended_cf)), 4)

            err_pct = abs(old_pred - actual_2026) / actual_2026 * 100
            # Rolling average error
            new_avg_err = (avg_err * months + err_pct) / (months + 1)
            errors.append(min(err_pct, 500))

            c.execute("""
                UPDATE item_learning
                SET correction_factor=?, months_tracked=?, avg_error_pct=?,
                    actual_2026=?, last_updated=?
                WHERE item_code=?
            """, (
                blended_cf, months + 1, round(new_avg_err, 1),
                actual_2026, datetime.now().isoformat(),
                item
            ))
            updates += 1
        else:
            # Just update actual 2026
            c.execute("""
                UPDATE item_learning SET actual_2026=?, last_updated=?
                WHERE item_code=?
            """, (actual_2026, datetime.now().isoformat(), item))

    conn.commit()

    # Record performance
    if errors:
        ea = np.array(errors)
        c.execute("""
            INSERT INTO model_performance
            (upload_id, upload_date, items_evaluated,
             mean_error_pct, median_error_pct,
             items_within_20pct, items_within_50pct, items_within_100pct,
             model_version)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (
            upload_id,
            datetime.now().strftime("%Y-%m-%d"),
            len(errors),
            round(float(ea.mean()), 1),
            round(float(np.median(ea)), 1),
            int((ea < 20).sum()),
            int((ea < 50).sum()),
            int((ea < 100).sum()),
            get_upload_count()
        ))
        conn.commit()

    conn.close()
    return updates


# ─────────────────────────────────────────────────────────────────
# SAVE FORECAST SNAPSHOT
# ─────────────────────────────────────────────────────────────────
def save_forecast_snapshot(result_df, upload_id, filename, summary):
    """Save this month's forecast to DB for future comparison."""
    conn = get_connection()
    c = conn.cursor()

    MONTH_LABELS = _get_month_labels()
    now = datetime.now().isoformat()

    # Log upload
    c.execute("""
        INSERT OR REPLACE INTO upload_log
        (upload_id, filename, uploaded_at, items_processed,
         buy_signals, stockout_risk, total_proposed_qty, is_bootstrapped)
        VALUES (?,?,?,?,?,?,?,1)
    """, (
        upload_id, filename, now,
        summary.get("total", 0), summary.get("buy", 0),
        summary.get("stockout_risk", 0), summary.get("proposed_qty", 0)
    ))

    # Save actuals from this upload
    for _, row in result_df.iterrows():
        for yr in [2021,2022,2023,2024,2025,2026]:
            sales = float(row.get(f"Sales_{yr}", 0) or 0)
            purch = float(row.get(f"Purch_{yr}", 0) or 0)
            if sales > 0 or purch > 0:
                c.execute("""
                    INSERT INTO monthly_actuals
                    (upload_id, item_code, year, actual_sales, actual_purchase, recorded_at)
                    VALUES (?,?,?,?,?,?)
                """, (upload_id, row["ItemCode"], yr, sales, purch, now))

    conn.commit()
    conn.close()


# ─────────────────────────────────────────────────────────────────
# ANALYTICS — For the Learning Dashboard
# ─────────────────────────────────────────────────────────────────
def get_learning_stats():
    """Return stats for the Learning Dashboard."""
    conn = get_connection()

    # Overall learning summary
    try:
        items_df = pd.read_sql("""
            SELECT item_code, item_class, correction_factor,
                   pred_2025, actual_2025, error_pct_2025,
                   months_tracked, avg_error_pct, last_updated
            FROM item_learning
            WHERE is_bootstrapped=1
            ORDER BY avg_error_pct DESC
        """, conn)
    except Exception:
        items_df = pd.DataFrame()

    # Model performance over time
    try:
        perf_df = pd.read_sql("""
            SELECT upload_date, items_evaluated, mean_error_pct,
                   median_error_pct, items_within_20pct, items_within_50pct,
                   items_within_100pct, model_version
            FROM model_performance
            ORDER BY upload_date
        """, conn)
    except Exception:
        perf_df = pd.DataFrame()

    # Upload history
    try:
        uploads_df = pd.read_sql("""
            SELECT upload_id, filename, uploaded_at, items_processed,
                   buy_signals, stockout_risk, total_proposed_qty
            FROM upload_log
            ORDER BY uploaded_at DESC
            LIMIT 20
        """, conn)
    except Exception:
        uploads_df = pd.DataFrame()

    conn.close()

    stats = {}
    if not items_df.empty:
        stats["total_items_learned"] = len(items_df)
        stats["items_good_cf"]  = int(((items_df.correction_factor >= 0.8) &
                                       (items_df.correction_factor <= 1.2)).sum())
        stats["items_over"]     = int((items_df.correction_factor < 0.8).sum())
        stats["items_under"]    = int((items_df.correction_factor > 1.2).sum())
        stats["median_error"]   = round(float(items_df.error_pct_2025.median()), 1)
        stats["mean_error"]     = round(float(items_df.error_pct_2025.mean()), 1)
        stats["items_df"]       = items_df
        stats["cf_distribution"] = {
            "< 0.5× (over-predicted)":    int((items_df.correction_factor < 0.5).sum()),
            "0.5×–0.8× (slight over)":    int(((items_df.correction_factor >= 0.5) & (items_df.correction_factor < 0.8)).sum()),
            "0.8×–1.2× (accurate)":       int(((items_df.correction_factor >= 0.8) & (items_df.correction_factor <= 1.2)).sum()),
            "1.2×–2.0× (slight under)":   int(((items_df.correction_factor > 1.2) & (items_df.correction_factor <= 2.0)).sum()),
            "> 2.0× (under-predicted)":   int((items_df.correction_factor > 2.0).sum()),
        }
    else:
        stats["total_items_learned"] = 0

    stats["perf_df"]    = perf_df
    stats["uploads_df"] = uploads_df
    return stats


def get_item_learning_detail(item_code):
    """Get full learning history for one item."""
    conn = get_connection()
    try:
        row = pd.read_sql(
            "SELECT * FROM item_learning WHERE item_code=?",
            conn, params=(item_code,)
        )
    except Exception:
        row = pd.DataFrame()
    conn.close()
    return row.iloc[0].to_dict() if not row.empty else None
