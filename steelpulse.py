"""
╔══════════════════════════════════════════════════════════════════╗
║          SteelPulse — Procurement Intelligence Platform          ║
║          Streamlit Single-File App  |  Version 2.0               ║
║                                                                  ║
║  ALGORITHM: Weighted Multi-Signal Procurement Scoring (WMSPS)   ║
║  FORECAST:  Trend-Weighted Moving Average Projection (TWMAP)     ║
╚══════════════════════════════════════════════════════════════════╝

HOW TO RUN:
    pip install streamlit pandas numpy openpyxl plotly xlsxwriter
    streamlit run steelpulse.py

HOW TO USE:
    1. Upload your SAP Excel export (.xlsx) in the sidebar
    2. Algorithm runs automatically
    3. Browse tabs: Dashboard → Forecast → 6-Month → Balance → Algorithm
    4. Click any item row → see full detail
    5. Export → download professional Excel report
"""

import io
import math
import calendar
import warnings
from datetime import date, datetime

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots
from learning import (
    init_db, is_bootstrapped, bootstrap_from_history,
    get_correction_factors, apply_corrections,
    update_from_new_upload, save_forecast_snapshot,
    get_learning_stats, get_item_learning_detail, get_upload_count
)

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────
YEARS = [2021, 2022, 2023, 2024, 2025, 2026]
YEAR_WEIGHTS = {2021: 0.40, 2022: 0.55, 2023: 0.70,
                2024: 1.00, 2025: 1.50, 2026: 2.00}
MONTHS_2026 = 4   # Jan–Apr complete as of May 2026

SIGNAL_COLORS = {
    "BUY":   "#28a745",
    "WATCH": "#007bff",
    "HOLD":  "#ffc107",
    "SKIP":  "#dc3545",
}
CLASS_COLORS = {
    "FAST_MOVER": "#28a745",
    "SLOW_MOVER": "#007bff",
    "PROJECT":    "#fd7e14",
    "DEAD":       "#6c757d",
}
CONF_COLORS = {"HIGH": "#28a745", "MEDIUM": "#ffc107", "LOW": "#dc3545"}


def _next_6_months():
    today = date.today()
    result = []
    for i in range(1, 7):
        m = today.month + i
        y = today.year + (m - 1) // 12
        m = ((m - 1) % 12) + 1
        result.append(f"{calendar.month_abbr[m]}'{str(y)[2:]}")
    return result


MONTH_LABELS = _next_6_months()


# ─────────────────────────────────────────────────────────────────
# STEP 1 — EXCEL PARSER
# ─────────────────────────────────────────────────────────────────
def parse_excel(uploaded_file):
    """Parse all relevant sheets from uploaded SAP Excel export."""
    xl = pd.ExcelFile(uploaded_file)
    sheets = xl.sheet_names
    data = {}

    # ── Quotation pivot ──
    for sh in sheets:
        raw = pd.read_excel(uploaded_file, sheet_name=sh, header=None)
        flat = " ".join(str(x) for x in raw.values.flatten())
        if "Tubing Quotation Table" in flat or (
            "Sum of Quantity" in flat and "Row Labels" in flat and "2021" in flat
        ):
            pv = _parse_pivot(raw)
            if pv is not None and len(pv) > 5:
                data["quotation_pivot"] = pv
                break

    # ── SO pivot ──
    for sh in sheets:
        if sh in ("SO-Table", "TSO Table", "Sheet2"):
            raw = pd.read_excel(uploaded_file, sheet_name=sh, header=None)
            pv = _parse_pivot(raw)
            if pv is not None and len(pv) > 2:
                data["so_pivot"] = pv
                break

    # ── Purchase pivot ──
    for sh in sheets:
        if sh in ("Purchase-Table", "TP History", "Sheet1"):
            raw = pd.read_excel(uploaded_file, sheet_name=sh, header=None)
            pv = _parse_pivot(raw)
            if pv is not None and len(pv) > 2:
                data["purchase_pivot"] = pv
                break

    # ── Stock balance ──
    for sh in sheets:
        raw = pd.read_excel(uploaded_file, sheet_name=sh, header=None)
        flat = " ".join(str(x) for x in raw.values.flatten())
        if "01-QOH" in flat and "ItemCode" in flat:
            data["stock"] = _parse_stock(raw)
            break

    # ── Pricing ──
    for sh in sheets:
        if "PRICE" in sh.upper() or "144" in sh:
            raw = pd.read_excel(uploaded_file, sheet_name=sh, header=None)
            data["pricing"] = _parse_pricing(raw)
            break

    # ── Master Sheet5 ──
    for sh in sheets:
        if sh == "Sheet5":
            raw = pd.read_excel(uploaded_file, sheet_name=sh, header=None)
            data["master_sheet"] = _parse_sheet5(raw)
            break

    return data


def _parse_pivot(raw):
    header_row = None
    for i, row in raw.iterrows():
        vals = row.dropna().astype(str).tolist()
        if any(str(y) in vals for y in YEARS) or "Row Labels" in vals:
            header_row = i
            break
    if header_row is None:
        return None
    df = raw.iloc[header_row:].reset_index(drop=True)
    headers = df.iloc[0].tolist()
    year_cols = {}
    for ci, h in enumerate(headers):
        try:
            yr = int(float(str(h)))
            if yr in YEARS:
                year_cols[yr] = ci
        except Exception:
            pass
    if not year_cols:
        return None
    records = []
    for _, row in df.iloc[1:].iterrows():
        item = str(row.iloc[0]).strip()
        if not item or item in ("nan", "Grand Total", "Row Labels", "None"):
            continue
        rec = {"ItemCode": item}
        for yr, ci in year_cols.items():
            val = row.iloc[ci]
            rec[yr] = float(val) if pd.notna(val) else 0.0
        records.append(rec)
    return pd.DataFrame(records) if records else None


def _parse_stock(raw):
    hdr_row = None
    for i, row in raw.iterrows():
        if "01-QOH" in row.astype(str).values:
            hdr_row = i
            break
    if hdr_row is None:
        return None
    df = raw.iloc[hdr_row:].reset_index(drop=True)
    df.columns = df.iloc[0]
    df = df.iloc[1:].reset_index(drop=True)
    df.columns = df.columns.astype(str).str.strip()
    keep = ["ItemCode", "Item Cost", "01-QOH", "Consignment",
            "01-Open SO", "01-Avail Stock", "01 PO", "01-Net Avail Stock"]
    available = [c for c in keep if c in df.columns]
    df = df[available].copy()
    df["ItemCode"] = df["ItemCode"].astype(str).str.strip()
    df = df[df["ItemCode"].notna() & (df["ItemCode"] != "nan") & (df["ItemCode"] != "")]
    for c in [x for x in df.columns if x != "ItemCode"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    return df


def _parse_pricing(raw):
    df = raw.copy()
    df.columns = df.iloc[0].astype(str).str.strip()
    df = df.iloc[1:].reset_index(drop=True)
    rename = {}
    for c in df.columns:
        cl = c.lower().replace("\n", " ")
        if "swagelok" in cl or "p/n" in cl:
            rename[c] = "ItemCode"
        elif "unit price" in cl:
            rename[c] = "UnitPriceUSD_mtr"
        elif "lead time" in cl and "week" in cl:
            rename[c] = "LeadTimeWeeks"
        elif "uom" in cl or ("length" in cl and "uom" in cl):
            rename[c] = "PricePerLength"
        elif "ex-mill" in cl or "ex mill" in cl:
            rename[c] = "Origin"
    df = df.rename(columns=rename)
    if "ItemCode" not in df.columns:
        return None
    df["ItemCode"] = df["ItemCode"].astype(str).str.strip()
    df = df[df["ItemCode"].notna() & (df["ItemCode"] != "nan")]
    for c in ["UnitPriceUSD_mtr", "LeadTimeWeeks", "PricePerLength"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    return df


def _parse_sheet5(raw):
    hdr_idx = None
    for i, row in raw.iterrows():
        if any("2021" in str(v) for v in row):
            hdr_idx = i
            break
    if hdr_idx is None:
        return None
    records = []
    for _, row in raw.iloc[hdr_idx + 1:].iterrows():
        item = str(row.iloc[1]).strip()
        if not item or item in ("nan", "None", "Grand Total"):
            continue
        rec = {"ItemCode": item}
        try:
            rec["NetAvail_manual"] = float(row.iloc[26]) if pd.notna(row.iloc[26]) else 0
            rec["ProposedQty_manual"] = float(row.iloc[27]) if pd.notna(row.iloc[27]) else 0
            rec["UnitPrice_sheet5"] = float(row.iloc[29]) if pd.notna(row.iloc[29]) else 0
        except Exception:
            pass
        records.append(rec)
    return pd.DataFrame(records) if records else None


# ─────────────────────────────────────────────────────────────────
# STEP 2 — BUILD MASTER TABLE
# ─────────────────────────────────────────────────────────────────
def build_master(data):
    all_items = set()
    for key in ["quotation_pivot", "so_pivot", "purchase_pivot"]:
        if key in data and data[key] is not None:
            all_items.update(data[key]["ItemCode"].tolist())
    if "stock" in data and data["stock"] is not None:
        all_items.update(data["stock"]["ItemCode"].tolist())
    all_items = {i for i in all_items if i and i != "nan" and len(i) > 3}

    rows = []
    for item in sorted(all_items):
        row = {"ItemCode": item}
        for key, prefix in [("quotation_pivot", "Inq"), ("so_pivot", "Sales"), ("purchase_pivot", "Purch")]:
            df = data.get(key)
            for yr in YEARS:
                col = f"{prefix}_{yr}"
                if df is not None and yr in df.columns:
                    match = df[df["ItemCode"] == item]
                    row[col] = float(match[yr].sum()) if not match.empty else 0.0
                else:
                    row[col] = 0.0

        st = data.get("stock")
        if st is not None:
            m = st[st["ItemCode"] == item]
            row["QOH"]          = float(m["01-QOH"].iloc[0])          if not m.empty and "01-QOH"          in m.columns else 0
            row["OpenSO"]       = float(m["01-Open SO"].iloc[0])       if not m.empty and "01-Open SO"      in m.columns else 0
            row["AvailStock"]   = float(m["01-Avail Stock"].iloc[0])   if not m.empty and "01-Avail Stock"  in m.columns else 0
            row["IncomingPO"]   = float(m["01 PO"].iloc[0])            if not m.empty and "01 PO"           in m.columns else 0
            row["NetAvailStock"]= float(m["01-Net Avail Stock"].iloc[0])if not m.empty and "01-Net Avail Stock" in m.columns else 0
            row["ItemCost"]     = float(m["Item Cost"].iloc[0])         if not m.empty and "Item Cost"       in m.columns else 0
        else:
            for f in ["QOH","OpenSO","AvailStock","IncomingPO","NetAvailStock","ItemCost"]:
                row[f] = 0

        pr = data.get("pricing")
        if pr is not None:
            m = pr[pr["ItemCode"] == item]
            row["UnitPriceUSD_mtr"] = float(m["UnitPriceUSD_mtr"].iloc[0]) if not m.empty and "UnitPriceUSD_mtr" in m.columns else 0
            row["LeadTimeWeeks"]    = float(m["LeadTimeWeeks"].iloc[0])     if not m.empty and "LeadTimeWeeks"    in m.columns else 8
            row["PricePerLength"]   = float(m["PricePerLength"].iloc[0])    if not m.empty and "PricePerLength"   in m.columns else 0
            row["Origin"]           = str(m["Origin"].iloc[0])              if not m.empty and "Origin"           in m.columns else ""
        else:
            row["UnitPriceUSD_mtr"] = 0; row["LeadTimeWeeks"] = 8
            row["PricePerLength"] = 0;   row["Origin"] = ""

        ms = data.get("master_sheet")
        if ms is not None:
            m = ms[ms["ItemCode"] == item]
            row["ProposedQty_manual"] = float(m["ProposedQty_manual"].iloc[0]) if not m.empty and "ProposedQty_manual" in m.columns else np.nan
        else:
            row["ProposedQty_manual"] = np.nan

        rows.append(row)

    master = pd.DataFrame(rows).fillna(0)
    return master


# ─────────────────────────────────────────────────────────────────
# STEP 3 — WMSPS ALGORITHM
# ─────────────────────────────────────────────────────────────────
def run_algorithm(master):
    df = master.copy()

    # Weighted average sales
    df["WeightedAvgSales"] = sum(df[f"Sales_{y}"] * YEAR_WEIGHTS[y] for y in YEARS) / sum(YEAR_WEIGHTS.values())
    df["AvgMonthlySales"]  = df["WeightedAvgSales"] / 12
    df["TotalInquiry"]     = df[[f"Inq_{y}"   for y in YEARS]].sum(axis=1)
    df["TotalSales"]       = df[[f"Sales_{y}" for y in YEARS]].sum(axis=1)
    df["TotalPurchase"]    = df[[f"Purch_{y}" for y in YEARS]].sum(axis=1)
    df["RecentSales"]      = df["Sales_2025"] + df["Sales_2026"]
    df["RecentInquiry"]    = df["Inq_2025"]   + df["Inq_2026"]

    # ── S1: Sales Velocity (35%) ──
    def _s1(row):
        vals = np.array([row[f"Sales_{y}"] for y in YEARS], dtype=float)
        if vals.sum() == 0:
            return 0.0
        w = np.array([YEAR_WEIGHTS[y] for y in YEARS])
        x = np.arange(len(YEARS), dtype=float)
        xm = np.average(x, weights=w); ym = np.average(vals, weights=w)
        slope = np.sum(w*(x-xm)*(vals-ym)) / (np.sum(w*(x-xm)**2) + 1e-9)
        avg = vals[vals > 0].mean() if (vals > 0).any() else 1
        score = min(100, max(0, 50 + (slope/(avg+1e-9))*50))
        if row["RecentSales"] > 0: score = min(100, score + 15)
        return round(score, 1)

    # ── S2: Inquiry Conversion (25%) ──
    def _s2(row):
        if row["TotalInquiry"] == 0: return 0.0
        conv = min(1.0, row["TotalSales"] / (row["TotalInquiry"] + 1e-9))
        boost = min(30, (row["RecentInquiry"] / 100) * 10)
        return round(min(100, conv * 100 + boost), 1)

    # ── S3: Stock Coverage (25%) ──
    def _s3(row):
        avg = row["AvgMonthlySales"]
        if avg <= 0:
            return 0 if row["NetAvailStock"] > 0 else 50
        net = row["NetAvailStock"] if row["NetAvailStock"] != 0 else row["AvailStock"]
        cov = net / (avg + 1e-9)
        lead = row["LeadTimeWeeks"] / 4.0
        if cov <= 0:         return 100
        elif cov < lead:     return 95
        elif cov < lead + 1: return 80
        elif cov < 3:        return 60
        elif cov < 6:        return 30
        else:                return 5

    # ── S4: Open SO Pressure (15%) ──
    def _s4(row):
        if row["OpenSO"] <= 0: return 0.0
        uncov = max(0, row["OpenSO"] - max(0, row["AvailStock"]))
        return round(min(100, (uncov / (row["OpenSO"] + 1e-9)) * 100), 1)

    df["S1_Velocity"]   = df.apply(_s1, axis=1)
    df["S2_Conversion"] = df.apply(_s2, axis=1)
    df["S3_Coverage"]   = df.apply(_s3, axis=1)
    df["S4_OpenSO"]     = df.apply(_s4, axis=1)
    df["Score"] = (df["S1_Velocity"]*0.35 + df["S2_Conversion"]*0.25 +
                   df["S3_Coverage"]*0.25 + df["S4_OpenSO"]*0.15).round(1)

    # ── Item Classification ──
    def _classify(row):
        sales = [row[f"Sales_{y}"] for y in YEARS]
        nz = sum(1 for v in sales if v > 0)
        total = sum(sales)
        if total == 0 and row["TotalInquiry"] < 3: return "DEAD"
        if nz <= 2 and total > 0:
            mx = max(sales)
            if mx > 0 and (total / mx) < 1.5: return "PROJECT"
        if nz >= 3: return "FAST_MOVER"
        if 1 <= nz < 3: return "SLOW_MOVER"
        return "DEAD"

    df["ItemClass"] = df.apply(_classify, axis=1)

    # ── Signal ──
    def _signal(row):
        if row["ItemClass"] == "DEAD": return "SKIP"
        s = row["Score"]
        if s >= 60: return "BUY"
        elif s >= 40: return "WATCH"
        elif s >= 20: return "HOLD"
        else: return "SKIP"

    df["Signal"] = df.apply(_signal, axis=1)
    df.loc[(df["OpenSO"] > df["AvailStock"]) & (df["TotalSales"] > 0), "Signal"] = "BUY"

    # ── Stock cover days ──
    def _cover(row):
        if row["AvgMonthlySales"] <= 0: return 9999
        net = row["NetAvailStock"] if row["NetAvailStock"] != 0 else row["AvailStock"]
        return round(max(0, net / row["AvgMonthlySales"] * 30), 0)

    df["StockCoverDays"] = df.apply(_cover, axis=1)
    return df


# ─────────────────────────────────────────────────────────────────
# STEP 4 — TWMAP 6-MONTH FORECAST
# ─────────────────────────────────────────────────────────────────
def run_forecast(df):
    RECENCY = {2021:0.40,2022:0.55,2023:0.70,2024:1.00,2025:1.50,2026:2.00}
    DECAY   = [1.00,0.98,0.96,0.94,0.92,0.90]

    records = []
    for _, row in df.iterrows():
        sales = {y: float(row.get(f"Sales_{y}", 0) or 0) for y in YEARS}
        inq   = {y: float(row.get(f"Inq_{y}",   0) or 0) for y in YEARS}
        # Annualise partial 2026
        if MONTHS_2026 > 0:
            sales[2026] = sales[2026] * (12.0 / MONTHS_2026)
            inq[2026]   = inq[2026]   * (12.0 / MONTHS_2026)

        total_sales = sum(sales.values())

        if total_sales == 0 and sum(inq.values()) < 3:
            rec = {f"Proj_Mid_{m}": 0.0 for m in MONTH_LABELS}
            rec.update({f"Proj_Low_{m}":  0.0 for m in MONTH_LABELS})
            rec.update({f"Proj_High_{m}": 0.0 for m in MONTH_LABELS})
            rec.update({"F6M_Low":0,"F6M_Mid":0,"F6M_High":0,
                        "NetStock_Now":float(row.get("QOH",0) or 0),
                        "StockEnd_Mid":float(row.get("QOH",0) or 0),
                        "StockEnd_Worst":float(row.get("QOH",0) or 0),
                        "ProposedQty_6M":0,"SafetyBuffer":0,
                        "StockoutMonth":"None","HasStockoutRisk":False,
                        "BaseMonthlySales":0,"TrendMult":1.0,"InqBoost":1.0,
                        "ForecastConf":"LOW"})
            records.append(rec)
            continue

        # Base monthly
        active = [y for y in YEARS if sales[y] > 0]
        if active:
            base_annual = (sum(sales[y]*RECENCY[y] for y in active) /
                           sum(RECENCY[y] for y in active))
        else:
            base_annual = 0
        base_monthly = base_annual / 12.0

        # Trend multiplier
        recent2 = sales[2025] + sales[2026]
        prior2  = sales[2023] + sales[2024]
        if prior2 > 0 and recent2 > 0:
            trend = max(0.50, min(2.00, recent2 / prior2))
        elif recent2 > 0:  trend = 1.20
        elif prior2 > 0:   trend = 0.50
        else:              trend = 1.00

        # Inquiry boost
        avg_inq = sum(inq.values()) / 6.0
        recent_inq = inq[2025] + inq[2026]
        inq_boost = max(0.80, min(1.50, (recent_inq/2.0)/(avg_inq+1e-9))) if avg_inq > 0 else 1.0

        # Monthly projections
        mid_vals  = [round(base_monthly*trend*inq_boost*DECAY[i], 2) for i in range(6)]
        low_vals  = [round(v*0.75, 2) for v in mid_vals]
        high_vals = [round(v*1.30, 2) for v in mid_vals]

        f6m_mid  = round(sum(mid_vals),  1)
        f6m_low  = round(sum(low_vals),  1)
        f6m_high = round(sum(high_vals), 1)

        qoh     = float(row.get("QOH",0) or 0)
        inc     = float(row.get("IncomingPO",0) or 0)
        oso     = float(row.get("OpenSO",0) or 0)
        net     = max(0, qoh + inc - oso)
        safety  = round(mid_vals[0], 1) if mid_vals else 0
        proposed = max(0, round(f6m_mid + safety - net, 0))

        # Stockout month
        running = net
        stockout = "None"
        for i, mv in enumerate(mid_vals):
            running -= mv
            if running < 0:
                stockout = MONTH_LABELS[i]
                break

        nz_yrs = sum(1 for y in YEARS if sales[y] > 0)
        conf = "HIGH" if nz_yrs >= 4 else ("MEDIUM" if nz_yrs >= 2 else "LOW")

        rec = {f"Proj_Mid_{m}":  mid_vals[i]  for i, m in enumerate(MONTH_LABELS)}
        rec.update({f"Proj_Low_{m}":  low_vals[i]  for i, m in enumerate(MONTH_LABELS)})
        rec.update({f"Proj_High_{m}": high_vals[i] for i, m in enumerate(MONTH_LABELS)})
        rec.update({
            "F6M_Low": f6m_low, "F6M_Mid": f6m_mid, "F6M_High": f6m_high,
            "NetStock_Now": round(net, 1),
            "StockEnd_Mid":   round(net - f6m_mid,  1),
            "StockEnd_Worst": round(net - f6m_high, 1),
            "ProposedQty_6M": proposed, "SafetyBuffer": safety,
            "StockoutMonth": stockout,
            "HasStockoutRisk": stockout != "None",
            "BaseMonthlySales": round(base_monthly, 2),
            "TrendMult": round(trend, 3),
            "InqBoost":  round(inq_boost, 3),
            "ForecastConf": conf,
        })
        records.append(rec)

    fc_df = pd.DataFrame(records).reset_index(drop=True)
    result = pd.concat([df.reset_index(drop=True), fc_df], axis=1)
    result["ProposedQty"] = result["ProposedQty_6M"]
    result["EstCostUSD"]  = (result["ProposedQty"] * result["PricePerLength"]).round(2)
    return result


# ─────────────────────────────────────────────────────────────────
# STEP 5 — FULL PIPELINE
# ─────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────
# SWAGELOK DECISION MATRIX ENGINE
# ─────────────────────────────────────────────────────────────────
def apply_decision_matrix(df):
    """
    Implements Swagelok's official Decision Matrix (from internal slide):

    Definitions:
      Quotation High  = inquiry qty in past 12 months > 100 pcs
      PO Received High = sales / inquiry conversion > 50% (past 12M)
      Stock High      = net stock / sales_12M > 50% (adequate cover)

    Rules:
      High Q + High PO + Low Stock  → BUY      (strong demand, need stock now)
      High Q + High PO + High Stock → MONITOR  (well stocked, watch)
      Low  Q + Low  PO + High Stock → HOLD     (weak demand, enough stock)
      High Q + Low  PO + Low Stock  → BUY*     (high interest, buy + review)
      Low  Q + Low  PO + Low Stock  → DROP     (discontinue or review)
      High Q + Low  PO + High Stock → REVIEW   (defer buying, monitor interest)
    """
    MONTHS_2026 = 4  # Jan-Apr complete as of May 2026

    df = df.copy()

    # Past 12 months = 2025 full + 2026 annualised
    df['Inq_12M']   = df['Inq_2025']   + df['Inq_2026']   * (12 / MONTHS_2026)
    df['Sales_12M'] = df['Sales_2025'] + df['Sales_2026'] * (12 / MONTHS_2026)

    # Quotation: High = > 100 pcs in past 12 months
    df['Q_High']   = df['Inq_12M'] >= 100

    # PO Received: High = conversion rate > 50%
    df['Conv_12M'] = df['Sales_12M'] / (df['Inq_12M'] + 1e-9)
    df['PO_High']  = df['Conv_12M'] >= 0.50

    # Stock Availability: High = net stock covers > 50% of 12M sales demand
    df['Stock_Ratio'] = df['NetAvailStock'] / (df['Sales_12M'] + 1e-9)
    df['Stock_High']  = df['Stock_Ratio'].clip(-1e9, 1e9) >= 0.50

    def _dm(row):
        Q = bool(row['Q_High'])
        P = bool(row['PO_High'])
        S = bool(row['Stock_High'])
        if     Q and     P and not S: return 'BUY',     '#28a745', 'Strong demand + confirmed orders + low stock → immediate replenishment required'
        if     Q and     P and     S: return 'MONITOR', '#007bff', 'Strong & steady demand with adequate stock → maintain watch, no immediate action'
        if not Q and not P and     S: return 'HOLD',    '#856404', 'Weak demand + sufficient stock → avoid replenishment, review with Sales & Costing'
        if     Q and not P and not S: return 'BUY',     '#fd7e14', 'High quotation + weak conversion + low stock → buy and review with Sales & Costing'
        if not Q and not P and not S: return 'DROP',    '#dc3545', 'No active demand or orders + low stock → discontinue or review with Sales'
        if     Q and not P and     S: return 'REVIEW',  '#6f42c1', 'High quotation + weak conversion + healthy stock → monitor interest, defer buying'
        return 'HOLD', '#856404', 'Insufficient data'

    dm_results = df.apply(_dm, axis=1, result_type='expand')
    dm_results.columns = ['DM_Action', 'DM_Color', 'DM_Reason']
    df = pd.concat([df, dm_results], axis=1)

    # Quotation/PO/Stock labels for display
    df['Q_Label']     = df['Q_High'].map({True: 'High', False: 'Low'})
    df['PO_Label']    = df['PO_High'].map({True: 'High', False: 'Low'})
    df['Stock_Label'] = df['Stock_High'].map({True: 'High', False: 'Low'})

    return df


@st.cache_data(show_spinner=False)
def run_full_analysis(file_bytes, filename):
    buf    = io.BytesIO(file_bytes)
    data   = parse_excel(buf)
    master = build_master(data)
    scored = run_algorithm(master)
    result = run_forecast(scored)
    # Apply Swagelok Decision Matrix
    result = apply_decision_matrix(result)
    # Apply learned correction factors if available
    if is_bootstrapped():
        corrections = get_correction_factors()
        result = apply_corrections(result, corrections)
    else:
        result["CorrectionFactor"] = 1.0
        result["CorrectedQty_6M"]  = result["ProposedQty_6M"]
        result["LearningApplied"]  = False
        result["CorrectedCostUSD"] = result["EstCostUSD"]
    return result


def compute_summary(df):
    return {
        "total": len(df),
        "buy":   int((df.Signal=="BUY").sum()),
        "watch": int((df.Signal=="WATCH").sum()),
        "hold":  int((df.Signal=="HOLD").sum()),
        "skip":  int((df.Signal=="SKIP").sum()),
        "dead":  int((df.ItemClass=="DEAD").sum()),
        "fast":  int((df.ItemClass=="FAST_MOVER").sum()),
        "slow":  int((df.ItemClass=="SLOW_MOVER").sum()),
        "project": int((df.ItemClass=="PROJECT").sum()),
        "stockout_risk": int(df.HasStockoutRisk.sum()),
        "proposed_qty":  int(df.ProposedQty_6M.sum()),
        "est_cost_usd":  round(float(df.EstCostUSD.sum()), 2),
        "annual": {
            yr: {
                "inq":   int(df[f"Inq_{yr}"].sum()),
                "sales": int(df[f"Sales_{yr}"].sum()),
                "purch": int(df[f"Purch_{yr}"].sum()),
            } for yr in YEARS
        },
    }


# ─────────────────────────────────────────────────────────────────
# STEP 6 — EXCEL EXPORT
# ─────────────────────────────────────────────────────────────────
def build_excel_export(df):
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        wb = writer.book

        # Formats
        hdr_fmt  = wb.add_format({"bold":True,"bg_color":"#1A1A2E","font_color":"#FFFFFF","border":1,"align":"center","valign":"vcenter","text_wrap":True})
        buy_fmt  = wb.add_format({"bold":True,"bg_color":"#d4edda","font_color":"#155724","border":1,"align":"center"})
        watch_fmt= wb.add_format({"bold":True,"bg_color":"#cce5ff","font_color":"#004085","border":1,"align":"center"})
        hold_fmt = wb.add_format({"bold":True,"bg_color":"#fff3cd","font_color":"#856404","border":1,"align":"center"})
        skip_fmt = wb.add_format({"bold":True,"bg_color":"#f8d7da","font_color":"#721c24","border":1,"align":"center"})
        cell_fmt = wb.add_format({"border":1,"align":"center","valign":"vcenter"})
        money_fmt= wb.add_format({"border":1,"align":"center","num_format":"$#,##0.00"})
        title_fmt= wb.add_format({"bold":True,"font_size":14,"font_color":"#1A1A2E"})
        red_fmt  = wb.add_format({"bold":True,"bg_color":"#ffe5e5","font_color":"#cc0000","border":1,"align":"center"})
        grn_fmt  = wb.add_format({"bold":True,"bg_color":"#d4edda","font_color":"#155724","border":1,"align":"center"})

        sig_fmts = {"BUY":buy_fmt,"WATCH":watch_fmt,"HOLD":hold_fmt,"SKIP":skip_fmt}

        def write_headers(ws, headers, row=0):
            for ci, h in enumerate(headers):
                ws.write(row, ci, h, hdr_fmt)

        def sig_fmt(s):
            return sig_fmts.get(s, cell_fmt)

        # ── Sheet 1: Executive Summary ──
        ws = wb.add_worksheet("Executive Summary")
        ws.write(0, 0, "🔩 SteelPulse — Procurement Intelligence Report", title_fmt)
        ws.write(1, 0, f"Generated: {datetime.now().strftime('%d %B %Y %H:%M')}")
        summary = compute_summary(df)
        kpi_data = [
            ("Total Items", summary["total"]),
            ("BUY Signals", summary["buy"]),
            ("WATCH Signals", summary["watch"]),
            ("Stockout Risk Items", summary["stockout_risk"]),
            ("Proposed Buy Qty (lengths)", summary["proposed_qty"]),
            ("Est. Total Cost (USD)", f"${summary['est_cost_usd']:,.0f}"),
        ]
        for ci, (label, val) in enumerate(kpi_data):
            ws.write(3, ci, label, hdr_fmt)
            ws.write(4, ci, val, cell_fmt)
            ws.set_column(ci, ci, 22)

        yr_hdrs = ["Year","Inquiries","Sales (lengths)","Purchases (lengths)","Conv. Rate %"]
        write_headers(ws, yr_hdrs, row=6)
        for ri, yr in enumerate(YEARS, 7):
            d = summary["annual"][yr]
            conv = round(d["sales"]/(d["inq"]+1)*100, 1) if d["inq"] > 0 else 0
            for ci, v in enumerate([yr, d["inq"], d["sales"], d["purch"], conv]):
                ws.write(ri, ci, v, cell_fmt)

        # ── Sheet 2: Buy List ──
        buy_df = df[df.Signal.isin(["BUY","WATCH"])].sort_values("Score", ascending=False)
        ws2 = wb.add_worksheet("Buy List")
        ws2.write(0, 0, "🛒 Procurement Buy List", title_fmt)
        hdrs2 = ["Item Code","Class","Signal","Score","S1 Vel","S2 Conv","S3 Cov","S4 SO",
                 "Net Stock","Open SO","Avail","Incoming PO","Proposed Qty (6M)",
                 "6M Demand Mid","Stockout Month","Lead Wks","Price/Len","Est Cost USD","Origin"]
        write_headers(ws2, hdrs2, row=1)
        ws2.freeze_panes(2, 0)
        ws2.autofilter(1, 0, 1, len(hdrs2)-1)
        cols2 = ["ItemCode","ItemClass","Signal","Score","S1_Velocity","S2_Conversion",
                 "S3_Coverage","S4_OpenSO","NetStock_Now","OpenSO","AvailStock","IncomingPO",
                 "ProposedQty_6M","F6M_Mid","StockoutMonth","LeadTimeWeeks",
                 "PricePerLength","EstCostUSD","Origin"]
        for ri, (_, row) in enumerate(buy_df.iterrows(), 2):
            for ci, col in enumerate(cols2):
                v = row.get(col, "")
                if isinstance(v, float) and math.isnan(v): v = 0
                fmt = sig_fmt(str(v)) if col == "Signal" else (money_fmt if col == "EstCostUSD" else cell_fmt)
                ws2.write(ri, ci, v, fmt)
        col2_widths = [24,14,9,8,8,8,8,8,11,10,10,13,16,14,13,10,12,14,12]
        for i, w in enumerate(col2_widths): ws2.set_column(i, i, w)

        # ── Sheet 3: 6-Month Forecast ──
        ws3 = wb.add_worksheet("6-Month Forecast")
        ws3.write(0, 0, "📅 6-Month Forward Demand Forecast (TWMAP Algorithm)", title_fmt)
        hdrs3 = ["Item Code","Class","Signal","Conf","Trend×","Base Monthly","Net Stock"]
        for m in MONTH_LABELS: hdrs3 += [f"{m} Low", f"{m} Mid", f"{m} High"]
        hdrs3 += ["6M Total Mid","Stock End (Mid)","Stockout Month","Proposed Buy","Est Cost USD"]
        write_headers(ws3, hdrs3, row=1)
        ws3.freeze_panes(2, 0)
        ws3.autofilter(1, 0, 1, len(hdrs3)-1)

        sort_df = df.copy()
        sort_df["_risk"] = sort_df.HasStockoutRisk.astype(int)
        sort_df = sort_df.sort_values(["_risk","ProposedQty_6M"], ascending=[False,False])

        for ri, (_, row) in enumerate(sort_df.iterrows(), 2):
            fixed = [row.ItemCode, str(row.ItemClass).replace("_"," "),
                     row.Signal, row.ForecastConf,
                     round(float(row.TrendMult),3), round(float(row.BaseMonthlySales),2),
                     round(float(row.NetStock_Now),1)]
            month_vals = []
            for m in MONTH_LABELS:
                month_vals += [round(float(row.get(f"Proj_Low_{m}",0)),2),
                               round(float(row.get(f"Proj_Mid_{m}",0)),2),
                               round(float(row.get(f"Proj_High_{m}",0)),2)]
            summary_vals = [round(float(row.F6M_Mid),1),
                            round(float(row.StockEnd_Mid),1),
                            str(row.StockoutMonth),
                            round(float(row.ProposedQty_6M),0),
                            round(float(row.EstCostUSD),2)]
            all_vals = fixed + month_vals + summary_vals
            for ci, v in enumerate(all_vals):
                if isinstance(v, float) and math.isnan(v): v = 0
                use_fmt = sig_fmt(str(v)) if ci == 2 else (
                    red_fmt if (ci == len(fixed)+len(month_vals)+2 and str(v) != "None") else (
                    grn_fmt if ci == len(all_vals)-2 and float(v or 0) > 0 else (
                    money_fmt if ci == len(all_vals)-1 else cell_fmt)))
                ws3.write(ri, ci, v, use_fmt)
        ws3.set_column(0, 0, 24)
        ws3.set_column(1, 6, 12)
        for i in range(7, 7+len(MONTH_LABELS)*3): ws3.set_column(i, i, 9)
        ws3.set_column(7+len(MONTH_LABELS)*3, 7+len(MONTH_LABELS)*3+4, 14)

        # ── Sheet 4: Full Analysis ──
        yr_cols = []
        for yr in YEARS: yr_cols += [f"Inq {yr}", f"Sales {yr}", f"Purch {yr}"]
        hdrs4 = ["Item Code","Class","Signal","Score","Total Inq","Total Sales",
                 "Total Purch","Avg Monthly Sales","QOH","Open SO","Avail Stock",
                 "Incoming PO","Net Avail","Cover Days","Lead Wks",
                 "Proposed Qty","Est Cost USD","Stockout Month"] + yr_cols
        df_out = df.sort_values("Score", ascending=False)
        df_out.to_excel(writer, sheet_name="Full Analysis", index=False,
                        startrow=1, header=False)
        ws4 = writer.sheets["Full Analysis"]
        ws4.write(0, 0, "📋 Full Item Analysis — All Items", title_fmt)
        for ci, h in enumerate(hdrs4): ws4.write(1, ci, h, hdr_fmt)

        # ── Sheet 5: Balance Sheet ──
        ws5 = wb.add_worksheet("Year Balance")
        ws5.write(0, 0, "📈 Year-wise Balance Sheet", title_fmt)
        write_headers(ws5, ["Year","Inquiries","Sales","Purchases","Conv Rate %","Net (Purch-Sales)"], row=1)
        for ri, yr in enumerate(YEARS, 2):
            d = summary["annual"][yr]
            conv = round(d["sales"]/(d["inq"]+1)*100,1) if d["inq"]>0 else 0
            net  = d["purch"] - d["sales"]
            for ci, v in enumerate([yr, d["inq"], d["sales"], d["purch"], conv, net]):
                f = grn_fmt if (ci == 5 and v >= 0) else (red_fmt if (ci == 5 and v < 0) else cell_fmt)
                ws5.write(ri, ci, v, f)
        for i, w in enumerate([10,14,14,16,14,18]): ws5.set_column(i, i, w)

    buf.seek(0)
    return buf


# ─────────────────────────────────────────────────────────────────
# STREAMLIT UI
# ─────────────────────────────────────────────────────────────────
def _badge(text, color):
    return f'<span style="background:{color};color:#fff;padding:2px 10px;border-radius:4px;font-size:12px;font-weight:700">{text}</span>'


def _kpi_card(label, value, color, icon=""):
    st.markdown(f"""
    <div style="background:#fff;border:1px solid #e0e0e0;border-radius:8px;
                padding:14px 18px;border-top:3px solid {color};
                box-shadow:0 1px 4px rgba(0,0,0,0.06)">
      <div style="font-size:10px;color:#888;letter-spacing:0.1em;text-transform:uppercase">{icon} {label}</div>
      <div style="font-size:28px;font-weight:800;color:{color};line-height:1.1">{value}</div>
    </div>""", unsafe_allow_html=True)


def _show_item_detail(row):
    sig_color = SIGNAL_COLORS.get(str(row.Signal), "#888")
    cls_color = CLASS_COLORS.get(str(row.ItemClass), "#888")
    conf_color = CONF_COLORS.get(str(row.ForecastConf), "#888")

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(f"""
        <div style="background:{sig_color}22;border:1px solid {sig_color};border-radius:8px;padding:12px;text-align:center">
          <div style="font-size:10px;color:#888;text-transform:uppercase">Signal</div>
          <div style="font-size:22px;font-weight:800;color:{sig_color}">{row.Signal}</div>
          <div style="font-size:11px;color:#888">Score: {row.Score:.1f}/100</div>
        </div>""", unsafe_allow_html=True)
    with c2:
        st.markdown(f"""
        <div style="background:{cls_color}22;border:1px solid {cls_color};border-radius:8px;padding:12px;text-align:center">
          <div style="font-size:10px;color:#888;text-transform:uppercase">Class</div>
          <div style="font-size:16px;font-weight:700;color:{cls_color}">{str(row.ItemClass).replace('_',' ')}</div>
        </div>""", unsafe_allow_html=True)
    with c3:
        proposed = int(row.ProposedQty_6M)
        st.markdown(f"""
        <div style="background:#d4edda;border:1px solid #28a745;border-radius:8px;padding:12px;text-align:center">
          <div style="font-size:10px;color:#888;text-transform:uppercase">Proposed Buy (6M)</div>
          <div style="font-size:22px;font-weight:800;color:#155724">{proposed:,} lengths</div>
          <div style="font-size:11px;color:#888">${row.EstCostUSD:,.0f} USD</div>
        </div>""", unsafe_allow_html=True)
    with c4:
        stkout = str(row.StockoutMonth)
        bg = "#ffe5e5" if stkout != "None" else "#d4edda"
        fg = "#cc0000" if stkout != "None" else "#155724"
        msg = f"Stockout: {stkout}" if stkout != "None" else "No Stockout Risk"
        st.markdown(f"""
        <div style="background:{bg};border-radius:8px;padding:12px;text-align:center">
          <div style="font-size:10px;color:#888;text-transform:uppercase">Stockout Risk</div>
          <div style="font-size:16px;font-weight:700;color:{fg}">{msg}</div>
          <div style="font-size:11px;color:#888">6M Demand: {row.F6M_Mid:.1f} lengths</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("<div style='margin:8px 0'></div>", unsafe_allow_html=True)

    col_l, col_r = st.columns(2)

    with col_l:
        st.markdown("**Algorithm Scores**")
        scores = {
            "Overall Score":         row.Score,
            "S1 Velocity (35%)":     row.S1_Velocity,
            "S2 Conversion (25%)":   row.S2_Conversion,
            "S3 Cov Coverage (25%)": row.S3_Coverage,
            "S4 Open SO (15%)":      row.S4_OpenSO,
        }
        for label, val in scores.items():
            val = float(val) if pd.notna(val) else 0
            bar_color = "#28a745" if val>=60 else "#007bff" if val>=40 else "#ffc107" if val>=20 else "#dc3545"
            st.markdown(f"""
            <div style="margin-bottom:8px">
              <div style="display:flex;justify-content:space-between;font-size:12px;margin-bottom:2px">
                <span style="color:#555">{label}</span>
                <span style="font-weight:700;color:{bar_color}">{val:.1f}</span>
              </div>
              <div style="background:#e9ecef;border-radius:4px;height:8px;overflow:hidden">
                <div style="width:{min(100,val)}%;background:{bar_color};height:100%;border-radius:4px"></div>
              </div>
            </div>""", unsafe_allow_html=True)

        st.markdown("**Forecast Parameters**")
        params = {
            "Base Monthly Sales":    f"{row.BaseMonthlySales:.2f} lengths/month",
            "Trend Multiplier":      f"{row.TrendMult:.3f}× (YoY growth)",
            "Inquiry Boost":         f"{row.InqBoost:.3f}× (demand signal)",
            "Lead Time":             f"{int(row.LeadTimeWeeks)} weeks",
            "Net Stock Now":         f"{row.NetStock_Now:.0f} lengths",
            "Safety Buffer":         f"{row.SafetyBuffer:.1f} lengths",
            "Forecast Confidence":   str(row.ForecastConf),
        }
        param_df = pd.DataFrame(params.items(), columns=["Parameter","Value"])
        st.dataframe(param_df.set_index("Parameter"), use_container_width=True)

    with col_r:
        # 6M forecast chart
        chart_data = pd.DataFrame({
            "Month": MONTH_LABELS,
            "Low":  [float(row.get(f"Proj_Low_{m}",0)) for m in MONTH_LABELS],
            "Mid":  [float(row.get(f"Proj_Mid_{m}",0)) for m in MONTH_LABELS],
            "High": [float(row.get(f"Proj_High_{m}",0)) for m in MONTH_LABELS],
        })
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=chart_data.Month, y=chart_data.High,
                                 fill=None, mode="lines", line_color="#28a745",
                                 line_dash="dash", name="High (+30%)"))
        fig.add_trace(go.Scatter(x=chart_data.Month, y=chart_data.Low,
                                 fill="tonexty", mode="lines", line_color="#dc3545",
                                 line_dash="dash", name="Low (−25%)",
                                 fillcolor="rgba(173,216,230,0.25)"))
        fig.add_trace(go.Scatter(x=chart_data.Month, y=chart_data.Mid,
                                 mode="lines+markers", line=dict(color="#007bff",width=3),
                                 marker=dict(size=8), name="Mid forecast"))
        fig.update_layout(title=f"6-Month Forecast — {row.ItemCode}",
                          height=260, plot_bgcolor="#fafafa",
                          legend=dict(orientation="h",yanchor="bottom",y=1.02))
        st.plotly_chart(fig, use_container_width=True)

        # History chart
        hist = pd.DataFrame({
            "Year": [str(y) for y in YEARS],
            "Inquiry": [float(row.get(f"Inq_{y}",0)) for y in YEARS],
            "Sales":   [float(row.get(f"Sales_{y}",0)) for y in YEARS],
            "Purchase":[float(row.get(f"Purch_{y}",0)) for y in YEARS],
        })
        fig2 = px.bar(hist, x="Year", y=["Inquiry","Sales","Purchase"], barmode="group",
                      color_discrete_map={"Inquiry":"#007bff","Sales":"#28a745","Purchase":"#f0a500"},
                      title="6-Year History")
        fig2.update_layout(height=240, plot_bgcolor="#fafafa",
                           legend=dict(orientation="h",yanchor="bottom",y=1.02))
        st.plotly_chart(fig2, use_container_width=True)


def _show_forecast_chart(row):
    chart_data = pd.DataFrame({
        "Month": MONTH_LABELS,
        "Low":   [float(row.get(f"Proj_Low_{m}",0))  for m in MONTH_LABELS],
        "Mid":   [float(row.get(f"Proj_Mid_{m}",0))  for m in MONTH_LABELS],
        "High":  [float(row.get(f"Proj_High_{m}",0)) for m in MONTH_LABELS],
    })
    fig = go.Figure()
    fig.add_trace(go.Bar(x=chart_data.Month, y=chart_data.Mid,
                         name="Mid forecast", marker_color="#007bff", opacity=0.85))
    fig.add_trace(go.Scatter(x=chart_data.Month, y=chart_data.High,
                             mode="lines+markers", line=dict(color="#28a745",dash="dash",width=2),
                             name="High (+30%)"))
    fig.add_trace(go.Scatter(x=chart_data.Month, y=chart_data.Low,
                             mode="lines+markers", line=dict(color="#dc3545",dash="dash",width=2),
                             name="Low (−25%)"))
    fig.update_layout(
        title=f"6-Month Demand Forecast — {row.ItemCode}  |  Trend: {row.TrendMult:.2f}×  |  Confidence: {row.ForecastConf}",
        height=320, plot_bgcolor="#fafafa",
        legend=dict(orientation="h",yanchor="bottom",y=1.02)
    )
    st.plotly_chart(fig, use_container_width=True)

    c1,c2,c3,c4 = st.columns(4)
    with c1: st.metric("6M Demand (Mid)",  f"{row.F6M_Mid:.0f} lengths")
    with c2: st.metric("Stock End (Mid)",   f"{row.StockEnd_Mid:.0f}", delta=None)
    with c3: st.metric("Proposed Buy",      f"{int(row.ProposedQty_6M):,} lengths")
    with c4: st.metric("Stockout Risk",     str(row.StockoutMonth) if row.HasStockoutRisk else "✅ None")


if __name__ == "__main__":
    main()


# ─────────────────────────────────────────────────────────────────
# LEARNING DASHBOARD
# ─────────────────────────────────────────────────────────────────
def _show_learning_dashboard(result_df):
    st.markdown("#### 🧠 Learning Dashboard — How the Model is Improving")
    st.caption("The model learns from every upload. Correction factors adjust forecasts based on past accuracy.")

    stats = get_learning_stats()

    if stats.get("total_items_learned", 0) == 0:
        st.info("🔄 Upload your first file to initialise the learning model.")
        return

    # ── KPI row ──
    c1,c2,c3,c4,c5 = st.columns(5)
    with c1:
        st.metric("Items Learned", stats["total_items_learned"])
    with c2:
        st.metric("✅ Accurate (CF 0.8–1.2×)",
                  stats.get("items_good_cf", 0),
                  help="Items where model prediction was within 20%")
    with c3:
        st.metric("📉 Over-predicted",
                  stats.get("items_over", 0),
                  help="We forecasted too high — correction factor < 0.8×")
    with c4:
        st.metric("📈 Under-predicted",
                  stats.get("items_under", 0),
                  help="We forecasted too low — correction factor > 1.2×")
    with c5:
        st.metric("Median Forecast Error",
                  f"{stats.get('median_error', 0):.0f}%",
                  help="Median absolute % error on 2025 validation")

    st.markdown("---")

    col_l, col_r = st.columns(2)

    with col_l:
        # Correction factor distribution bar chart
        st.markdown("**📊 Correction Factor Distribution**")
        st.caption("How far off the model was per item category")
        cf_dist = stats.get("cf_distribution", {})
        if cf_dist:
            cf_df = pd.DataFrame({
                "Category":  list(cf_dist.keys()),
                "Items":     list(cf_dist.values()),
            })
            colors = ["#dc3545","#fd7e14","#28a745","#007bff","#6f42c1"]
            fig = go.Figure(go.Bar(
                x=cf_df["Items"], y=cf_df["Category"],
                orientation="h",
                marker_color=colors,
                text=cf_df["Items"],
                textposition="outside"
            ))
            fig.update_layout(height=260, plot_bgcolor="#fafafa",
                              xaxis_title="Number of items",
                              margin=dict(l=0, r=40, t=10, b=10))
            st.plotly_chart(fig, use_container_width=True)

            st.markdown("""
            <div style="background:#f8f9fa;border-radius:6px;padding:12px;font-size:12px;line-height:1.8">
            <b>What correction factor means:</b><br>
            🟢 <b>CF ≈ 1.0</b> — Model was accurate, no adjustment needed<br>
            🔴 <b>CF &lt; 0.8</b> — Model over-predicted. Future forecasts reduced<br>
            🔵 <b>CF &gt; 1.2</b> — Model under-predicted. Future forecasts boosted<br>
            📐 Formula: <code>Corrected Qty = Raw Forecast × CF</code>
            </div>
            """, unsafe_allow_html=True)

    with col_r:
        # Model performance over time
        perf_df = stats.get("perf_df", pd.DataFrame())
        if not perf_df.empty and len(perf_df) > 1:
            st.markdown("**📈 Model Accuracy Over Time**")
            fig2 = go.Figure()
            fig2.add_trace(go.Scatter(
                x=perf_df["upload_date"], y=perf_df["median_error_pct"],
                name="Median Error %", mode="lines+markers",
                line=dict(color="#007bff", width=2), marker=dict(size=8)
            ))
            fig2.add_trace(go.Scatter(
                x=perf_df["upload_date"], y=perf_df["items_within_50pct"],
                name="Items within 50% error", mode="lines+markers",
                line=dict(color="#28a745", width=2, dash="dash"),
                marker=dict(size=8), yaxis="y2"
            ))
            fig2.update_layout(
                height=260, plot_bgcolor="#fafafa",
                yaxis=dict(title="Error %"),
                yaxis2=dict(title="Items count", overlaying="y", side="right"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
                margin=dict(l=0, r=0, t=10, b=10)
            )
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.markdown("**📈 Model Accuracy**")
            st.markdown("""
            <div style="background:#f0f7ff;border:1px solid #007bff;border-radius:8px;padding:20px;text-align:center">
            <div style="font-size:32px;margin-bottom:8px">📅</div>
            <div style="font-weight:700;color:#004085">Accuracy chart builds over time</div>
            <div style="color:#888;font-size:12px;margin-top:4px">
            Upload a new file next month to see how accuracy improves
            </div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("---")

    # ── Validation results — 2025 forecast vs actual ──
    st.markdown("#### 🔍 2025 Validation — Predicted vs Actual")
    st.caption("Trained on 2021–2024 data → predicted 2025 → compared with actual 2025 sales")

    items_df = stats.get("items_df", pd.DataFrame())
    if not items_df.empty:
        # Show with filters
        fc1, fc2 = st.columns([3,1])
        with fc2:
            cf_filter = st.selectbox(
                "Filter by accuracy:",
                ["All", "✅ Accurate (CF 0.8–1.2×)",
                 "📉 Over-predicted (CF < 0.8×)",
                 "📈 Under-predicted (CF > 1.2×)"],
                key="cf_filter"
            )
        with fc1:
            item_search = st.text_input("Search item:", placeholder="SS-T8...", key="learn_search")

        disp = items_df.copy()
        if cf_filter == "✅ Accurate (CF 0.8–1.2×)":
            disp = disp[(disp.correction_factor >= 0.8) & (disp.correction_factor <= 1.2)]
        elif cf_filter == "📉 Over-predicted (CF < 0.8×)":
            disp = disp[disp.correction_factor < 0.8]
        elif cf_filter == "📈 Under-predicted (CF > 1.2×)":
            disp = disp[disp.correction_factor > 1.2]
        if item_search:
            disp = disp[disp.item_code.str.contains(item_search, case=False, na=False)]

        disp_show = disp[[
            "item_code","item_class","pred_2025","actual_2025",
            "error_pct_2025","correction_factor","months_tracked"
        ]].copy()
        disp_show.columns = [
            "Item Code","Class","Predicted 2025","Actual 2025",
            "Error %","Correction Factor","Months Tracked"
        ]
        disp_show["Predicted 2025"]    = disp_show["Predicted 2025"].round(0)
        disp_show["Actual 2025"]       = disp_show["Actual 2025"].round(0)
        disp_show["Error %"]           = disp_show["Error %"].round(1)
        disp_show["Correction Factor"] = disp_show["Correction Factor"].round(3)
        disp_show = disp_show.sort_values("Error %", ascending=False)

        st.dataframe(disp_show.set_index("Item Code"),
                     use_container_width=True, height=380)

        st.caption(f"Showing {len(disp_show)} items · CF = Actual ÷ Predicted (applied to all future forecasts)")

    st.markdown("---")

    # ── Side-by-side: raw vs corrected qty for BUY items ──
    st.markdown("#### ⚖️ Raw Forecast vs Learning-Corrected Forecast — BUY Items")
    st.caption("Green = corrected qty after applying learned correction factor")

    buy_df = result_df[result_df.Signal == "BUY"].copy()
    if not buy_df.empty and "CorrectedQty_6M" in buy_df.columns:
        comp = buy_df[["ItemCode","ProposedQty_6M","CorrectedQty_6M",
                        "CorrectionFactor","F6M_Mid"]].copy()
        comp.columns = ["Item","Raw Qty (algorithm)","Corrected Qty (learned)",
                         "CF Applied","6M Demand"]
        comp = comp.sort_values("Corrected Qty (learned)", ascending=False)

        fig3 = go.Figure()
        fig3.add_trace(go.Bar(
            name="Raw Forecast", x=comp["Item"], y=comp["Raw Qty (algorithm)"],
            marker_color="#007bff", opacity=0.6
        ))
        fig3.add_trace(go.Bar(
            name="Corrected (Learned)", x=comp["Item"], y=comp["Corrected Qty (learned)"],
            marker_color="#28a745", opacity=0.9
        ))
        fig3.update_layout(
            barmode="group", height=320, plot_bgcolor="#fafafa",
            xaxis_tickangle=-45,
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            margin=dict(b=80)
        )
        st.plotly_chart(fig3, use_container_width=True)
        st.dataframe(comp.set_index("Item").round(1), use_container_width=True)

    st.markdown("---")

    # ── Upload history ──
    st.markdown("#### 📂 Upload History")
    uploads_df = stats.get("uploads_df", pd.DataFrame())
    if not uploads_df.empty:
        disp_up = uploads_df[[
            "filename","uploaded_at","items_processed",
            "buy_signals","stockout_risk","total_proposed_qty"
        ]].copy()
        disp_up.columns = ["File","Uploaded At","Items","BUY Signals","Stockout Risk","Proposed Qty"]
        st.dataframe(disp_up.set_index("File"), use_container_width=True)
    else:
        st.info("No uploads recorded yet.")

    # ── Item-level drill down ──
    st.markdown("---")
    st.markdown("#### 🔎 Item Learning Detail")
    if not items_df.empty:
        selected_item = st.selectbox(
            "Select item to inspect learning:",
            items_df["item_code"].tolist(),
            key="learn_item_sel"
        )
        if selected_item:
            detail = get_item_learning_detail(selected_item)
            if detail:
                d1,d2,d3,d4 = st.columns(4)
                cf = detail.get("correction_factor", 1.0)
                cf_color = "#28a745" if 0.8<=cf<=1.2 else ("#dc3545" if cf<0.8 else "#007bff")
                with d1:
                    st.markdown(f"""
                    <div style="background:{cf_color}22;border:2px solid {cf_color};
                         border-radius:8px;padding:14px;text-align:center">
                      <div style="font-size:10px;color:#888;text-transform:uppercase">Correction Factor</div>
                      <div style="font-size:28px;font-weight:800;color:{cf_color}">{cf:.3f}×</div>
                      <div style="font-size:11px;color:#888">
                        {'Accurate' if 0.8<=cf<=1.2 else ('Over-predicted' if cf<0.8 else 'Under-predicted')}
                      </div>
                    </div>""", unsafe_allow_html=True)
                with d2:
                    st.metric("Predicted 2025", f"{detail.get('pred_2025',0):.0f} lengths")
                with d3:
                    st.metric("Actual 2025",    f"{detail.get('actual_2025',0):.0f} lengths")
                with d4:
                    st.metric("Error %",        f"{detail.get('error_pct_2025',0):.1f}%")

                st.markdown(f"""
                <div style="background:#f8f9fa;border-radius:6px;padding:12px;font-size:12px;margin-top:8px">
                <b>What this means:</b><br>
                Our algorithm predicted <b>{detail.get('pred_2025',0):.0f} lengths</b> for 2025,
                but actual sales were <b>{detail.get('actual_2025',0):.0f} lengths</b>.<br>
                The model was {'over-predicting by ' if cf<1 else 'under-predicting by '}
                <b>{abs(1-cf)*100:.0f}%</b>.<br>
                Future forecasts for <b>{selected_item}</b> are now
                {'reduced' if cf<1 else 'boosted'} by <b>{cf:.2f}×</b> automatically.
                </div>
                """, unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────
# PROCUREMENT BOARD
# ─────────────────────────────────────────────────────────────────
def _procurement_board_filter(df):
    """
    Uses Swagelok Decision Matrix as primary filter.
    Shows BUY + REVIEW items sorted by 12-month inquiry volume.
    Also adds legacy conditions as additional context columns.
    """
    YEARS = [2021, 2022, 2023, 2024, 2025, 2026]
    df = df.copy()
    df['_inq_years_100plus'] = (df[[f'Inq_{y}' for y in YEARS]] >= 100).sum(axis=1)
    df['_total_inq']         = df[[f'Inq_{y}' for y in YEARS]].sum(axis=1)
    df['_sales_years_active']= (df[[f'Sales_{y}' for y in YEARS]] > 0).sum(axis=1)

    # Primary: use Decision Matrix
    if 'DM_Action' in df.columns:
        board = df[df['DM_Action'].isin(['BUY','MONITOR','REVIEW'])].copy()
    else:
        # Fallback to original conditions
        cond1 = df['_inq_years_100plus'] >= 3
        cond2 = df['_total_inq']         >= 100
        cond3 = df['NetAvailStock']       < 500
        cond4 = df['_sales_years_active'] >= 3
        board = df[cond1 & cond2 & cond3 & cond4].copy()

    board = board.sort_values('Inq_12M' if 'Inq_12M' in board.columns else '_total_inq',
                              ascending=False)
    return board


def _show_procurement_board(df):
    YEARS = [2021, 2022, 2023, 2024, 2025, 2026]

    st.markdown("#### 🎯 Procurement Board — Swagelok Decision Matrix")

    # Matrix legend
    st.markdown("""
    <div style="background:#1A1A2E;border-radius:8px;padding:16px 20px;margin-bottom:16px">
      <div style="color:#f0a500;font-weight:700;font-size:13px;margin-bottom:10px">
        📋 Swagelok Decision Matrix — Applied to Past 12 Months Data
      </div>
      <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:8px;font-size:11px;color:#ccc;margin-bottom:10px">
        <div>📊 <b>Quotation:</b> High = inquiry &gt; 100 pcs (12M)</div>
        <div>📦 <b>PO Received:</b> High = conversion &gt; 50%</div>
        <div>🏭 <b>Stock:</b> High = net stock &gt; 50% of 12M sales</div>
      </div>
      <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:6px;font-size:11px">
        <div style="background:#28a74522;border:1px solid #28a745;border-radius:4px;padding:6px">
          🟢 <b>BUY</b>: High Q + High PO + Low Stock<br>
          <span style="color:#aaa">Immediate replenishment needed</span>
        </div>
        <div style="background:#28a74522;border:1px solid #fd7e14;border-radius:4px;padding:6px">
          🟢 <b>BUY*</b>: High Q + Low PO + Low Stock<br>
          <span style="color:#aaa">Buy + review with Sales & Costing</span>
        </div>
        <div style="background:#007bff22;border:1px solid #007bff;border-radius:4px;padding:6px">
          🔵 <b>MONITOR</b>: High Q + High PO + High Stock<br>
          <span style="color:#aaa">Watch only, no action</span>
        </div>
        <div style="background:#85640422;border:1px solid #856404;border-radius:4px;padding:6px">
          🟡 <b>HOLD</b>: Low Q + Low PO + High Stock<br>
          <span style="color:#aaa">Avoid buying, manage existing</span>
        </div>
        <div style="background:#6f42c122;border:1px solid #6f42c1;border-radius:4px;padding:6px">
          👁️ <b>REVIEW</b>: High Q + Low PO + High Stock<br>
          <span style="color:#aaa">Defer buying, monitor interest</span>
        </div>
        <div style="background:#dc354522;border:1px solid #dc3545;border-radius:4px;padding:6px">
          ⛔ <b>DROP</b>: Low Q + Low PO + Low Stock<br>
          <span style="color:#aaa">Discontinue or review with Sales</span>
        </div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    board = _procurement_board_filter(df)

    # KPIs
    buy_items  = board[board.DM_Action=='BUY']   if 'DM_Action' in board.columns else board
    mon_items  = board[board.DM_Action=='MONITOR'] if 'DM_Action' in board.columns else pd.DataFrame()
    rev_items  = board[board.DM_Action=='REVIEW']  if 'DM_Action' in board.columns else pd.DataFrame()

    k1,k2,k3,k4,k5 = st.columns(5)
    with k1: st.metric("🟢 BUY",     len(buy_items),  help="Needs immediate procurement")
    with k2: st.metric("🔵 MONITOR", len(mon_items),   help="Watch only")
    with k3: st.metric("👁️ REVIEW",  len(rev_items),   help="High interest, defer buying")
    with k4: st.metric("Total in View", len(board))
    with k5:
        qty = int(buy_items.ProposedQty_6M.sum()) if len(buy_items) > 0 else 0
        st.metric("Proposed Buy Qty", f"{qty:,} lengths")

    st.markdown("<div style='margin:8px 0'></div>", unsafe_allow_html=True)

    # Filter tabs
    view_filter = st.radio(
        "Show items:",
        ["🟢 BUY (action required)", "👁️ REVIEW (monitor)", "🔵 MONITOR", "All"],
        horizontal=True, key="dm_filter"
    )
    if "BUY" in view_filter:
        show_df = buy_items
    elif "REVIEW" in view_filter:
        show_df = rev_items
    elif "MONITOR" in view_filter:
        show_df = mon_items
    else:
        show_df = board

    if show_df.empty:
        st.info("No items in this category.")
    else:
        # Build display table
        disp_cols = {
            'ItemCode':      'Item Code',
            'DM_Action':     'Decision',
            'Q_Label':       'Quotation',
            'PO_Label':      'PO Received',
            'Stock_Label':   'Stock Avail.',
            'Inq_12M':       'Inquiry 12M',
            'Conv_12M':      'Conv. Rate',
            'Sales_12M':     'Sales 12M',
            'NetAvailStock': 'Net Stock',
            'ProposedQty_6M':'Proposed Buy',
            'F6M_Mid':       '6M Demand',
            'StockoutMonth': 'Stockout In',
            'DM_Reason':     'Interpretation',
        }
        avail = [c for c in disp_cols if c in show_df.columns]
        disp = show_df[avail].rename(columns=disp_cols).copy()
        if 'Inquiry 12M' in disp.columns: disp['Inquiry 12M']  = disp['Inquiry 12M'].round(0).astype(int)
        if 'Sales 12M'   in disp.columns: disp['Sales 12M']    = disp['Sales 12M'].round(0).astype(int)
        if 'Conv. Rate'  in disp.columns: disp['Conv. Rate']   = (disp['Conv. Rate']*100).round(1).astype(str) + '%'
        if 'Net Stock'   in disp.columns: disp['Net Stock']    = disp['Net Stock'].round(0).astype(int)
        if 'Proposed Buy' in disp.columns: disp['Proposed Buy']= disp['Proposed Buy'].round(0).astype(int)
        if '6M Demand'   in disp.columns: disp['6M Demand']    = disp['6M Demand'].round(1)

        def _color_dm(row):
            action = row.get('Decision','')
            if action == 'BUY':     return ['background-color:#d4edda'] * len(row)
            if action == 'MONITOR': return ['background-color:#cce5ff'] * len(row)
            if action == 'REVIEW':  return ['background-color:#ede7f6'] * len(row)
            if action == 'HOLD':    return ['background-color:#fff3cd'] * len(row)
            return [''] * len(row)

        st.dataframe(
            disp.set_index('Item Code').style.apply(_color_dm, axis=1),
            use_container_width=True, height=440
        )
        st.caption(f"Showing {len(show_df)} items · Based on past 12 months data (2025 + annualised 2026)")

    # ── Item detail ──
    st.markdown("---")
    st.markdown("#### 🔍 Item Detail — Click to Inspect")

    all_items = board['ItemCode'].tolist() if not board.empty else []
    if all_items:
        selected = st.selectbox("Select item:", all_items, key="board_item_select")
        if selected:
            irow = df[df.ItemCode == selected].iloc[0]
            _show_board_item_detail(irow, YEARS)


def _show_board_item_detail(row, YEARS):
    """Full detail panel for a procurement board item."""
    item = row['ItemCode']
    sig_color = SIGNAL_COLORS.get(str(row.Signal), "#888")

    # ── Header cards ──
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.markdown(f"""
        <div style="background:{sig_color}22;border:2px solid {sig_color};border-radius:8px;
             padding:12px;text-align:center">
          <div style="font-size:9px;color:#888;text-transform:uppercase;margin-bottom:2px">Signal</div>
          <div style="font-size:22px;font-weight:800;color:{sig_color}">{row.Signal}</div>
          <div style="font-size:11px;color:#888">Score: {row.Score:.0f}/100</div>
        </div>""", unsafe_allow_html=True)
    with c2:
        qoh = int(row.get('QOH', 0))
        color = '#cc0000' if qoh == 0 else '#155724'
        st.markdown(f"""
        <div style="background:#f8f9fa;border:1px solid #ddd;border-radius:8px;padding:12px;text-align:center">
          <div style="font-size:9px;color:#888;text-transform:uppercase;margin-bottom:2px">Stock On Hand</div>
          <div style="font-size:22px;font-weight:800;color:{color}">{qoh:,}</div>
          <div style="font-size:11px;color:#888">lengths</div>
        </div>""", unsafe_allow_html=True)
    with c3:
        net = int(row.get('NetAvailStock', 0))
        color = '#cc0000' if net <= 0 else ('#856404' if net < 100 else '#155724')
        st.markdown(f"""
        <div style="background:#f8f9fa;border:1px solid #ddd;border-radius:8px;padding:12px;text-align:center">
          <div style="font-size:9px;color:#888;text-transform:uppercase;margin-bottom:2px">Net Available</div>
          <div style="font-size:22px;font-weight:800;color:{color}">{net:,}</div>
          <div style="font-size:11px;color:#888">QOH + PO − Open SO</div>
        </div>""", unsafe_allow_html=True)
    with c4:
        proposed = int(row.get('ProposedQty_6M', 0))
        st.markdown(f"""
        <div style="background:#d4edda;border:1px solid #28a745;border-radius:8px;padding:12px;text-align:center">
          <div style="font-size:9px;color:#888;text-transform:uppercase;margin-bottom:2px">Proposed Buy</div>
          <div style="font-size:22px;font-weight:800;color:#155724">{proposed:,}</div>
          <div style="font-size:11px;color:#888">lengths (6M cover)</div>
        </div>""", unsafe_allow_html=True)
    with c5:
        stkout = str(row.get('StockoutMonth', 'None'))
        bg = '#ffe5e5' if stkout != 'None' else '#d4edda'
        fg = '#cc0000' if stkout != 'None' else '#155724'
        msg = stkout if stkout != 'None' else '✅ Covered'
        st.markdown(f"""
        <div style="background:{bg};border-radius:8px;padding:12px;text-align:center">
          <div style="font-size:9px;color:#888;text-transform:uppercase;margin-bottom:2px">Stockout Risk</div>
          <div style="font-size:18px;font-weight:800;color:{fg}">{msg}</div>
          <div style="font-size:11px;color:#888">6M projection</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("<div style='margin:12px 0'></div>", unsafe_allow_html=True)

    # ── Two charts side by side ──
    col_left, col_right = st.columns(2)

    with col_left:
        # Inquiry trend chart — BAR per year
        st.markdown(f"**📊 Inquiry Trend — {item}**")
        st.caption("How many customer inquiries received each year")

        inq_data = pd.DataFrame({
            'Year': [str(y) for y in YEARS],
            'Inquiries': [float(row.get(f'Inq_{y}', 0)) for y in YEARS],
        })
        fig_inq = go.Figure()
        colors_inq = [
            '#28a745' if v >= 100 else '#ffc107' if v > 0 else '#e9ecef'
            for v in inq_data['Inquiries']
        ]
        fig_inq.add_trace(go.Bar(
            x=inq_data['Year'],
            y=inq_data['Inquiries'],
            marker_color=colors_inq,
            text=inq_data['Inquiries'].apply(lambda x: f'{x:,.0f}'),
            textposition='outside',
            name='Inquiries'
        ))
        fig_inq.add_hline(
            y=100, line_dash='dash', line_color='#dc3545',
            annotation_text='100 threshold', annotation_position='top right'
        )
        fig_inq.update_layout(
            height=300, plot_bgcolor='#fafafa',
            yaxis_title='Inquiries (lengths)',
            showlegend=False,
            margin=dict(t=20, b=20)
        )
        st.plotly_chart(fig_inq, use_container_width=True)

    with col_right:
        # Sales trend chart — LINE
        st.markdown(f"**📈 Sales Trend — {item}**")
        st.caption("Confirmed sales / consumption each year")

        sales_data = pd.DataFrame({
            'Year': [str(y) for y in YEARS],
            'Sales': [float(row.get(f'Sales_{y}', 0)) for y in YEARS],
            'Purchase': [float(row.get(f'Purch_{y}', 0)) for y in YEARS],
        })
        fig_sales = go.Figure()
        fig_sales.add_trace(go.Bar(
            x=sales_data['Year'],
            y=sales_data['Purchase'],
            name='Purchased from Vendor',
            marker_color='#f0a500',
            opacity=0.7
        ))
        fig_sales.add_trace(go.Scatter(
            x=sales_data['Year'],
            y=sales_data['Sales'],
            name='Sold to Customer',
            mode='lines+markers',
            line=dict(color='#28a745', width=3),
            marker=dict(size=9, color='#28a745')
        ))
        fig_sales.update_layout(
            height=300, plot_bgcolor='#fafafa',
            yaxis_title='Lengths',
            legend=dict(orientation='h', yanchor='bottom', y=1.02),
            margin=dict(t=20, b=20)
        )
        st.plotly_chart(fig_sales, use_container_width=True)

    # ── Year by year table ──
    st.markdown(f"**📋 Full Year-by-Year History — {item}**")

    yr_rows = []
    for y in YEARS:
        inq  = float(row.get(f'Inq_{y}',   0))
        sal  = float(row.get(f'Sales_{y}',  0))
        pur  = float(row.get(f'Purch_{y}',  0))
        conv = round(sal / inq * 100, 1) if inq > 0 else 0
        yr_rows.append({
            'Year':              y,
            'Inquiries':         int(inq),
            'Sales (consumed)':  int(sal),
            'Purchased':         int(pur),
            'Conversion %':      conv,
            'Inq ≥ 100?':       '✅ Yes' if inq >= 100 else '❌ No',
            'Has Sales?':        '✅ Yes' if sal > 0 else '—',
        })

    yr_df = pd.DataFrame(yr_rows).set_index('Year')

    def _color_yr(col):
        if col.name == 'Inquiries':
            return ['background-color:#d4edda' if v >= 100
                    else 'background-color:#fff3cd' if v > 0
                    else '' for v in col]
        if col.name == 'Sales (consumed)':
            return ['background-color:#d4edda' if v > 0 else '' for v in col]
        return ['' for _ in col]

    st.dataframe(
        yr_df.style.apply(_color_yr, axis=0),
        use_container_width=True
    )

    # ── Stock breakdown ──
    st.markdown(f"**📦 Current Stock Breakdown — {item}**")
    s1, s2, s3, s4, s5 = st.columns(5)
    with s1: st.metric("QOH (physical)",   int(row.get('QOH', 0)))
    with s2: st.metric("Open SO (reserved)", int(row.get('OpenSO', 0)))
    with s3: st.metric("Available Stock",  int(row.get('AvailStock', 0)))
    with s4: st.metric("Incoming PO",      int(row.get('IncomingPO', 0)))
    with s5:
        net = int(row.get('NetAvailStock', 0))
        delta_color = "normal" if net >= 0 else "inverse"
        st.metric("Net Available", net,
                  delta=f"{'⚠️ Below 500' if 0 < net < 500 else ('🔴 Zero/Negative' if net <= 0 else '✅ OK')}",
                  delta_color=delta_color)

    # ── Decision box ──
    net_stock = float(row.get('NetAvailStock', 0))
    total_inq = float(row.get('TotalInquiry', 0))
    sales_yrs = int((pd.Series([row.get(f'Sales_{y}', 0) for y in YEARS]) > 0).sum())
    inq_yrs_100 = int((pd.Series([row.get(f'Inq_{y}', 0) for y in YEARS]) >= 100).sum())

    cond1_ok = inq_yrs_100 >= 3
    cond2_ok = total_inq >= 100
    cond3_ok = net_stock < 500
    cond4_ok = sales_yrs >= 3

    all_ok = cond1_ok and cond2_ok and cond3_ok and cond4_ok

    bg   = '#d4edda' if all_ok else '#fff3cd'
    fg   = '#155724' if all_ok else '#856404'
    icon = '🟢' if all_ok else '🟡'
    decision = 'PROCURE FROM VENDOR' if all_ok else 'REVIEW MANUALLY'

    st.markdown(f"""
    <div style="background:{bg};border-radius:10px;padding:18px 22px;margin-top:16px;
         border:2px solid {fg}">
      <div style="font-size:18px;font-weight:800;color:{fg};margin-bottom:12px">
        {icon} DECISION: {decision}
      </div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;font-size:13px">
        <div>{'✅' if cond1_ok else '❌'} <b>Condition 1:</b> Inquiry ≥ 100 in {inq_yrs_100}/6 years
             {'(need 3+)' if not cond1_ok else '(met ✓)'}</div>
        <div>{'✅' if cond2_ok else '❌'} <b>Condition 2:</b> Total inquiry = {int(total_inq):,}
             {'(need ≥100)' if not cond2_ok else '(met ✓)'}</div>
        <div>{'✅' if cond3_ok else '❌'} <b>Condition 3:</b> Net stock = {int(net_stock):,}
             {'(need <500)' if not cond3_ok else '(met ✓)'}</div>
        <div>{'✅' if cond4_ok else '❌'} <b>Condition 4:</b> Sales in {sales_yrs}/6 years
             {'(need 3+)' if not cond4_ok else '(met ✓)'}</div>
      </div>
      {f'<div style="margin-top:12px;font-size:13px;font-weight:700;color:{fg}">Proposed quantity to buy: {int(row.get("ProposedQty_6M",0)):,} lengths — covers next 6 months demand + safety buffer</div>' if all_ok else ''}
    </div>
    """, unsafe_allow_html=True)


def main():
    init_db()   # ensure SQLite tables exist
    st.set_page_config(
        page_title="SteelPulse — Procurement Intelligence",
        page_icon="🔩",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # ── Global CSS ──
    st.markdown("""
    <style>
    #MainMenu,footer {visibility:hidden}
    .block-container {padding-top:1rem;padding-bottom:1rem}
    .stDataFrame {font-size:12px}
    div[data-testid="metric-container"] {
        background:#fff;border:1px solid #e0e0e0;border-radius:8px;
        padding:10px 14px;border-top:3px solid #2D3561
    }
    </style>
    """, unsafe_allow_html=True)

    # ── Header ──
    st.markdown("""
    <div style="background:#1A1A2E;padding:16px 24px;border-radius:8px;margin-bottom:16px;display:flex;align-items:center;gap:12px">
      <span style="font-size:28px;font-weight:800;color:#fff">🔩 Steel<span style="color:#f0a500">Pulse</span></span>
      <span style="color:#555;font-size:13px">| Procurement Intelligence Platform</span>
      <span style="margin-left:auto;color:#888;font-size:11px">WMSPS Algorithm + TWMAP 6-Month Forecast</span>
    </div>
    """, unsafe_allow_html=True)

    # ─────────────────────────────────
    # SIDEBAR
    # ─────────────────────────────────
    with st.sidebar:
        st.markdown("### 📁 Upload SAP Export")
        uploaded = st.file_uploader(
            "Drop your .xlsx file here",
            type=["xlsx","xls"],
            help="Upload the SAP Excel export containing: Quotation-Table, SO-Table, Purchase-Table, Tubing Stock balance, 144 PRICE sheets"
        )

        st.markdown("---")
        st.markdown("### 🔍 Filters")
        filter_signal = st.multiselect("Signal", ["BUY","WATCH","HOLD","SKIP"], default=[])
        filter_class  = st.multiselect("Item Class", ["FAST_MOVER","SLOW_MOVER","PROJECT","DEAD"], default=[])
        filter_risk   = st.checkbox("⚠️ Stockout Risk Only", value=False)
        search_term   = st.text_input("Search Item Code", placeholder="e.g. SS-T8-S-065")

        st.markdown("---")
        st.markdown("### 📖 Required Sheets")
        for s in ["Quotation-Table","SO-Table / TSO Table","Purchase-Table / TP History","Tubing Stock balance","144 PRICE"]:
            st.markdown(f"- `{s}`")

        st.markdown("---")
        st.markdown("""
        <div style='font-size:11px;color:#888'>
        <b>Algorithm:</b> WMSPS<br>
        S1 Velocity 35% · S2 Conversion 25%<br>
        S3 Coverage 25% · S4 Open SO 15%<br><br>
        <b>Forecast:</b> TWMAP<br>
        Base × Trend × Inquiry Boost × Decay
        </div>
        """, unsafe_allow_html=True)

    # ── No file state ──
    if uploaded is None:
        st.markdown("""
        <div style="text-align:center;padding:80px 20px;background:#111318;border-radius:12px;border:2px dashed #444">
          <div style="font-size:60px;margin-bottom:16px">🔩</div>
          <div style="font-size:24px;font-weight:700;color:#ffffff;margin-bottom:8px">Upload your SAP Excel export to begin</div>
          <div style="font-size:14px;color:#aaa">Drag and drop your .xlsx file in the sidebar<br>
          The algorithm will process all sheets automatically</div>
          <br>
          <div style="display:inline-block;background:#1A1A2E;border-radius:8px;padding:20px 36px;margin-top:8px;text-align:left;border:1px solid #333">
            <div style="color:#f0a500;font-weight:700;margin-bottom:10px;font-size:14px">What you get:</div>
            <div style="color:#fff;font-size:13px;line-height:2">
            ✅ &nbsp;Procurement Score (0–100) per item<br>
            ✅ &nbsp;BUY / WATCH / HOLD / SKIP signals<br>
            ✅ &nbsp;6-Month demand forecast with confidence bands<br>
            ✅ &nbsp;Stockout risk detection per item<br>
            ✅ &nbsp;Proposed purchase quantity (lengths)<br>
            ✅ &nbsp;Professional Excel report — 5 sheets
            </div>
          </div>
        </div>
        """, unsafe_allow_html=True)
        return

    # ── Run analysis ──
    with st.spinner("⚙️ Running WMSPS algorithm + TWMAP 6-month forecast..."):
        try:
            file_bytes = uploaded.read()
            df = run_full_analysis(file_bytes, uploaded.name)
        except Exception as e:
            st.error(f"❌ Error processing file: {str(e)}")
            st.info("💡 Make sure your file contains: Quotation-Table, SO-Table/TSO Table, Purchase-Table/TP History, Tubing Stock balance, and 144 PRICE sheets.")
            st.stop()

    summary = compute_summary(df)

    # ── Bootstrap learning on first upload ──
    upload_id = f"UPLOAD_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    if not is_bootstrapped():
        with st.spinner("🧠 Initialising learning model from 6 years of history..."):
            n = bootstrap_from_history(df)
            save_forecast_snapshot(df, upload_id, uploaded.name, summary)
        st.success(f"✅ Learning model initialised — {n} items learned from 2021–2024 data, validated on 2025")
        st.cache_data.clear()
        df = run_full_analysis(file_bytes, uploaded.name)
        summary = compute_summary(df)
    else:
        # Update learning with new upload data
        update_from_new_upload(df, upload_id)
        save_forecast_snapshot(df, upload_id, uploaded.name, summary)

    # ── Apply sidebar filters ──
    filtered = df.copy()
    if filter_signal: filtered = filtered[filtered.Signal.isin(filter_signal)]
    if filter_class:  filtered = filtered[filtered.ItemClass.isin(filter_class)]
    if filter_risk:   filtered = filtered[filtered.HasStockoutRisk == True]
    if search_term:   filtered = filtered[filtered.ItemCode.str.contains(search_term, case=False, na=False)]

    # ── Export button ──
    col_exp, col_info = st.columns([2, 8])
    with col_exp:
        excel_buf = build_excel_export(df)
        st.download_button(
            label="⬇️ Export Excel Report",
            data=excel_buf,
            file_name=f"SteelPulse_Report_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            type="primary",
        )
    with col_info:
        st.caption(f"📁 {uploaded.name} · {summary['total']} items processed · {len(filtered)} shown after filters")

    # ─────────────────────────────────
    # KPI ROW
    # ─────────────────────────────────
    c1,c2,c3,c4,c5,c6,c7,c8 = st.columns(8)
    with c1: st.metric("Total Items",   f"{summary['total']:,}")
    with c2: st.metric("🟢 BUY",         summary['buy'],    help="Order immediately")
    with c3: st.metric("🔵 WATCH",       summary['watch'],  help="Prepare to order")
    with c4: st.metric("🟡 HOLD",        summary['hold'],   help="Monitor monthly")
    with c5: st.metric("⛔ SKIP",        summary['skip'],   help="Do not order")
    with c6: st.metric("⚠️ Stockout Risk", summary['stockout_risk'], help="Stock runs out within 6 months")
    with c7: st.metric("🛒 Proposed Qty", f"{summary['proposed_qty']:,}", help="Lengths to buy (6M cover)")
    with c8: st.metric("💵 Est. Cost",   f"${summary['est_cost_usd']/1000:.0f}K", help="USD (priced items only)")

    st.markdown("<div style='margin:8px 0'></div>", unsafe_allow_html=True)

    # ─────────────────────────────────
    # TABS
    # ─────────────────────────────────
    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
        "📈 Forecast & Buy Signals",
        "📅 6-Month Projection",
        "🎯 Procurement Board",
        "📊 Analytics",
        "📋 Balance Sheet",
        "🧠 Learning Dashboard",
        "⚙️ Algorithm Explained",
    ])

    # ══════════════════════════════════════════════════════════════
    # TAB 1 — FORECAST & BUY SIGNALS
    # ══════════════════════════════════════════════════════════════
    with tab1:
        st.markdown("#### 🛒 Procurement Signals — Click any row for full detail")

        # Urgency alerts
        critical = df[(df.Signal=="BUY") & (df.StockoutMonth.isin([MONTH_LABELS[0], MONTH_LABELS[1]]))]
        if len(critical) > 0:
            st.error(f"🔴 **{len(critical)} items** will stock out in the next 2 months ({MONTH_LABELS[0]}, {MONTH_LABELS[1]}) — ORDER IMMEDIATELY")

        open_so_risk = df[(df.OpenSO > df.AvailStock) & (df.TotalSales > 0)]
        if len(open_so_risk) > 0:
            st.warning(f"🟠 **{len(open_so_risk)} items** have Open SO exceeding Available Stock — customer deliveries at risk")

        # Build display table
        display_cols = {
            "ItemCode":       "Item Code",
            "ItemClass":      "Class",
            "Signal":         "Signal",
            "Score":          "Score",
            "F6M_Mid":        "6M Demand (Mid)",
            "F6M_Low":        "6M Low",
            "F6M_High":       "6M High",
            "NetStock_Now":   "Net Stock",
            "StockEnd_Mid":   "Stock End (Mid)",
            "StockoutMonth":  "Stockout In",
            "ProposedQty_6M": "Proposed Buy",
            "EstCostUSD":     "Est Cost (USD)",
            "ForecastConf":   "Confidence",
            "TrendMult":      "Trend ×",
            "LeadTimeWeeks":  "Lead Wks",
        }

        tbl = filtered[list(display_cols.keys())].rename(columns=display_cols).copy()
        tbl["Score"] = tbl["Score"].round(1)
        tbl["Est Cost (USD)"] = tbl["Est Cost (USD)"].apply(
            lambda x: f"${x:,.0f}" if x > 0 else "—"
        )
        tbl["Stock End (Mid)"] = tbl["Stock End (Mid)"].round(1)
        tbl["6M Demand (Mid)"] = tbl["6M Demand (Mid)"].round(1)
        tbl["Trend ×"] = tbl["Trend ×"].round(2)

        def highlight_row(row):
            sig = row["Signal"]
            if sig == "BUY":
                return ["background-color:#d4edda;color:#155724"] * len(row)
            elif sig == "WATCH":
                return ["background-color:#e8f4ff;color:#004085"] * len(row)
            elif sig == "HOLD":
                return ["background-color:#fffde7;color:#856404"] * len(row)
            elif row.get("Stockout In","None") != "None":
                return ["background-color:#fff5f5;color:#cc0000"] * len(row)
            return [""] * len(row)

        sorted_tbl = tbl.sort_values("Score", ascending=False)
        st.dataframe(sorted_tbl, use_container_width=True, height=520)

        st.caption(f"Showing {len(filtered):,} items · Green = BUY · Blue = WATCH · Red background = Stockout risk · Click column header to sort")

        # ── Item detail expander ──
        st.markdown("---")
        st.markdown("#### 🔎 Item Deep Dive")
        item_list = filtered[filtered.Signal.isin(["BUY","WATCH"])]["ItemCode"].tolist()
        if not item_list:
            item_list = filtered["ItemCode"].tolist()

        if item_list:
            selected = st.selectbox("Select an item to inspect:", item_list)
            if selected:
                irow = df[df.ItemCode == selected].iloc[0]
                _show_item_detail(irow)


    # ══════════════════════════════════════════════════════════════
    # TAB 2 — 6-MONTH PROJECTION
    # ══════════════════════════════════════════════════════════════
    with tab2:
        st.markdown("#### 📅 6-Month Forward Demand Forecast")
        st.caption(f"Forecast months: **{' → '.join(MONTH_LABELS)}** · Method: TWMAP (Trend-Weighted Moving Average Projection)")

        # ── Aggregate chart for BUY items ──
        buy_items = df[df.Signal == "BUY"]
        if len(buy_items) > 0:
            agg = pd.DataFrame({
                "Month": MONTH_LABELS,
                "Low":  [buy_items[f"Proj_Low_{m}"].sum()  for m in MONTH_LABELS],
                "Mid":  [buy_items[f"Proj_Mid_{m}"].sum()  for m in MONTH_LABELS],
                "High": [buy_items[f"Proj_High_{m}"].sum() for m in MONTH_LABELS],
            })

            fig = go.Figure()
            fig.add_trace(go.Bar(name="Mid Forecast", x=agg.Month, y=agg.Mid,
                                 marker_color="#007bff", opacity=0.85))
            fig.add_trace(go.Scatter(name="High (+30%)", x=agg.Month, y=agg.High,
                                     mode="lines+markers", line=dict(color="#28a745", dash="dash", width=2),
                                     marker=dict(size=6)))
            fig.add_trace(go.Scatter(name="Low (−25%)", x=agg.Month, y=agg.Low,
                                     mode="lines+markers", line=dict(color="#dc3545", dash="dash", width=2),
                                     marker=dict(size=6), fill="tonexty", fillcolor="rgba(200,230,255,0.15)"))
            fig.update_layout(title="Aggregate 6-Month Demand — All BUY Items (lengths)",
                              xaxis_title="Month", yaxis_title="Lengths",
                              height=340, plot_bgcolor="#fafafa",
                              legend=dict(orientation="h", yanchor="bottom", y=1.02))
            st.plotly_chart(fig, use_container_width=True)

        # ── Stockout risk table ──
        st.markdown("#### ⚠️ Stockout Risk Items")
        risk_df = df[df.HasStockoutRisk == True].copy()
        risk_df = risk_df.sort_values("StockoutMonth")

        if len(risk_df) == 0:
            st.success("✅ No stockout risk detected in the next 6 months")
        else:
            st.error(f"**{len(risk_df)} items** at risk of stocking out within 6 months")

            risk_cols = ["ItemCode","Signal","NetStock_Now","F6M_Mid","StockEnd_Mid",
                         "StockoutMonth","ProposedQty_6M"] + [f"Proj_Mid_{m}" for m in MONTH_LABELS]
            risk_display = risk_df[risk_cols].copy()
            risk_display.columns = (
                ["Item Code","Signal","Net Stock","6M Demand","Stock End","Stockout In","Buy Now"] +
                [f"{m}" for m in MONTH_LABELS]
            )
            risk_display = risk_display.round(1)

            def highlight_risk(row):
                mo = str(row.get("Stockout In","None"))
                if mo in [MONTH_LABELS[0], MONTH_LABELS[1]]:
                    return ["background-color:#ffe5e5;font-weight:bold"] * len(row)
                return ["background-color:#fff8e1"] * len(row)

            st.dataframe(risk_display, use_container_width=True, height=420)
            st.caption("Red rows = stock out within 2 months. Yellow = 3–6 months.")

        # ── Per-item forecast chart ──
        st.markdown("---")
        st.markdown("#### 🔍 Per-Item 6-Month Forecast")
        item_sel2 = st.selectbox(
            "Select item:", df[df.TotalSales > 0]["ItemCode"].tolist(), key="fc_item"
        )
        if item_sel2:
            irow2 = df[df.ItemCode == item_sel2].iloc[0]
            _show_forecast_chart(irow2)


    # ══════════════════════════════════════════════════════════════
    # TAB 3 — PROCUREMENT BOARD
    # ══════════════════════════════════════════════════════════════
    with tab3:
        _show_procurement_board(df)

    # ══════════════════════════════════════════════════════════════
    # TAB 4 — ANALYTICS
    # ══════════════════════════════════════════════════════════════
    with tab4:
        st.markdown("#### 📊 Analytics Dashboard")

        col_a, col_b = st.columns([3, 2])

        with col_a:
            # Annual trend chart
            annual_df = pd.DataFrame([
                {"Year": yr, "Type": "Inquiry",  "Quantity": summary["annual"][yr]["inq"]}
                for yr in YEARS
            ] + [
                {"Year": yr, "Type": "Sales",    "Quantity": summary["annual"][yr]["sales"]}
                for yr in YEARS
            ] + [
                {"Year": yr, "Type": "Purchase", "Quantity": summary["annual"][yr]["purch"]}
                for yr in YEARS
            ])
            fig2 = px.bar(annual_df, x="Year", y="Quantity", color="Type", barmode="group",
                          color_discrete_map={"Inquiry":"#007bff","Sales":"#28a745","Purchase":"#f0a500"},
                          title="Annual Inquiry vs Sales vs Purchase (all items · lengths)")
            fig2.update_layout(height=320, plot_bgcolor="#fafafa")
            st.plotly_chart(fig2, use_container_width=True)

            # Conversion rate trend
            conv_df = pd.DataFrame([{
                "Year": yr,
                "Conversion %": round(
                    summary["annual"][yr]["sales"] / (summary["annual"][yr]["inq"]+1) * 100, 2
                )
            } for yr in YEARS])
            fig3 = px.line(conv_df, x="Year", y="Conversion %",
                           title="Inquiry-to-Sales Conversion Rate (%)",
                           markers=True, color_discrete_sequence=["#fd7e14"])
            fig3.update_layout(height=260, plot_bgcolor="#fafafa")
            st.plotly_chart(fig3, use_container_width=True)

        with col_b:
            # Signal pie
            pie_data = pd.DataFrame({
                "Signal": ["BUY","WATCH","HOLD","SKIP"],
                "Count": [summary["buy"],summary["watch"],summary["hold"],summary["skip"]]
            })
            fig4 = px.pie(pie_data, values="Count", names="Signal",
                          color="Signal",
                          color_discrete_map={"BUY":"#28a745","WATCH":"#007bff","HOLD":"#ffc107","SKIP":"#dc3545"},
                          title="Signal Distribution")
            fig4.update_traces(textposition="inside", textinfo="percent+label")
            fig4.update_layout(height=280, showlegend=False)
            st.plotly_chart(fig4, use_container_width=True)

            # Class breakdown
            class_data = pd.DataFrame({
                "Class": ["FAST MOVER","SLOW MOVER","PROJECT","DEAD"],
                "Count": [summary["fast"],summary["slow"],summary["project"],summary["dead"]]
            })
            fig5 = px.bar(class_data, x="Count", y="Class", orientation="h",
                          color="Class",
                          color_discrete_map={"FAST MOVER":"#28a745","SLOW MOVER":"#007bff",
                                              "PROJECT":"#fd7e14","DEAD":"#6c757d"},
                          title="Item Classification")
            fig5.update_layout(height=250, showlegend=False, plot_bgcolor="#fafafa",
                               yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig5, use_container_width=True)

        # Top items by proposed qty
        st.markdown("#### 🏆 Top Items by Proposed Purchase Quantity")
        top_df = df[df.ProposedQty_6M > 0].nlargest(15, "ProposedQty_6M")[
            ["ItemCode","Signal","ItemClass","ProposedQty_6M","F6M_Mid",
             "NetStock_Now","StockoutMonth","LeadTimeWeeks","PricePerLength","EstCostUSD"]
        ].copy()
        top_df.columns = ["Item Code","Signal","Class","Proposed Qty","6M Demand",
                          "Net Stock","Stockout","Lead Wks","Price/Len","Est Cost USD"]
        top_df["Est Cost USD"] = top_df["Est Cost USD"].apply(lambda x: f"${x:,.0f}" if x > 0 else "—")
        top_df = top_df.round(1)

        fig6 = px.bar(top_df.head(15), x="Proposed Qty", y="Item Code",
                      orientation="h", color="Signal",
                      color_discrete_map={"BUY":"#28a745","WATCH":"#007bff","HOLD":"#ffc107","SKIP":"#dc3545"},
                      title="Top 15 Items — Proposed Purchase Quantity (lengths)")
        fig6.update_layout(height=400, plot_bgcolor="#fafafa",
                           yaxis=dict(autorange="reversed"), showlegend=True)
        st.plotly_chart(fig6, use_container_width=True)
        st.dataframe(top_df.set_index("Item Code"), use_container_width=True)


    # ══════════════════════════════════════════════════════════════
    # TAB 5 — BALANCE SHEET
    # ══════════════════════════════════════════════════════════════
    with tab5:
        st.markdown("#### 📋 Year-wise Balance Sheet — Inquiry vs Sales vs Purchase")

        balance_rows = []
        for yr in YEARS:
            inq   = summary["annual"][yr]["inq"]
            sales = summary["annual"][yr]["sales"]
            purch = summary["annual"][yr]["purch"]
            conv  = round(sales/(inq+1)*100, 2) if inq > 0 else 0
            balance_rows.append({
                "Year": yr,
                "Inquiries (lengths)": inq,
                "Sales / Consumption (lengths)": sales,
                "Purchases from Vendor (lengths)": purch,
                "Conversion Rate %": conv,
                "Net Position (Purch − Sales)": purch - sales,
            })

        bal_df = pd.DataFrame(balance_rows).set_index("Year")

        def style_balance(val, col):
            if col == "Net Position (Purch − Sales)":
                return "color:#155724;font-weight:bold" if val >= 0 else "color:#cc0000;font-weight:bold"
            return ""

        st.dataframe(bal_df, use_container_width=True)

        # Balance waterfall
        fig7 = make_subplots(specs=[[{"secondary_y": True}]])
        yrs_str = [str(y) for y in YEARS]
        fig7.add_trace(go.Bar(name="Inquiries", x=yrs_str,
                              y=[summary["annual"][y]["inq"]   for y in YEARS],
                              marker_color="#007bff", opacity=0.7))
        fig7.add_trace(go.Bar(name="Sales",     x=yrs_str,
                              y=[summary["annual"][y]["sales"] for y in YEARS],
                              marker_color="#28a745", opacity=0.9))
        fig7.add_trace(go.Bar(name="Purchase",  x=yrs_str,
                              y=[summary["annual"][y]["purch"] for y in YEARS],
                              marker_color="#f0a500", opacity=0.9))
        fig7.add_trace(go.Scatter(
            name="Conv Rate %", x=yrs_str,
            y=[round(summary["annual"][y]["sales"]/(summary["annual"][y]["inq"]+1)*100,2) for y in YEARS],
            mode="lines+markers", line=dict(color="#dc3545",width=2),
            marker=dict(size=8)
        ), secondary_y=True)
        fig7.update_layout(barmode="group", height=380, plot_bgcolor="#fafafa",
                           title="6-Year History — Inquiry / Sales / Purchase + Conversion Rate")
        fig7.update_yaxes(title_text="Lengths",       secondary_y=False)
        fig7.update_yaxes(title_text="Conv Rate (%)", secondary_y=True)
        st.plotly_chart(fig7, use_container_width=True)

        # Stock position table
        st.markdown("#### 📦 Current Stock Position")
        stock_df = df[["ItemCode","QOH","OpenSO","AvailStock","IncomingPO",
                        "NetAvailStock","ItemClass","Signal","StockCoverDays"]].copy()
        stock_df = stock_df[stock_df.QOH > 0].sort_values("QOH", ascending=False)
        stock_df.columns = ["Item","QOH","Open SO","Avail Stock","Incoming PO",
                             "Net Avail","Class","Signal","Cover Days"]
        st.dataframe(stock_df.set_index("Item"), use_container_width=True, height=400)


    # ══════════════════════════════════════════════════════════════
    # TAB 6 — LEARNING DASHBOARD
    # ══════════════════════════════════════════════════════════════
    with tab6:
        _show_learning_dashboard(df)

    # ══════════════════════════════════════════════════════════════
    # TAB 7 — ALGORITHM EXPLAINED
    # ══════════════════════════════════════════════════════════════
    with tab7:
        st.markdown("#### ⚙️ How the Algorithm Works")

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("""
            <div style="background:#f8f9fa;border-radius:10px;padding:20px;border-left:4px solid #1A1A2E">
            <h4 style="color:#1A1A2E;margin-top:0">🧮 WMSPS — Procurement Scoring</h4>
            <p style="color:#555;font-size:13px">
            <b>Weighted Multi-Signal Procurement Scoring</b> gives each item a
            0–100 score from 4 independent signals, then derives a buy/watch/hold/skip decision.
            </p>
            </div>
            """, unsafe_allow_html=True)

            for title, pct, desc, color, detail in [
                ("🔵 S1 — Sales Velocity", "35%",
                 "Is demand growing or dying?",
                 "#007bff",
                 "Weighted least-squares regression across 6 years. Years 2025–2026 carry 2×–3× more weight than 2021. A positive slope = growing sales = high score. Items with zero recent sales score near 0."),
                ("🟣 S2 — Inquiry Conversion", "25%",
                 "Do inquiries turn into orders?",
                 "#6f42c1",
                 "Conversion Rate = Total Sales ÷ Total Inquiries. High recent inquiry volume adds a boost. This catches items where demand is building but not yet converted (project pipeline)."),
                ("🟠 S3 — Stock Coverage", "25%",
                 "How many months of stock remain vs lead time?",
                 "#fd7e14",
                 "Coverage = NET Stock ÷ Avg Monthly Sales. Lead-time aware: if stock covers less than lead time → score 95–100 (will stockout before PO arrives). Zero stock = score 100 (critical)."),
                ("🔴 S4 — Open SO Pressure", "15%",
                 "Are booked customer orders at risk?",
                 "#dc3545",
                 "% of Open Sales Orders uncovered by Available Stock. If Open SO > Stock → customer commitments are at risk → forces BUY signal regardless of score."),
            ]:
                with st.expander(f"{title} — Weight: {pct}  ·  {desc}"):
                    st.markdown(f"""
                    <div style="border-left:3px solid {color};padding:8px 12px;font-size:13px;color:#444">
                    {detail}
                    </div>""", unsafe_allow_html=True)

            st.markdown("""
            <div style="background:#1A1A2E;color:#fff;padding:12px 16px;border-radius:6px;font-family:monospace;font-size:13px;margin-top:12px">
            Score = S1×0.35 + S2×0.25 + S3×0.25 + S4×0.15<br><br>
            🟢 BUY   → Score ≥ 60  (or Open SO > Available Stock)<br>
            🔵 WATCH → Score 40–59<br>
            🟡 HOLD  → Score 20–39<br>
            ⛔ SKIP  → Score &lt; 20 or DEAD item
            </div>
            """, unsafe_allow_html=True)

        with col2:
            st.markdown("""
            <div style="background:#f8f9fa;border-radius:10px;padding:20px;border-left:4px solid #007bff">
            <h4 style="color:#1A1A2E;margin-top:0">📅 TWMAP — 6-Month Forecast</h4>
            <p style="color:#555;font-size:13px">
            <b>Trend-Weighted Moving Average Projection</b> computes month-by-month
            demand for the next 6 months using 3 multiplied signals.
            </p>
            </div>
            """, unsafe_allow_html=True)

            for step, title, color, detail in [
                ("1","Base Monthly Rate","#2D3561",
                 "Weighted average annual sales ÷ 12. Only years with actual sales are included (zeros excluded from average). Recent years weighted 2×–3× more. 2026 data is annualised from partial-year actuals."),
                ("2","YoY Trend Multiplier","#007bff",
                 "Compares recent 2-year sales (2025+2026) vs prior 2-year (2023+2024). If demand doubled → multiplier = 2.0×. If demand halved → 0.5×. Capped at 0.5×–2.0× to prevent extreme projections."),
                ("3","Inquiry Momentum Boost","#6f42c1",
                 "Recent inquiry volume vs 6-year average. High recent inquiry = demand pipeline building = boost up to 1.5×. Low recent inquiry = pullback = down to 0.8×."),
                ("4","Monthly Decay Factor","#fd7e14",
                 "Months further in the future are less certain. Decay: 1.00, 0.98, 0.96, 0.94, 0.92, 0.90. Low band = Mid × 0.75. High band = Mid × 1.30."),
            ]:
                with st.expander(f"Step {step}: {title}"):
                    st.markdown(f"""
                    <div style="border-left:3px solid {color};padding:8px 12px;font-size:13px;color:#444">
                    {detail}
                    </div>""", unsafe_allow_html=True)

            st.markdown("""
            <div style="background:#1A1A2E;color:#fff;padding:12px 16px;border-radius:6px;font-family:monospace;font-size:13px;margin-top:12px">
            Monthly[i] = Base × Trend × InqBoost × Decay[i]<br>
            Low  = Mid × 0.75 &nbsp;|&nbsp; High = Mid × 1.30<br><br>
            NET Stock = QOH + Incoming PO − Open SO<br>
            BuyNow = max(0, 6M Total + Safety − NET Stock)<br>
            Safety Buffer = 1 month of base forecast<br><br>
            Confidence: HIGH = sales 4+ yrs · MED = 2-3 yrs · LOW = 1 yr
            </div>
            """, unsafe_allow_html=True)

            st.markdown("##### 📦 Item Classification Rules")
            class_rules = pd.DataFrame([
                ["FAST MOVER",  "Sales in 3+ of the 6 years", "Standard 6M cover + 2mo safety"],
                ["SLOW MOVER",  "Sales in 1–2 years only",    "6M cover + 1mo safety"],
                ["PROJECT",     "1 big spike year, quiet rest","6M cover + 1mo (manual review)"],
                ["DEAD",        "Zero sales, <3 total inquiries","Never buy — score forced to 0"],
            ], columns=["Class","Trigger","Buy Logic"])
            st.dataframe(class_rules.set_index("Class"), use_container_width=True)


# ─────────────────────────────────────────────────────────────────
# HELPER: Item detail panel
# ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    main()
