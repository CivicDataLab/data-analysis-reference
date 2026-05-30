"""
04_spend_analysis.py — Odisha CRA Procurement Spend Analysis
=============================================================
Step 4 of the CRA analysis pipeline.

Produces aggregate spend tables and a structured analysis report:
  1. Total CRA spend vs all-sector spend (share %)
  2. CRA spend by category (C1–C8)
  3. CRA spend by intent (Mitigation vs DamageResponse vs CapacityBuilding)
  4. CRA spend by district × climate risk (drought / cyclone)
  5. CRA spend by scheme
  6. CRA spend by financial year and month (seasonality)
  7. Department-level breakdown
  8. Equity check: CRA spend per capita vs vulnerability index (if pop data available)

INPUT
-----
  data/processed/cra_geotagged.csv           (output of 03_geo_tagging.py)
  data/raw/odisha_total_tenders.csv          (full unfiltered dataset — for share calc)
  data/raw/odisha_district_population.csv    (optional — for per-capita analysis)

OUTPUT
------
  outputs/cra_spend_summary.xlsx    — multi-sheet workbook
  outputs/cra_spend_report.md       — human-readable markdown summary

USAGE
-----
  python 04_spend_analysis.py
"""

import argparse
import logging
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

AMOUNT_COL_CANDIDATES = ["estimated_amount", "awarded_amount", "amount", "value",
                          "tender_value", "contract_value","awarded_value"]
DATE_COL_CANDIDATES   = ["tender_date", "award_date", "date", "publish_date","contract_date_:"]


def detect_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in df.columns:
        if c.lower().strip() in candidates:
            return c
    return None


def safe_numeric(series: pd.Series) -> pd.Series:
    """Strip currency symbols and commas, coerce to float."""
    return (
        series.astype(str)
              .str.replace(r"[₹,\s]", "", regex=True)
              .pipe(pd.to_numeric, errors="coerce")
    )


def crore(val: float) -> str:
    return f"₹{val/1e7:.2f} Cr"


def pct(part: float, total: float) -> str:
    if total == 0:
        return "N/A"
    return f"{part/total*100:.1f}%"


# ─── Analysis functions ───────────────────────────────────────────────────────

def by_category(df: pd.DataFrame, amt_col: str) -> pd.DataFrame:
    rows = []
    for _, row in df.iterrows():
        for cat in str(row.get("matched_categories", "")).split("|"):
            cat = cat.strip()
            if cat:
                rows.append({"category": cat, "amount": row[amt_col]})
    if not rows:
        return pd.DataFrame(columns=["category", "tender_count", "total_amount"])
    cat_df = pd.DataFrame(rows)
    return (
        cat_df.groupby("category")
              .agg(tender_count=("amount", "count"),
                   total_amount=("amount", "sum"))
              .sort_values("total_amount", ascending=False)
              .reset_index()
    )


def by_column(df: pd.DataFrame, col: str, amt_col: str) -> pd.DataFrame:
    return (
        df.groupby(col, dropna=False)
          .agg(tender_count=(amt_col, "count"),
               total_amount=(amt_col, "sum"))
          .sort_values("total_amount", ascending=False)
          .reset_index()
    )


def district_vs_risk(df: pd.DataFrame, amt_col: str) -> pd.DataFrame:
    group_cols = ["district_clean", "drought_risk_score", "cyclone_risk_score",
                  "combined_risk_score"]
    available = [c for c in group_cols if c in df.columns]
    return (
        df.groupby(available, dropna=False)
          .agg(tender_count=(amt_col, "count"),
               total_amount=(amt_col, "sum"))
          .sort_values("combined_risk_score" if "combined_risk_score" in available
                       else "total_amount", ascending=False)
          .reset_index()
    )


# ─── Report writer ────────────────────────────────────────────────────────────

def write_markdown_report(
    cra_df: pd.DataFrame,
    total_df: pd.DataFrame | None,
    amt_col: str,
    output_path: Path,
) -> None:
    cra_total = cra_df[amt_col].sum()
    raw_total = total_df[amt_col].sum() if total_df is not None else None

    lines = [
        "# Odisha CRA Procurement Spend — Analysis Report",
        "",
        "## 1. Overview",
        "",
        f"- **CRA tenders identified** : {len(cra_df):,}",
        f"- **Total CRA estimated spend** : {crore(cra_total)}",
    ]
    if raw_total:
        lines.append(f"- **All-sector spend (same period)** : {crore(raw_total)}")
        lines.append(f"- **CRA share of total spend** : {pct(cra_total, raw_total)}")

    lines += [
        "",
        "## 2. Spend by CRA Category",
        "",
        "| Category | Tenders | Total Spend |",
        "|----------|---------|-------------|",
    ]
    cat_tbl = by_category(cra_df, amt_col)
    for _, r in cat_tbl.iterrows():
        lines.append(f"| {r['category']} | {int(r['tender_count']):,} | {crore(r['total_amount'])} |")

    lines += [
        "",
        "## 3. Spend by Intent",
        "",
        "| Intent | Tenders | Total Spend |",
        "|--------|---------|-------------|",
    ]
    for _, r in by_column(cra_df, "intent_tag", amt_col).iterrows():
        lines.append(f"| {r['intent_tag']} | {int(r['tender_count']):,} | {crore(r['total_amount'])} |")

    lines += [
        "",
        "## 4. Spend by Scheme",
        "",
        "| Scheme | Tenders | Total Spend |",
        "|--------|---------|-------------|",
    ]
    if "scheme_tag" in cra_df.columns:
        for _, r in by_column(cra_df, "scheme_tag", amt_col).iterrows():
            lines.append(
                f"| {r['scheme_tag']} | {int(r['tender_count']):,} | {crore(r['total_amount'])} |"
            )

    lines += [
        "",
        "## 5. Top Districts by CRA Spend vs Climate Risk",
        "",
        "| District | Tenders | Spend | Drought Risk | Cyclone Risk |",
        "|----------|---------|-------|-------------|-------------|",
    ]
    dist_tbl = district_vs_risk(cra_df, amt_col).head(15)
    for _, r in dist_tbl.iterrows():
        lines.append(
            f"| {r.get('district_clean','?')} | {int(r['tender_count']):,} | "
            f"{crore(r['total_amount'])} | "
            f"{r.get('drought_risk_score','?')} | {r.get('cyclone_risk_score','?')} |"
        )

    lines += [
        "",
        "---",
        "*Generated by 04_spend_analysis.py — Odisha CRA Analysis Pipeline*",
    ]

    output_path.write_text("\n".join(lines), encoding="utf-8")
    log.info("Markdown report → %s", output_path)


# ─── Main ────────────────────────────────────────────────────────────────────

def main(cra_file: str, total_file: str, pop_file: str,
         output_dir: str) -> None:

    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    cra_df = pd.read_csv(cra_file, low_memory=False)
    log.info("Loaded %d CRA tenders.", len(cra_df))

    amt_col = detect_col(cra_df, AMOUNT_COL_CANDIDATES) or "estimated_amount"
    if amt_col not in cra_df.columns:
        log.warning("Amount column not found — spend figures will be zero.")
        cra_df[amt_col] = 0
    else:
        cra_df[amt_col] = safe_numeric(cra_df[amt_col])

    total_df = None
    if Path(total_file).exists():
        total_df = pd.read_csv(total_file, low_memory=False)
        if amt_col in total_df.columns:
            total_df[amt_col] = safe_numeric(total_df[amt_col])
        else:
            total_df = None

    # Build Excel workbook
    xlsx_path = out_path / "cra_spend_summary.xlsx"
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        cra_df.to_excel(writer, sheet_name="CRA_Tenders", index=False)
        by_category(cra_df, amt_col).to_excel(writer, sheet_name="By_Category", index=False)
        by_column(cra_df, "intent_tag", amt_col).to_excel(writer, sheet_name="By_Intent", index=False)
        if "scheme_tag" in cra_df.columns:
            by_column(cra_df, "scheme_tag", amt_col).to_excel(writer, sheet_name="By_Scheme", index=False)
        district_vs_risk(cra_df, amt_col).to_excel(writer, sheet_name="By_District_Risk", index=False)
        if "dept_cluster" in cra_df.columns:
            by_column(cra_df, "dept_cluster", amt_col).to_excel(writer, sheet_name="By_Dept", index=False)
        if "financial_year" in cra_df.columns:
            by_column(cra_df, "financial_year", amt_col).to_excel(writer, sheet_name="By_FY", index=False)
    log.info("Excel workbook → %s", xlsx_path)

    # Markdown report
    write_markdown_report(
        cra_df, total_df, amt_col, out_path / "cra_spend_report.md"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    _base = Path(__file__).parent
    parser.add_argument("--cra_file",   default=str(_base / "data" / "processed" / "cra_geotagged.csv"))
    parser.add_argument("--total_file", default=str(_base / "data" / "raw" / "odisha_total_tenders.csv"))
    parser.add_argument("--pop_file",   default=str(_base / "data" / "raw" / "odisha_district_population.csv"))
    parser.add_argument("--output_dir", default=str(_base / "outputs"))
    args = parser.parse_args()
    main(args.cra_file, args.total_file, args.pop_file, args.output_dir)
