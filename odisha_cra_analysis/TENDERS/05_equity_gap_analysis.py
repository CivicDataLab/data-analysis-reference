"""
05_equity_gap_analysis.py — CRA Spend vs Climate Vulnerability Gap
===================================================================
Step 5 of the CRA analysis pipeline.

Asks: Is CRA procurement money reaching the districts that need it most?
Mirrors the MCH equity analysis (IMR vs tenders) but for climate:
  vulnerability proxy = combined_risk_score (drought + cyclone)
  spend proxy         = CRA tenders per lakh agricultural population

INPUT
-----
  data/processed/cra_geotagged.csv
  data/raw/odisha_district_population.csv        (district, total_pop, agri_pop)
  data/raw/odisha_district_climate_risk.csv      (as used in 03_geo_tagging.py)

OUTPUT
------
  outputs/equity_gap_table.csv    — district-level spend vs vulnerability table
  outputs/equity_gap_chart.png    — scatter plot (vulnerability vs spend density)
  outputs/equity_gap_findings.md  — narrative interpretation

USAGE
-----
  python 05_equity_gap_analysis.py
"""

import argparse
import logging
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


# ─── Inline fallback population data (2011 Census; update when 2021 data available) ─

DISTRICT_POPULATION = {
    "Angul":          {"total_pop": 1271703, "agri_pop": 750000},
    "Balasore":       {"total_pop": 2317419, "agri_pop": 1300000},
    "Bargarh":        {"total_pop": 1478833, "agri_pop": 950000},
    "Bhadrak":        {"total_pop": 1506522, "agri_pop": 900000},
    "Bolangir":       {"total_pop": 1648574, "agri_pop": 1100000},
    "Boudh":          {"total_pop":  441162, "agri_pop": 280000},
    "Cuttack":        {"total_pop": 2618708, "agri_pop": 1200000},
    "Deogarh":        {"total_pop":  312520, "agri_pop": 200000},
    "Dhenkanal":      {"total_pop": 1192948, "agri_pop": 700000},
    "Gajapati":       {"total_pop":  575880, "agri_pop": 400000},
    "Ganjam":         {"total_pop": 3520151, "agri_pop": 2000000},
    "Jagatsinghpur":  {"total_pop": 1136604, "agri_pop": 750000},
    "Jajpur":         {"total_pop": 1826275, "agri_pop": 1100000},
    "Jharsuguda":     {"total_pop":  590300, "agri_pop": 250000},
    "Kalahandi":      {"total_pop": 1573054, "agri_pop": 1000000},
    "Kandhamal":      {"total_pop":  731952, "agri_pop": 500000},
    "Kendrapara":     {"total_pop": 1440891, "agri_pop": 900000},
    "Kendujhar":      {"total_pop": 1802777, "agri_pop": 1100000},
    "Khordha":        {"total_pop": 2246341, "agri_pop": 600000},
    "Koraput":        {"total_pop": 1376934, "agri_pop": 900000},
    "Malkangiri":     {"total_pop":  612727, "agri_pop": 420000},
    "Mayurbhanj":     {"total_pop": 2513895, "agri_pop": 1700000},
    "Nabarangpur":    {"total_pop": 1218762, "agri_pop": 850000},
    "Nayagarh":       {"total_pop":  962215, "agri_pop": 650000},
    "Nuapada":        {"total_pop":  606490, "agri_pop": 420000},
    "Puri":           {"total_pop": 1498604, "agri_pop": 750000},
    "Rayagada":       {"total_pop": 1001544, "agri_pop": 680000},
    "Sambalpur":      {"total_pop":  1044410, "agri_pop": 500000},
    "Sonepur":        {"total_pop":  652107, "agri_pop": 450000},
    "Sundargarh":     {"total_pop": 2093437, "agri_pop": 900000},
}


def load_population(pop_file: Path) -> pd.DataFrame:
    if pop_file.exists():
        log.info("Loading population from %s", pop_file)
        return pd.read_csv(pop_file)
    log.warning("Population file not found — using 2011 Census inline defaults.")
    rows = [
        {"district_clean": d, "total_pop": v["total_pop"], "agri_pop": v["agri_pop"]}
        for d, v in DISTRICT_POPULATION.items()
    ]
    return pd.DataFrame(rows)


def compute_equity_table(
    cra_df: pd.DataFrame, pop_df: pd.DataFrame
) -> pd.DataFrame:
    amt_col = next(
        (c for c in cra_df.columns
         if c.lower() in ["estimated_amount", "awarded_amount", "amount", "awarded_value"]),
        None,
    )

    _DROP = {"West Singhbhum", "CONFLICT", "NA", ""}
    cra_df = cra_df[~cra_df["district_clean"].fillna("").isin(_DROP)].copy()

    if amt_col:
        cra_df[amt_col] = pd.to_numeric(
            cra_df[amt_col].astype(str).str.replace(",", "", regex=False),
            errors="coerce",
        )

    agg = cra_df.groupby("district_clean", dropna=False).agg(
        cra_tender_count=("district_clean", "count"),
        cra_total_spend=(amt_col, "sum") if amt_col else ("district_clean", "count"),
        drought_risk_score=("drought_risk_score", "first"),
        cyclone_risk_score=("cyclone_risk_score", "first"),
        combined_risk_score=("combined_risk_score", "first"),
    ).reset_index()

    pop_df.columns = [c.lower().strip() for c in pop_df.columns]
    merged = agg.merge(pop_df, on="district_clean", how="left")

    if "agri_pop" in merged.columns and amt_col:
        merged["spend_per_lakh_agri_pop"] = (
            merged["cra_total_spend"] / (merged["agri_pop"] / 1e5)
        ).round(0)
        merged["tenders_per_lakh_agri_pop"] = (
            merged["cra_tender_count"] / (merged["agri_pop"] / 1e5)
        ).round(2)

    # Gap flag: high risk but low spend density
    if "combined_risk_score" in merged.columns and "spend_per_lakh_agri_pop" in merged.columns:
        high_risk = merged["combined_risk_score"] >= merged["combined_risk_score"].median()
        low_spend = merged["spend_per_lakh_agri_pop"] <= merged["spend_per_lakh_agri_pop"].median()
        merged["equity_gap_flag"] = (high_risk & low_spend)

    return merged.sort_values("combined_risk_score", ascending=False)


def write_findings(equity_df: pd.DataFrame, output_path: Path) -> None:
    gap_districts = equity_df[equity_df.get("equity_gap_flag", False) == True][
        "district_clean"
    ].tolist() if "equity_gap_flag" in equity_df.columns else []

    lines = [
        "# CRA Spend Equity Gap Analysis — Odisha",
        "",
        "## Key Question",
        "Are CRA tenders being allocated to districts with the highest climate risk?",
        "",
        "## Methodology",
        "- Climate vulnerability proxy: `combined_risk_score` (mean of drought + cyclone risk, 1–5 scale)",
        "- Spend density proxy: CRA tenders and estimated spend per lakh agricultural population",
        "- Gap flag: district is **high risk** (≥ median risk) AND **low spend** (≤ median spend density)",
        "",
        "## Equity Gap Districts",
        "",
    ]
    if gap_districts:
        lines.append(
            "The following districts show **high climate vulnerability but below-median CRA spend density** — "
            "potential candidates for prioritisation:"
        )
        lines.append("")
        for d in gap_districts:
            lines.append(f"- {d}")
    else:
        lines.append("No gap districts identified — either data is incomplete or coverage is relatively equitable.")

    lines += [
        "",
        "## Caveats",
        "- Tender count ≠ actual expenditure; use awarded amounts when available.",
        "- Population denominators use 2011 Census; update when 2021 data is released.",
        "- Risk scores are from OCCAP 2018; district-level granularity is coarse.",
        "- Some tenders span multiple districts — they are counted in the tagged district only.",
        "",
        "---",
        "*Generated by 05_equity_gap_analysis.py*",
    ]

    output_path.write_text("\n".join(lines), encoding="utf-8")
    log.info("Findings → %s", output_path)


def main(cra_file: str, pop_file: str, output_dir: str) -> None:
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    cra_df = pd.read_csv(cra_file, low_memory=False)
    log.info("Loaded %d CRA tenders.", len(cra_df))

    pop_df = load_population(Path(pop_file))
    equity_df = compute_equity_table(cra_df, pop_df)

    out_csv = out_path / "equity_gap_table.csv"
    equity_df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    log.info("Equity table → %s", out_csv)

    # Attempt chart (optional — skipped gracefully if matplotlib not installed)
    try:
        import matplotlib.pyplot as plt
        if "combined_risk_score" in equity_df.columns and "spend_per_lakh_agri_pop" in equity_df.columns:
            fig, ax = plt.subplots(figsize=(10, 7))
            colors = equity_df.get("equity_gap_flag", pd.Series([False] * len(equity_df))).map(
                {True: "red", False: "steelblue"}
            )
            ax.scatter(equity_df["combined_risk_score"], equity_df["spend_per_lakh_agri_pop"],
                       c=colors, alpha=0.8, s=80)
            for _, row in equity_df.iterrows():
                ax.annotate(row["district_clean"],
                            (row["combined_risk_score"], row["spend_per_lakh_agri_pop"]),
                            fontsize=7, alpha=0.75)
            ax.set_xlabel("Combined Climate Risk Score (drought + cyclone)", fontsize=11)
            ax.set_ylabel("CRA Spend per lakh agricultural population (₹)", fontsize=11)
            ax.set_title("Odisha: CRA Spend vs Climate Risk by District\n"
                         "(Red = high-risk, low-spend — equity gap districts)", fontsize=12)
            ax.axvline(equity_df["combined_risk_score"].median(), color="grey", linestyle="--", lw=0.8)
            ax.axhline(equity_df["spend_per_lakh_agri_pop"].median(), color="grey", linestyle="--", lw=0.8)
            chart_path = out_path / "equity_gap_chart.png"
            fig.savefig(chart_path, dpi=150, bbox_inches="tight")
            log.info("Chart → %s", chart_path)
    except ImportError:
        log.info("matplotlib not installed — skipping chart. Run: pip install matplotlib")

    write_findings(equity_df, out_path / "equity_gap_findings.md")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    _base = Path(__file__).parent
    parser.add_argument("--cra_file",  default=str(_base / "data" / "processed" / "cra_geotagged.csv"))
    parser.add_argument("--pop_file",  default=str(_base / "data" / "raw" / "odisha_district_population.csv"))
    parser.add_argument("--output_dir", default=str(_base / "outputs"))
    args = parser.parse_args()
    main(args.cra_file, args.pop_file, args.output_dir)
