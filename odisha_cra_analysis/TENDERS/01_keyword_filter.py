"""
01_keyword_filter.py — Odisha CRA Tender Identification
========================================================
Step 1 of the CRA analysis pipeline.

INPUT
-----
  data/raw/*.csv  — Monthly tender CSV files downloaded from the Odisha
                    eProcurement portal (https://tendersodisha.gov.in).

Expected CSV columns (case-insensitive match attempted):
  tender_id | work_description | department | district | estimated_amount |
  financial_year | month | tender_date | award_date | awarded_amount

OUTPUT
------
  data/processed/cra_filtered_tenders.csv
    All tenders that pass the keyword filter, with added columns:
      matched_categories  — pipe-separated list of category labels (e.g. C1|C3)
      matched_keywords    — pipe-separated list of exact matched keywords
      intent_tag          — DamageResponse | Mitigation | CapacityBuilding | Unknown
      is_coastal_district — True/False
      is_drought_district — True/False

  data/processed/cra_filter_log.txt
    Per-category match counts for QA.

USAGE
-----
  python 01_keyword_filter.py
  python 01_keyword_filter.py --input_dir data/raw --output_dir data/processed
"""

import argparse
import csv
import logging
import os
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import pandas as pd

from cra_keywords import (
    INTENT_TAGS,
    NEGATIVE_KEYWORDS,
    ODISHA_DISTRICTS,
    POSITIVE_CATEGORIES,
)

# ─── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger(__name__)

# ─── Helpers ─────────────────────────────────────────────────────────────────

def normalise(text: str) -> str:
    """Lower-case, collapse whitespace."""
    if not isinstance(text, str):
        return ""
    return re.sub(r"\s+", " ", text.lower().strip())


def match_keywords(text: str, keywords: list[str]) -> list[str]:
    """Return list of keywords found in text (substring match)."""
    norm = normalise(text)
    return [kw for kw in keywords if kw in norm]


def tag_intent(text: str) -> str:
    for tag, meta in INTENT_TAGS.items():
        if match_keywords(text, meta["keywords"]):
            return meta["label"]
    return "Unknown"


def tag_district(district_val: str) -> tuple[bool, bool]:
    """Return (is_coastal, is_drought_prone) for a given district string."""
    d = normalise(district_val)
    is_coastal = any(cd in d for cd in ODISHA_DISTRICTS["coastal"])
    is_drought = any(dd in d for dd in ODISHA_DISTRICTS["drought_prone"])
    return is_coastal, is_drought


def find_desc_column(columns: list[str]) -> str:
    """Flexible column name detection for work description field."""
    candidates = ["work_description", "workdescription", "description",
                  "work description", "tender_description", "work_name",
                  "title", "subject"]
    for c in columns:
        if c.lower().strip() in candidates:
            return c
    # fallback: first text-heavy column
    return columns[0]


# ─── Core filter ─────────────────────────────────────────────────────────────

def filter_tenders(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply positive + negative keyword logic to produce a CRA-tagged dataframe.
    """
    # Normalise column names
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    df = df.loc[df["status"] == "Accepted-AOC"] if "status" in df.columns else df
    desc_col = find_desc_column(list(df.columns))
    log.info("Using '%s' as the work-description column.", desc_col)

    results = []
    category_counts: dict[str, int] = defaultdict(int)

    for _, row in df.iterrows():
        text = str(row.get(desc_col, "")) + " " + str(row.get("department", ""))

        # ── Negative filter ───────────────────────────────────────────────
        neg_hits = match_keywords(text, NEGATIVE_KEYWORDS)
        if neg_hits:
            continue

        # ── Positive filter ───────────────────────────────────────────────
        matched_cats = []
        matched_kws = []

        for cat_key, meta in POSITIVE_CATEGORIES.items():
            hits = match_keywords(text, meta["keywords"])
            if hits:
                matched_cats.append(meta["label"])
                matched_kws.extend(hits)
                category_counts[cat_key] += 1

        if not matched_cats:
            continue

        # ── Intent tagging ────────────────────────────────────────────────
        intent = tag_intent(text)

        # ── District flags ────────────────────────────────────────────────
        district_val = str(row.get("district", ""))
        is_coastal, is_drought = tag_district(district_val)

        result_row = row.to_dict()
        result_row["matched_categories"] = "|".join(matched_cats)
        result_row["matched_keywords"] = "|".join(sorted(set(matched_kws)))
        result_row["intent_tag"] = intent
        result_row["is_coastal_district"] = is_coastal
        result_row["is_drought_district"] = is_drought

        results.append(result_row)

    log.info("Matched %d CRA tenders from %d total.", len(results), len(df))
    return pd.DataFrame(results), category_counts


# ─── Main ────────────────────────────────────────────────────────────────────

def main(input_dir: str, output_dir: str) -> None:
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    all_files = list(input_path.glob("*.csv"))
    if not all_files:
        log.error("No CSV files found in %s", input_dir)
        return

    log.info("Loading %d file(s)…", len(all_files))
    frames = []
    for f in sorted(all_files):
        try:
            frames.append(pd.read_csv(f, encoding="utf-8", low_memory=False))
            log.info("  Loaded: %s (%d rows)", f.name, frames[-1].shape[0])
        except Exception as exc:
            log.warning("  Skipped %s: %s", f.name, exc)

    raw = pd.concat(frames, ignore_index=True)
    log.info("Total rows loaded: %d", len(raw))

    filtered_df, cat_counts = filter_tenders(raw)

    out_csv = output_path / "cra_filtered_tenders.csv"
    filtered_df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    log.info("Saved filtered tenders → %s", out_csv)

    # Write category QA log
    log_path = output_path / "cra_filter_log.txt"
    with open(log_path, "w") as lf:
        lf.write(f"CRA Keyword Filter Run — {datetime.now().isoformat()}\n")
        lf.write(f"Input files     : {len(all_files)}\n")
        lf.write(f"Total raw rows  : {len(raw)}\n")
        lf.write(f"CRA matches     : {len(filtered_df)}\n\n")
        lf.write("Matches by category:\n")
        for cat, count in sorted(cat_counts.items(), key=lambda x: -x[1]):
            lf.write(f"  {cat:<35} {count}\n")
    log.info("Filter log → %s", log_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Filter Odisha tenders for CRA.")
    _base = Path(__file__).parent
    parser.add_argument("--input_dir", default=str(_base / "data" / "raw"))
    parser.add_argument("--output_dir", default=str(_base / "data" / "processed"))
    args = parser.parse_args()
    main(args.input_dir, args.output_dir)
