"""
03_geo_tagging.py — District-Level Geo-Tagging for Odisha CRA Tenders
======================================================================
Step 3 of the CRA analysis pipeline.

Extracts the Odisha district name from the tender's work description
when the district field is empty or ambiguous, then merges climate
vulnerability indicators (drought severity, cyclone frequency) for
spatial analysis.

INPUT
-----
  data/processed/cra_scheme_tagged.csv   (output of 02_scheme_tagging.py)
  data/raw/odisha_district_climate_risk.csv  (reference file — see README)

OUTPUT
------
  data/processed/cra_geotagged.csv
    Adds columns:
      district_clean        — standardised district name
      district_source       — 'field' | 'extracted' | 'unknown'
      drought_risk_score    — from reference file (1–5 scale)
      cyclone_risk_score    — from reference file (1–5 scale)
      combined_risk_score   — simple mean of drought + cyclone scores

USAGE
-----
  python 03_geo_tagging.py
"""

import argparse
import logging
import re
from pathlib import Path

import pandas as pd

from cra_keywords import ODISHA_DISTRICTS

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ─── District name standardisation map ───────────────────────────────────────
# Maps raw spellings → canonical name (matching district risk reference file)

DISTRICT_ALIASES = {
    "balasore": "Balasore",   "baleswar": "Balasore",
    "bargarh": "Bargarh",     "baragarh": "Bargarh",
    "bhadrak": "Bhadrak",
    "bolangir": "Bolangir",   "balangir": "Bolangir",
    "boudh": "Boudh",         "baudh": "Boudh",
    "cuttack": "Cuttack",
    "deogarh": "Deogarh",     "debagarh": "Deogarh",
    "dhenkanal": "Dhenkanal",
    "gajapati": "Gajapati",
    "ganjam": "Ganjam",
    "jagatsinghpur": "Jagatsinghpur",
    "jajpur": "Jajpur",
    "jharsuguda": "Jharsuguda",
    "kalahandi": "Kalahandi",
    "kandhamal": "Kandhamal", "phulbani": "Kandhamal",
    "kendrapara": "Kendrapara",
    "keonjhar": "Keonjhar",   "kendujhar": "Keonjhar",
    "khordha": "Khordha",     "khurda": "Khordha",
    "koraput": "Koraput",
    "malkangiri": "Malkangiri",
    "mayurbhanj": "Mayurbhanj",
    "nabarangpur": "Nabarangpur",
    "nayagarh": "Nayagarh",
    "nuapada": "Nuapada",
    "puri": "Puri",
    "rayagada": "Rayagada",
    "sambalpur": "Sambalpur",
    "sonepur": "Sonepur",     "subarnapur": "Sonepur",
    "sundargarh": "Sundargarh",
    "angul": "Angul",
}

ALL_DISTRICT_NAMES = list(DISTRICT_ALIASES.keys())


def normalise(text: str) -> str:
    return re.sub(r"\s+", " ", str(text).lower().strip())


def extract_district_from_text(text: str) -> str | None:
    """Scan work description for any known district name."""
    norm = normalise(text)
    for alias in ALL_DISTRICT_NAMES:
        if re.search(r"\b" + re.escape(alias) + r"\b", norm):
            return DISTRICT_ALIASES[alias]
    return None


def standardise_district(raw: str) -> tuple[str, str]:
    """
    Returns (canonical_district, source) where source is 'field' or 'alias'.
    """
    norm = normalise(raw)
    if norm in DISTRICT_ALIASES:
        return DISTRICT_ALIASES[norm], "field"
    # Partial match
    for alias, canonical in DISTRICT_ALIASES.items():
        if alias in norm:
            return canonical, "field"
    return raw.title(), "field"


# ─── Default climate risk reference (inline fallback) ────────────────────────
# Replace with data/raw/odisha_district_climate_risk.csv if available.
# Sources: Odisha Climate Change Action Plan 2018; IMD district profiles

DEFAULT_RISK_DATA = {
    "Balasore":       {"drought_risk": 2, "cyclone_risk": 5},
    "Bargarh":        {"drought_risk": 4, "cyclone_risk": 1},
    "Bhadrak":        {"drought_risk": 2, "cyclone_risk": 5},
    "Bolangir":       {"drought_risk": 5, "cyclone_risk": 1},
    "Boudh":          {"drought_risk": 4, "cyclone_risk": 1},
    "Cuttack":        {"drought_risk": 2, "cyclone_risk": 3},
    "Deogarh":        {"drought_risk": 3, "cyclone_risk": 1},
    "Dhenkanal":      {"drought_risk": 3, "cyclone_risk": 2},
    "Gajapati":       {"drought_risk": 3, "cyclone_risk": 4},
    "Ganjam":         {"drought_risk": 3, "cyclone_risk": 5},
    "Jagatsinghpur":  {"drought_risk": 2, "cyclone_risk": 5},
    "Jajpur":         {"drought_risk": 2, "cyclone_risk": 3},
    "Jharsuguda":     {"drought_risk": 3, "cyclone_risk": 1},
    "Kalahandi":      {"drought_risk": 5, "cyclone_risk": 1},
    "Kandhamal":      {"drought_risk": 3, "cyclone_risk": 2},
    "Kendrapara":     {"drought_risk": 2, "cyclone_risk": 5},
    "Keonjhar":       {"drought_risk": 3, "cyclone_risk": 1},
    "Khordha":        {"drought_risk": 2, "cyclone_risk": 4},
    "Koraput":        {"drought_risk": 3, "cyclone_risk": 2},
    "Malkangiri":     {"drought_risk": 3, "cyclone_risk": 1},
    "Mayurbhanj":     {"drought_risk": 2, "cyclone_risk": 1},
    "Nabarangpur":    {"drought_risk": 4, "cyclone_risk": 1},
    "Nayagarh":       {"drought_risk": 3, "cyclone_risk": 2},
    "Nuapada":        {"drought_risk": 5, "cyclone_risk": 1},
    "Puri":           {"drought_risk": 2, "cyclone_risk": 5},
    "Rayagada":       {"drought_risk": 3, "cyclone_risk": 2},
    "Sambalpur":      {"drought_risk": 3, "cyclone_risk": 1},
    "Sonepur":        {"drought_risk": 4, "cyclone_risk": 1},
    "Sundargarh":     {"drought_risk": 3, "cyclone_risk": 1},
    "Angul":          {"drought_risk": 3, "cyclone_risk": 2},
}


def load_risk_reference(ref_path: Path) -> pd.DataFrame:
    if ref_path.exists():
        log.info("Loading district risk reference from %s", ref_path)
        return pd.read_csv(ref_path)
    log.warning("Risk reference not found at %s — using inline defaults.", ref_path)
    rows = [
        {"district_clean": d, "drought_risk_score": v["drought_risk"],
         "cyclone_risk_score": v["cyclone_risk"]}
        for d, v in DEFAULT_RISK_DATA.items()
    ]
    return pd.DataFrame(rows)


def main(input_file: str, output_file: str, risk_ref: str) -> None:
    df = pd.read_csv(input_file, low_memory=False)
    log.info("Loaded %d tenders.", len(df))

    desc_col = next(
        (c for c in df.columns if "description" in c.lower() or "work_name" in c.lower()),
        df.columns[0],
    )
    district_col = next(
        (c for c in df.columns if "district" in c.lower()), None
    )

    district_clean_list, source_list = [], []

    for _, row in df.iterrows():
        raw_district = str(row.get(district_col, "")) if district_col else ""
        text = str(row.get(desc_col, ""))

        if raw_district and raw_district.lower() not in ("nan", ""):
            canonical, src = standardise_district(raw_district)
        else:
            extracted = extract_district_from_text(text)
            if extracted:
                canonical, src = extracted, "extracted"
            else:
                canonical, src = "Unknown", "unknown"

        district_clean_list.append(canonical)
        source_list.append(src)

    df["district_clean"] = district_clean_list
    df["district_source"] = source_list

    # Merge climate risk scores
    risk_df = load_risk_reference(Path(risk_ref))
    risk_df.columns = [c.lower().strip() for c in risk_df.columns]
    df = df.merge(risk_df, on="district_clean", how="left")

    if "drought_risk_score" in df.columns and "cyclone_risk_score" in df.columns:
        df["combined_risk_score"] = (
            df["drought_risk_score"].fillna(0) + df["cyclone_risk_score"].fillna(0)
        ) / 2
    else:
        df["combined_risk_score"] = None

    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    log.info("Saved geo-tagged tenders → %s", output_file)

    unknown_pct = (df["district_source"] == "unknown").mean() * 100
    log.info("District tagging: %.1f%% unknown — review 'extracted' rows manually.", unknown_pct)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    _base = Path(__file__).parent
    parser.add_argument("--input",    default=str(_base / "data" / "processed" / "cra_scheme_tagged.csv"))
    parser.add_argument("--output",   default=str(_base / "data" / "processed" / "cra_geotagged.csv"))
    parser.add_argument("--risk_ref", default=str(_base / "data" / "raw" / "odisha_district_climate_risk.csv"))
    args = parser.parse_args()
    main(args.input, args.output, args.risk_ref)
