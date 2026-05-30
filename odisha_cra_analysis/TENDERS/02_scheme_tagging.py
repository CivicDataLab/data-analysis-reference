"""
02_scheme_tagging.py — Odisha CRA Scheme & Department Tagging
=============================================================
Step 2 of the CRA analysis pipeline.

INPUT
-----
  data/processed/cra_filtered_tenders.csv   (output of 01_keyword_filter.py)

OUTPUT
------
  data/processed/cra_scheme_tagged.csv
    Adds columns:
      scheme_tag        — matched scheme name or 'Unschemed'
      executing_dept    — normalised department bucket
      dept_cluster      — high-level group (Water Resources / Agriculture /
                          Horticulture / Forest / Rural Dev / Other)

USAGE
-----
  python 02_scheme_tagging.py
"""

import argparse
import logging
import re
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ─── Scheme patterns (check in order — first match wins) ─────────────────────

SCHEME_PATTERNS = [
    # National / Centrally Sponsored
    ("PMKSY-HKKP",      r"pmksy|hkkp|har khet ko pani|more crop per drop"),
    ("PMKSY-WDC",       r"watershed development component|wdc"),
    ("PMKSY-AIBP",      r"accelerated irrigation benefit|aibp"),
    ("RKVY",            r"rkvy|rashtriya krishi vikas"),
    ("NFSM",            r"nfsm|national food security mission"),
    ("NMSA",            r"nmsa|national mission.*sustainable agri"),
    ("PMFBY",           r"pmfby|fasal bima|crop insurance"),
    ("MGNREGS",         r"mgnregs|mgnrega|mahatma gandhi.*rural employ"),
    ("NABARD-RIDF",     r"nabard|ridf|rural infrastructure development fund"),
    ("CAMPA",           r"campa|compensatory afforestation"),
    # Odisha State Schemes
    ("Odisha Millet Mission",           r"odisha millet mission|omm|millet"),
    ("BKKY",                            r"biju krushak|bkky"),
    ("KALIA",                           r"kalia|krushak assistance.*livelihood"),
    ("Odisha Lift Irrigation (OLIC)",   r"olic|odisha lift irrigation"),
    ("RRR Water Bodies",                r"\brrr\b|renovation.*restoration.*recharge"),
    ("MATY",                            r"\bmaty\b"),
    # Catch-all
    ("State Plan",                      r"state plan|annual plan|state budget"),
]

# ─── Department clustering ────────────────────────────────────────────────────

DEPT_CLUSTERS = {
    "Water Resources": [
        "water resources", "irrigation", "minor irrigation", "command area",
        "water development", "bwssb", "owssb",
    ],
    "Agriculture": [
        "agriculture", "directorate of agriculture", "krishi",
        "agri ", "seed ", "fertiliser",
    ],
    "Horticulture": [
        "horticulture", "cashew", "sericulture",
    ],
    "Forest & Environment": [
        "forest", "environment", "ecology", "wildlife",
    ],
    "Rural Development": [
        "rural development", "panchayati raj", "drda", "zilla parishad",
        "block development", "gram panchayat",
    ],
    "Fisheries & AH": [
        "fisheries", "animal husbandry", "dairy",
    ],
}


def normalise(text: str) -> str:
    return re.sub(r"\s+", " ", str(text).lower().strip())


def tag_scheme(row: pd.Series, desc_col: str) -> str:
    text = normalise(str(row.get(desc_col, "")) + " " + str(row.get("matched_keywords", "")))
    for scheme_name, pattern in SCHEME_PATTERNS:
        if re.search(pattern, text):
            return scheme_name
    return "Unschemed"


def cluster_dept(dept_val: str) -> str:
    d = normalise(dept_val)
    for cluster, keywords in DEPT_CLUSTERS.items():
        if any(kw in d for kw in keywords):
            return cluster
    return "Other"


def main(input_file: str, output_file: str) -> None:
    df = pd.read_csv(input_file, low_memory=False)
    log.info("Loaded %d filtered tenders.", len(df))

    # Detect description column
    desc_col = next(
        (c for c in df.columns if "description" in c.lower() or "work_name" in c.lower()),
        df.columns[0],
    )

    df["scheme_tag"] = df.apply(lambda r: tag_scheme(r, desc_col), axis=1)

    dept_col = next(
        (c for c in df.columns if "department" in c.lower() or "dept" in c.lower()),
        None,
    )
    if dept_col:
        df["dept_cluster"] = df[dept_col].apply(cluster_dept)
    else:
        df["dept_cluster"] = "Unknown"

    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    log.info("Saved scheme-tagged tenders → %s", output_file)

    # Summary
    log.info("\nScheme distribution:\n%s", df["scheme_tag"].value_counts().to_string())
    log.info("\nDept cluster distribution:\n%s", df["dept_cluster"].value_counts().to_string())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    _base = Path(__file__).parent
    parser.add_argument("--input",  default=str(_base / "data" / "processed" / "cra_filtered_tenders.csv"))
    parser.add_argument("--output", default=str(_base / "data" / "processed" / "cra_scheme_tagged.csv"))
    args = parser.parse_args()
    main(args.input, args.output)
