"""
03_geo_tagging.py — District & Block Geo-Tagging for Odisha CRA Tenders
========================================================================
Step 3 of the CRA analysis pipeline.

Geocodes each tender to district and block using three independent signals,
then resolves conflicts. Methodology matches geocode_district.py +
geocode_blocks.py from the flood-data-ecosystem-Odisha pipeline.

Geographic reference: ODISHA_VILLAGES_MASTER.csv
  Columns used: vilnam_soi, block_name, gp_name, sdtname, dtname

District signals (each checked independently, then reconciled):
  1. external_reference — prefix / slug of the tender reference number
  2. title_description  — tender title + work description text
  3. location_field     — the district / location column in the data
  Each signal tries: district names → block names → subdistrict names

Conflict logic:
  All signals agree  → DISTRICT_FINALISED = that district
  Zero signals match → 'NA'
  Signals disagree   → 'CONFLICT'

Block geocoding runs only on tenders with a finalised district.
It searches village, block, GP, and subdistrict names scoped to that district.

INPUT
-----
  data/processed/cra_scheme_tagged.csv        (output of 02_scheme_tagging.py)
  [--villages_master]  ODISHA_VILLAGES_MASTER.csv
  [--risk_ref]         data/raw/odisha_district_climate_risk.csv  (optional)

OUTPUT
------
  data/processed/cra_geotagged.csv
    Adds columns:
      district_signal_ext   — district from external reference signal
      district_signal_text  — district from title/description signal
      district_signal_loc   — district from location/district field signal
      district_clean        — DISTRICT_FINALISED (title-case canonical name)
      district_source       — 'single' | 'conflict' | 'unknown'
      block_finalised       — block name (empty if not resolved)
      gp                    — gram panchayat (empty if not resolved)
      drought_risk_score    — from risk reference (1–5 scale)
      cyclone_risk_score    — from risk reference (1–5 scale)
      combined_risk_score   — mean of drought + cyclone scores

USAGE
-----
  python 03_geo_tagging.py
  python 03_geo_tagging.py --villages_master /path/to/ODISHA_VILLAGES_MASTER.csv
"""

import argparse
import logging
import re
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

_base = Path(__file__).parent

# ─── Inline climate risk fallback ────────────────────────────────────────────
# Maps raw district names (from villages master) → canonical shapefile names.
# Apply after district_clean is set so all downstream joins use one name.
CANONICAL_DISTRICT_MAP = {
    "Anugul":         "Angul",
    "Baleshwar":      "Balasore",
    "Balangir":       "Bolangir",
    "Jajapur":        "Jajpur",
    "Jagatsinghapur": "Jagatsinghpur",
    "Keonjhar":       "Kendujhar",
}

DEFAULT_RISK_DATA = [
    {"district_clean": "Balasore",      "drought_risk_score": 2, "cyclone_risk_score": 5},
    {"district_clean": "Bargarh",       "drought_risk_score": 4, "cyclone_risk_score": 1},
    {"district_clean": "Bhadrak",       "drought_risk_score": 2, "cyclone_risk_score": 5},
    {"district_clean": "Bolangir",      "drought_risk_score": 5, "cyclone_risk_score": 1},
    {"district_clean": "Boudh",         "drought_risk_score": 4, "cyclone_risk_score": 1},
    {"district_clean": "Cuttack",       "drought_risk_score": 2, "cyclone_risk_score": 3},
    {"district_clean": "Deogarh",       "drought_risk_score": 3, "cyclone_risk_score": 1},
    {"district_clean": "Dhenkanal",     "drought_risk_score": 3, "cyclone_risk_score": 2},
    {"district_clean": "Gajapati",      "drought_risk_score": 3, "cyclone_risk_score": 4},
    {"district_clean": "Ganjam",        "drought_risk_score": 3, "cyclone_risk_score": 5},
    {"district_clean": "Jagatsinghpur", "drought_risk_score": 2, "cyclone_risk_score": 5},
    {"district_clean": "Jajpur",        "drought_risk_score": 2, "cyclone_risk_score": 3},
    {"district_clean": "Jharsuguda",    "drought_risk_score": 3, "cyclone_risk_score": 1},
    {"district_clean": "Kalahandi",     "drought_risk_score": 5, "cyclone_risk_score": 1},
    {"district_clean": "Kandhamal",     "drought_risk_score": 3, "cyclone_risk_score": 2},
    {"district_clean": "Kendrapara",    "drought_risk_score": 2, "cyclone_risk_score": 5},
    {"district_clean": "Kendujhar",      "drought_risk_score": 3, "cyclone_risk_score": 1},
    {"district_clean": "Khordha",       "drought_risk_score": 2, "cyclone_risk_score": 4},
    {"district_clean": "Koraput",       "drought_risk_score": 3, "cyclone_risk_score": 2},
    {"district_clean": "Malkangiri",    "drought_risk_score": 3, "cyclone_risk_score": 1},
    {"district_clean": "Mayurbhanj",    "drought_risk_score": 2, "cyclone_risk_score": 1},
    {"district_clean": "Nabarangpur",   "drought_risk_score": 4, "cyclone_risk_score": 1},
    {"district_clean": "Nayagarh",      "drought_risk_score": 3, "cyclone_risk_score": 2},
    {"district_clean": "Nuapada",       "drought_risk_score": 5, "cyclone_risk_score": 1},
    {"district_clean": "Puri",          "drought_risk_score": 2, "cyclone_risk_score": 5},
    {"district_clean": "Rayagada",      "drought_risk_score": 3, "cyclone_risk_score": 2},
    {"district_clean": "Sambalpur",     "drought_risk_score": 3, "cyclone_risk_score": 1},
    {"district_clean": "Sonepur",       "drought_risk_score": 4, "cyclone_risk_score": 1},
    {"district_clean": "Sundargarh",    "drought_risk_score": 3, "cyclone_risk_score": 1},
    {"district_clean": "Angul",         "drought_risk_score": 3, "cyclone_risk_score": 2},
]


# ─── Geographic reference loading ────────────────────────────────────────────

def load_villages_master(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        log.warning("Villages master not found at %s — district geocoding will use location field only.", path)
        return None
    vm = pd.read_csv(path, encoding="utf-8").dropna(subset=["dtname"])
    vm.columns = [c.lower().strip() for c in vm.columns]
    log.info("Loaded villages master: %d rows, %d districts.", len(vm), vm["dtname"].nunique())
    return vm


def build_lookup_dicts(vm: pd.DataFrame) -> tuple[list, dict, dict, dict]:
    """Return (districts, blocks→district, subdists→district, village_dict)."""
    districts = list(vm["dtname"].dropna().unique())

    blocks_raw = vm[["block_name", "dtname"]].dropna().drop_duplicates()
    non_dup_blocks = blocks_raw.drop_duplicates("block_name", keep=False)
    blocks_dict = {
        row["block_name"].lower().strip(): row["dtname"]
        for _, row in non_dup_blocks.iterrows()
    }

    subdists_raw = vm[["sdtname", "dtname"]].dropna().drop_duplicates()
    non_dup_sdt = subdists_raw.drop_duplicates("sdtname", keep=False)
    subdists_dict = {
        row["sdtname"].lower().strip(): row["dtname"]
        for _, row in non_dup_sdt.iterrows()
    }

    villages_raw = vm[["vilnam_soi", "block_name", "gp_name", "dtname"]].dropna(subset=["vilnam_soi"])
    non_dup_vil = villages_raw.drop_duplicates("vilnam_soi", keep=False)
    village_dict = {
        re.sub(r'[^a-zA-Z]', '', row["vilnam_soi"]): {
            "block_name": row["block_name"],
            "gp_name": row.get("gp_name", ""),
            "dtname": row["dtname"],
        }
        for _, row in non_dup_vil.iterrows()
        if re.sub(r'[^a-zA-Z]', '', str(row["vilnam_soi"])) not in ("RIVER", "NO", "TOWN", "")
    }

    return districts, blocks_dict, subdists_dict, village_dict


def load_risk_reference(ref_path: Path) -> pd.DataFrame:
    if ref_path.exists():
        log.info("Loading district risk reference from %s", ref_path)
        df = pd.read_csv(ref_path)
        df.columns = [c.lower().strip() for c in df.columns]
        return df
    log.warning("Risk reference not found at %s — using inline defaults.", ref_path)
    return pd.DataFrame(DEFAULT_RISK_DATA)


# ─── Geocoding helpers ────────────────────────────────────────────────────────

def clean_slug(text: str) -> str:
    return re.sub(r'[^a-zA-Z0-9 \n.]', ' ', str(text)).lower()


def find_first_match(slug: str, names: list[str]) -> str | None:
    for name in names:
        if re.search(r'\b' + re.escape(name.lower().strip()) + r'\b', slug):
            return name
    return None


def geocode_signal(slug: str, districts: list, blocks_dict: dict, subdists_dict: dict) -> str | None:
    """Try district → block → subdistrict; return district name (as in master) or None."""
    hit = find_first_match(slug, [d.lower() for d in districts])
    if hit:
        return next(d for d in districts if d.lower() == hit)
    hit = find_first_match(slug, list(blocks_dict.keys()))
    if hit:
        return blocks_dict[hit]
    hit = find_first_match(slug, list(subdists_dict.keys()))
    if hit:
        return subdists_dict[hit]
    return None


def resolve_district(sig_ext, sig_text, sig_loc) -> tuple[str, str]:
    """Return (DISTRICT_FINALISED, district_source)."""
    signals = {s for s in [sig_ext, sig_text, sig_loc] if s and s != "NA"}
    if len(signals) == 1:
        return list(signals)[0], "single"
    if len(signals) == 0:
        return "NA", "unknown"
    return "CONFLICT", "conflict"


# ─── Block geocoding ──────────────────────────────────────────────────────────

def geocode_blocks(df: pd.DataFrame, vm: pd.DataFrame,
                   desc_col: str, ref_col: str | None) -> pd.DataFrame:
    """Add block_finalised and gp columns, scoped per finalised district."""
    df["block_finalised"] = ""
    df["gp"] = ""

    clean_pat = re.compile(r'[^a-zA-Z0-9 \n.]')
    noise_pat = re.compile(r'\(pt\)|\n', re.IGNORECASE)

    for focus_district in df["district_clean"].unique():
        if focus_district in ("NA", "CONFLICT", ""):
            continue

        dist_vm = vm[vm["dtname"] == focus_district]
        if dist_vm.empty:
            continue

        # Build district-scoped lookups
        block_dict = {
            row["block_name"]: row["dtname"]
            for _, row in dist_vm[["block_name", "dtname"]].drop_duplicates().iterrows()
            if pd.notna(row["block_name"])
        }
        gp_dict = {
            row["gp_name"]: row["dtname"]
            for _, row in dist_vm[["gp_name", "dtname"]].drop_duplicates().iterrows()
            if pd.notna(row["gp_name"])
        }
        village_dict = {}
        for _, row in dist_vm.iterrows():
            vil = re.sub(r'[^a-zA-Z]', '', str(row.get("vilnam_soi", "")))
            if vil and vil not in ("RIVER", "NO", "TOWN"):
                village_dict[vil] = {
                    "block_name": row.get("block_name", ""),
                    "gp_name": row.get("gp_name", ""),
                }

        mask = df["district_clean"] == focus_district
        for idx, row in df[mask].iterrows():
            ref_part = str(row[ref_col]) if ref_col else ""
            desc_part = str(row.get(desc_col, ""))
            slug = clean_slug(ref_part + " " + desc_part)

            tender_block = ""
            tender_gp = ""

            # Village → derives block
            for vil, meta in village_dict.items():
                vil_search = noise_pat.sub(" ", vil.lower())
                if re.search(r'\b' + re.escape(vil_search.strip()) + r'\b', slug):
                    tender_block = meta["block_name"]
                    tender_gp = meta["gp_name"]
                    break

            # Block (explicit match overrides village-derived block)
            for block in block_dict:
                blk_search = noise_pat.sub(" ", block.lower())
                if re.search(r'\b' + re.escape(blk_search.strip()) + r'\b', slug):
                    tender_block = block
                    break

            # GP
            if not tender_gp:
                for gp in gp_dict:
                    gp_search = noise_pat.sub(" ", gp.lower())
                    if re.search(r'\b' + re.escape(gp_search.strip()) + r'\b', slug):
                        tender_gp = gp
                        break

            df.loc[idx, "block_finalised"] = tender_block
            df.loc[idx, "gp"] = tender_gp

    return df


# ─── Column detection helpers ─────────────────────────────────────────────────

def detect_col(columns: list[str], candidates: list[str]) -> str | None:
    for c in columns:
        if c.lower().strip() in candidates:
            return c
    return None


# ─── Main ─────────────────────────────────────────────────────────────────────

def main(input_file: str, output_file: str, risk_ref: str, villages_master: str) -> None:
    df = pd.read_csv(input_file, low_memory=False)
    log.info("Loaded %d tenders.", len(df))

    vm = load_villages_master(Path(villages_master))

    if vm is not None:
        districts, blocks_dict, subdists_dict, _ = build_lookup_dicts(vm)
        log.info("Lookup built: %d districts, %d blocks, %d subdistricts.",
                 len(districts), len(blocks_dict), len(subdists_dict))
    else:
        districts, blocks_dict, subdists_dict = [], {}, {}

    # Detect relevant columns
    ref_col  = detect_col(list(df.columns), ["tender_id", "tender_no", "external_reference",
                                              "tender_externalreference", "ref_no"])
    desc_col = detect_col(list(df.columns), ["work_description", "description", "tender_title",
                                              "work_name", "workdescription", "subject"]) or df.columns[0]
    loc_col  = detect_col(list(df.columns), ["district", "location", "district_name"])

    log.info("Columns → ref: %s | desc: %s | location: %s", ref_col, desc_col, loc_col)

    # ── Three-signal district geocoding ──────────────────────────────────────
    sig_ext_list, sig_text_list, sig_loc_list = [], [], []

    for _, row in df.iterrows():
        ref_slug  = clean_slug(str(row[ref_col]))  if ref_col  else ""
        text_slug = clean_slug(str(row.get(desc_col, "")))
        loc_slug  = clean_slug(str(row[loc_col]))  if loc_col  else ""

        sig_ext_list.append(geocode_signal(ref_slug,  districts, blocks_dict, subdists_dict))
        sig_text_list.append(geocode_signal(text_slug, districts, blocks_dict, subdists_dict))
        sig_loc_list.append(geocode_signal(loc_slug,  districts, blocks_dict, subdists_dict))

    df["district_signal_ext"]  = sig_ext_list
    df["district_signal_text"] = sig_text_list
    df["district_signal_loc"]  = sig_loc_list

    # ── Resolve to final district ─────────────────────────────────────────────
    resolved = [
        resolve_district(e, t, l)
        for e, t, l in zip(sig_ext_list, sig_text_list, sig_loc_list)
    ]
    df["district_clean"]  = [r[0].title() if r[0] not in ("NA", "CONFLICT") else r[0] for r in resolved]
    df["district_source"] = [r[1] for r in resolved]

    df["district_clean"] = df["district_clean"].map(
        lambda x: CANONICAL_DISTRICT_MAP.get(x, x)
    )

    na_n       = (df["district_clean"] == "NA").sum()
    conflict_n = (df["district_clean"] == "CONFLICT").sum()
    log.info("District results — resolved: %d | NA: %d | CONFLICT: %d",
             len(df) - na_n - conflict_n, na_n, conflict_n)

    # ── Block geocoding ───────────────────────────────────────────────────────
    if vm is not None:
        # Align district_clean capitalisation with dtname in master
        dtname_map = {d.title(): d for d in districts}
        df["_dtname_key"] = df["district_clean"].map(dtname_map).fillna(df["district_clean"])
        df_temp = df.copy()
        df_temp["district_clean"] = df_temp["_dtname_key"]
        df_temp = geocode_blocks(df_temp, vm, desc_col, ref_col)
        df["block_finalised"] = df_temp["block_finalised"]
        df["gp"] = df_temp["gp"]
        df.drop(columns=["_dtname_key"], inplace=True)
        log.info("Block geocoding complete — %d tenders matched a block.",
                 (df["block_finalised"] != "").sum())
    else:
        df["block_finalised"] = ""
        df["gp"] = ""

    # ── Merge climate risk scores ─────────────────────────────────────────────
    risk_df = load_risk_reference(Path(risk_ref))
    df = df.merge(risk_df, on="district_clean", how="left")

    if "drought_risk_score" in df.columns and "cyclone_risk_score" in df.columns:
        df["combined_risk_score"] = (
            df["drought_risk_score"].fillna(0) + df["cyclone_risk_score"].fillna(0)
        ) / 2

    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    log.info("Saved geo-tagged tenders → %s", output_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",           default=str(_base / "data" / "processed" / "cra_scheme_tagged.csv"))
    parser.add_argument("--output",          default=str(_base / "data" / "processed" / "cra_geotagged.csv"))
    parser.add_argument("--risk_ref",        default=str(_base / "data" / "raw" / "odisha_district_climate_risk.csv"))
    parser.add_argument("--villages_master", default=str(
        Path.home() / "/Users/saurabhlevin/Deployment/data-analysis-reference/odisha_cra_analysis/TENDERS/data/reference/odisha_villages.csv"
    ))
    args = parser.parse_args()
    main(args.input, args.output, args.risk_ref, args.villages_master)
