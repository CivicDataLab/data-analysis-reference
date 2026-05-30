# Odisha Climate-Resilient Agriculture (CRA) Procurement Analysis

A Python pipeline to identify, tag, and analyse climate-resilient agriculture (CRA) tenders from Odisha's eProcurement portal — adapted from [CivicDataLab's Assam MCH analysis framework](https://github.com/CivicDataLab/data-analysis-reference/tree/main/Assam_MCH_analysis/TENDERS).

**Research question:** Is public procurement spending reaching districts with the highest climate vulnerability, and is it oriented towards long-term resilience (mitigation) rather than just post-disaster repair?

---

## Repository structure

```
TENDERS/
├── cra_keywords.py               # Keyword taxonomy + district reference data
├── 01_keyword_filter.py          # Step 1: identify CRA tenders from raw data
├── 02_scheme_tagging.py          # Step 2: tag by government scheme & department
├── 03_geo_tagging.py             # Step 3: standardise districts, merge climate risk
├── 04_spend_analysis.py          # Step 4: aggregate spend tables + markdown report
├── 05_equity_gap_analysis.py     # Step 5: spend vs vulnerability equity check
├── data/
│   ├── raw/                      # Input CSVs (not committed — see Data Sources)
│   └── processed/                # Pipeline outputs (gitignored)
└── outputs/                      # Final tables, charts, reports
```

---

## Data sources

| File | Source | Notes |
|------|--------|-------|
| `data/raw/*.csv` | [Odisha eProcurement Portal](https://tendersodisha.gov.in) | Monthly tender CSVs; download manually or via scraper |
| `data/raw/odisha_total_tenders.csv` | Same portal | Full dataset for share-of-spend calculation |
| `data/raw/odisha_district_climate_risk.csv` | Odisha Climate Change Action Plan 2018 | Columns: `district_clean`, `drought_risk_score`, `cyclone_risk_score` |
| `data/raw/odisha_district_population.csv` | Census 2011 / 2021 | Columns: `district_clean`, `total_pop`, `agri_pop` |

> If reference CSVs are absent, the pipeline uses inline fallback values (clearly logged as warnings). Inline values are sourced from OCCAP 2018 and Census 2011 — update when better data is available.

---

## Pipeline steps

### Step 1 — Keyword filter (`01_keyword_filter.py`)
Scans tender work descriptions for CRA-relevant keywords across **8 thematic categories** (C1–C8). Applies negative keyword exclusions to remove false positives. Adds `matched_categories`, `matched_keywords`, `intent_tag`, `is_coastal_district`, `is_drought_district`.

### Step 2 — Scheme tagging (`02_scheme_tagging.py`)
Matches scheme names and acronyms (PMKSY, RKVY, NMSA, Odisha Millet Mission, BKKY, etc.) and normalises department names into high-level clusters.

### Step 3 — Geo-tagging (`03_geo_tagging.py`)
Standardises district spellings, extracts district names from free-text descriptions where the structured field is empty, and merges district-level climate risk scores.

### Step 4 — Spend analysis (`04_spend_analysis.py`)
Produces a multi-sheet Excel workbook and a markdown report with spend breakdowns by category, intent, scheme, district, and financial year.

### Step 5 — Equity gap analysis (`05_equity_gap_analysis.py`)
Computes CRA spend per lakh agricultural population, cross-tabulates with climate risk scores, and flags **equity gap districts** (high vulnerability + low spend density). Outputs a scatter chart and narrative findings.

---

## CRA keyword taxonomy (summary)

| Code | Theme | Example keywords |
|------|-------|-----------------|
| C1 | Water Conservation & Groundwater Recharge | check dam, farm pond, desilting, water harvesting |
| C2 | Irrigation & Water Infrastructure | lift irrigation, drip/sprinkler, canal, tube well |
| C3 | Drought Resilience | drought tolerant variety, DSR, soil moisture, rainfed |
| C4 | Flood & Cyclone Protection | embankment, saline embankment, mangrove, cyclone shelter |
| C5 | Climate-Smart Farming | SRI, submergence-tolerant rice, soil health card, CRA |
| C6 | Agroforestry & Watershed | watershed development, contour bund, CAMPA, plantation |
| C7 | Post-Harvest & Supply Chain | cold storage, solar pump, custom hiring centre |
| C8 | Scheme-Specific | PMKSY, RKVY, NMSA, Odisha Millet Mission, BKKY |

Full keyword lists, negative keywords, and intent-tag definitions are in `cra_keywords.py`.

---

## Odisha context notes

- Odisha has 30 districts. **Coastal districts** (Balasore, Bhadrak, Kendrapara, Jagatsinghpur, Puri, Khordha, Ganjam, Gajapati) face cyclone and salinity risk. Filter C4 tenders by coastal districts for higher precision.
- **KBK districts** (Kalahandi, Bolangir/Balangir, Nuapada, Koraput, Malkangiri, Nabarangpur, Rayagada) are chronically drought-prone and tribal — key equity gap candidates.
- **Key data gap**: Odisha's eProcurement portal (NICGEP) does not publish all departmental tenders centrally; Water Resources Department and Agriculture Department tenders may need separate scrapes.
- **Relevant schemes to watch**: PMKSY-HKKP (irrigation), RKVY-RAFTAAR, Odisha Millet Mission (tribal dryland farming), BKKY (direct farmer support), OCCAP project funds.

---

## Running the pipeline

```bash
# Install dependencies
pip install pandas openpyxl matplotlib

# Place monthly tender CSVs in data/raw/
# Then run each step in order:
python 01_keyword_filter.py
python 02_scheme_tagging.py
python 03_geo_tagging.py
python 04_spend_analysis.py
python 05_equity_gap_analysis.py
```

All scripts accept `--help` for argument details. Each step can also be run independently by pointing `--input` to a previously saved intermediate file.

---

## Known limitations & next steps

1. **OCR / non-machine-readable tenders**: Some tenders are uploaded as scanned PDFs; these are missed by keyword matching. Estimate scope of OCR gap.
2. **Duplicate tenders**: The same work may be re-tendered; de-duplication logic needed (match on tender ID + work description hash).
3. **Award vs estimate gap**: Use awarded amounts, not estimated amounts, for actual spend analysis. Award data is often delayed.
4. **Multi-district works**: Large irrigation schemes span multiple districts. Current logic tags the first district found.
5. **Keyword precision QA**: After first run, sample 50 tenders per category and compute precision; adjust keywords in `cra_keywords.py` accordingly.
6. **Extend to contracts/awards**: The current pipeline works on tenders (procurement intent). Linking to awarded contracts enables actual expenditure analysis.

---

## Relationship to Assam MCH framework

| MCH (Assam) | CRA (Odisha) |
|-------------|-------------|
| MCH keyword categories | CRA keyword categories (C1–C8) |
| IMR / MMR as outcome proxy | Combined climate risk score (drought + cyclone) |
| District-level equity gap: MCH spend vs infant mortality | District equity gap: CRA spend vs climate vulnerability |
| Scheme tags: NHM, PMSMA | Scheme tags: PMKSY, RKVY, Odisha Millet Mission |
| Intent: Treatment vs Prevention | Intent: DamageResponse vs Mitigation vs CapacityBuilding |

---

## Licence

Scripts: GPL v2 | Processed datasets: ODbL

Queries: open-contracting@civicdatalab.in
