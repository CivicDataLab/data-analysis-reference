import pandas as pd
import os
import re
import dateutil.parser
import glob

# input_df - after the scraper code is run
data_path = os.getcwd() + r'/Assam_MCH_analysis/TENDERS/data/monthly_tenders/'

# -----------------------------
# Helpers
# -----------------------------

def populate_keyword_dict(keyword_list):
    return {keyword: 0 for keyword in keyword_list}

def normalize_text(text: str) -> str:
    """
    Lowercase, strip non-alphanumeric (except space), collapse spaces.
    Used for both tender text and scheme identifiers.
    """
    if pd.isna(text):
        text = ""
    text = str(text)
    text = re.sub(r'[^a-zA-Z0-9\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.lower().strip()

def text_contains_pattern(text_norm: str, pattern: str) -> bool:
    """
    Decide how to search:
    - For short, single word tokens (<=4 chars, no spaces): use word-boundary regex.
    - For longer / multi-word phrases: use simple substring match on normalized text.
    """
    pat_norm = normalize_text(pattern)
    if not pat_norm:
        return False

    if len(pat_norm) <= 4 and " " not in pat_norm:
        # short acronym-like token: nhm, jsy, uip, etc.
        return re.search(r"\b%s\b" % re.escape(pat_norm), text_norm) is not None

    # multi-word or longer phrase, or spaced acronym: substring match
    return pat_norm in text_norm

def enrich_scheme_patterns(schemes_identifier: dict) -> dict:
    """
    For acronym-like patterns (alpha-only, no spaces), also add a spaced-out version
    like 'n h m' so that 'N.H.M.' -> 'n h m' can be matched after normalization.
    """
    enriched = {}
    for scheme, patterns in schemes_identifier.items():
        new_patterns = set()
        for p in patterns:
            new_patterns.add(p)
            base = p.replace(" ", "")
            if base.isalpha() and len(base) <= 6:
                spaced = " ".join(list(base.lower()))
                new_patterns.add(spaced)
        enriched[scheme] = list(new_patterns)
    return enriched

def find_schemes_for_tender(tender_text: str, schemes_identifier: dict) -> str:
    """
    Given the concatenated tender text and SCHEMES_Identifier mapping,
    return a ';'-joined list of identified schemes (or '' if none).
    """
    text_norm = normalize_text(tender_text)
    matched = set()

    for scheme, patterns in schemes_identifier.items():
        for pat in patterns:
            if text_contains_pattern(text_norm, pat):
                matched.add(scheme)
                break

    if not matched:
        return ""
    return ";".join(sorted(matched))

# -----------------------------
# MCH keyword logic
# -----------------------------

# Strong signals: if any of these hit, we are very likely in MCH land
CORE_MCH_KEYWORDS = [
    # Maternal & delivery
    "maternal",
    "maternity",
    "mother and child",
    "maternity and child health",
    "pregnant woman",
    "pregnant women",
    "pregnancy care",
    "institutional delivery",
    "delivery point",
    "obstetric",
    "obstetrics",
    "labour room",
    "labor room",
    "labour ward",
    "maternity ward",

    # Newborn & neonatal care
    "newborn",
    "neonatal",
    "infant",
    "nicu",
    "neonatal intensive care unit",
    "sncu",
    "special newborn care unit",
    "newborn care unit",
    "newborn stabilization unit",
]

ANC_PNC_KEYWORDS = [
    "antenatal care",
    "antenatal clinic",
    "anc clinic",
    "anc check-up",
    "anc check up",
    "postnatal care",
    "pnc visit",
    "pnc clinic",
    "gestational diabetes",
    "screening of gestational diabetes",
    "pregnant women screening",
]

IMM_CHILD_KEYWORDS = [
    # Immunization & child health
    "immunization",
    "immunisation",
    "vaccination",
    "vaccine",
    "cold chain",
    "ice lined refrigerator",
    "ilr",
    "deep freezer",
    "vaccine carrier",
    "cold box",
    "child health",
    "child health care",
    "child health screening",
    "school health programme",
    "school health program",
    "rbsk",
    "rbsk screening",
    "deic centre",
    "deic center",
    "early intervention centre",
    "early intervention center",
]

NUTRITION_ICDS_KEYWORDS = [
    "nutrition rehabilitation",
    "growth monitoring",
    "malnutrition reduction",
    "anganwadi centre",
    "anganwadi center",
    "icds centre",
    "icds center",
    "poshan",
    "poshan abhiyaan",
]

SCHEME_EXPLICIT_KEYWORDS = [
    # Maternal / child schemes
    "jssk",
    "janani shishu suraksha karyakram",
    "jsy",
    "janani suraksha yojana",
    "pmsma",
    "pmmvy",
    "suman programme",
    "suman maternity",
    "mamoni",
    "mamoni scheme",
    "mamata kit",
    "mamata",
    "sneha sparsha",
    "operation smile",
    "assam free diagnostics",
    "national maternity benefit scheme",
    "nmbs",
    "laqshya",
    "laqshya guideline",
    "equipment under maternal health",
    "maternal health equipment",
    "rch programme",
    "rch program",
    "reproductive and child health",
]

# ALL positive keywords actually used for counting
POSITIVE_KEYWORDS = list(dict.fromkeys(
    CORE_MCH_KEYWORDS
    + ANC_PNC_KEYWORDS
    + IMM_CHILD_KEYWORDS
    + NUTRITION_ICDS_KEYWORDS
    + SCHEME_EXPLICIT_KEYWORDS
))

# Keep this as you had it, or add real negatives later (veterinary, animal, etc.)
NEGATIVE_KEYWORDS = []


# -----------------------------
# MCH filter
# -----------------------------

def mch_filter(row):
    """
    :param row: row of the dataframe that contains tender title, work description
    :return: Tuple of (is_mch_tender, positive_kw_dict, negative_kw_dict) for every row
    """
    positive_keywords_dict = populate_keyword_dict(POSITIVE_KEYWORDS)
    negative_keywords_dict = populate_keyword_dict(NEGATIVE_KEYWORDS)

    tender_slug = f"{row.get('tender_externalreference', '')} {row.get('tender_title', '')} {row.get('Work Description', '')}"
    tender_slug_norm = normalize_text(tender_slug)

    # group-level scores
    core_hits = 0
    anc_pnc_hits = 0
    imm_child_hits = 0
    nutrition_hits = 0
    scheme_hits = 0

    # Prebuild sets for quick membership checks
    core_set = set(CORE_MCH_KEYWORDS)
    anc_pnc_set = set(ANC_PNC_KEYWORDS)
    imm_child_set = set(IMM_CHILD_KEYWORDS)
    nutrition_set = set(NUTRITION_ICDS_KEYWORDS)
    scheme_set = set(SCHEME_EXPLICIT_KEYWORDS)

    # Positive keywords
    for keyword in POSITIVE_KEYWORDS:
        kw_norm = normalize_text(keyword)
        if not kw_norm:
            count = 0
        else:
            count = len(re.findall(r"\b%s\b" % re.escape(kw_norm), tender_slug_norm))
        positive_keywords_dict[keyword] = count

        if count > 0:
            if keyword in core_set:
                core_hits += count
            elif keyword in anc_pnc_set:
                anc_pnc_hits += count
            elif keyword in imm_child_set:
                imm_child_hits += count
            elif keyword in nutrition_set:
                nutrition_hits += count
            elif keyword in scheme_set:
                scheme_hits += count

    # Negative keywords (if you add any later)
    for keyword in NEGATIVE_KEYWORDS:
        kw_norm = normalize_text(keyword)
        if not kw_norm:
            count = 0
        else:
            count = len(re.findall(r"\b%s\b" % re.escape(kw_norm), tender_slug_norm))
        negative_keywords_dict[keyword] = count

    # Decision logic:
    # 1. Any core / ANC / child / neonatal keyword → MCH
    # 2. OR schemes + some supporting context (nutrition / child / ANC or generic health facility)
    is_mch_tender = False

    strong_signal = (core_hits > 0) or (anc_pnc_hits > 0) or (imm_child_hits > 0)
    support_signal = (nutrition_hits > 0)
    health_context = any(
        h in tender_slug_norm
        for h in [
            "phc", "chc", "sub centre", "sub-center", "subcentre",
            "civil hospital", "district hospital", "sdch", "mdch",
            "medical college", "health centre", "health center"
        ]
    )

    if strong_signal:
        is_mch_tender = True
    elif scheme_hits > 0 and (strong_signal or support_signal or health_context):
        # scheme-only hits allowed only if there is some health-ish context
        is_mch_tender = True

    return str(is_mch_tender), str(positive_keywords_dict), str(negative_keywords_dict)


# -----------------------------
# Main process
# -----------------------------

csvs = glob.glob(data_path + '*.csv')
print('Total CSVs to process: ', len(csvs))

for csv in csvs:
    filename  = csv.split(r'/')[-1]
    filename  = re.split(r'\\', csv)[-1]
    input_df = pd.read_csv(csv)

    # De-Duplication (basic)
    input_df = input_df.drop_duplicates()
    tender_ids = input_df["Tender ID"]
    '''
    # MCH Keywords (global so mch_filter sees them)
    global POSITIVE_KEYWORDS
    POSITIVE_KEYWORDS = [
        # Core MCH concepts
        "maternal",
        "maternity",
        "mother and child",
        "maternity and child health",
        "pregnant woman",
        "pregnant women",
        "pregnancy care",
        "institutional delivery",
        "labour room",
        "labor room",
        "labour ward",
        "maternity ward",
        "newborn",
        "neonatal",
        "infant",
        "nicu",
        "sncu",
        "child health",
        "paediatric",
        "pediatric",

        # ANC / PNC
        "antenatal care",
        "antenatal clinic",
        "anc check-up",
        "anc clinic",
        "postnatal care",
        "pnc visit",

        # Immunization & child screening
        "immunization",
        "immunisation",
        "vaccination",
        "vaccine",
        "cold chain",
        "ice lined refrigerator",
        "ilr",
        "deep freezer",
        "vaccine carrier",
        "cold box",

        # Nutrition & poshan-type work
        "nutrition rehabilitation",
        "growth monitoring",
        "malnutrition reduction",
        "anganwadi centre",
        "anganwadi center",
        "icds centre",
        "icds center",

        # Explicit scheme tokens that actually show up in your dataset
        "jssk",                 # JSSK drugs, diet, etc.
        "samahar kit",          # under JSSK diet programme
        "poshan",               # POSHAN Abhiyaan
        "rbsk",                 # RBSK printing, screening materials
        "rch register",         # RCH printing
        "rch programme",
        "nhm",                  # NHM health infra that supports MCH
        "nrhm",
        "mamoni",               # Mamoni scheme (Assam)

        # Other scheme names you may start seeing in later data dumps
        "asha worker",
        "asha training",
        "asha incentive",
        "janani suraksha yojana",
        "jsy",
        "janani shishu suraksha karyakram",
        "pmsma",
        "mission indradhanush",
        "intensified mission indradhanush",
        "poshan abhiyaan",
        "pmmvy",
        "suman programme",
        "suman maternity",
        "mcts",
        "mamata kit",
        "sneha sparsha",
        "congenital heart disease scheme",
        "operation smile",
        "majoni scheme",
        "assam free diagnostics",
        "national maternity benefit scheme",
        "rch",
    ]

    global NEGATIVE_KEYWORDS
    NEGATIVE_KEYWORDS = []
    '''
    # Apply MCH filter
    mch_filter_tuples = input_df.apply(mch_filter, axis=1)
    input_df.loc[:, 'is_mch_tender'] = [var[0] for var in list(mch_filter_tuples)]
    input_df.loc[:, 'positive_keywords_dict'] = [var[1] for var in list(mch_filter_tuples)]
    input_df.loc[:, 'negative_keywords_dict'] = [var[2] for var in list(mch_filter_tuples)]

    # Removing tenders from certain departments that are not related to mch management.
    idea_frm_tenders_df = input_df[
        (input_df.is_mch_tender == 'True') &
        (~input_df.Department.isin(["Directorate of Agriculture and Assam Seed Corporation",
                                    "Department of Handloom Textile and Sericulture"]))
    ]
    idea_frm_tenders_df = idea_frm_tenders_df.loc[idea_frm_tenders_df['Status'] == 'Accepted-AOC']

    print('Number of mch related tenders filtered: ', idea_frm_tenders_df.shape[0])
    if idea_frm_tenders_df.shape[0] == 0:
        continue

    # -----------------------------
    # Identify scheme related information (IMPROVED LOGIC)
    # -----------------------------
    SCHEMES_Identifier = {
        # Immunization & child health
        "UIP": [
            "universal immunization programme",
            "universal immunisation programme",
            "UIP",
            "uip",
            "vaccine",
            "immunization",
            "immunisation",
            "cold chain",
            "ice lined refrigerator",
            "ilr",
            "deep freezer",
            "vaccine carrier",
            "cold box",
        ],
        "MISSION_INDRADHANUSH": [
            "mission indradhanush",
            "MI",
            "mi",
            "immunization campaign outreach",
            "left-out children vaccination",
            "dropout children vaccination",
        ],
        "IMI": [
            "intensified mission indradhanush",
            "IMI",
            "imi",
            "imi 2.0",
            "imi immunization",
            "imi drive",
            "high-risk area vaccination",
        ],

        # ASHA & community health workers
        "ASHA": [
            "ASHA worker",
            "asha worker",
            "ASHA training",
            "asha training",
            "ASHA incentive",
            "asha incentive",
            "ASHA module",
            "asha module",
            "community health volunteer",
            "field health worker training",
            "asha reporting tools",
            "accredited social health activist",
        ],

        # Maternal benefit & delivery schemes
        "JSY": [
            "janani suraksha yojana",
            "JSY",
            "jsy",
            "institutional delivery incentive",
            "cash incentive delivery",
            "referral transport pregnant women",
            "jsy beneficiary",
            "delivery incentive scheme",
        ],
        "JSSK": [
            "janani shishu suraksha karyakram",
            "JSSK",
            "jssk",
            "free delivery",
            "free c-section",
            "free medicines pregnant women",
            "free diagnostics pregnant women",
            "free transport mother newborn",
            "diet provision pregnant women",
            "jssk newborn package",
            "samahar kit",
        ],
        "PMSMA": [
            "PMSMA",
            "pmsma",
            "pmsma clinic",
            "anc check-up 9th of month",
            "pmsma diagnostics",
            "specialist anc camp",
            "pregnancy screening pmsma",
            "pradhan mantri surakshit matritva abhiyan",
        ],
        "LAQSHYA": [
            "LAQSHYA",
            "laqshya",
            "laqshya labour room",
            "labour room strengthening",
            "delivery room quality",
            "maternity ot upgradation",
            "laqshya certification",
            "laqshya facility improvement",
        ],
        "PMMVY": [
            "PMMVY",
            "pmmvy",
            "pradhan mantri matru vandana yojana",
            "maternity benefit first child",
            "cash benefit pregnant women",
            "pmmvy payment system",
            "mother benefit scheme",
        ],
        "NMBS": [
            "NMBS",
            "nmbs",
            "national maternity benefit scheme",
            "maternal nutritional benefit",
            "pregnant women cash support nmbs",
        ],

        # Child screening & DEIC
        "RBSK": [
            "RBSK",
            "rbsk",
            "rbsk screening",
            "deic centre",
            "deic center",
            "early intervention centre",
            "child screening 0-18 years",
            "birth defect screening",
            "rbsk mobile team",
        ],

        # Nutrition & POSHAN
        "POSHAN_ABHIYAAN": [
            "POSHAN",
            "poshan",
            "POSHAN Abhiyaan",
            "poshan abhiyaan",
            "nutrition monitoring",
            "growth monitoring devices",
            "ict-rtm anganwadi",
            "nutrition rehabilitation",
            "malnutrition reduction",
        ],

        # Respectful maternity care
        "SUMAN": [
            "SUMAN",
            "suman",
            "suman programme",
            "suman maternity service",
            "respectful maternity care",
            "suman certification",
            "zero expense delivery",
            "maternal newborn assured care",
        ],

        # Digital tracking
        "MCTS": [
            "MCTS",
            "mcts",
            "mother child tracking",
            "mcts portal",
            "digital anc tracking",
            "digital pnc tracking",
            "immunization tracking system",
        ],

        # Assam-specific MCH schemes
        "MAMONI": [
            "MAMONI",
            "mamoni",
            "mamoni scheme",
            "anc nutrition assistance",
            "pregnant women nutrition cash",
            "assam mamoni",
        ],
        "MAMATA_KIT": [
            "mamata kit",
            "Mamata kit",
            "newborn care kit",
            "mother kit distribution",
            "post-delivery kit",
            "assam mamata kit",
        ],
        "SNEHA_SPARSHA": [
            "sneha sparsha",
            "Sneha Sparsha",
            "financial aid child treatment",
            "specialized treatment child",
            "paediatric tertiary care support",
        ],
        "CHD_SCHEME": [
            "congenital heart disease scheme",
            "CHD",
            "chd",
            "chd scheme",
            "congenital heart surgery child",
            "paediatric cardiac surgery",
            "free heart surgery child",
        ],
        "OPERATION_SMILE": [
            "Operation Smile",
            "operation smile",
            "cleft lip surgery",
            "cleft palate surgery",
            "free cleft surgery assam",
        ],
        "MAJONI": [
            "Majoni",
            "majoni",
            "majoni scheme",
            "girl child security scheme",
            "assam majoni benefit",
        ],
        "ASSAM_FREE_DIAGNOSTICS": [
            "assam free diagnostics",
            "Assam Free Diagnostics",
            "free maternal diagnostics",
            "free pregnancy tests",
            "free drugs pregnant women",
            "free lab tests assam",
        ],

        # Broader health-system schemes supporting MCH
        "RCH": [
            "RCH",
            "rch",
            "rch programme",
            "reproductive and child health",
            "maternal child health rch",
            "rch facility strengthening",
            "rch phase ii",
            "rch register",
        ],
        "NHM": [
            "NHM",
            "nhm",
            "NRHM",
            "nrhm",
            "nhm facility strengthening",
            "nrhm infrastructure",
            "chc upgradation nhm",
            "phc upgradation nhm",
            "district hospital mch strengthening",
        ],

        # New schemes / departments
        "HFW": [
            "Health and Family Welfare Department",
            "Health & Family Welfare Department",
            "Department of Health and Family Welfare",
            "H&FW",
            "H & FW",
            "HFW",
            "health and family welfare",
        ],
        "PWD_NH": [
            "Public Works Department (NH)",
            "Public Works Department - National Health",
            "PWD (NH)",
            "PWD-NH",
            "PWD NH",
            "pwd nh",
            "chief engineer pwd nh",
        ],
    }

    # Enrich scheme patterns with spaced-out acronym forms
    SCHEMES_Identifier = enrich_scheme_patterns(SCHEMES_Identifier)

    # Apply scheme detection row-wise
    idea_frm_tenders_df['Scheme'] = idea_frm_tenders_df.apply(
        lambda r: find_schemes_for_tender(
            " ".join([
                str(r.get('tender_title', '')),
                str(r.get('tender_externalreference', '')),
                str(r.get('Work Description', '')),
                str(r.get('Department', '')),
                str(r.get('Organisation_Chain', '')),
            ]),
            SCHEMES_Identifier
        ),
        axis=1
    )

    idea_frm_tenders_df.to_csv(
        os.getcwd() + r'/Assam_MCH_analysis/TENDERS/data/mch_tenders/' + filename,
        encoding='utf-8',
        index=False
    )

# -----------------------------
# Concatenate monthly outputs
# -----------------------------
data_path = os.getcwd() + r'/Assam_MCH_analysis/TENDERS/data/'
csvs = glob.glob(data_path + r'/mch_tenders/*.csv')
dfs = []
for csv in csvs:
    csv = csv.replace("//", "/").replace("\\", "/")
    month = csv.split(r'/')[-1][:7]
    df = pd.read_csv(csv)
    df['month'] = month
    dfs.append(df)

idea_frm_tenders_df = pd.concat(dfs)
idea_frm_tenders_df.to_csv(data_path + 'mch_tenders_all.csv', index=False)
