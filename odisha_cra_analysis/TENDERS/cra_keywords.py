"""
CRA Keyword Taxonomy — Odisha Climate-Resilient Agriculture
===========================================================
Positive keyword sets organised by climate theme.
Negative keywords filter out non-agricultural / infrastructure tenders
that accidentally contain matching terms.

Each category is a dict with:
  - 'keywords'  : list of strings (matched via substring, case-insensitive)
  - 'label'     : short code used in output columns
  - 'notes'     : rationale / caveats

Usage
-----
    from cra_keywords import POSITIVE_CATEGORIES, NEGATIVE_KEYWORDS
"""

# ─────────────────────────────────────────────────────────────────────────────
# POSITIVE KEYWORD SETS
# ─────────────────────────────────────────────────────────────────────────────

POSITIVE_CATEGORIES = {

    # ── C1 ──────────────────────────────────────────────────────────────────
    "C1_water_conservation": {
        "label": "C1_WaterConservation",
        "notes": (
            "Farm-level water storage, groundwater recharge structures. "
            "Core CRA intervention in drought-prone Odisha districts."
        ),
        "keywords": [
            "check dam", "checkdam", "percolation tank", "farm pond",
            "recharge", "recharge shaft", "water harvesting", "whs",
            "nala bund", "nallah bund", "desilting", "de-silting", "desilt",
            "dredging", "rejuvenation", "rrr",            # renovation/restoration/recharge
            "water body", "water bodies", "pond",
            "mit",                                         # minor irrigation tank
            "mip",                                         # minor irrigation project
            "dug well", "dugwell",
            "groundwater recharge", "ground water recharge",
        ],
    },

    # ── C2 ──────────────────────────────────────────────────────────────────
    "C2_irrigation_infrastructure": {
        "label": "C2_Irrigation",
        "notes": (
            "Canal systems, lift irrigation, bore wells for agricultural water supply. "
            "PMKSY/HKKP-linked tenders are prominent here."
        ),
        "keywords": [
            "irrigation", "irr ",
            "minor irrigation", "lift irrigation", "lip",
            "l.i. project", "l.i.",
            "canal", "distributary", "disty", "sub-minor",
            "rmc", "lmc",                                  # right/left main canal
            "barrage", "weir", "head regulator", "cross regulator",
            "aqueduct", "field channel", "command area", "ayacut",
            "tube well", "tw ",                            # trailing space avoids 'two'
            "bore well", "borewell",
            "gsr",                                         # ground service reservoir
            "drip irrigation", "sprinkler irrigation",
            "micro irrigation", "micro-irrigation",
            "water user association", "wua",
        ],
    },

    # ── C3 ──────────────────────────────────────────────────────────────────
    "C3_drought_resilience": {
        "label": "C3_Drought",
        "notes": (
            "Direct drought-response and dry-spell adaptation interventions. "
            "Overlaps with C1 on recharge; dual-tag where both match."
        ),
        "keywords": [
            "drought", "water scarcity", "water stress",
            "dry land", "dryland", "rainfed",
            "drought tolerant", "drought resistant",
            "water saving", "water use efficiency",
            "deficit irrigation", "moisture conservation",
            "mulching", "soil moisture",
            "groundwater", "ground water",
            "deepening", "defunct", "revival",
        ],
    },

    # ── C4 ──────────────────────────────────────────────────────────────────
    "C4_flood_cyclone_protection": {
        "label": "C4_FloodCyclone",
        "notes": (
            "Odisha is India's most cyclone-exposed state. "
            "Saline embankments protect coastal agricultural land from inundation. "
            "Filter by coastal districts for higher precision."
        ),
        "keywords": [
            "flood protection", "flood embankment", "embankment",
            "anti-erosion", "anti erosion", "river bank protection",
            "cyclone", "mpcs",                             # multipurpose cyclone shelter
            "cyclone shelter", "saline embankment",
            "sea wall", "seawall", "coastal agriculture",
            "storm surge", "shelter belt",
            "mangrove", "mangrove plantation",
            "drainage improvement", "waterlogging", "water logging",
            "salinity", "saline water intrusion",
            "bund", "protection bund",
        ],
    },

    # ── C5 ──────────────────────────────────────────────────────────────────
    "C5_climate_smart_farming": {
        "label": "C5_ClimateSmartFarming",
        "notes": (
            "Inputs, seeds, practices that directly build crop-level climate resilience. "
            "Includes SRI/SWI, stress-tolerant varieties, soil health."
        ),
        "keywords": [
            "climate smart", "climate-smart", "climate resilient", "climate-resilient",
            "cra",                                         # climate-resilient agriculture
            "stress tolerant", "stress-tolerant",
            "flood tolerant", "submergence tolerant", "submergence-tolerant",
            "swarna sub1", "swarna-sub1",
            "drought tolerant variety", "heat tolerant",
            "sri",                                         # system of rice intensification
            "swi",                                         # system of wheat intensification
            "dsr",                                         # direct seeded rice
            "direct seeded rice", "direct-seeded rice",
            "bio-fortified", "bio fortified",
            "soil health", "soil health card", "shc",
            "organic farming", "natural farming",
            "integrated crop management", "icm",
            "crop diversification",
            "agro-meteorology", "agrometeorology",
        ],
    },

    # ── C6 ──────────────────────────────────────────────────────────────────
    "C6_agroforestry_ecosystem": {
        "label": "C6_Agroforestry",
        "notes": (
            "Tree-based and ecosystem-level interventions on agricultural land. "
            "CAMPA-funded tenders are typical here."
        ),
        "keywords": [
            "agroforestry", "agro-forestry",
            "plantation", "tree plantation", "avenue plantation",
            "windbreak", "wind break",
            "watershed", "watershed development", "watershed management",
            "wdp", "iwmp",                                 # integrated watershed management
            "campa",
            "green cover", "vegetative bund",
            "contour bund", "contour trenching",
            "land development", "wasteland development",
        ],
    },

    # ── C7 ──────────────────────────────────────────────────────────────────
    "C7_post_harvest_supply_chain": {
        "label": "C7_PostHarvestSupply",
        "notes": (
            "Cold storage, drying, storage infrastructure reduce climate-driven "
            "post-harvest losses. Include only when agriculture context is clear."
        ),
        "keywords": [
            "cold storage", "cold chain",
            "warehouse", "grain storage", "storage godown",
            "silos", "metal silos",
            "custom hiring centre", "chc",
            "farm machinery", "power tiller",
            "solar pump", "solar powered pump",
            "solar dryer", "drying platform",
        ],
    },

    # ── C8 ──────────────────────────────────────────────────────────────────
    "C8_scheme_specific": {
        "label": "C8_Scheme",
        "notes": (
            "Odisha and national schemes with explicit CRA mandates. "
            "Match on scheme acronym/name and verify against tender description."
        ),
        "keywords": [
            "pmksy", "hkkp",                              # pradhan mantri krishi sinchayee
            "rkvy", "rkvy-raftaar",                       # rashtriya krishi vikas yojana
            "nfsm",                                        # national food security mission
            "nmsa",                                        # national mission for sustainable agri
            "pmfby",                                       # pradhan mantri fasal bima
            "odisha climate change action plan", "occap",
            "odisha millet mission", "omm",
            "biju krushak kalyan", "bkkp",
            "nabard", "ridf",
            "jaga mission",                                # land rights → livelihood security
        ],
    },
}

# ─────────────────────────────────────────────────────────────────────────────
# NEGATIVE KEYWORDS
# Filter out tenders whose work descriptions indicate non-agricultural context
# ─────────────────────────────────────────────────────────────────────────────

NEGATIVE_KEYWORDS = [
    # Buildings & civil infrastructure unrelated to CRA
    "residential building", "office building", "quarters", "f-type",
    "r.i. office", "ri office",
    "school building", "anganwadi building", "anganwadi centre",
    "community hall", "panchayat ghar",
    "crematorium", "burial ground",
    "stadium", "gymnasium", "sports complex",
    "hospital building", "primary health centre building",

    # Urban furniture / utilities
    "cctv", "surveillance",
    "toilet", "latrine", "urinal", "sanitation",
    "street light", "street lighting",
    "smart park", "park development", "beautification",
    "boundary wall", "compound wall", "fencing", "gate",
    "furniture",

    # Market / commercial infrastructure not linked to agri supply
    "haat bazaar",                      # rural market sheds (keep if agri context present)
    "bus stand", "bus shelter",
    "parking",

    # Administrative / police / judicial
    "police station", "police outpost", "police barracks",
    "court building", "jail", "prison",

    # Roads (unless explicitly for agricultural connectivity)
    # NOTE: 'road' is deliberately not included—farm roads are CRA-relevant.
    # Add if false-positive rate is high after initial run.
]

# ─────────────────────────────────────────────────────────────────────────────
# INTENT TAGS
# ─────────────────────────────────────────────────────────────────────────────

INTENT_TAGS = {
    "DAMAGE_RESPONSE": {
        "label": "DamageResponse",
        "notes": "Restoration after climate event — flood, cyclone, drought crop loss.",
        "keywords": [
            "restoration", "repair", "special repair", "s/r", "r&m",
            "reconstruction", "breach closing", "breach repair",
            "closure of breach", "damaged", "flood damage",
            "washed away", "emergent", "emergency",
            "relief", "rehabilitation",
        ],
    },
    "MITIGATION_PREVENTION": {
        "label": "Mitigation",
        "notes": "New or improved infrastructure for future climate resilience.",
        "keywords": [
            "construction", "constn", "improvement", "provision",
            "installation", "strengthening", "raising", "widening",
            "new ", "providing", "augmentation",
            "creation", "development", "establishment",
        ],
    },
    "CAPACITY_TRAINING": {
        "label": "CapacityBuilding",
        "notes": "Farmer training, extension, demonstrations — softer CRA interventions.",
        "keywords": [
            "training", "capacity building", "demonstration",
            "farmer field school", "ffs", "exposure visit",
            "technology transfer", "extension",
        ],
    },
}

# ─────────────────────────────────────────────────────────────────────────────
# ODISHA DISTRICT LOOKUP
# Full list of 30 districts for geo-tagging; coastal districts flagged
# ─────────────────────────────────────────────────────────────────────────────

ODISHA_DISTRICTS = {
    # Coastal (higher relevance for C4 cyclone/salinity keywords)
    "coastal": [
        "balasore", "baleswar", "bhadrak", "kendrapara", "jagatsinghpur",
        "puri", "khordha", "khurda", "ganjam", "gajapati",
    ],
    # Inland — drought-prone (higher relevance for C1/C3)
    "drought_prone": [
        "bolangir", "balangir", "nuapada", "kalahandi", "bargarh",
        "sonepur", "subarnapur", "boudh", "kandhamal",
    ],
    # All 30
    "all": [
        "angul", "balasore", "baleswar", "bargarh", "bhadrak", "bolangir",
        "balangir", "boudh", "cuttack", "deogarh", "dhenkanal", "gajapati",
        "ganjam", "jagatsinghpur", "jajpur", "jharsuguda", "kalahandi",
        "kandhamal", "kendrapara", "keonjhar", "khordha", "khurda",
        "koraput", "malkangiri", "mayurbhanj", "nabarangpur", "nayagarh",
        "nuapada", "puri", "rayagada", "sambalpur", "sonepur", "subarnapur",
        "sundargarh",
    ],
}
