import pandas as pd
import os
import re
import dateutil.parser
import glob

# input_df - after the scraper code is run
data_path = os.getcwd() + r'/Assam_MCH_analysis/TENDERS/data/monthly_tenders/'

# Identify mch related tenders using keywords
def populate_keyword_dict(keyword_list): 
    keywords_dict = {}
    for keyword in keyword_list:
        keywords_dict[keyword] = 0
    return keywords_dict

def mch_filter(row):
    '''
    :param row: row of the dataframe that contains tender title, work description
    
    :return: Tuple of (is_mch_tender, positive_kw_dict, negative_kw_dict) for every row
    '''
    positive_keywords_dict = populate_keyword_dict(POSITIVE_KEYWORDS)
    negative_keywords_dict = populate_keyword_dict(NEGATIVE_KEYWORDS)
    tender_slug = str(row['tender_externalreference']) + ' ' + str(row['tender_title']) + ' ' + str(row['Work Description'])
    tender_slug = re.sub('[^a-zA-Z0-9 \n\.]', ' ', tender_slug)
    
    is_mch_tender = False
    for keyword in POSITIVE_KEYWORDS:
        keyword_count = len(re.findall(r"\b%s\b" % keyword.lower(), tender_slug.lower()))
        positive_keywords_dict[keyword] = keyword_count
        if keyword_count > 0:
            is_mch_tender = True
            
    for keyword in NEGATIVE_KEYWORDS:
        keyword_count = len(re.findall(r"\b%s\b" % keyword.lower(), tender_slug.lower()))
        negative_keywords_dict[keyword] = keyword_count
        if keyword_count > 0:
            is_mch_tender = False
           
    return str(is_mch_tender), str(positive_keywords_dict), str(negative_keywords_dict)

csvs = glob.glob(data_path+'*.csv')
print('Total CSVs to process: ', len(csvs))
for csv in csvs:
    filename  = csv.split(r'/')[-1]
    filename  = re.split(r'\\',csv)[-1]
    input_df = pd.read_csv(csv)
    
    # De-Duplication (Change the logic once the time of scraping is added in the input_df)
    input_df = input_df.drop_duplicates()
    tender_ids = input_df["Tender ID"]
    # duplicates_df = input_df[tender_ids.isin(tender_ids[tender_ids.duplicated()])].sort_values("Tender ID")
    # input_df = input_df.drop(duplicates_df[duplicates_df['No of Bids Received'].isnull()].index)
    # input_df.reset_index(drop=True, inplace=True)
    # deduped_df = input_df.drop_duplicates(subset=['Tender ID'],keep='last')
    # deduped_df.to_csv(os.getcwd()+'/Sources/TENDERS/data/deduped_master_tender_list.csv', encoding='utf-8')

    #mch Keywords
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

    mch_filter_tuples = input_df.apply(mch_filter,axis=1)
    input_df.loc[:,'is_mch_tender'] = [var[0] for var in list(mch_filter_tuples)]
    input_df.loc[:,'positive_keywords_dict'] = [var[1] for var in list(mch_filter_tuples)]
    input_df.loc[:,'negative_keywords_dict'] = [var[2] for var in list(mch_filter_tuples)]

    # Removing tenders from certain departments that are not related to mch management.
    idea_frm_tenders_df = input_df[(input_df.is_mch_tender=='True')&
                                    (~input_df.Department.isin(["Directorate of Agriculture and Assam Seed Corporation","Department of Handloom Textile and Sericulture"])) ]
    idea_frm_tenders_df = idea_frm_tenders_df.loc[idea_frm_tenders_df['Status']=='Accepted-AOC']
    print('Number of mch related tenders filtered: ', idea_frm_tenders_df.shape[0])
    if idea_frm_tenders_df.shape[0]==0:
        continue

    # Classify tenders based on Monsoons
    for index, row in idea_frm_tenders_df.iterrows():
        monsoon = "" 
        published_date = dateutil.parser.parse(row['Published Date'])
        if 1 <= published_date.month <= 5:
            monsoon = "Pre-Monsoon"
            if published_date.month == 5 and published_date.day > 14:
                monsoon = "Monsoon"
        elif 6 <= published_date.month <= 10:
            monsoon = "Monsoon"
            if published_date.month == 10 and published_date.day > 14:
                monsoon = "Post-Monsoon"
        else:
            monsoon = "Post-Monsoon"
        idea_frm_tenders_df.loc[index, "Season"] = monsoon # type: ignore

    # identify scheme related information
    schemes_identified = []
    SCHEMES_Identifier = {
    # Immunization & child health
    "UIP": [
        "universal immunization programme",
        "universal immunisation programme",
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
        "immunization campaign outreach",
        "left-out children vaccination",
        "dropout children vaccination",
    ],
    "IMI": [
        "intensified mission indradhanush",
        "imi 2.0",
        "imi immunization",
        "imi drive",
        "high-risk area vaccination",
    ],

    # ASHA & community health workers
    "ASHA": [
        "asha worker",
        "asha training",
        "asha incentive",
        "asha module",
        "community health volunteer",
        "field health worker training",
        "asha reporting tools",
        "accredited social health activist",
    ],

    # Maternal benefit & delivery schemes
    "JSY": [
        "janani suraksha yojana",
        "jsy",
        "institutional delivery incentive",
        "cash incentive delivery",
        "referral transport pregnant women",
        "jsy beneficiary",
        "delivery incentive scheme",
    ],
    "JSSK": [
        "janani shishu suraksha karyakram",
        "jssk",
        "free delivery",
        "free c-section",
        "free medicines pregnant women",
        "free diagnostics pregnant women",
        "free transport mother newborn",
        "diet provision pregnant women",
        "jssk newborn package",
        "samahar kit",  # appears explicitly in your data under JSSK diet program
    ],
    "PMSMA": [
        "pmsma clinic",
        "anc check-up 9th of month",
        "pmsma diagnostics",
        "specialist anc camp",
        "pregnancy screening pmsma",
        "pradhan mantri surakshit matritva abhiyan",
    ],
    "LAQSHYA": [
        "laqshya labour room",
        "labour room strengthening",
        "delivery room quality",
        "maternity ot upgradation",
        "laqshya certification",
        "laqshya facility improvement",
    ],
    "PMMVY": [
        "pmmvy",
        "pradhan mantri matru vandana yojana",
        "maternity benefit first child",
        "cash benefit pregnant women",
        "pmmvy payment system",
        "mother benefit scheme",
    ],
    "NMBS": [
        "national maternity benefit scheme",
        "nmbs",
        "maternal nutritional benefit",
        "pregnant women cash support nmbs",
    ],

    # Child screening & DEIC
    "RBSK": [
        "rbsk screening",
        "rbsk",
        "deic centre",
        "deic center",
        "early intervention centre",
        "child screening 0-18 years",
        "birth defect screening",
        "rbsk mobile team",
    ],

    # Nutrition & POSHAN
    "POSHAN_ABHIYAAN": [
        "poshan abhiyaan",
        "poshan",
        "nutrition monitoring",
        "growth monitoring devices",
        "ict-rtm anganwadi",
        "nutrition rehabilitation",
        "malnutrition reduction",
    ],

    # Respectful maternity care
    "SUMAN": [
        "suman programme",
        "suman maternity service",
        "respectful maternity care",
        "suman certification",
        "zero expense delivery",
        "maternal newborn assured care",
    ],

    # Digital tracking
    "MCTS": [
        "mcts",
        "mother child tracking",
        "mcts portal",
        "digital anc tracking",
        "digital pnc tracking",
        "immunization tracking system",
    ],

    # Assam-specific MCH schemes
    "MAMONI": [
        "mamoni scheme",
        "mamoni",
        "anc nutrition assistance",
        "pregnant women nutrition cash",
        "assam mamoni",
    ],
    "MAMATA_KIT": [
        "mamata kit",
        "newborn care kit",
        "mother kit distribution",
        "post-delivery kit",
        "assam mamata kit",
    ],
    "SNEHA_SPARSHA": [
        "sneha sparsha",
        "financial aid child treatment",
        "specialized treatment child",
        "paediatric tertiary care support",
    ],
    "CHD_SCHEME": [
        "congenital heart disease scheme",
        "chd scheme",
        "congenital heart surgery child",
        "paediatric cardiac surgery",
        "free heart surgery child",
    ],
    "OPERATION_SMILE": [
        "operation smile",
        "cleft lip surgery",
        "cleft palate surgery",
        "free cleft surgery assam",
    ],
    "MAJONI": [
        "majoni scheme",
        "majoni",
        "girl child security scheme",
        "assam majoni benefit",
    ],
    "ASSAM_FREE_DIAGNOSTICS": [
        "assam free diagnostics",
        "free maternal diagnostics",
        "free pregnancy tests",
        "free drugs pregnant women",
        "free lab tests assam",
    ],

    # Broader health-system schemes supporting MCH
    "RCH": [
        "rch programme",
        "reproductive and child health",
        "maternal child health rch",
        "rch facility strengthening",
        "rch phase ii",
        "rch register",
    ],
    "NHM": [
        "nhm facility strengthening",
        "nrhm infrastructure",
        "chc upgradation nhm",
        "phc upgradation nhm",
        "district hospital mch strengthening",
        "nhm",
        "nrhm",
    ],
    }
    scheme_kw = {'universal immunization programme','uip','asha','jsy','JSSK','PMSMA','laqshya','rbsk','mission indradhanush','imi','poshan','poshan abhiyan','pmmvy','suman','mcts','mamoni','mkds','mamata kit','sneha sparsha','chd','operation smile','majoni','afdds','nmbs','rch','nrhm','nhm'}
    for idx, row in idea_frm_tenders_df.iterrows():
        tender_slug = row['tender_title']+' '+row['tender_externalreference']+' '+row['Work Description']
        tender_slug = re.sub('[^a-zA-Z0-9 \n\.]', ' ', tender_slug).lower()

        tender_slug = set(re.split(r'[-.,()_\s/]\s*',tender_slug))
        try:
            schemes_identified.append(list(tender_slug & scheme_kw)[0].upper())
        except:
            schemes_identified.append('')

    idea_frm_tenders_df.loc[:,'Scheme'] = schemes_identified
    idea_frm_tenders_df.to_csv(os.getcwd()+r'/Assam_MCH_analysis/TENDERS/data/mch_tenders/'+filename,
                            encoding='utf-8',
                            index=False)
    

data_path = os.getcwd() + r'/Assam_MCH_analysis/TENDERS/data/'
csvs = glob.glob(data_path+r'/mch_tenders/*.csv')
dfs=[]
for csv in csvs:
    csv = csv.replace("//", "/")
    csv = csv.replace("\\", "/")
    month = csv.split(r'/')[-1][:7]
    df = pd.read_csv(csv)
    df['month'] = month
    dfs.append(df)

idea_frm_tenders_df = pd.concat(dfs)
idea_frm_tenders_df.to_csv(data_path+'mch_tenders_all.csv', index=False)
