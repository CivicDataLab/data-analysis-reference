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
    "maternal",
    "maternity",
    "women",
    "maternity and child health",
    "newborn",
    "infant",
    "paediatric",
    "labor",
    "health",
    "hospital",
    "vaccine",
    "vaccination",
    "immunization",
    "immunisation",
    "cold chain",
    "ice lined refrigerator",
    "ilr",
    "deep freezer",
    "walk-in cooler",
    "walk-in freezer",
    "vaccine carrier",
    "cold box",
    "polio vaccine",
    "universal immunization",
    "asha worker",
    "asha training",
    "asha incentive",
    "asha module",
    "community health volunteer",
    "field health worker training",
    "asha reporting tools",
    "jsy",
    "pregnant women",
    "jssk",
    "free delivery",
    "c-section",
    "pregnant women",
    "free diagnostics pregnant women",
    "free transport mother newborn",
    "diet provision pregnant women",
    "jssk newborn package",
    "pmsma clinic",
    "anc check-up 9th of month",
    "pmsma diagnostics",
    "specialist anc camp",
    "pregnancy screening pmsma",
    "laqshya labour room",
    "labour room strengthening",
    "delivery room quality",
    "maternity ot upgradation",
    "laqshya certification",
    "laqshya facility improvement",
    "rbsk screening",
    "deic centre",
    "early intervention centre",
    "child screening 0-18 years",
    "birth defect screening",
    "rbsk mobile team",
    "mission indradhanush",
    "mi round",
    "left-out children vaccination",
    "dropout children vaccination",
    "immunization campaign outreach",
    "imi immunization",
    "imi drive",
    "high-risk area vaccination",
    "imi microplan",
    "poshan abhiyaan",
    "nutrition monitoring",
    "growth monitoring devices",
    "ict-rtm anganwadi",
    "nutrition rehabilitation",
    "malnutrition reduction",
    "pmmvy",
    "maternity benefit first child",
    "cash benefit pregnant women",
    "pmmvy payment system",
    "mother benefit scheme",
    "suman maternity service",
    "respectful maternity care",
    "suman certification",
    "zero expense delivery",
    "maternal newborn assured care",
    "mcts",
    "mother child tracking",
    "mcts portal",
    "digital anc tracking",
    "digital pnc tracking",
    "immunization tracking system",
    "mamoni scheme",
    "anc nutrition assistance",
    "pregnant women nutrition cash",
    "assam mamoni",
    "mamata kit",
    "newborn care kit",
    "mother kit distribution",
    "post-delivery kit",
    "assam mamata",
    "sneha sparsha",
    "financial aid child treatment",
    "specialized treatment child",
    "paediatric tertiary care support",
    "chd",
    "congenital heart surgery child",
    "paediatric cardiac surgery",
    "free heart surgery child",
    "operation smile",
    "cleft lip surgery",
    "cleft palate surgery",
    "free cleft surgery assam",
    "majoni scheme",
    "girl child",
    "majoni",
    "assam free diagnostics",
    "free maternal diagnostics",
    "free pregnancy tests",
    "free drugs pregnant women",
    "free lab tests assam",
    "nmbs",
    "maternal nutritional benefit",
    "pregnant women cash support nmbs",
    "rch",
    "nrhm",
    "chc upgradation nhm",
    "phc upgradation nhm",
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
