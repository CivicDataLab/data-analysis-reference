# Tenders
Public procurement datasets are scraped from the [Assam Tenders](https://assamtenders.gov.in/nicgep/app) website. Flood tenders are identified and geotagged with revenue circles using the names of villages, revenue circles etc., present in tender work descriptions, IDs etc.

## Project Structure
- `scripts` : Contains the scripts used to obtain the data
    - `scraper`: Contains codes for scraping tenders from assamtenders.in
        - `scraper_assam_recent_tenders_tender_status.py`: Scrapes tenders from [Assam Tenders](https://assamtenders.gov.in/nicgep/app). Takes year and month as system arguments. Eg: `python3 ~/scraper_assam_recent_tenders_tender_status.py 2023 6`
        - `concatinate_raw_tenders.py`: Creates one csv for each month in the `monthly_tenders` folder in `data`
    - `mch_tenders.py`: Identification of flood tenders
    - `geocode_district.py`: Geocode districts
    - `geocode_rc.py`: Geocode revenue circles
- `data`: Contains datasets generated using the scripts, as well as reference data such as "Keyword list.csv", which contains keywords used to identify tenders