import argparse
import os
import pandas as pd
import pyarrow as pa
import time

from datetime import datetime
from glob import glob
from pandas_gbq import to_gbq

from pathlib import Path
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

from tqdm import tqdm

def init_driver(local_download_path):
    os.makedirs(local_download_path, exist_ok=True)

    # Set Chrome Options    
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--remote-debugging-port=9222")

    # more chrome options recommended from https://stackoverflow.com/a/78936680/1870832
    # to avoid timeout error
    chrome_options.add_argument('--dns-prefetch-disable')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--enable-cdp-events')

    prefs = {
        "download.default_directory": local_download_path,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    chrome_options.add_experimental_option("prefs", prefs)

    # Set up the driver
    service = Service()
    driver = webdriver.Chrome(service=service, options=chrome_options)

    # Set download behavior
    driver.execute_cdp_cmd("Page.setDownloadBehavior", {
        "behavior": "allow",
        "downloadPath": local_download_path
    })

    return driver

def submit_election(driver, homepage, election):

    # Open the webpage
    driver.get(homepage)

    # Wait for the dropdown to be loaded (change the wait time as necessary)
    wait = WebDriverWait(driver, 2)
    wait.until(EC.element_to_be_clickable((By.ID, 'idElection')))

    # Select the election from the dropdown by visible text
    dropdown_element = driver.find_element(By.ID, 'idElection')  # Updated to the new method
    select = Select(dropdown_element)
    select.select_by_visible_text(election)

    # Click the submit button
    submit_button = driver.find_element(By.XPATH, '//button[@onclick="return submitForm();"]')
    submit_button.click()
    return driver

def get_selected_ev_date_dropdown(driver, officialness):
    dropdown_name = f"{officialness} Early Voting Turnout by Date"
    print(f"Looking for dropdown with text: {dropdown_name}")
    wait = WebDriverWait(driver, 2)
    dropdown_container = wait.until(EC.visibility_of_element_located((By.XPATH, f"//div[contains(text(), '{dropdown_name}')]")))
    dropdown_element = dropdown_container.find_element(By.XPATH, "./following-sibling::div//select[@id='selectedDate']")
    select = Select(dropdown_element)
    return select

def get_report_dates(driver, origin_url, election, officialness):    
    # navigate to the election page to see dates of eligible reports
    driver = submit_election(driver, origin_url, election)

    # Locate the parent div by checking for unique text within, then find the dropdown within this div
    select = get_selected_ev_date_dropdown(driver, officialness)
    dates = [option.text.strip() for option in select.options][1:]  # w/o 'Select Early Voting Date'
    return dates

def get_ev_turnout_data(driver, csv_dl_dir, origin_url, election, officialness):

    # clear local downloads folder before beginning 
    # (later we use count of files in this folder to determine when a new file has finished 
    #  downloading and is ready to be renamed; so we need to start with a clean slate)
    for f in os.listdir(csv_dl_dir):
        fpath = os.path.join(csv_dl_dir, f)
        if os.path.isfile(fpath):
            os.remove(fpath)

    # Get report-dates we'll need to iterate through
    report_dates = get_report_dates(driver, origin_url, election, officialness)

    num_csvs_downloaded = 0  # tracking total downloaded csvs lets us confirm each is downloaded
    final_df = pd.DataFrame()
    for d in tqdm(report_dates):
        print(f"Downloading report for {d}")
        # navigate back to the main Early Voter page for this election 
        driver = submit_election(driver, origin_url, election)

        # Select current date from dropdown
        select = get_selected_ev_date_dropdown(driver, officialness)
        select.select_by_visible_text(d)

        # Click the submit button for fetching table of EV detailed data
        time.sleep(3)
        driver.execute_script("validateSubmit();")

        # Click the "Generate Report" button to download as a csv
        # ...unless we got a pop-up saying there's no data for this date
        print(f"Executing downloadReport() button / js script")
        DOWNLOAD_WAIT_SECONDS = 20
        try:
            driver.execute_script("downloadReport('');")
            WebDriverWait(driver, DOWNLOAD_WAIT_SECONDS).until(EC.alert_is_present())
            alert = driver.switch_to.alert
            print(f"Alert text: {alert.text}")
            alert.accept()
        except TimeoutException:
            print(f"No alert found after {DOWNLOAD_WAIT_SECONDS} seconds; attempting to process file download")

            # Wait for the download to complete
            num_csvs_downloaded += 1
            while len([f for f in os.listdir(csv_dl_dir) if f.endswith('.csv')]) < num_csvs_downloaded:
                print(f"waiting for {d} to download...")
                time.sleep(1)

            # read that latest-downloaded csv into a df; append to results
            csv_files = [f for f in os.listdir(csv_dl_dir) if f.endswith('.csv')]
            latest_file = max(csv_files, key=lambda x: os.path.getctime(os.path.join(csv_dl_dir, x)))

            # not including all columns here; just the ones that seem like they might get mistaken for ints (but shouldn't be)
            dtypes = {c:'string' for c in ['ID_VOTER', 'PRECINCT', 'POLL PLACE ID']}
            df = pd.read_csv(os.path.join(csv_dl_dir, latest_file), dtype_backend='pyarrow', dtype=dtypes)            
            df['filedate'] = datetime.strptime(d, "%B %d,%Y")

            final_df = pd.concat([final_df, df], axis=0, ignore_index=True)

    # unindent two levels; out of the try/except block and out of the for loop of dates
    return final_df


def get_poll_places_last_updated(driver, origin_url, election):
    driver = submit_election(driver, origin_url, election)
    
    DOWNLOAD_WAIT_SECONDS = 60
    try:
        dt_element = WebDriverWait(driver, DOWNLOAD_WAIT_SECONDS).until(
                EC.presence_of_element_located((By.ID, "ppLastUpdatedVal"))
            )
    except Exception as e:
        print(f"Failed to find html element with ID: 'ppLastUpdatedVal' within {DOWNLOAD_WAIT_SECONDS} seconds")
        raise e

    return dt_element.text

if __name__ == "__main__":
    # CLI params:
    parser = argparse.ArgumentParser(description="Early Voting Scraper",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--mode', type=str, default='turnout_data', 
                        choices=['turnout_data', 'polling_places_last_updated'],
                        help="Scraping mode: 'turnout_data' or 'polling_places_last_updated'")
    parser.add_argument('--election', type=str, default='2024 NOVEMBER 5TH GENERAL ELECTION')

    args = parser.parse_args()


    # Other params
    OFFICIAL_RESULTS_AVAILABLE = False # should become True for past elections

    # Constants and derived params
    GBQ_DEST_DATASET = "evav_processing_2024"
    ORIGIN_URL = "https://earlyvoting.texas-election.com/Elections/getElectionDetails.do"
    CSV_DL_DIR = "downloaded_files"

    OFFICIALNESS = "Official" if OFFICIAL_RESULTS_AVAILABLE else "Unofficial"


    # initialize the driver (mainly to ensure CSVs we download stay in this project folder)    
    driver = init_driver(local_download_path=CSV_DL_DIR)

    # Scrape what we want, based on param `mode`
    if args.mode == 'turnout_data':
        df = get_ev_turnout_data(driver, CSV_DL_DIR, ORIGIN_URL, args.election, OFFICIALNESS) 
    else: 
        last_updated_time = get_poll_places_last_updated(driver, ORIGIN_URL, args.election)
        df = pd.DataFrame({'ppLastUpdatedVal': [last_updated_time]})

    # Upload to GBQ
    bq_tbl_suffix = "" if args.mode == 'turnout_data' else "_pp_last_updated"
    GBQ_DEST_TABLENAME = args.election.replace(" ", "_").lower() + bq_tbl_suffix
    print(f"uploading to GBQ: {GBQ_DEST_DATASET}.{GBQ_DEST_TABLENAME}...\n{df.head()}")
    to_gbq(df, 
            f"{GBQ_DEST_DATASET}.{GBQ_DEST_TABLENAME}", 
            if_exists='replace',
            project_id='demstxsp')
