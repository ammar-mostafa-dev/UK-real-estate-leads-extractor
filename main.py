import csv
import time
from AgencyStructure import Agency
from AgenciesDatasExtractor import AgencyDataExtractor 
from AgenciesLinksExtractor import AgencyLinksExtractor 
from helper_functions import setup_logger,get_soup
from os.path import exists
from DataProccesor import DataProcessor
import asyncio 
import aiohttp
from bs4 import BeautifulSoup
import pandas as pd
import random 
# setup logger 
logger = setup_logger(__name__)
class MainScraper: 
    def __init__(self):
        # the main data columns 
        self.file_headers = Agency.get_headers()

        self.main_url = "https://www.yelu.uk/"
        self.current_url = ""
        self.csv_file = "agencies_csv.csv"
        self.excel_file = "UK_RealEstate_leads_data_sample.xlsx"
        self.log_file = "scraper.log"
        # attributes for tracking
        self.page_limit = 500 # this attribute is responsible for detecting page number we stop the scraper at 
        self.failed_page_links = 0 # to track number of pages failed pages failed
        self.current_page_num = 1
        self.failure_limit = 7 # this attribute is responsible for the number of failed attempts you allow the scraper to continue until it reaches it 
    async def get_soup_async(self, session, url): 
       # Define Headers
        headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive", 
}
        try:
            async with session.get(url, headers=headers, timeout=10) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, "lxml")
                    return soup
                else:
                    logger.debug(f"Failed to fetch {url}, Status: {response.status}")
                    return None
        except Exception:
            return None
    async def scrape_agency_async(self,session,url) : 
        """"Scrape A single agency url asynchronously"""
        # Get the soup using our async method
        await asyncio.sleep(random.uniform(0.5,1.5))
        soup = await self.get_soup_async(session, url)
        
        if not soup:
            return None
        # Extract Agency Data
        try:
            agency_data_extractor = AgencyDataExtractor(soup)
            agency_data = agency_data_extractor.agency_data_extractor(url)
            if agency_data : 
                return agency_data
            logger.warning(f"Couldn't Scrape Agency {url} Data")
        except Exception as e:
            logger.warning(f"Error parsing data for {url}: {e}")
            return None

    def scraper_status_checker(self): 
        """This function checks if the scraper has just started or if it\"s been resumed"""
        leads_urls = self.get_scraped_leads_urls()
        leads_count = len(leads_urls)
        if leads_count: 
            file_opening_mode = "a"
            pages_scraped = leads_count // 20 
            logger.info(f"Scraper Resumed At Page {pages_scraped+1}")
        
        else:
            file_opening_mode = "w"
            pages_scraped = 0 
            logger.info("Scraper Started") 
            # force clear the log file 
            open("scraper.log","w").close()
        return {"FileOpeningMode":file_opening_mode,"PagesScraped":pages_scraped,"LeadsUrls":leads_urls}


    def get_scraped_leads_urls(self) : 
        """Returns the number of already scraped leads if file exists, Else None ,Highly efficient line counting for large datasets."""
        existing_urls = set()
        if not exists(self.csv_file):
            # an empty set
            return existing_urls
   
        try:
            with open(self.csv_file, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    url = row.get("source_url") # get the source url from each row 
                    if url:
                        existing_urls.add(url)
            
            logger.info(f"Detected {len(existing_urls)} existing leads in CSV.")
            return existing_urls
        except Exception as e:
            logger.error(f"Error reading existing CSV: {e}")
            # an empty set 
            return existing_urls 
    async def run(self): 
        """Main Loop To manage the concurrent scraping """
        scraper_status  = self.scraper_status_checker()

        file_opening_mode = scraper_status["FileOpeningMode"] 
        pages_scraped = scraper_status["PagesScraped"]
        leads_scraped_urls = scraper_status["LeadsUrls"]

        self.current_page_num = pages_scraped + 1 
        self.current_url = AgencyLinksExtractor.build_next_page_url(self.main_url,self.current_page_num)
        # to manage speed of concurrent tasks
        semaphore = asyncio.Semaphore(2)
        # if the scraper has just started we"ll open the file in w mode (to add headers and also force clear the file),otherwise we"ll open it at a mode (to append new values without having to write headers again)
        with open(self.csv_file,file_opening_mode, newline="", encoding="utf-8") as f: 
            writer = csv.DictWriter(f, fieldnames=self.file_headers)
            if file_opening_mode == "w" : writer.writeheader() # if that"s the first time the file opened (the scraper has just started)
            # to track failures that occured 
            self.failed_page_links = 0 
            # Define a single session to reuse connections 
            async with aiohttp.ClientSession() as session : 
                while self.current_page_num <= self.page_limit : 
                    #Get Page Soup (Using Async)
                    self.current_url = AgencyLinksExtractor.build_next_page_url(self.main_url, self.current_page_num)
                    for i in range (0,3): 
                        soup = await self.get_soup_async(session, self.current_url)
                        if soup : break 
                        logger.warning(f"Couldn't Get page {self.current_page_num} Soup , Trying {3-i} More Times")
                    if not soup:
                        logger.error(f"Couldn't Get Soup For Page {self.current_page_num}")
                        self.failed_page_links += 1
                        if self.failed_page_links >= self.failure_limit: break
                        self.current_page_num += 1
                        continue

                    # Get Links from Soup
                    links_extractor = AgencyLinksExtractor(soup, self.main_url)
                    agencies_urls = links_extractor.get_all_urls()
                    
                    if not agencies_urls:
                        logger.warning(f"No agencies found on page {self.current_page_num}")
                        self.current_page_num += 1
                        continue
                    toscrape_urls = {url for url in agencies_urls if url not in leads_scraped_urls}
                    # create the tasks 
                    tasks = []
                    for url in toscrape_urls :
                        tasks.append(self.sem_task(semaphore,session,url))
                    
                    # Run tasks concurrently and get the results (agencies datas)
                    agencies_datas = await asyncio.gather(*tasks)
                    # to track agencies scraped 
                    scraped_agencies = 0 
                    for agency_data in agencies_datas : 
                        if agency_data : 
                            writer.writerow(agency_data)
                            scraped_agencies +=1 
                        else : 
                          logger.debug(f"Couldn't Extract Agency : {url} data ")
                    # a retry logic for all failed agencies in the page
                    failed_on_this_page = [url for i, url in enumerate(toscrape_urls) if agencies_datas[i] is None]

                    if failed_on_this_page:
                        await asyncio.sleep(4) # delay
                        retry_tasks = [self.sem_task(semaphore, session, url) for url in failed_on_this_page]
                        retry_results = await asyncio.gather(*retry_tasks)
                        if retry_results : 
                            for data in retry_results:
                                if data:
                                    writer.writerow(data)
                                    scraped_agencies += 1
                        else : 
                            logger.warning(f"Couldn't Scrape {len(failed_on_this_page) }Agencies On Page {self.current_page_num}")

                    if scraped_agencies != 0 : 
                        logger.info(f"Page {self.current_page_num} Agencies Have Been Scraped,+{scraped_agencies},missing {len(toscrape_urls)-scraped_agencies}")
                        self.failed_page_links = 0 
                    else : 
                        logger.warning(f"Couldn't Scrape Page {self.current_page_num}")
                        self.failed_page_links += 1 
                        if self.failed_page_links == self.failure_limit : 
                            logger.error(f"{self.failure_limit} consequitive failures , stopping to protect IP")
                            break
                    self.current_page_num += 1

                    # the url for the page we"re on  
                    self.current_url = AgencyLinksExtractor.build_next_page_url(self.main_url,self.current_page_num)
    def handle_getting_soup(self) : 
        """This function trys to get the page soup for three times , returns soup or None"""
        for i in range(1,4):
            soup = get_soup(self.current_url)
            if soup:
                return soup  # Return immediately on success

            logger.warning(f"Attempt {i}/3 failed for Page {self.current_page_num}. Retrying...")
            time.sleep(3)
        
        return None  # All attempts failed
    async def sem_task(self, semaphore, session, url):
        async with semaphore:
            return await self.scrape_agency_async(session, url)

def main() : 
    scraper = MainScraper()
    asyncio.run(scraper.run())
    # Exporting the data and finalizing scraper 
    processor = DataProcessor("agencies_csv.csv")
    
    processor.run_pipeline()

if __name__ == "__main__" : 
    main()

