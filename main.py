import csv
import time
import requests
from helper_functions import get_soup
from AgencyStructure import Agency
from AgenciesDatasExtractor import AgencyDataExtractor 
from AgenciesLinksExtractor import AgencyLinksExtractor 
from helper_functions import setup_logger
from os.path import exists
from DataProccesor import DataProcessor
# setup logger 
logger = setup_logger(__name__)
class MainScraper: 
    def __init__(self):
        # the main data columns 
        self.file_headers = Agency.get_headers()
        self.main_url = 'https://www.yelu.uk/'
        self.current_url = ''
        self.csv_file = 'agencies_csv.csv'
        self.excel_file = 'UK_RealEstate_leads_data_sample.xlsx'
        self.log_file = 'scraper.log'
        # attributes for tracking
        self.page_limit = 10 # this attribute is responsible for detecting page number we stop the scraper at 
        self.failed_page_links = 0 # to track number of pages failed pages failed
        self.current_page_num = 1
        self.failure_limit = 10 # this attribute is responsible for the number of failed attempts you allow the scraper to continue until it reaches it 
    def scraper_status_checker(self): 
        '''This function checks if the scraper has just started or if it\'s been resumed'''
        leads_urls = self.get_scraped_leads_urls()
        leads_count = len(leads_urls)
        if leads_count: 
            file_opening_mode = 'a'
            pages_scraped = leads_count // 20 
            logger.info(f'Scraper Resumed At Page {pages_scraped+1}')
        
        else:
            file_opening_mode = 'w'
            pages_scraped = 0 
            logger.info('Scraper Started') 
            # force clear the log file 
            open('scraper.log','w').close()
        return {'FileOpeningMode':file_opening_mode,'PagesScraped':pages_scraped,"LeadsUrls":leads_urls}
    def handle_soup_validation(self):
        '''This function handles soup validation process '''
        # get soup 
        soup = self.handle_getting_soup()
        # validate soup 
        if not soup :  
            logger.error(f'Couldn\'t Get Soup For Page {self.current_page_num} After 3 Attempts')
            
            if self.failed_page_links >= self.failure_limit :
                logger.error(f'{self.failure_limit} Continued Pages Soup Failures , Stopping the scraper to protect IP')
                raise Exception
            self.current_url = AgencyLinksExtractor.build_next_page_url(self.main_url,self.current_page_num)     
            self.current_page_num += 1 
        else : 
            return soup 
         
    def scrape_page_agencies(self,to_scrape_urls): 
        expected_urls = len(to_scrape_urls)
        scraped_agencies = 0 # to track agencies scraped
        '''This function gets the agencies urls and scrapes them , uses yield for effeciency'''
        # go through each agency url 
        for url in to_scrape_urls: 
            # get the soup for the url for this agency 
            agency_soup = get_soup(url)
            if not(agency_soup): 
                logger.warning(f"Couldn't Find Agency {url} soup")
                # continue to the next agency, Add a polite delay
                time.sleep(0.5)
                continue
            # extract its data
            data_extractor = AgencyDataExtractor(agency_soup)
            agency_data_dict = data_extractor.all_data_extractor(url)
            if not(agency_data_dict) : 
                logger.warning(f"Couldn't Find Any Data to agency {url}")
                time.sleep(0.5)
                continue 
            yield agency_data_dict 
            scraped_agencies += 1
        # log the Number Of leads Extracted after each scraped page
        logger.info(f'+ {scraped_agencies} Scraped , Missing {expected_urls - scraped_agencies} agencies')


    def get_page_agencies(self) :
        '''This function extracts the agencies with their data in a single'''
        # handle soup failure if not found 
        try : 
            soup = self.handle_soup_validation()
        except Exception : 
            return {}
        
        self.failed_page_links = 0 # Reset failures because we captured a valid page        
        # get the agencies links from the page 
        links_extractor = AgencyLinksExtractor(soup,self.main_url)
        agencies_urls = links_extractor.get_all_urls()
        if not(agencies_urls): 
            logger.error(f"Couldn't Find Page {self.current_page_num} Agencies Links, Continuing to the next page")
            self.failed_page_links +=1 

        logger.info(f'Page {self.current_page_num} agencies links has been captured, Scraping Them Now')
        
        return agencies_urls
    def get_scraped_leads_urls(self) : 
        """Returns the number of already scraped leads if file exists, Else None ,Highly efficient line counting for large datasets."""
        existing_urls = set()
        if not exists(self.csv_file):
            # an empty set
            return existing_urls
   
        try:
            with open(self.csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    url = row.get('source_url') # get the source url from each row 
                    if url:
                        existing_urls.add(url)
            
            logger.info(f"Detected {len(existing_urls)} existing leads in CSV.")
            return existing_urls
        except Exception as e:
            logger.error(f"Error reading existing CSV: {e}")
            # an empty set 
            return existing_urls 
    def run(self): 
        # check if the scraper has just started or it's been resumed 
        scraper_status  = self.scraper_status_checker()

        file_opening_mode = scraper_status['FileOpeningMode'] 
        pages_scraped = scraper_status['PagesScraped']
        leads_scraped_urls = scraper_status['LeadsUrls']

        self.current_page_num = pages_scraped + 1 
        self.current_url = AgencyLinksExtractor.build_next_page_url(self.main_url,self.current_page_num)
       

        # if the scraper has just started we'll open the file in w mode (to add headers and also force clear the file),otherwise we'll open it at a mode (to append new values without having to write headers again)
        with open(self.csv_file,file_opening_mode, newline='', encoding='utf-8') as f: 
            writer = csv.DictWriter(f, fieldnames=self.file_headers)
            if file_opening_mode == 'w' : writer.writeheader() # if that's the first time the file has been opened (The Scraper Has just Started)
            # to track pages that we failed getting their agencies links (entirely)
            self.failed_page_links = 0 
            # the main loop 
            while self.current_page_num <= self.page_limit : 
                # get the current page agencies links 
                agencies_urls = self.get_page_agencies()
                
                if agencies_urls :
                    toscrape_urls = {url for url in agencies_urls if url not in leads_scraped_urls}
                    # we used yield generator to enhance program effeciency 
                    for agency_data in self.scrape_page_agencies(toscrape_urls):  
                        writer.writerow(agency_data)
                        leads_scraped_urls.add(agency_data['source_url'])
                        # to view data instantly
                        f.flush()
                else : 
                    logger.error(f"Couldn't Find Agencies Links For Page {self.current_page_num}")
                    continue
                self.current_url = AgencyLinksExtractor.build_next_page_url(self.main_url,self.current_page_num)
                self.current_page_num +=1 
                        
    def handle_getting_soup(self) : 
        '''This function trys to get the page soup for three times , returns soup or None'''
        for i in range(1,4):
            soup = get_soup(self.current_url)
            if soup:
                return soup  # Return immediately on success

            logger.warning(f"Attempt {i}/3 failed for Page {self.current_page_num}. Retrying...")
            time.sleep(3)
        
        return None  # All attempts failed


def main() : 
    scraper = MainScraper()
    scraper.run()
    scraper_reporter = DataProcessor('agencies_csv.csv')
    scraper_reporter.finalize_scraper()
    scraper_reporter.convert_to_excel()
if __name__ == '__main__' : 
    main()


