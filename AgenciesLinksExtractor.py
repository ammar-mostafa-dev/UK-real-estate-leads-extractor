from helper_functions import get_soup
from urllib.parse import urljoin
class AgencyLinksExtractor:
    def __init__(self, soup,main_url='https://www.yelu.uk/'):
        # check if user passed a non value soup 
        if not soup  :
            raise ValueError("AgenciesLinksExtractor received an empty or invalid Soup .")
        self.main_url = main_url
        self.soup = soup
        # the selector to get the links in the page
        self.links_selector = '.company_header h3 a' # Or whatever your final selector is
        # the selector to get next page link 
        self.next_page_url_selector = 'a[rel="next"]'
    def get_all_urls(self):
        '''This function gets the agencies links from a page url and returns them as full urls ready to be scraped, handles invalid soup and any errors'''
        try :
            tags = self.soup.select(self.links_selector)
            return [urljoin(self.main_url,tag['href']) for tag in tags if tag.has_attr('href')]
        except Exception :
            print("Invalid Soup passed")
    def get_next_url(self,page_num=None) : 
        '''This function returns the next page url by trying two ways , first dynamically by finding the button , second manaually by building a url using the page number only ''' 
        # try to extract the next page url dynamically first
        next_page_button = self.soup.select_one(self.next_page_url_selector)
        
        if next_page_button and next_page_button.has_attr('href'): 
            return urljoin(self.main_url, next_page_button['href']) 
        # since the dynamic way failed , try building it manually

        # check if a page number is passed
        if page_num is not None:
            return self.build_next_page_url(self.main_url,page_num)
        # If no button and no page_num were found , return None to STOP the scraper
        return None
    @staticmethod
    def build_next_page_url(main_url,page_num) : 
        '''This function builds a url manually using a pattern (the pattern which the website uses in the current date (2/22/2026))'''
        pattern = 'category/estate-agents/{page_number}'
        return urljoin(main_url,pattern.format(page_number=page_num))
def main(): 
    page = 'https://www.yelu.uk/category/estate-agents/1'

    extractor = AgencyLinksExtractor(get_soup(page),'https://www.yelu.uk/')

    print(extractor.get_next_url())
if __name__ == '__main__' : 
    main()
