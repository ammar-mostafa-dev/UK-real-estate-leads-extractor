import requests 
from bs4 import BeautifulSoup
from helper_functions import get_soup

class AgencyDataExtractor: 
    def __init__(self,soup):
        self.soup = soup 
        self.selectors = {
            "name": '#company_name',
            "address": '#company_address',
            'website_url':'.text.weblinks > a',
            'phone': '.text.phone a',
            'extra_info' : '.extra_info .info .label' , # this selector gets three labels (employees range, establishment date,company manager )
        }
    def agency_data_extractor(self,src_url) : 
        '''This function extracts all data using selectors we defined , returns a dictionary full of values and whenever an value aint found it puts N/A as its value '''
        # a dict to store the data we extracted so we can pass it to the agency class and assign it with default values 
        dict_data = {'source_url':src_url,'employees_range':'N/A','establish_date':'N/A'}
        for property,selector in self.selectors.items() : 
            if property != 'extra_info': 
                value = self.soup.select_one(selector)
                value = value.text.strip() if value else 'N/A'
                dict_data[property] = value 
            # if we're extracting the extra info (The 3 labels (employees range , company establishment date , company manager name (we won't get the manager name))
            else :
                # get 3 labels 
                labels = self.soup.select(selector)
                # 1. get label tag
                for label_tag in labels:
                    # 2. Get label title (Employees or establishment date, etc...)
                    title = label_tag.text.strip() 
                    # 3. get the label value (the value that comes immediatly after the taag)
                    value = label_tag.next_sibling
                    if value:
                        clean_value = value.strip()
                    else:
                        clean_value = "N/A"

                    if title == 'Establishment year':
                        dict_data['establish_date'] = clean_value
                    elif title == 'Employees':
                        dict_data['employees_range'] = clean_value                    
        return dict_data          

def main():
    url = 'https://www.yelu.uk/company/718140/aaa-property-co'
    agency_extractor = AgencyDataExtractor(get_soup(url))

    value = agency_extractor.agency_data_extractor(url)

    print(value)

if __name__ == '__main__' : 
    main()