from bs4 import BeautifulSoup
import requests
import logging
def get_soup(url):
    '''This function makes a good request to the url and returns the soup (HTML) of the passed URL if the request had succeeded, otherwise returns None '''  
    # mandatory headers
    headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive" ,
}
    # Using Try To handle Errors 
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()   # catches 4xx and 5xx status codes
    except Exception as e:
        return None
    response.encoding = 'utf-8'
    soup = BeautifulSoup(response.text, 'lxml')
    return soup


def setup_logger(
    name: str,
    log_file: str = "scraper.log",
    level: int = logging.INFO
):
    """
    Standard function to configure and return a logger.
    """
    logger = logging.getLogger(name)
    
    # If the logger already has handlers, don't add them again (prevents duplicate logs)
    if not logger.handlers:
        logger.setLevel(level)

        # 1. Console Format (Clean for you to read while running)
        console_formatter = logging.Formatter("%(levelname)s | %(message)s")
        
        # 2. File Format (Detailed for debugging later)
        file_formatter = logging.Formatter("%(asctime)s | %(levelname)s |  %(message)s")

        # Console Handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(console_formatter)

        # File Handler
        file_handler = logging.FileHandler(log_file, encoding="utf-8", mode="a")
        file_handler.setFormatter(file_formatter)

        logger.addHandler(console_handler)
        logger.addHandler(file_handler)
        logger.propagate = False

    return logger