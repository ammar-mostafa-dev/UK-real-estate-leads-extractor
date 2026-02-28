import pandas as pd 
from helper_functions import setup_logger
from os.path import exists
import time
# setup logger
logger = setup_logger(__name__)

class DataProcessor: 
    def __init__(self,file_path,excel_file_name='UK_RealEstate_leads_data_sample.xlsx'):
        self.csv_file = file_path 
        self.df = None
        self.excel_file = excel_file_name

    def finalize_scraper(self):
        if not exists(self.csv_file):
            logger.error("Report Failed: No data file found.")
            return
        try:
            df = self.df
            total_rows = len(df)
            
            if total_rows == 0:
                logger.warning("Integrity Report: No leads found to analyze.")
                return

            report_lines = [
                "", 
                "═" * 20,
                f"       Final Data Integrity Report  ({time.strftime('%Y-%m-%d %H:%M')})",
                f"       Total Agencies Scraped: {total_rows}",
                "═" * 20
            ]

            for column in df.columns:
                missing_count = df[column].isna().sum()
                success_rate = ((total_rows - missing_count) / total_rows) * 100
                
                report_lines.append(f" {column.replace('_', ' ').title():<20} | Success: {success_rate:>6.2f}% ")

            report_lines.append("═" * 20)
            logger.info("\n".join(report_lines))
            logger.info()            
        except Exception as e:
            logger.error(f"Integrity check failed: {e}")
    def convert_to_excel(self):
        try:
            # Load the CSV
            df = self.df
            # Save to Excel
            df.to_excel(self.excel_file, index=False)
            logger.info(f"Success! Saved {len(df)} agencies to {self.excel_file}")
        except Exception as e:
            logger.error(f"Excel conversion failed: {e}")