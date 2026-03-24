import pandas as pd 
from helper_functions import setup_logger
from os.path import exists
import time
# setup logger
logger = setup_logger(__name__)

class DataProcessor: 
    def __init__(self,file_path,excel_file_name='UK_RealEstate_leads_data_sample.xlsx'):
        self.csv_file = file_path 
        self.df = pd.read_csv(self.csv_file)
        self.excel_file = excel_file_name
        # Normalize Columns Names 
        self.df.columns = self.df.columns.str.strip().str.lower().str.replace(" ", "_")

        # Standarize Null values
        self.df = self.df.replace(["N/A", "NA", "na", "-", ""], pd.NA)
        # Initialize Tiers 
        self.gold_df = None
        self.platinum_df = None 
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
                "═" * 60,
                f"       Final Data Integrity Report  ({time.strftime('%Y-%m-%d %H:%M')})",
                f"       Total Agencies Scraped: {total_rows}",
                "═" * 60
            ]

            for column in df.columns:
                missing_count = df[column].isna().sum()
                success_rate = ((total_rows - missing_count) / total_rows) * 100
                
                report_lines.append(f" {column.replace('_', ' ').title():<20} | Existance: {success_rate:>6.2f}% ")

            report_lines.append("═" * 20)
            logger.info("\n".join(report_lines))
            duplicates = df.duplicated(subset=['source_url']).sum()
            logger.info(f'Found {duplicates} Duplicates')            
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

    def filter_gold_data(self): 
        # load data frame 
        df = self.df
        # Define Required Fields
        gold_fields = [
            "name",
            "address",
            "source_url",
            "website_url",
            "phone",
            "employees_range",
            "established_date"
        ]
        # Gold Data Filter 
        self.gold_df = df.dropna(subset=gold_fields)
        # Export Gold Data 
        self.gold_df.to_csv(f"Uk_RealEstate_Gold_Leads.csv", index=False)
    def filter_platinum_data(self): 
        # Load Data Frame
        df = self.df 
        platinum_fields = [
            "name",
            "address",
            "source_url",
            "website_url",
            "phone"
        ]
        # Apply Platinum Data filter
        self.platinum_df = df.dropna(subset=platinum_fields)

        # Remove Repeated Gold rows from Platinum to avoid duplication
        self.platinum_df = self.platinum_df[~self.platinum_df.index.isin(self.gold_df.index)]
        # Export Platinum data 
        self.platinum_df.to_csv(f'Uk_RealEstate_Platinum_Leads')