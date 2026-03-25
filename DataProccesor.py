import pandas as pd
import os
from helper_functions import setup_logger
from os.path import exists 
import time 

logger = setup_logger(__name__)
class DataProcessor:
    """
    Automated pipeline for cleaning, tiering, and exporting UK Real Estate leads.
    Filters leads into Gold, Platinum, and Regular tiers based on data depth.
    """

    def __init__(self, file_path):
        self.file_path = file_path
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Input file not found: {file_path}")
            
        self.raw_df = pd.read_csv(file_path)
        
        # Define hierarchical column structures
        self.basic_cols = ['name', 'phone', 'address', 'source_url']
        self.platinum_cols = self.basic_cols + ['website_url']
        self.gold_cols = self.platinum_cols + ['establish_date', 'employees_range']

        # Professional header mapping
        self.header_mapping = {
            'source_url': 'Lead Source URL',
            'name': 'Agency Name',
            'phone': 'Phone Number',
            'website_url': 'Website',
            'address': 'Business Address',
            'employees_range': 'Team Size',
            'establish_date': 'Founded Year'
        }

    def clean_data(self):
        """Standardizes raw extraction data and removes duplicates."""
        df = self.raw_df.copy()

        # Primary deduplication based on unique source URL
        df = df.drop_duplicates(subset=['source_url'])

        # Global whitespace stripping for all string-based entries
        df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

        # URL normalization to ensure secure protocol prefix
        df['website_url'] = df['website_url'].apply(
            lambda x: f"https://{x}" if pd.notnull(x) and not str(x).startswith('http') else x
        )
        
        self.master_df = df

    def assign_tiers(self):
        """
        Executes hierarchical isolation logic to categorize leads by data fidelity.
        Priority Flow: Gold -> Platinum -> Regular.
        """
        df = self.master_df.copy()

        # 1. Gold Tier: Premium leads with full  metadata
        # Required: Website AND Establish Date AND Employees Range
        self.gold_df = df[
            df['website_url'].notnull() & 
            df['establish_date'].notnull() & 
            df['employees_range'].notnull()
        ]
        self.gold_df = self.gold_df[self.gold_cols]

        # 2. Platinum Tier: High-intent leads with validated web presence
        # Required: Website (but missing full metadata)
        remaining_after_gold = df[~df['source_url'].isin(self.gold_df['source_url'])]
        self.platinum_df = remaining_after_gold[remaining_after_gold['website_url'].notnull()]
        self.platinum_df = self.platinum_df[self.platinum_cols]

        # 3. Regular Tier: Standard leads with basic contact information
        # Required: Minimal attributes only
        already_processed = pd.concat([self.gold_df['source_url'], self.platinum_df['source_url']])
        self.regular_df = df[~df['source_url'].isin(already_processed)]
        self.regular_df = self.regular_df[self.basic_cols]

    def finalize_and_export(self, output_name="UK_RealEstate_Master_Leads.xlsx"):
        """Consolidates all tiers into a single multi-sheet professional deliverable."""
        try:
            with pd.ExcelWriter(output_name, engine='xlsxwriter') as writer:
                # Export Tiered Sheets with professional headers
                self.gold_df.rename(columns=self.header_mapping).to_excel(writer, sheet_name="Gold_Tier", index=False)
                self.platinum_df.rename(columns=self.header_mapping).to_excel(writer, sheet_name="Platinum_Tier", index=False)
                self.regular_df.rename(columns=self.header_mapping).to_excel(writer, sheet_name="Regular_Tier", index=False)
                
                # Full master list for reference
                self.master_df.rename(columns=self.header_mapping).to_excel(writer, sheet_name="All_Leads_Raw", index=False)

            logger.info(f"Export successful: {output_name}")
            
        except Exception as e:
            logger.error(f"Export failed: {str(e)}")

    def run_pipeline(self):
        """Execution entry point for the post-processing sequence."""
        logger.info("Initializing Post-Processing...")
        self.clean_data()
        self.assign_tiers()
        self.finalize_and_export()
        logger.info("Lead processing complete.")

        # Generate Full Verified Data Integrity Report 
        if not exists(self.file_path):
            logger.error("Report Failed: No data file found.")
            return
        try:
            df = self.raw_df
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
