#!/usr/bin/env python3
"""
SUP Metrics Download Script

Downloads data from the SUP metrics table in Dune and saves it to a local CSV file.
Table name is specified by SUP_METRICS_TABLE_NAME environment variable (default: sup_metrics_history)
"""

import os
import csv
from dune_utils import DuneSyncBase

def main():
    """Download SUP metrics table data to CSV"""
    # Get table name from environment variable
    table_name = os.getenv("SUP_METRICS_TABLE_NAME", "sup_metrics_history")
    
    # Initialize base class for Dune operations
    sync = DuneSyncBase()
    
    # Download table data
    sync.logger.info(f"Downloading data from table: {table_name}")
    data = sync.download_table_data(table_name)
    
    if not data:
        sync.logger.error("Failed to download data")
        return False
    
    if len(data) == 0:
        sync.logger.warning("Table is empty, no data to save")
        return True
    
    # Determine output filename
    output_file = f"{table_name}.csv"
    
    # Write data to CSV
    try:
        with open(output_file, 'w', newline='') as f:
            if data:
                fieldnames = data[0].keys()
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(data)
        
        sync.logger.info(f"Successfully saved {len(data)} rows to {output_file}")
        return True
        
    except Exception as e:
        sync.logger.error(f"Failed to write CSV file: {e}")
        return False

if __name__ == "__main__":
    main()

