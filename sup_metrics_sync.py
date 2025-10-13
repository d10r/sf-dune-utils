#!/usr/bin/env python3
"""
SUP Distribution Metrics Sync Daemon

Fetches SUP distribution metrics from the Superfluid API and uploads to Dune.
Supposed to run daily, appending the metrics for that day in a new row to the table.
Table: dune.superfluid_hq.sup_metrics_history (or as specified by TABLE_NAME environment variable)
"""

import requests
import time
import csv
import os
from pathlib import Path
from dune_utils import DuneSyncBase

class SupMetricsSync(DuneSyncBase):
    """SUP Distribution Metrics Sync"""
    
    def __init__(self):
        super().__init__()
        # Hardcode the update interval to 1 day
        self.update_interval = 86400
        self.api_url = "https://sup-metrics-api.superfluid.dev/v1/distribution_metrics"
    
    def fetch_metrics(self):
        """Fetch SUP distribution metrics from API"""
        try:
            response = requests.get(self.api_url, timeout=30)
            
            if response.status_code != 200:
                self.logger.error(f"HTTP {response.status_code}: {response.text}")
                return None
            
            data = response.json()
            return data
        except requests.exceptions.RequestException as e:
            self.logger.error(f"API request failed: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error fetching metrics: {e}")
            return None
    
    
    def get_table_schema(self):
        """Create table schema for SUP metrics"""
        schema = [
            {"name": "timestamp", "type": "timestamp", "nullable": False},
            {"name": "lockers", "type": "bigint", "nullable": True},
            {"name": "staked", "type": "bigint", "nullable": True},
            {"name": "lp", "type": "bigint", "nullable": True},
            {"name": "fontaines", "type": "bigint", "nullable": True},
            {"name": "community_charge", "type": "bigint", "nullable": True},
            {"name": "investors_team_locked", "type": "bigint", "nullable": True},
            {"name": "dao_treasury", "type": "bigint", "nullable": True},
            {"name": "foundation_treasury", "type": "bigint", "nullable": True},
            {"name": "other", "type": "bigint", "nullable": True},
            {"name": "total_supply", "type": "bigint", "nullable": True},
        ]
        return schema
    
    def validate_csv_structure(self, csv_file_path, expected_schema):
        """Validate that CSV structure matches expected schema"""
        try:
            with open(csv_file_path, 'r') as f:
                reader = csv.DictReader(f)
                csv_columns = set(reader.fieldnames)
            
            expected_columns = {col["name"] for col in expected_schema}
            
            if csv_columns != expected_columns:
                self.logger.error(f"CSV columns mismatch. Expected: {expected_columns}, Found: {csv_columns}")
                return False
            
            self.logger.info(f"CSV structure validation passed for {csv_file_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to validate CSV structure: {e}")
            return False
    
    def load_preset_csv_data(self, csv_file_path):
        """Load and parse CSV data from preset file"""
        try:
            preset_data = []
            with open(csv_file_path, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Convert string values to appropriate types
                    processed_row = {}
                    for key, value in row.items():
                        if key == 'timestamp':
                            processed_row[key] = value
                        else:
                            # Convert numeric values, handle empty strings as None
                            processed_row[key] = int(value) if value and value.strip() else None
                    preset_data.append(processed_row)
            
            self.logger.info(f"Loaded {len(preset_data)} rows from preset CSV file")
            return preset_data
            
        except Exception as e:
            self.logger.error(f"Failed to load preset CSV data: {e}")
            return None
    
    def process_metrics(self):
        """Process and upload metrics data using Dune's programmatic table management"""
        self.logger.info(f"Starting SUP metrics sync at {self.timestamp}")
        
        # Get configuration
        table_name = os.getenv("SUP_METRICS_TABLE_NAME", "sup_metrics_history")
        init_table = os.getenv("SUP_METRICS_INIT")
        
        # Initialize table if requested
        if init_table:
            return self._initialize_table(table_name)
        
        # Normal operation: assume table exists and add latest entry
        return self._add_latest_entry(table_name)
    
    def _initialize_table(self, table_name):
        """Initialize table: create, clear, and load preset data"""
        self.logger.info("SUP_METRICS_INIT is set, initializing table")
        
        # Create table (allow failure if exists)
        schema = self.get_table_schema()
        self.create_table(
            table_name=table_name,
            schema=schema,
            description="SUP distribution metrics historical data",
            is_private=False
        )
        
        # Clear table data
        if not self.clear_table_data(table_name):
            self.logger.error("Failed to clear table data, aborting sync")
            return False
        
        # Load preset data
        return self._load_preset_data(table_name)
    
    def _add_latest_entry(self, table_name):
        """Add latest metrics entry to existing table"""
        # Fetch and insert current metrics data
        data = self.fetch_metrics()
        if not data:
            self.logger.error("Failed to fetch metrics data")
            return False
        
        metrics = data.get("metrics", {})
        self.logger.info(f"Fetched metrics data for {len(metrics)} metrics")
        
        success = self._insert_current_data(table_name, metrics)
        
        # Provide helpful error message if table doesn't exist
        if not success:
            self.logger.error(f"Failed to insert data into table '{table_name}'. If the table doesn't exist, run with SUP_METRICS_INIT=true to initialize it.")
        
        return success
    
    def _load_preset_data(self, table_name):
        """Load and insert preset CSV data if file exists"""
        preset_csv_path = Path("sup_metrics_preset.csv")
        
        if not preset_csv_path.exists():
            self.logger.info("No preset CSV file found")
            return True
        
        self.logger.info(f"Preset CSV file found: {preset_csv_path}")
        
        preset_data = self.load_preset_csv_data(preset_csv_path)
        if not preset_data:
            self.logger.error("Failed to load preset data")
            return False
        
        self.logger.info(f"Inserting {len(preset_data)} preset rows into table")
        preset_success = self.insert_data_to_dune(table_name, preset_data)
        if preset_success:
            self.logger.info("Successfully inserted preset data")
            return True
        else:
            self.logger.error("Failed to insert preset data")
            return False
    
    def _insert_current_data(self, table_name, metrics):
        """Insert current metrics data"""
        from datetime import datetime
        current_timestamp = datetime.now().isoformat()
        
        new_row = {
            'timestamp': current_timestamp,
            'lockers': metrics.get('reserveBalances'),
            'staked': metrics.get('stakedSup'),
            'lp': metrics.get('lpSup'),
            'fontaines': metrics.get('streamingOut'),
            'community_charge': metrics.get('communityCharge'),
            'investors_team_locked': metrics.get('investorsTeamLocked'),
            'dao_treasury': metrics.get('daoTreasury'),
            'foundation_treasury': metrics.get('foundationTreasury'),
            'other': metrics.get('other'),
            'total_supply': metrics.get('totalSupply'),
        }
        
        try:
            success = self.insert_data_to_dune(table_name, [new_row])
            if success:
                self.logger.info(f"Successfully inserted data for {self.date_string}")
                return True
            else:
                self.logger.error("Failed to insert data")
                return False
        except Exception as e:
            self.logger.error(f"Insert process failed: {e}")
            return False
    
    def sync_once(self):
        """Execute a single sync cycle"""
        self.logger.info(f"Starting SUP metrics sync cycle at {self.timestamp}")
        
        success = self.process_metrics()
        
        if success:
            self.logger.info(f"SUP metrics sync completed successfully at {self.timestamp}")
        else:
            self.logger.error(f"SUP metrics sync failed at {self.timestamp}")
        
        return success
    
    def run(self):
        """Main daemon execution loop"""
        self.logger.info("Starting SUP Metrics Sync Daemon")
        self.logger.info(f"Update interval: {self.update_interval} seconds ({self.update_interval // 3600} hours)")
        self.logger.info("Press Ctrl+C to stop")
        
        try:
            while True:
                self.sync_once()
                self.logger.info(f"Waiting {self.update_interval} seconds until next sync...")
                time.sleep(self.update_interval)
        except KeyboardInterrupt:
            self.logger.info("Received keyboard interrupt, shutting down...")
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
        
        self.logger.info("SUP Metrics Sync Daemon stopped")


if __name__ == "__main__":
    SupMetricsSync().run()
