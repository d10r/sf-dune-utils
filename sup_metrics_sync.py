#!/usr/bin/env python3
"""
SUP Distribution Metrics Sync Daemon

Fetches SUP distribution metrics from the Superfluid API and uploads to Dune.
Supposed to run daily, appending the metrics for that day in a new row to the table.
Table: dune.superfluid_hq.sup_metrics_history
"""

import requests
import time
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
    
    
    def create_table_schema(self, metrics):
        """Create table schema for SUP metrics"""
        schema = [
            {"name": "date", "type": "string", "nullable": False},
            {"name": "reserve_balances", "type": "bigint", "nullable": True},
            {"name": "staked_sup", "type": "bigint", "nullable": True},
            {"name": "lp_sup", "type": "bigint", "nullable": True},
            {"name": "streaming_out", "type": "bigint", "nullable": True},
            {"name": "community_charge", "type": "bigint", "nullable": True},
            {"name": "investors_team_locked", "type": "bigint", "nullable": True},
            {"name": "dao_treasury", "type": "bigint", "nullable": True},
            {"name": "foundation_treasury", "type": "bigint", "nullable": True},
            {"name": "other", "type": "bigint", "nullable": True},
            {"name": "total_supply", "type": "bigint", "nullable": True},
        ]
        return schema
    
    def process_metrics(self):
        """Process and upload metrics data using Dune's programmatic table management"""
        self.logger.info(f"Starting SUP metrics sync at {self.timestamp}")
        
        # Fetch metrics data
        data = self.fetch_metrics()
        if not data:
            self.logger.error("Failed to fetch metrics data")
            return False
        
        metrics = data.get("metrics", {})
        self.logger.info(f"Fetched metrics data for {len(metrics)} metrics")
        
        table_name = "sup_metrics_history"
        
        # Create new row data with correct column names
        new_row = {
            'date': self.date_string,
            'reserve_balances': metrics.get('reserveBalances'),
            'staked_sup': metrics.get('stakedSup'),
            'lp_sup': metrics.get('lpSup'),
            'streaming_out': metrics.get('streamingOut'),
            'community_charge': metrics.get('communityCharge'),
            'investors_team_locked': metrics.get('investorsTeamLocked'),
            'dao_treasury': metrics.get('daoTreasury'),
            'foundation_treasury': metrics.get('foundationTreasury'),
            'other': metrics.get('other'),
            'total_supply': metrics.get('totalSupply'),
        }
        
        # Create table (will succeed if exists, fail gracefully if already exists)
        schema = self.create_table_schema(metrics)
        self.create_table(
            table_name=table_name,
            schema=schema,
            description="SUP distribution metrics historical data",
            is_private=False
        )
        
        # Insert new row using Dune's insert API
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
