#!/usr/bin/env python3
"""
SuperToken Holders Sync Daemon

Fetches SuperToken holders from all mainnet Superfluid networks and uploads to Dune.
Runs continuously with configurable update intervals.
"""

import csv
import os
import requests
import time
from pathlib import Path
from dune_utils import DuneSyncBase

API_BASE = "https://supertoken-api.s.superfluid.dev/v0"

class SuperTokenSync(DuneSyncBase):
    def __init__(self):
        super().__init__()
        # Override default update interval with SUPERTOKEN_HOLDERS_UPDATE_INTERVAL if set
        self.update_interval = int(os.getenv("SUPERTOKEN_HOLDERS_UPDATE_INTERVAL", self.update_interval))

    def get_networks(self):
        """Get all mainnet networks from Superfluid metadata"""
        try:
            response = requests.get("https://raw.githubusercontent.com/superfluid-org/protocol-monorepo/refs/heads/dev/packages/metadata/networks.json")
            networks = response.json()
            return [net for net in networks if not net.get("isTestnet", True)]
        except:
            self.logger.error("Failed to fetch networks")
            return []

    def get_tokens(self, network):
        """Get SuperTokens from subgraph"""
        try:
            endpoint = network["subgraphV1"]["hostedEndpoint"]
            
            query = """
            query { tokens(first: 1000, where: {isListed: true}) { id symbol name } }
            """
            
            response = requests.post(endpoint, json={"query": query}, timeout=30)
            
            if response.status_code != 200:
                self.logger.error(f"    Subgraph HTTP {response.status_code}: {response.text}")
                return []
            
            data = response.json()
            if "errors" in data:
                self.logger.error(f"    Subgraph errors: {data['errors']}")
                return []
            
            return data["data"]["tokens"]
        except requests.exceptions.RequestException as e:
            self.logger.error(f"    Subgraph request failed: {e}")
            return []
        except Exception as e:
            self.logger.error(f"    Failed to get tokens: {e}")
            return []

    def get_holders(self, token_address, chain_id):
        """Get holders from API"""
        try:
            url = f"{API_BASE}/tokens/{token_address}/holders"
            response = requests.get(url, params={"chainId": chain_id, "limit": 1000000}, timeout=30)
            
            # Check HTTP status code
            if response.status_code == 404:
                self.logger.warning(f"    Token not found (404) for {token_address}")
                return None
            elif response.status_code != 200:
                self.logger.warning(f"    HTTP {response.status_code} for {token_address}: {response.text}")
                return None
            
            data = response.json()
            if "error" in data:
                self.logger.warning(f"    API error for {token_address}: {data['error']}")
                return None
            
            return data
        except requests.exceptions.RequestException as e:
            self.logger.warning(f"    API request failed for {token_address}: {e}")
            return None
        except Exception as e:
            self.logger.warning(f"    Unexpected error getting holders for {token_address}: {e}")
            return None

    def process_token(self, token, network):
        """Process a single token"""
        # Validate token data
        if not token.get("id"):
            self.logger.warning(f"  Skipping token with missing address: {token}")
            return
        
        token_address = token["id"]
        token_symbol = token.get("symbol")
        
        # Skip tokens with missing or empty symbols
        if not token_symbol or token_symbol.strip() == "":
            self.logger.warning(f"  Skipping token with missing symbol: {token_address} ({token.get('name', 'Unknown')})")
            return
        
        chain_id = network["chainId"]
        dune_name = network.get("duneName", f"chain_{chain_id}")
        
        self.logger.info(f"  Processing {token_symbol} ({token_address}) on {network['name']}")
        
        # Get holders
        holders_data = self.get_holders(token_address, chain_id)
        if holders_data is None:
            self.logger.warning(f"    Failed to get holders data for {token_symbol} - skipping")
            return
        
        if not holders_data.get("holders"):
            self.logger.info(f"    No holders for {token_symbol}")
            return
        
        holders = holders_data["holders"]
        self.logger.info(f"    Found {len(holders)} holders for {token_symbol}")
        
        # Create CSV
        try:
            csv_path = Path(f"data/{dune_name}_{token_symbol.lower()}_holders.csv")
            with open(csv_path, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['address', 'balance', 'net_flowrate'])
                for holder in holders:
                    writer.writerow([holder.get('address', ''), holder.get('balance', ''), holder.get('netFlowRate', '')])
            
            self.logger.info(f"    Created CSV: {csv_path}")
        except Exception as e:
            self.logger.error(f"    Failed to create CSV for {token_symbol}: {e}")
            return
        
        # Upload to Dune
        try:
            with open(csv_path, 'r') as f:
                csv_data = f.read()
            
            table_name = f"{dune_name}_{token_symbol.lower()}_holders"
            
            # if env var DRY_RUN is set to true, skip the upload
            if os.getenv("DRY_RUN"):
                self.logger.info(f"    Skipping upload for {token_symbol} - DRY_RUN is set")
                return

            success = self.upload_csv_to_dune(
                csv_data=csv_data,
                table_name=table_name,
                description=f"{token_symbol} holders",
                is_private=False
            )
            
            if success:
                # Archive immediately after successful processing
                self.archive_file(csv_path)
        except Exception as e:
            self.logger.error(f"    Upload process failed for {token_symbol}: {e}")
        
        

    def sync_once(self):
        """Execute a single sync cycle"""
        self.logger.info(f"Starting sync cycle at {self.timestamp}")
        
        networks = self.get_networks()
        self.logger.info(f"Found {len(networks)} mainnet networks")
        
        # Debug mode: process only first network
        if os.getenv("DEBUG"):
            networks = networks[:1]
            self.logger.info("DEBUG mode: processing only first network")
        
        for network in networks:
            chain_id = network["chainId"]
            
            # Skip chains without duneName
            if not network.get("duneName"):
                self.logger.info(f"Skipping {network['name']} (chainId {chain_id}) - no Dune support")
                continue
            
            self.logger.info(f"Processing {network['name']} (chainId {chain_id})")
            
            tokens = self.get_tokens(network)
            self.logger.info(f"  Found {len(tokens)} SuperTokens")
            
            for token in tokens:
                self.process_token(token, network)
                time.sleep(0.5)  # Rate limiting
        
        self.logger.info(f"Sync cycle completed at {self.timestamp}")

    def run(self):
        """Main daemon execution loop"""
        self.logger.info(f"Starting SuperToken Sync Daemon")
        self.logger.info(f"Update interval: {self.update_interval} seconds")
        self.logger.info(f"Press Ctrl+C to stop")
        
        try:
            while True:
                self.sync_once()
                self.logger.info(f"Waiting {self.update_interval} seconds until next sync...")
                time.sleep(self.update_interval)
        except KeyboardInterrupt:
            self.logger.info("Received keyboard interrupt, shutting down...")
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
        
        self.logger.info("SuperToken Sync Daemon stopped")

if __name__ == "__main__":
    SuperTokenSync().run()
