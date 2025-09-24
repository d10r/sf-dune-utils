#!/usr/bin/env python3
"""
SuperToken Holders Sync Daemon

Fetches SuperToken holders from all mainnet Superfluid networks and uploads to Dune.
Runs continuously with configurable update intervals.
"""

# Load environment variables from .env file first
import os
from pathlib import Path
try:
    import dotenv
    dotenv.load_dotenv()
except ImportError:
    pass  # dotenv not available, continue without it

import csv
import requests
import time
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

API_BASE = "https://supertoken-api.s.superfluid.dev/v0"

class SuperTokenSync:
    def __init__(self):
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        Path("data").mkdir(exist_ok=True)
        
        # Daemon control
        self.update_interval = int(os.getenv("UPDATE_INTERVAL", "3600"))  # Default 1 hour
        
        # Initialize Dune client
        try:
            from dune_client.client import DuneClient
            self.dune = DuneClient.from_env()
        except:
            self.dune = None

    def get_networks(self):
        """Get all mainnet networks from Superfluid metadata"""
        try:
            response = requests.get("https://raw.githubusercontent.com/superfluid-org/protocol-monorepo/refs/heads/dev/packages/metadata/networks.json")
            networks = response.json()
            return [net for net in networks if not net.get("isTestnet", True)]
        except:
            logger.error("Failed to fetch networks")
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
                logger.error(f"    Subgraph HTTP {response.status_code}: {response.text}")
                return []
            
            data = response.json()
            if "errors" in data:
                logger.error(f"    Subgraph errors: {data['errors']}")
                return []
            
            return data["data"]["tokens"]
        except requests.exceptions.RequestException as e:
            logger.error(f"    Subgraph request failed: {e}")
            return []
        except Exception as e:
            logger.error(f"    Failed to get tokens: {e}")
            return []

    def get_holders(self, token_address, chain_id):
        """Get holders from API"""
        try:
            url = f"{API_BASE}/tokens/{token_address}/holders"
            response = requests.get(url, params={"chainId": chain_id, "limit": 1000000}, timeout=30)
            
            # Check HTTP status code
            if response.status_code == 404:
                logger.warning(f"    Token not found (404) for {token_address}")
                return None
            elif response.status_code != 200:
                logger.warning(f"    HTTP {response.status_code} for {token_address}: {response.text}")
                return None
            
            data = response.json()
            if "error" in data:
                logger.warning(f"    API error for {token_address}: {data['error']}")
                return None
            
            return data
        except requests.exceptions.RequestException as e:
            logger.warning(f"    API request failed for {token_address}: {e}")
            return None
        except Exception as e:
            logger.warning(f"    Unexpected error getting holders for {token_address}: {e}")
            return None

    def process_token(self, token, network):
        """Process a single token"""
        # Validate token data
        if not token.get("id"):
            logger.warning(f"  Skipping token with missing address: {token}")
            return
        
        token_address = token["id"]
        token_symbol = token.get("symbol")
        
        # Skip tokens with missing or empty symbols
        if not token_symbol or token_symbol.strip() == "":
            logger.warning(f"  Skipping token with missing symbol: {token_address} ({token.get('name', 'Unknown')})")
            return
        
        chain_id = network["chainId"]
        dune_name = network.get("duneName", f"chain_{chain_id}")
        
        logger.info(f"  Processing {token_symbol} ({token_address}) on {network['name']}")
        
        # Get holders
        holders_data = self.get_holders(token_address, chain_id)
        if holders_data is None:
            logger.warning(f"    Failed to get holders data for {token_symbol} - skipping")
            return
        
        if not holders_data.get("holders"):
            logger.info(f"    No holders for {token_symbol}")
            return
        
        holders = holders_data["holders"]
        logger.info(f"    Found {len(holders)} holders for {token_symbol}")
        
        # Create CSV
        try:
            csv_path = Path(f"data/{dune_name}_{token_symbol.lower()}_holders.csv")
            with open(csv_path, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['address', 'balance', 'net_flowrate'])
                for holder in holders:
                    writer.writerow([holder.get('address', ''), holder.get('balance', ''), holder.get('netFlowRate', '')])
            
            logger.info(f"    Created CSV: {csv_path}")
        except Exception as e:
            logger.error(f"    Failed to create CSV for {token_symbol}: {e}")
            return
        
        # Upload to Dune
        if self.dune:
            try:
                with open(csv_path, 'r') as f:
                    csv_data = f.read()
                
                table_name = f"{dune_name}_{token_symbol.lower()}_holders"
                
                self.dune.upload_csv(
                    data=csv_data,
                    description=f"{token_symbol} holders",
                    table_name=table_name,
                    is_private=False
                )
                logger.info(f"    Uploaded {table_name} to Dune")

                # Archive immediately after successful processing
                try:
                    archive_path = Path(f"data/archive/{csv_path.name}_{self.timestamp}")
                    archive_path.parent.mkdir(parents=True, exist_ok=True)
                    import shutil
                    shutil.move(str(csv_path), str(archive_path))
                    logger.info(f"    Archived: {csv_path.name}")
                except Exception as e:
                    logger.error(f"    Failed to archive {token_symbol}: {e}")
            except Exception as e:
                logger.error(f"    Upload failed for {token_symbol}: {e}")
        
        

    def sync_once(self):
        """Execute a single sync cycle"""
        logger.info(f"Starting sync cycle at {datetime.now().strftime('%Y%m%d_%H%M%S')}")
        
        networks = self.get_networks()
        logger.info(f"Found {len(networks)} mainnet networks")
        
        # Debug mode: process only first network
        if os.getenv("DEBUG"):
            networks = networks[:1]
            logger.info("DEBUG mode: processing only first network")
        
        for network in networks:
            chain_id = network["chainId"]
            
            # Skip chains without duneName
            if not network.get("duneName"):
                logger.info(f"Skipping {network['name']} (chainId {chain_id}) - no Dune support")
                continue
            
            logger.info(f"Processing {network['name']} (chainId {chain_id})")
            
            tokens = self.get_tokens(network)
            logger.info(f"  Found {len(tokens)} SuperTokens")
            
            for token in tokens:
                self.process_token(token, network)
                time.sleep(0.5)  # Rate limiting
        
        logger.info(f"Sync cycle completed at {datetime.now().strftime('%Y%m%d_%H%M%S')}")

    def run(self):
        """Main daemon execution loop"""
        logger.info(f"Starting SuperToken Sync Daemon")
        logger.info(f"Update interval: {self.update_interval} seconds")
        logger.info(f"Press Ctrl+C to stop")
        
        try:
            while True:
                self.sync_once()
                logger.info(f"Waiting {self.update_interval} seconds until next sync...")
                time.sleep(self.update_interval)
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt, shutting down...")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
        
        logger.info("SuperToken Sync Daemon stopped")

if __name__ == "__main__":
    SuperTokenSync().run()
