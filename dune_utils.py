#!/usr/bin/env python3
"""
Shared utilities for Dune sync operations
"""

import os
import logging
from pathlib import Path
from datetime import datetime

# Load environment variables from .env file first
try:
    import dotenv
    dotenv.load_dotenv()
except ImportError:
    pass  # dotenv not available, continue without it


def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
    return logging.getLogger(__name__)


def get_dune_client():
    """Initialize and return Dune client"""
    try:
        from dune_client.client import DuneClient
        return DuneClient.from_env()
    except Exception as e:
        logging.warning(f"Failed to initialize Dune client: {e}")
        return None


def ensure_data_directory():
    """Ensure data directory exists"""
    Path("data").mkdir(exist_ok=True)


def get_timestamp():
    """Get current timestamp string"""
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def get_date_string():
    """Get current date string for daily data"""
    return datetime.now().strftime("%Y%m%d")


# Dune namespace constant
DUNE_NAMESPACE = "superfluid_hq"

class DuneSyncBase:
    """Base class for Dune sync operations"""
    
    def __init__(self):
        self.logger = setup_logging()
        self.timestamp = get_timestamp()
        self.date_string = get_date_string()
        ensure_data_directory()
        self.dune = get_dune_client()
        
        # Daemon control
        self.update_interval = 86400
    
    def upload_csv_to_dune(self, csv_data, table_name, description, is_private=False):
        """Upload CSV data to Dune (replaces entire table)"""
        if not self.dune:
            self.logger.warning("Dune client not available, skipping upload")
            return False
            
        try:
            self.dune.upload_csv(
                data=csv_data,
                description=description,
                table_name=table_name,
                is_private=is_private
            )
            self.logger.info(f"Uploaded {table_name} to Dune")
            return True
        except Exception as e:
            self.logger.error(f"Upload failed for {table_name}: {e}")
            return False
    
    def create_table(self, table_name, schema, description="", is_private=False):
        """Create a new table in Dune with defined schema"""
        if not self.dune:
            self.logger.warning("Dune client not available, skipping table creation")
            return False
            
        try:
            result = self.dune.create_table(
                namespace=DUNE_NAMESPACE,
                table_name=table_name,
                schema=schema,
                description=description,
                is_private=is_private
            )
            self.logger.info(f"Created table {table_name} in Dune")
            return True
        except Exception as e:
            # Table might already exist, which is fine
            self.logger.info(f"Table {table_name} creation skipped (likely already exists): {e}")
            return True  # Return True since we can proceed with inserts
    
    def insert_data_to_dune(self, table_name, data):
        """Insert data rows into an existing Dune table"""
        if not self.dune:
            self.logger.warning("Dune client not available, skipping data insert")
            return False
        
        self.logger.info(f"Inserting {len(data)} rows into {table_name}")

        try:
            # Convert data to NDJSON format (newline-delimited JSON)
            import json
            ndjson_content = '\n'.join(json.dumps(row) for row in data)
            
            # Create a temporary file with the NDJSON content
            import tempfile
            with tempfile.NamedTemporaryFile(mode='w', suffix='.ndjson', delete=False) as temp_file:
                temp_file.write(ndjson_content)
                temp_file_path = temp_file.name
            
            try:
                # Upload the file using Dune's insert_table method
                with open(temp_file_path, 'rb') as f:
                    result = self.dune.insert_table(
                        namespace=DUNE_NAMESPACE,
                        table_name=table_name,
                        data=f,
                        content_type='application/x-ndjson'
                    )
                
                self.logger.info(f"Successfully inserted {len(data)} rows into {table_name}")
                return True
            finally:
                # Clean up temporary file
                import os
                os.unlink(temp_file_path)
                
        except Exception as e:
            import traceback
            self.logger.error(f"Data insert failed for {table_name}: {e}")
            self.logger.error(f"Full traceback: {traceback.format_exc()}")
            return False
    
    
    def archive_file(self, file_path):
        """Archive a file with timestamp"""
        try:
            archive_path = Path(f"data/archive/{Path(file_path).name}_{self.timestamp}")
            archive_path.parent.mkdir(parents=True, exist_ok=True)
            import shutil
            shutil.move(str(file_path), str(archive_path))
            self.logger.info(f"Archived: {Path(file_path).name}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to archive {file_path}: {e}")
            return False

