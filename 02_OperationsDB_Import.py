"""ETL script to migrate Operations database tables.

The script reads SQL under ``sql_scripts/operations`` and applies it to the
database defined by the ``MSSQL_TARGET_CONN_STR`` environment variable.  Command
line arguments mirror those of ``01_JusticeDB_Import.py`` allowing CSV and log
locations to be overridden.
"""

import logging
from utils.logging_helper import setup_logging, operation_counts
import time
import json
import sys
import os
import argparse
from dotenv import load_dotenv
import pandas as pd
import urllib
import sqlalchemy
from db.mssql import get_target_connection
from etl import core
from etl import BaseDBImporter
from sqlalchemy.types import Text
import tkinter as tk
from tkinter import N, messagebox
from config import settings

from utils.etl_helpers import (
    log_exception_to_file,
    load_sql,
    run_sql_step,
    run_sql_script,
)

logger = logging.getLogger(__name__)

DEFAULT_LOG_FILE = "PreDMSErrorLog_Operations.txt"

# Determine the target database name from environment variables/connection
# string. This value replaces the hard coded 'ELPaso_TX' references in the SQL
# scripts so the ETL can run against any target database.
DB_NAME = settings.MSSQL_TARGET_DB_NAME or settings._parse_database_name(settings.MSSQL_TARGET_CONN_STR)

class OperationsDBImporter(BaseDBImporter):
    """Operations database import implementation."""
    
    DB_TYPE = "Operations"
    DEFAULT_LOG_FILE = "PreDMSErrorLog_Operations.txt"
    DEFAULT_CSV_FILE = "EJ_Operations_Selects_ALL.csv"
    
    def parse_args(self):
        """Parse command line arguments for the Operations DB import script."""
        parser = argparse.ArgumentParser(description="Operations DB Import ETL Process")
        parser.add_argument(
            "--log-file",
            help="Path to the error log file. Overrides the EJ_LOG_DIR environment variable."
        )
        parser.add_argument(
            "--csv-file",
            help="Path to the Operations Selects CSV file. Overrides the EJ_CSV_DIR environment variable."
        )
        parser.add_argument(
            "--include-empty", 
            action="store_true",
            help="Include empty tables in the migration process."
        )
        parser.add_argument(
            "--skip-pk-creation", 
            action="store_true",
            help="Skip primary key and constraint creation step."
        )
        parser.add_argument(
            "--config-file",
            help="Path to JSON configuration file with all settings."
        )
        parser.add_argument(
            "--verbose", "-v", 
            action="store_true",
            help="Enable verbose logging."
        )
        return parser.parse_args()
        
    def execute_preprocessing(self, conn):
        """Define supervision scope for Operations DB."""
        logger.info("Defining supervision scope...")
        steps = [
            {'name': 'GatherDocumentIDs', 'sql': load_sql('operations/gather_documentids.sql', self.db_name)},
                ]
        
        for step in self.safe_tqdm(steps, desc="SQL Script Progress", unit="step"):
            run_sql_step(conn, step['name'], step['sql'], timeout=self.config['sql_timeout'])
            conn.commit()
        
        logger.info("All Staging steps completed successfully. Document Conversion Scope Defined.")
    
    def prepare_drop_and_select(self, conn):
        """Prepare SQL statements for dropping and selecting data."""
        logger.info("Gathering list of Operations tables with SQL Commands to be migrated.")
        additional_sql = load_sql('operations/gather_drops_and_selects_operations.sql', self.db_name)
        run_sql_script(conn, 'gather_drops_and_selects_operations', additional_sql, timeout=self.config['sql_timeout'])
    
    def update_joins_in_tables(self, conn):
        """Update the TablesToConvert table with JOINs."""
        logger.info("Updating JOINS in TablesToConvert List")
        update_joins_sql = load_sql('operations/update_joins_operations.sql', self.db_name)
        run_sql_script(conn, 'update_joins', update_joins_sql, timeout=self.config['sql_timeout'])
        logger.info("Updating JOINS for Operations tables is complete.")
    
    def get_next_step_name(self):
        """Return the name of the next step in the ETL process."""
        return "Financial migration"

def main():
    """Main entry point for Operations DB Import."""
    setup_logging()
    load_dotenv()
    importer = OperationsDBImporter()
    importer.run()
    logger.info(
        "Run completed - successes: %s failures: %s",
        operation_counts["success"],
        operation_counts["failure"],
    )

if __name__ == "__main__":
    main()
