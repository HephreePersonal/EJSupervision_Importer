"""ETL script to migrate Justice database tables to the target server.

The script loads SQL files under ``sql_scripts/justice`` and executes them
against the database specified via ``MSSQL_TARGET_CONN_STR``.  Command line
arguments allow overriding the log file location, CSV directory and other
options.
"""

import logging
from utils.logging_helper import setup_logging, operation_counts
import time
import json
import sys
import os
import argparse
from typing import Any
from dotenv import load_dotenv
import pandas as pd
import urllib
import sqlalchemy
from db.mssql import get_target_connection
from tqdm import tqdm
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

DEFAULT_LOG_FILE = "PreDMSErrorLog_Justice.txt"

# Determine the target database name from environment variables/connection
# string. This value replaces the hard coded 'ELPaso_TX' references in the SQL
# scripts so the ETL can run against any target database.
DB_NAME = settings.MSSQL_TARGET_DB_NAME or settings._parse_database_name(settings.MSSQL_TARGET_CONN_STR)

class JusticeDBImporter(BaseDBImporter):
    """Justice database import implementation."""
    
    DB_TYPE = "Justice"
    DEFAULT_LOG_FILE = "PreDMSErrorLog_Justice.txt"
    DEFAULT_CSV_FILE = "EJ_Justice_Selects_ALL.csv"
    
    def parse_args(self) -> argparse.Namespace:
        """Parse command line arguments for the Justice DB import script."""
        parser = argparse.ArgumentParser(description="Justice DB Import ETL Process")
        parser.add_argument(
            "--log-file",
            help="Path to the error log file. Overrides the EJ_LOG_DIR environment variable."
        )
        parser.add_argument(
            "--csv-file",
            help="Path to the Justice Selects CSV file. Overrides the EJ_CSV_DIR environment variable."
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
        
    def execute_preprocessing(self, conn: Any) -> None:
        """Define supervision scope for Justice DB."""
        logger.info("Defining supervision scope...")
        steps = [
            {'name': 'GatherCaseIDs', 'sql': load_sql('justice/gather_caseids.sql', self.db_name)},
            {'name': 'GatherChargeIDs', 'sql': load_sql('justice/gather_chargeids.sql', self.db_name)},
            {'name': 'GatherPartyIDs', 'sql': load_sql('justice/gather_partyids.sql', self.db_name)},
            {'name': 'GatherWarrantIDs', 'sql': load_sql('justice/gather_warrantids.sql', self.db_name)},
            {'name': 'GatherHearingIDs', 'sql': load_sql('justice/gather_hearingids.sql', self.db_name)},
            {'name': 'GatherEventIDs', 'sql': load_sql('justice/gather_eventids.sql', self.db_name)}
        ]
        
        for step in self.safe_tqdm(steps, desc="SQL Script Progress", unit="step"):
            run_sql_step(conn, step['name'], step['sql'], timeout=self.config['sql_timeout'])
            conn.commit()
        
        logger.info("All Staging steps completed successfully. Supervision Scope Defined.")
    
    def prepare_drop_and_select(self, conn: Any) -> None:
        """Prepare SQL statements for dropping and selecting data."""
        logger.info("Gathering list of Justice tables with SQL Commands to be migrated.")
        additional_sql = load_sql('justice/gather_drops_and_selects.sql', self.db_name)
        run_sql_script(conn, 'gather_drops_and_selects', additional_sql, timeout=self.config['sql_timeout'])
    
    def update_joins_in_tables(self, conn: Any) -> None:
        """Update the TablesToConvert table with JOINs."""
        logger.info("Updating JOINS in TablesToConvert List")
        update_joins_sql = load_sql('justice/update_joins.sql', self.db_name)
        run_sql_script(conn, 'update_joins', update_joins_sql, timeout=self.config['sql_timeout'])
        logger.info("Updating JOINS for Justice tables is complete.")

       
    def get_next_step_name(self) -> str:
        """Return the name of the next step in the ETL process."""
        return "Operations migration"

def main():
    """Main entry point for Justice DB Import."""
    setup_logging()
    load_dotenv()
    importer = JusticeDBImporter()
    importer.run()
    logger.info(
        "Run completed - successes: %s failures: %s",
        operation_counts["success"],
        operation_counts["failure"],
    )

if __name__ == "__main__":
    main()
