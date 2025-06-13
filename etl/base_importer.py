"""Base class for database import operations."""

import logging
import os
import tkinter as tk
from tkinter import messagebox
import pandas as pd
import urllib
import sqlalchemy
from sqlalchemy.types import Text

from db.mssql import get_target_connection
from utils.etl_helpers import (
    load_sql,
    run_sql_script,
    log_exception_to_file,
    run_sql_step_with_retry,
)
from etl.core import (
    sanitize_sql,
    safe_tqdm,
    load_config,
    validate_environment,
    validate_sql_identifier,
)
from config import ETLConstants

logger = logging.getLogger(__name__)

class BaseDBImporter:
    """Base class for database import operations."""
    
    # Override these in subclasses
    DB_TYPE = "base"
    DEFAULT_LOG_FILE = "PreDMSErrorLog_Base.txt"
    DEFAULT_CSV_FILE = "EJ_Base_Selects_ALL.csv"
    
    def __init__(self):
        """Initialize the importer with default values."""
        self.config = None
        self.db_name = None

    def parse_args(self):
        """Parse command line arguments - implement in subclasses."""
        raise NotImplementedError("Subclasses must implement parse_args()")

    def validate_environment(self):
        """Validate required environment variables."""
        required_vars = {
            'MSSQL_TARGET_CONN_STR': "Database connection string is required",
            'EJ_CSV_DIR': "Directory containing ETL CSV files is required"
        }
        
        optional_vars = {
            'EJ_LOG_DIR': "Directory for log files (defaults to current directory)",
            'INCLUDE_EMPTY_TABLES': "Set to '1' to include empty tables (defaults to '0')",
            'SQL_TIMEOUT': "Timeout in seconds for SQL operations (defaults to 300)"
        }
        
        validate_environment(required_vars, optional_vars)

    def load_config(self, args):
        """Load configuration from arguments and environment."""
        default_config = {
            "include_empty_tables": False,
            "csv_filename": self.DEFAULT_CSV_FILE,
            "log_filename": self.DEFAULT_LOG_FILE,
            "skip_pk_creation": False,
            "sql_timeout": ETLConstants.DEFAULT_SQL_TIMEOUT,  # seconds
        }
        
        self.config = load_config(args.config_file, default_config)
        
        # Override config with environment variables
        if os.environ.get("INCLUDE_EMPTY_TABLES") == "1":
            self.config["include_empty_tables"] = True
        if os.environ.get("SQL_TIMEOUT"):
            self.config["sql_timeout"] = int(os.environ.get("SQL_TIMEOUT"))
        
        # Override config with command line arguments
        if args.include_empty:
            self.config["include_empty_tables"] = True
        if args.skip_pk_creation:
            self.config["skip_pk_creation"] = True
        
        # Set up paths
        self.config['log_file'] = args.log_file or os.path.join(
            os.environ.get("EJ_LOG_DIR", ""), 
            self.config["log_filename"]
        )
        
        self.config['csv_file'] = args.csv_file or os.path.join(
            os.environ.get("EJ_CSV_DIR", ""),
            self.config["csv_filename"]
        )

    def import_joins(self):
        """Import JOIN statements from CSV to build selection queries."""
        logger.info(f"Importing JOINS from {self.DB_TYPE} Selects CSV")
        
        # Set up database connection for pandas
        conn_str = os.environ['MSSQL_TARGET_CONN_STR']
        params = urllib.parse.quote_plus(conn_str)
        db_url = f"mssql+pyodbc:///?odbc_connect={params}"
        engine = sqlalchemy.create_engine(db_url)
        
        csv_path = self.config['csv_file']
        log_file = self.config['log_file']
        
        if not os.path.exists(csv_path):
            error_msg = f"CSV file not found: {csv_path}"
            logger.error(error_msg)
            log_exception_to_file(error_msg, log_file)
            raise FileNotFoundError(error_msg)
        
        # Read and import CSV
        df = pd.read_csv(csv_path, delimiter='|', encoding='utf-8')
        df = df.astype({
            'DatabaseName': 'str', 'SchemaName': 'str', 'TableName': 'str',
            'Freq': 'str', 'InScopeFreq': 'str', 'Select_Only': 'str',
            'fConvert': 'str', 'Drop_IfExists': 'str', 'Selection': 'str',
            'Select_Into': 'str'
        })
        
        # Write to SQL table - use DB_TYPE to create appropriate table name
        table_name = f'TableUsedSelects_{self.DB_TYPE}' if self.DB_TYPE != 'Justice' else 'TableUsedSelects'
        df.to_sql(
            table_name,
            con=engine,
            if_exists='replace',
            index=False,
            dtype={'Select_Into': Text(), 'Drop_IfExists': Text()}
        )
        
        logger.info(f"Successfully imported {len(df)} JOIN definitions from {csv_path}")
        return engine

    def execute_table_operations(self, conn):
        """Execute DROP and SELECT INTO operations."""
        logger.info("Executing table operations (DROP/SELECT)")
        log_file = self.config['log_file']
        
        original_autocommit = conn.autocommit
        conn.autocommit = False
        table_name = f"TablesToConvert_{self.DB_TYPE}" if self.DB_TYPE != 'Justice' else 'TablesToConvert'
        table_name = validate_sql_identifier(table_name)

        try:
            db_name = validate_sql_identifier(self.db_name)
            # Use a robust query with explicit encoding handling
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                SELECT RowID, DatabaseName, SchemaName, TableName, fConvert, ScopeRowCount,
                       CAST(Drop_IfExists AS NVARCHAR(MAX)) AS Drop_IfExists,
                       CAST(CAST(Select_Into AS NVARCHAR(MAX)) + CAST(ISNULL(Joins, N'') AS NVARCHAR(MAX)) AS NVARCHAR(MAX)) AS [Select_Into]
                FROM {db_name}.dbo.{table_name} S
                WHERE fConvert=1
                ORDER BY DatabaseName, SchemaName, TableName
                """
                )

                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]

                successful_tables = 0
                failed_tables = 0

                for idx, row in enumerate(safe_tqdm(rows, desc="Drop/Select", unit="table"), 1):
                    try:
                        row_dict = dict(zip(columns, row))

                        # Enhanced sanitization
                        drop_sql = sanitize_sql(row_dict.get('Drop_IfExists'))
                        select_into_sql = sanitize_sql(row_dict.get('Select_Into'))

                        table_name = validate_sql_identifier(row_dict.get('TableName'))
                        schema_name = validate_sql_identifier(row_dict.get('SchemaName'))
                        scope_row_count = row_dict.get('ScopeRowCount')
                        full_table_name = f"{schema_name}.{table_name}"

                        # Skip if sanitization completely failed
                        if not drop_sql or not select_into_sql:
                            error_msg = f"Skipping row {idx} ({full_table_name}): SQL sanitization failed"
                            logger.error(error_msg)
                            log_exception_to_file(error_msg, log_file)
                            failed_tables += 1
                            continue

                        # Skip empty tables unless configured to include them
                        if not self.config['include_empty_tables'] and (
                            scope_row_count is None or int(scope_row_count) <= 0
                        ):
                            logger.info(
                                f"Skipping Select INTO for {full_table_name}: scope_row_count is {scope_row_count}"
                            )
                            continue

                        # Execute with individual error handling
                        if drop_sql.strip():
                            logger.info(
                                f"RowID:{idx} Drop If Exists:({self.DB_TYPE}.{full_table_name})"
                            )
                            try:
                                run_sql_step_with_retry(
                                    conn,
                                    f"Drop {full_table_name}",
                                    drop_sql,
                                    timeout=self.config['sql_timeout'],
                                )

                                if select_into_sql.strip():
                                    logger.info(
                                        f"RowID:{idx} Select INTO:({self.DB_TYPE}.{full_table_name})"
                                    )
                                    run_sql_step_with_retry(
                                        conn,
                                        f"SelectInto {full_table_name}",
                                        select_into_sql,
                                        timeout=self.config['sql_timeout'],
                                    )

                                conn.commit()
                                successful_tables += 1

                            except Exception as sql_error:
                                conn.rollback()
                                error_msg = (
                                    f"SQL execution error for row {idx} ({full_table_name}): {str(sql_error)}"
                                )
                                logger.error(error_msg)
                                log_exception_to_file(error_msg, log_file)
                                failed_tables += 1
                                # Continue with next table instead of stopping

                    except Exception as row_error:
                        error_msg = f"Row processing error for row {idx}: {str(row_error)}"
                        logger.error(error_msg)
                        log_exception_to_file(error_msg, log_file)
                        failed_tables += 1
                        continue

        except Exception as query_error:
            error_msg = f"Fatal query error: {str(query_error)}"
            logger.error(error_msg)
            log_exception_to_file(error_msg, log_file)
            raise
        finally:
            conn.autocommit = original_autocommit
            
        logger.info(f"Table operations completed: {successful_tables} successful, {failed_tables} failed")

    def create_primary_keys(self, conn):
        """Create primary keys and NOT NULL constraints."""
        if self.config['skip_pk_creation']:
            logger.info("Skipping primary key and constraint creation as requested in configuration")
            return
        
        log_file = self.config['log_file']
        pk_table = (
            f"PrimaryKeyScripts_{self.DB_TYPE}" if self.DB_TYPE != 'Justice' else "PrimaryKeyScripts"
        )
        tables_table = (
            f"TablesToConvert_{self.DB_TYPE}" if self.DB_TYPE != 'Justice' else "TablesToConvert"
        )
        pk_table = validate_sql_identifier(pk_table)
        tables_table = validate_sql_identifier(tables_table)
        
        logger.info(f"Generating List of Primary Keys and NOT NULL Columns for {self.DB_TYPE} Database")
        pk_script_name = f"create_primarykeys_{self.DB_TYPE.lower()}" if self.DB_TYPE != 'Justice' else 'create_primarykeys'
        pk_sql = load_sql(f'{self.DB_TYPE.lower()}/{pk_script_name}.sql', self.db_name)
        
        run_sql_script(conn, pk_script_name, pk_sql, timeout=self.config['sql_timeout'])
        
        pk_original_autocommit = conn.autocommit
        conn.autocommit = False
        db_name = validate_sql_identifier(self.db_name)
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
            WITH CTE_PKS AS (
                SELECT 1 AS TYPEY, S.DatabaseName, S.SchemaName, S.TableName, S.Script
                FROM {db_name}.dbo.{pk_table} S
                WHERE S.ScriptType='NOT_NULL'
                UNION
                SELECT 2 AS TYPEY, S.DatabaseName, S.SchemaName, S.TableName, S.Script
                FROM {db_name}.dbo.{pk_table} S
                WHERE S.ScriptType='PK'
            )
            SELECT S.TYPEY, TTC.ScopeRowCount, S.DatabaseName, S.SchemaName, S.TableName,
                   REPLACE(S.Script, 'FLAG NOT NULL', 'BIT NOT NULL') AS [Script], TTC.fConvert
            FROM CTE_PKS S
            INNER JOIN {db_name}.dbo.{tables_table} TTC WITH (NOLOCK)
                ON S.SCHEMANAME=TTC.SchemaName AND S.TABLENAME=TTC.TableName
            WHERE TTC.fConvert=1
            ORDER BY S.SCHEMANAME, S.TABLENAME, S.TYPEY
            """
            )
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

            for idx, row in enumerate(safe_tqdm(rows, desc="PK Creation", unit="table"), 1):
                row_dict = dict(zip(columns, row))
                createpk_sql = row_dict.get('Script')
                scope_row_count = row_dict.get('ScopeRowCount')
                schema_name = validate_sql_identifier(row_dict.get('SchemaName'))
                table_name = validate_sql_identifier(row_dict.get('TableName'))
                full_table_name = f"{schema_name}.{table_name}"

                logger.info(f"Executing Primary Key/NOT NULL for row {idx} ({self.DB_TYPE}.{full_table_name})")
                if (scope_row_count != 0 or scope_row_count is not None) or self.config['include_empty_tables']:
                    try:
                        run_sql_step_with_retry(
                            conn,
                            f"PK {full_table_name}",
                            createpk_sql,
                            timeout=self.config['sql_timeout'],
                        )
                        conn.commit()
                    except Exception as e:
                        conn.rollback()
                        error_msg = (
                            f"Error executing PK statements for row {idx} ({self.DB_TYPE}.{full_table_name}): {e}"
                        )
                        logger.error(error_msg)
                        log_exception_to_file(error_msg, log_file)

        conn.autocommit = pk_original_autocommit
        logger.info(f"All Primary Key/NOT NULL statements executed FOR THE {self.DB_TYPE} DATABASE.")

    def show_completion_message(self, next_step_name=None):
        """Show a message box indicating completion and asking to continue."""
        root = tk.Tk()
        root.withdraw()  # Hide the main window
        
        message = f"{self.DB_TYPE} database migration is complete.\n\n"
        message += f"You may now drop the {self.DB_TYPE} database if desired.\n\n"
        
        if next_step_name:
            message += f"Click Yes to proceed to {next_step_name}, or No to stop."
            proceed = messagebox.askyesno(f"{self.DB_TYPE} DB Migration Complete", message)
            root.destroy()
            return proceed
        else:
            message += "Click OK to continue."
            messagebox.showinfo(f"{self.DB_TYPE} DB Migration Complete", message)
            root.destroy()
            return False

    def run(self):
        """Template method - main execution flow."""
        try:
            # Parse command line args and load config
            args = self.parse_args()
            self.validate_environment()
            self.load_config(args)

            # Set up logging level
            if args.verbose:
                logging.getLogger().setLevel(logging.DEBUG)

            # Get target database name
            from config import settings
            self.db_name = settings.MSSQL_TARGET_DB_NAME or settings._parse_database_name(settings.MSSQL_TARGET_CONN_STR)

            # Begin database operations
            with get_target_connection() as target_conn:
                # Execute specific pre-processing steps
                self.execute_preprocessing(target_conn)
                
                # Prepare SQL commands for drops and inserts
                self.prepare_drop_and_select(target_conn)
                
                # Import joins from CSV
                self.import_joins()
                
                # Update joins in tables
                self.update_joins_in_tables(target_conn)
                
                # Execute table operations
                self.execute_table_operations(target_conn)
                
                # Create primary keys and constraints
                self.create_primary_keys(target_conn)
                
                # Show completion message and determine next steps
                next_step_name = self.get_next_step_name()
                proceed = self.show_completion_message(next_step_name)
                
                if proceed and next_step_name:
                    logger.info(f"User chose to proceed to {next_step_name}.")
                    return True
                else:
                    logger.info(f"User chose to stop after {self.DB_TYPE} migration.")
                    return False
                    
        except Exception as e:
            logger.exception("Unexpected error")
            import traceback
            error_details = traceback.format_exc()
            
            # Try to log the error to file
            try:
                log_file = self.config.get('log_file', self.DEFAULT_LOG_FILE)
                log_exception_to_file(error_details, log_file)
            except Exception as log_exc:
                logger.error(f"Failed to write to error log: {log_exc}")
            
            # Try to show error message box
            try:
                root = tk.Tk()
                root.withdraw()
                messagebox.showerror("ETL Script Error", f"An error occurred:\n\n{error_details}")
                root.destroy()
            except Exception as msgbox_exc:
                logger.error(f"Failed to show error message box: {msgbox_exc}")
            
            return False
    
    # Methods that must be implemented by subclasses
    
    def execute_preprocessing(self, conn):
        """Execute database-specific preprocessing steps."""
        raise NotImplementedError("Subclasses must implement execute_preprocessing()")
    
    def prepare_drop_and_select(self, conn):
        """Prepare SQL statements for dropping and selecting data."""
        raise NotImplementedError("Subclasses must implement prepare_drop_and_select()")
    
    def update_joins_in_tables(self, conn):
        """Update tables with JOINs."""
        raise NotImplementedError("Subclasses must implement update_joins_in_tables()")
    
    def get_next_step_name(self):
        """Return the name of the next step in the ETL process."""
        raise NotImplementedError("Subclasses must implement get_next_step_name()")
