import logging
import time
import os
from dotenv import load_dotenv
import pandas as pd
import urllib
import sqlalchemy
from db.mssql import get_target_connection
from botocore.exceptions import ClientError
from tqdm import tqdm
from sqlalchemy.types import Text
import tkinter as tk
from tkinter import messagebox

from utils.etl_helpers import (
    log_exception_to_file,
    load_sql,
    run_sql_step,
    run_sql_script,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

LOG_FILE = r"C:\\LargeFileHolder\\7373\\PreDMSErrorLog_Operations.txt"

def main():
    load_dotenv()
    include_empty = os.environ.get("INCLUDE_EMPTY_TABLES", "0") == "1"
    target_conn = None
    target_conn = get_target_connection()

    try:
        # Gather List of DocumentIDs to migrate, using the ParentTypes from ParentLinks, see "gather_documentids.sql"
        logger.info("Gathering list of DocumentIDs that are in Scope for Supervision.")        
        gather_documents_sql = load_sql('gather_documentids.sql')
        run_sql_script(target_conn, 'gather_documentids', gather_documents_sql)

        logger.info("Gathering list of Operations tables with SQL Commands to be migrated.")
        additional_sql = load_sql('gather_drops_and_selects_Operations.sql')
        run_sql_script(target_conn, 'gather_drops_and_selects_Operations', additional_sql)

        # Import list of JOINS necessary to migrate each table in the Operations database.
        logger.info("Importing JOINS from EJ_Operations_Selects_ALL Source")
        conn_str = os.environ['MSSQL_TARGET_CONN_STR']
        params = urllib.parse.quote_plus(conn_str)
        db_url = f"mssql+pyodbc:///?odbc_connect={params}"
        engine = sqlalchemy.create_engine(db_url)

        csv_dir = os.environ.get("EJ_CSV_DIR")
        if not csv_dir:
            raise EnvironmentError("EJ_CSV_DIR environment variable is not set")
        csv_path = os.path.join(csv_dir, "EJ_Operations_Selects.csv")
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        df = pd.read_csv(csv_path, delimiter='|')
        df = df.astype({'DatabaseName': 'str','SchemaName': 'str','TableName': 'str','Freq': 'str','InScopeFreq': 'str','Select_Only': 'str','fConvert': 'str','Drop_IfExists': 'str','Selection': 'str','Select_Into': 'str'})
        df.to_sql(
            'TableUsedSelects_Operations',
            con=engine,
            if_exists='replace',
            index=False,
            dtype={'Select_Into': Text(), 'Drop_IfExists': Text()}  
        )
        #Now we need to update the List of tables with the appropriate joins that we gathered from the csv file above.
        update_joins_Operations_sql = load_sql('update_joins_Operations.sql')  
        run_sql_script(target_conn, 'update_joins_Operations', update_joins_Operations_sql)
        logger.info("Joins for Operations tables are updated")
        logger.info("Updating JOINS for Operations database is complete.")

        #Now we have gathered all Operations tables with supervision scope along with the insert into statements that will move them to the target database.
        cursor = target_conn.cursor()
        cursor.execute("""SELECT RowID, DatabaseName, SchemaName, TableName, fConvert, ScopeRowCount, Drop_IfExists,CAST(Select_Into AS VARCHAR(MAX)) + Joins AS [Select_Into] FROM ELPaso_TX.dbo.TablesToConvert_Operations S WHERE fConvert=1 ORDER BY DatabaseName, SchemaName, TableName""")
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        for idx, row in enumerate(tqdm(rows, desc="Executing Drop/Select", unit="table"), 1):
            row_dict = dict(zip(columns, row))
            drop_sql = row_dict.get('Drop_IfExists')
            select_into_sql = row_dict.get('Select_Into')
            table_name = row_dict.get('TableName')
            schema_name = row_dict.get('SchemaName')
            scope_row_count = row_dict.get('ScopeRowCount')
            full_table_name = f"{schema_name}.{table_name}"  # Concatenate schema and table

            if not include_empty and (scope_row_count is None or int(scope_row_count) <= 0):
                logger.info(f"Skipping Select INTO for {full_table_name}: scope_row_count is {scope_row_count}")
                continue

            if drop_sql and drop_sql.strip():
                logger.info(f"RowID:{idx} Drop If Exists:(Operations.{full_table_name})")
                try:
                    cursor.execute(drop_sql)
                    target_conn.commit()

                    if select_into_sql and select_into_sql.strip():
                        logger.info(f"RowID:{idx} Select INTO:(Operations.{full_table_name})")
                        cursor.execute(select_into_sql)
                        target_conn.commit()
                        
                except Exception as e:
                    logger.error(
                        f"Error executing statements for row {idx} (Operations.{full_table_name}): {e}"
                    )
                    log_exception_to_file(
                        f"Error executing statements for row {idx} (Operations.{full_table_name}): {e}",
                        LOG_FILE,
                    )

        cursor.close()
        logger.info("All Drop_IfExists and Select_Into statements executed for Operations Database.")
        # Now that we are done with out SELECT INTO statements, we need to generate a list of Primary Keys and NOT Nullable Columns for each table.
        pk_sql = load_sql('create_primarykeys_Operations.sql')  
        run_sql_script(target_conn, 'create_primarykeys_Operations', pk_sql)
        logger.info("Generating List of Primary Keys and NOT Nullable Columns IN THE Operations DATABASE")
       # Now that we have our list, run through a cursor to execute each NOT NULL and Primary Key statement (in schema,table name order)
        cursor = target_conn.cursor()
        cursor.execute("""WITH CTE_PKS AS (SELECT 1 AS TYPEY,S.DatabaseName,S.SchemaName,S.TableName,S.Script FROM ELPaso_TX.dbo.PrimaryKeyScripts_Operations S WHERE S.ScriptType='NOT_NULL' UNION SELECT 2 AS TYPEY,S.DatabaseName,S.SchemaName,S.TableName,S.Script FROM ELPaso_TX.dbo.PrimaryKeyScripts_Operations S WHERE S.ScriptType='PK') SELECT S.TYPEY,S.DatabaseName,S.SchemaName,S.TableName,REPLACE(S.Script,'FLAG NOT NULL','BIT NOT NULL') AS [Script],TTC.fConvert FROM CTE_PKS S INNER JOIN ELPaso_TX.dbo.TablesToConvert_Operations TTC WITH (NOLOCK) ON S.SCHEMANAME=TTC.SchemaName AND S.TABLENAME=TTC.TableName WHERE TTC.fConvert=1 ORDER BY S.SCHEMANAME,S.TABLENAME,S.TYPEY""")
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        for idx, row in enumerate(tqdm(rows, desc="Executing Target PK Creation", unit="table"), 1):
            row_dict = dict(zip(columns, row))
            createpk_sql = row_dict.get('Script')
            schema_name = row_dict.get('SchemaName')
            table_name = row_dict.get('TableName')
            full_table_name = f"{schema_name}.{table_name}"
            logger.info(f"Executing Primary Key/NOT NULL for row {idx} (Operations.{full_table_name})")
            try:
                cursor.execute(createpk_sql)
                target_conn.commit()
            except Exception as e:
                logger.error(
                    f"Error executing statements for row {idx} (Operations.{full_table_name}): {e}"
                )
                log_exception_to_file(
                    f"Error executing statements for row {idx} (Operations.{full_table_name}): {e}",
                    LOG_FILE,
                )

        cursor.close()
        logger.info("All Primary Key/NOT NULL statements executed FOR THE Operations DATABASE.")

        # Notify user and prompt to continue
        root = tk.Tk()
        root.withdraw()  # Hide the main window
        proceed = messagebox.askyesno(
            "Operations DB Migration Complete",
            "Operations database migration is complete. You may now drop the Operations database if desired.\n\nClick Yes to proceed to Financial migration, or No to stop."
        )
        root.destroy()
        if not proceed:
            logger.info("User chose to stop after Operations migration.")
            if target_conn:
                target_conn.close()
            return

    except (ClientError, Exception) as e:
        logger.exception("Unexpected error")
        import traceback
        error_details = traceback.format_exc()
        log_exception_to_file(error_details, LOG_FILE)
        try:
            root = tk.Tk()
            root.withdraw()  # Hide the main window
            messagebox.showerror("ETL Script Error", f"An error occurred:\n\n{error_details}")
            root.destroy()
        except Exception as msgbox_exc:
            logger.error(f"Failed to show error message box: {msgbox_exc}")

    finally:
        if target_conn:
            target_conn.close()

if __name__ == "__main__":
    main()