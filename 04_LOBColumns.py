import logging
import time
import os
from dotenv import load_dotenv
import pandas as pd
import urllib
import sqlalchemy
from etl_sandbox.db.mssql import get_target_connection
from botocore.exceptions import ClientError
from tqdm import tqdm
from sqlalchemy.types import Text
import tkinter as tk
from tkinter import messagebox

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

def log_exception_to_file(error_details, log_path=r"C:\LargeFileHolder\7373\PreDMSErrorLog_LOBS.txt"):
    try:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(error_details + "\n")
    except Exception as file_exc:
        logger.error(f"Failed to write to error log file: {file_exc}")
def load_sql(filename):
    base_dir = os.path.dirname(__file__)
    sql_path = os.path.join(base_dir, 'sql_scripts', filename)
    if not os.path.exists(sql_path):
        logger.error(f"SQL file not found: {sql_path}")
        raise FileNotFoundError(f"SQL file not found: {sql_path}")
    with open(sql_path, 'r', encoding='utf-8') as f:
        return f.read()
def run_sql_step(conn, name, sql):
    logger.info(f"Starting step: {name}")
    start_time = time.time()
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        try:
            results = cursor.fetchall()
            logger.info(f"{name}: Retrieved {len(results)} rows")
        except Exception:
            results = None
            logger.info(f"{name}: Statement executed (no results to fetch)")
        cursor.close()
        elapsed = time.time() - start_time
        logger.info(f"Completed step: {name} in {elapsed:.2f} seconds")
        return results
    except Exception as e:
        logger.error(f"Error in step {name}: {str(e)}")
        raise
def run_sql_script(conn, name, sql):
    logger.info(f"Starting script: {name}")
    start_time = time.time()
    try:
        cursor = conn.cursor()
        # Split on semicolon, filter out empty statements
        statements = [stmt.strip() for stmt in sql.split(';') if stmt.strip()]
        for stmt in statements:
            cursor.execute(stmt)
            conn.commit()  # Commit after each statement
        cursor.close()
        elapsed = time.time() - start_time
        logger.info(f"Completed script: {name} in {elapsed:.2f} seconds")
    except Exception as e:
        logger.error(f"Error in script {name}: {str(e)}")
        raise
def get_max_length(conn, schema, table, column, datatype):
    cursor = conn.cursor()
    try:
        if datatype.lower() in ('varchar', 'nvarchar'):
            sql = f"SELECT MAX(LEN([{column}])) FROM [{schema}].[{table}]"
        elif datatype.lower() in ('text', 'ntext'):
            # For text/ntext, cast to nvarchar(max) for LEN
            sql = f"SELECT MAX(LEN(CAST([{column}] AS NVARCHAR(MAX)))) FROM [{schema}].[{table}]"
        else:
            return None
        cursor.execute(sql)
        result = cursor.fetchone()
        return result[0] if result else None
    except Exception as e:
        logger.error(f"Error getting max length for {schema}.{table}.{column}: {e}")
        return None
    finally:
        cursor.close()
def build_alter_column_sql(schema, table, column, datatype, max_length):
    if max_length is None or max_length == 0:
        return f"ALTER TABLE [{schema}].[{table}] ALTER COLUMN [{column}] CHAR(1) NULL"
    elif max_length > 8000:
        return f"ALTER TABLE [{schema}].[{table}] ALTER COLUMN [{column}] TEXT NULL"
    else:
        return f"ALTER TABLE [{schema}].[{table}] ALTER COLUMN [{column}] VARCHAR({max_length}) NULL"

def main():
    load_dotenv()
    include_empty = os.environ.get("INCLUDE_EMPTY_TABLES", "0") == "1"
    target_conn = get_target_connection()

    try:
        logger.info("Altering LOB Column Lengths based on Max")
        
        logger.info("Creating LOB_COLUMN_UPDATES")        
        gather_lobs_sql = load_sql('gather_lobs.sql')
        run_sql_script(target_conn, 'gather_lobs', gather_lobs_sql)


        cursor = target_conn.cursor()
        cursor.execute("""SELECT s.[NAME] AS SchemaName,t.[NAME] AS TableName,c.[NAME] AS ColumnName,TYPE_NAME(c.user_type_id) AS DataType,CASE WHEN TYPE_NAME(c.user_type_id) IN ('varchar', 'nvarchar') THEN c.max_length ELSE NULL END AS CurrentLength,(SELECT COUNT(*) FROM sys.objects o WHERE o.object_id=t.object_id) AS RowCnt FROM ELPaso_TX.sys.tables t INNER JOIN ELPaso_TX.sys.schemas s ON t.schema_id=s.schema_id INNER JOIN ELPaso_TX.sys.columns c ON t.object_id=c.object_id WHERE t.[NAME] NOT IN ('SupContact','CaseEvent','TablesToConvert','TablesToConvert_Financial','TablesToConvert_Operations') AND (TYPE_NAME(c.user_type_id) IN ('text', 'ntext') OR (TYPE_NAME(c.user_type_id) IN ('varchar', 'nvarchar') AND (c.max_length > 5000 OR c.max_length=-1))) ORDER BY s.[NAME], t.[NAME], c.[NAME]""")
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        update_cursor = target_conn.cursor()
        for idx, row in enumerate(tqdm(rows, desc="LobColumn_Gathering", unit="table"), 1):
            row_dict = dict(zip(columns, row))
            schema_name = row_dict.get('SchemaName')
            table_name = row_dict.get('TableName')
            column_name = row_dict.get('ColumnName')
            datatype = row_dict.get('DataType')
            row_cnt = row_dict.get('RowCnt')

            if not include_empty and row_cnt is not None and int(row_cnt) <= 0:
                logger.info(f"Skipping {schema_name}.{table_name}.{column_name}: row count is {row_cnt}")
                continue

            max_length = get_max_length(target_conn, schema_name, table_name, column_name, datatype)
            alter_column_sql = build_alter_column_sql(schema_name, table_name, column_name, datatype, max_length)
            insert_sql = """
                INSERT INTO ELPaso_TX.dbo.LOB_COLUMN_UPDATES
                (SchemaName, TableName, ColumnName, DataType, CurrentLength, RowCnt, MaxLen, AlterStatement)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
            update_cursor.execute(
                insert_sql,
                (
                    schema_name,
                    table_name,
                    column_name,
                    datatype,
                    row_dict.get('CurrentLength'),
                    row_dict.get('RowCnt'),
                    max_length,
                    alter_column_sql
                )
            )
            target_conn.commit()
        update_cursor.close()
        cursor.close()

        cursor = target_conn.cursor()
        cursor.execute("""SELECT REPLACE(S.ALTERSTATEMENT,' NULL',';') AS Alter_Statement FROM ELPaso_TX.dbo.LOB_COLUMN_UPDATES S WHERE S.TABLENAME NOT LIKE '%LOB_COL%' ORDER BY S.MAXLEN DESC""")
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        update_cursor = target_conn.cursor()


        # 	ALTER TABLE [ELPaso_TX].[dbo].[TablesToConvert] ADD CONSTRAINT [PK_TablesToConvert] PRIMARY KEY ([RowID]);
	    #   ALTER TABLE [ELPaso_TX].[dbo].[TablesToConvert_Operations] ADD CONSTRAINT [PK_Op_TablesToConvert] PRIMARY KEY ([RowID]);
	    #   ALTER TABLE [ELPaso_TX].[dbo].[TablesToConvert_Financial] ADD CONSTRAINT [PK_Fi_TablesToConvert] PRIMARY KEY ([RowID]);


        for idx, row in enumerate(tqdm(rows, desc="LobColumn_Updating", unit="table"), 1):
            row_dict = dict(zip(columns, row))
            alter_sql = dict(row_dict).get('Alter_Statement')
            if alter_sql:
                try:
                    run_sql_step(target_conn, f"Alter Column {idx}", alter_sql)
                    target_conn.commit()  # <-- Commit on the
                except Exception as e:
                    logger.error(f"Failed to alter column: {e}")
                    raise

    except (ClientError, Exception) as e:
        logger.exception("Unexpected error")
        import traceback
        error_details = traceback.format_exc()
        log_exception_to_file(error_details)  # <-- Add this line
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