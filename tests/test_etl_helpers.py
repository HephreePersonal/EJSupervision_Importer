import pytest

from utils.etl_helpers import run_sql_step, run_sql_script, SQLExecutionError

class DummyCursor:
    def __init__(self, fail=False, fail_sql=None):
        self.fail = fail
        self.fail_sql = fail_sql
    def execute(self, sql, params=None):
        if 'SET LOCK_TIMEOUT' in sql:
            return
        if self.fail or (self.fail_sql and sql.strip() == self.fail_sql):
            raise RuntimeError('boom')
    def fetchall(self):
        return [('row',)]
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc, tb):
        pass

class DummyConn:
    def __init__(self, fail=False, fail_sql=None):
        self.fail = fail
        self.fail_sql = fail_sql
    def cursor(self):
        return DummyCursor(self.fail, self.fail_sql)
    def commit(self):
        pass


def test_run_sql_step_success():
    conn = DummyConn()
    result = run_sql_step(conn, 'test', 'SELECT 1')
    assert result == [('row',)]


def test_run_sql_step_failure():
    conn = DummyConn(fail=True)
    with pytest.raises(SQLExecutionError) as exc:
        run_sql_step(conn, 'table', 'SELECT 1')
    assert exc.value.sql == 'SELECT 1'
    assert exc.value.table_name == 'table'


def test_run_sql_script_failure():
    sql = 'SELECT 1; FAIL; SELECT 2'
    conn = DummyConn(fail_sql='FAIL')
    with pytest.raises(SQLExecutionError) as exc:
        run_sql_script(conn, 'table', sql)
    assert exc.value.sql.strip() == 'FAIL'
    assert exc.value.table_name == 'table'
