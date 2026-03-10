from typing import Any, Dict, Optional, List, Tuple, Union
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.engine import Result
from sqlalchemy import text, bindparam
import pandas as pd


class QueryRunner:
    """
    High-level SQL helper for AsyncSession.
    Provides:
      - fetch_value(), fetch_one(), fetch_all(), execute()
      - scalar_one(), scalar_required()
      - exists(), count()
      - insert(), update(), delete()
      - bulk_insert(), bulk_update(), bulk_delete()
      - dataframe() → pandas DataFrame
      - transaction()
      - auto-expanding IN params
    """
    def __init__(self, session: AsyncSession):
        self.session = session

    # -----------------------------------------------------
    # Internal: Detect list/tuple params and enable expanding=True
    # -----------------------------------------------------

    def _prepare_statement(self, sql: str, params: Optional[Dict[str, Any]]):
        """
        Automatically convert list/tuple params into expanding bindparams.
        Example:
            WHERE id IN :ids   +   {"ids": [1,2,3]}
        becomes:
            WHERE id IN (:ids_0, :ids_1, :ids_2)
        """
        stmt = text(sql)
        if params:
            for key, value in params.items():
                if isinstance(value, (list, tuple)):
                    stmt = stmt.bindparams(bindparam(key, expanding=True))
        return stmt

    # -----------------------------------------------------
    # BASIC HELPERS
    # -----------------------------------------------------
    async def fetch_value(
        self,
        sql: str,
        params: Optional[Dict[str, Any]] = None
    ) -> Any:
        """Return first column of first row (scalar)."""
        stmt = self._prepare_statement(sql, params)
        result: Result = await self.session.execute(stmt, params or {})
        return result.scalar()

    # -----------------------------------------------------
    async def fetch_one(
        self,
        sql: str,
        params: Optional[Dict[str, Any]] = None,
        as_mapping: bool = True
    ) -> Optional[Union[Dict[str, Any], Tuple]]:
        """Return a single row (dict-like or tuple) or None."""
        stmt = self._prepare_statement(sql, params)
        result: Result = await self.session.execute(stmt, params or {})
        return result.mappings().first() if as_mapping else result.first()

    # -----------------------------------------------------
    async def fetch_all(
        self,
        sql: str,
        params: Optional[Dict[str, Any]] = None,
        as_mapping: bool = True
    ) -> List[Union[Dict[str, Any], Tuple]]:
        """Return all rows."""
        stmt = self._prepare_statement(sql, params)
        result: Result = await self.session.execute(stmt, params or {})
        return result.mappings().all() if as_mapping else result.fetchall()

    # -----------------------------------------------------
    async def execute(
        self,
        sql: str,
        params: Optional[Dict[str, Any]] = None
    ) -> int:
        """Execute INSERT/UPDATE/DELETE. Returns affected rowcount."""
        stmt = self._prepare_statement(sql, params)
        result: Result = await self.session.execute(stmt, params or {})
        return result.rowcount or 0

    # -----------------------------------------------------
    # ADVANCED HELPERS
    # -----------------------------------------------------

    async def scalar_one(self, sql: str, params: Dict[str, Any] = None) -> Any:
        """
        Return exactly one scalar value.
        Raises error if 0 or >1 rows returned.
        """
        stmt = self._prepare_statement(sql, params)
        result: Result = await self.session.execute(stmt, params or {})
        return result.scalar_one()  # SQLAlchemy built-in

    async def scalar_required(self, sql: str, params: Dict[str, Any] = None) -> Any:
        """
        Return a scalar value or raise an error if None.
        Useful for "must exist" queries.
        """
        stmt = self._prepare_statement(sql, params)
        result: Result = await self.session.execute(stmt, params or {})
        value = result.scalar()
        if value is None:
            raise ValueError("Expected a value but got None.")
        return value

    async def exists(self, sql: str, params: Dict[str, Any] = None) -> bool:
        """
        Returns True if the query returns at least one row.
        """
        stmt = self._prepare_statement(sql, params)
        result: Result = await self.session.execute(stmt, params or {})
        row = result.first()
        return row is not None

    async def count(self, sql: str, params: Dict[str, Any] = None) -> int:
        """
        Returns COUNT(*) as an integer.
        You must pass a SQL statement that returns a count.
        """
        stmt = self._prepare_statement(sql, params)
        result: Result = await self.session.execute(stmt, params or {})
        return result.scalar() or 0

    # -----------------------------------------------------
    # INSERT / UPDATE / DELETE HELPERS
    # -----------------------------------------------------
    async def insert(
        self,
        table: str,
        values: Dict[str, Any],
        returning: Optional[str] = None,
    ):
        """
        INSERT INTO table (cols...) VALUES (:vals...)
        Optional: RETURNING id
        """
        cols = ", ".join(values.keys())
        binds = ", ".join(f":{v}" for v in values.keys())

        sql = f"INSERT INTO {table} ({cols}) VALUES ({binds})"
        if returning:
            sql += f" RETURNING {returning}"

        stmt = self._prepare_statement(sql, values)
        result = await self.session.execute(stmt, values)

        return result.scalar() if returning else (result.rowcount or 0)

    async def update(
        self,
        table: str,
        values: Dict[str, Any],
        where: str,
        params: Optional[Dict[str, Any]] = None,
        returning: Optional[str] = None,
    ):
        """
        UPDATE table SET a=:a, b=:b WHERE ...
        Optional: RETURNING field
        """
        set_clause = ", ".join(f"{k} = :{k}" for k in values.keys())

        all_params = {**values, **(params or {})}

        sql = f"UPDATE {table} SET {set_clause} WHERE {where}"
        if returning:
            sql += f" RETURNING {returning}"

        stmt = self._prepare_statement(sql, all_params)
        result = await self.session.execute(stmt, all_params)

        return result.scalar() if returning else (result.rowcount or 0)

    async def delete(
        self,
        table: str,
        where: str,
        params: Optional[Dict[str, Any]] = None,
        returning: Optional[str] = None,
    ):
        """
        DELETE FROM table WHERE ...
        Optional: RETURNING field
        """
        sql = f"DELETE FROM {table} WHERE {where}"
        if returning:
            sql += f" RETURNING {returning}"

        stmt = self._prepare_statement(sql, params or {})
        result = await self.session.execute(stmt, params or {})

        return result.scalar() if returning else (result.rowcount or 0)

    # -----------------------------------------------------
    # Bulk operations
    # -----------------------------------------------------
    async def bulk_insert(self, table: str, rows: list[dict]):
        """
        Bulk insert using one INSERT ... VALUES ... statement.
        rows = [ {"col":val,...}, {"col":val,...} ]
        """
        if not rows:
            return 0

        cols = rows[0].keys()
        col_names = ", ".join(cols)
        value_binds = ", ".join(f":{c}" for c in cols)

        sql = f"INSERT INTO {table} ({col_names}) VALUES ({value_binds})"
        stmt = text(sql)
        # session.execute can accept a list of dicts → batched exec
        result = await self.session.execute(stmt, rows)
        return result.rowcount or len(rows)

    async def bulk_update(self, table: str, rows: list[dict], key: str):
        """
        Bulk update.
        rows MUST contain the key column (PK or unique).
        Example row: {"employeeid": 5, "city": "Aarhus"}
        """
        if not rows:
            return 0

        count = 0
        for r in rows:
            where_val = r[key]
            set_vals = {k: v for k, v in r.items() if k != key}

            set_clause = ", ".join(f"{k} = :{k}" for k in set_vals.keys())
            params = {**set_vals, key: where_val}

            sql = f"UPDATE {table} SET {set_clause} WHERE {key} = :{key}"
            result = await self.session.execute(sql, params)

            count += result.rowcount or 0

        return count

    async def bulk_delete(
        self,
        table: str,
        rows: list[dict],
        *,
        keys: list[str] = None,
        returning: str | None = None,
    ):
        """
        Bulk delete:
        rows = [{key1: val1, key2: val2, ...}, ...]

        keys: list of columns forming the primary/composite key.
            If None, uses the keys of the first row.
        """
        if not rows:
            return [] if returning else 0

        # Determine keys (delete conditions)
        if keys is None:
            keys = list(rows[0].keys())

        # WHERE key1 = :key1 AND key2 = :key2 ...
        where_clause = " AND ".join(f"{k} = :{k}" for k in keys)

        sql = f"DELETE FROM {table} WHERE {where_clause}"
        stmt = text(sql)

        # Like bulk_insert: executemany with list[dict]
        if returning == "*":
            return await self.fetch_all(sql + " RETURNING *", rows, as_mapping=True)

        elif returning:
            out = await self.fetch_all(sql + f" RETURNING {returning}", rows, as_mapping=False)
            return [r[0] for r in out]

        else:
            result = await self.session.execute(stmt, rows)
            return result.rowcount or len(rows)

    # -----------------------------------------------------
    # Dataframe helper
    # -----------------------------------------------------
    async def dataframe(self, sql: str, params=None):
        """
        Execute a query and return results as a pandas DataFrame.
        Requires pandas to be installed.
        """
        rows = await self.fetch_all(sql, params)
        return pd.DataFrame(rows)

    # -----------------------------------------------------
    # Transaction context manager
    # -----------------------------------------------------
    def transaction(self):
        """
        Usage:
            async with QueryRunner(session).transaction():
                await q.execute(...)
                await q.execute(...)
        """
        return _AutoCleanupTransaction(self.session)


# ---------------------------------------------------------
# INTERNAL CONTEXT MANAGER — AUTO CLEANUP MODE
# ---------------------------------------------------------
class _AutoCleanupTransaction:
    """
    If the session already has an active transaction (SELECT or DML),
    automatically roll it back BEFORE starting a new explicit tx.
    """

    def __init__(self, session: AsyncSession):
        self.session = session
        self._tx = None

    async def __aenter__(self):
        # If SQLAlchemy opened a transaction implicitly, clean it
        if self.session.in_transaction():
            print("[DB] Detected active transaction. Rolling back before starting new transaction...")
            await self.session.rollback()

        # Start new explicit transaction
        print("[DB] Starting new transaction...")
        self._tx = await self.session.begin()
        return self.session

    async def __aexit__(self, exc_type, exc, tb):
        print("[DB] Exiting transaction...")
        if exc_type:
            print(f"[DB] Exception detected: {exc_type.__name__}: {exc}. Rolling back transaction...")
            await self.session.rollback()
        else:
            print("[DB] Committing transaction...")
            await self.session.commit()
        return False
