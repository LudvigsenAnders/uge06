
from typing import Optional, Any
import pandas as pd
from db.db_utils import QueryRunner
from db.connection import get_session


async def runner():
    async for session in get_session():
        q = QueryRunner(session)
        async with q.transaction():
            query = "SELECT * FROM observations WHERE station_id = :id"
            params = {'id': "06072"}
            df_observations: pd.DataFrame = await q.dataframe(query, params)

            print(df_observations.info())


class AsyncObservationRepository:
    """
    Encapsulates all SQL related to observations and returns pandas DataFrames.
    Keeps the 'what to query' here; analysis logic goes to a separate service.
    """

    def __init__(self, query_runner: QueryRunner) -> None:
        self.qr = query_runner

    async def get_observations_multi_station(
        self,
        station_ids: list[str],
        parameter_id: Optional[str] = None,
        since: Optional[str] = None,  # "2018-02-21T08:50:00Z"
        until: Optional[str] = None,  # "2018-02-21T08:50:00Z"
    ) -> pd.DataFrame:
        """
        Fetch observations for multiple stations (and optional variable filter).
        Uses a temporary IN-list; for very large lists consider JOIN with temp table.
        """
        if not station_ids:
            return pd.DataFrame()

        # Build IN clause safely: :s0, :s1, :s2...
        in_keys = [f"s{i}" for i in range(len(station_ids))]
        in_params = {k: v for k, v in zip(in_keys, station_ids)}
        station_predicate = "station_id IN (" + ", ".join(f":{k}" for k in in_keys) + ")"

        clauses = [station_predicate]
        params: dict[str, Any] = {**in_params}

        if parameter_id:
            clauses.append("parameter_id = :parameter_id")
            params["parameter_id"] = parameter_id
        if since:
            clauses.append("observed >= :since")
            params["since"] = since
        if until:
            clauses.append("observed < :until")
            params["until"] = until

        where_sql = " AND ".join(clauses)

        sql = f"""
            SELECT station_id, observed, value, parameter_id
            FROM observations
            WHERE {where_sql}
            ORDER BY station_id, observed ASC
        """

        df = await self.qr.dataframe(sql, params)
        return self._coerce_dtypes(df)

    # --- Internal helpers ---
    def _coerce_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()
        # Normalize expected dtypes
        if "observed" in df.columns:
            df["observed"] = pd.to_datetime(df["observed"], errors="coerce", utc=True)
        if "station_id" in df.columns:
            df["station_id"] = df["station_id"].astype("string")
        if "parameter_id" in df.columns:
            df["parameter_id"] = df["parameter_id"].astype("string")
        # numeric conversion (soft)
        if "value" in df.columns:
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
        return df
