
from __future__ import annotations
from typing import Optional, Literal
import pandas as pd
import plotly.graph_objects as go
from .dataframe_repository import AsyncObservationRepository


class ObservationAnalysisService:
    """
    Provides reusable analytics over observation DataFrames.
    """

    def __init__(self, repo: AsyncObservationRepository) -> None:
        self.repo = repo

    # --- Fetch + analyze convenience methods ---

    async def daily_stats_for_station(
        self,
        station_id: str,
        parameter_id: Optional[str] = None,
        since: Optional[str] = None,
        until: Optional[str] = None,
        agg: Literal["mean", "sum", "min", "max", "median"] = "mean",
    ) -> pd.DataFrame:
        """
        Pulls observations then returns daily aggregated stats.
        """
        if parameter_id:
            df = await self.repo.get_observations_multi_station(
                [station_id], parameter_id=parameter_id, since=since, until=until
            )
        else:
            df = await self.repo.get_observations_by_station(
                station_id=station_id, since=since, until=until
            )

        return self._daily_aggregate(df, agg=agg)

    async def anomalies_zscore_for_station(
        self,
        station_id: str,
        parameter_id: Optional[str] = None,
        since: Optional[str] = None,
        until: Optional[str] = None,
        z_threshold: float = 3.0,
        rolling: Optional[str] = "7D",
    ) -> pd.DataFrame:
        """
        Simple z-score anomaly detector. Returns rows flagged as anomalies.
        """
        if parameter_id:
            df = await self.repo.get_observations_multi_station(
                [station_id], parameter_id=parameter_id, since=since, until=until
            )
        else:
            df = await self.repo.get_observations_by_station(
                station_id=station_id, since=since, until=until
            )

        return self._zscore_anomalies(df, z_threshold=z_threshold, rolling=rolling)

    async def completeness_report(
        self,
        station_id: str,
        frequency: str = "1H",
        parameter_id: Optional[str] = None,
        since: Optional[str] = None,
        until: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Computes % completeness over a given frequency grid.
        """
        if parameter_id:
            df = await self.repo.get_observations_multi_station(
                [station_id], parameter_id=parameter_id, since=since, until=until
            )
        else:
            df = await self.repo.get_observations_by_station(
                station_id=station_id, since=since, until=until
            )

        return self._completeness(df, frequency=frequency)

    # --- Pure DataFrame transforms (testable without DB) ---

    def _daily_aggregate(self, df: pd.DataFrame, agg: str = "mean") -> pd.DataFrame:
        """Aggregate observation data to daily frequency.

        Args:
            df: DataFrame with observation data containing 'observed' and 'value' columns.
            agg: Aggregation function to apply ('mean', 'sum', 'min', 'max', 'median').

        Returns:
            DataFrame with daily aggregated values.
        """

        if df is None or df.empty:
            return pd.DataFrame()

        # Keep only what’s needed
        cols = [c for c in ["observed", "value", "station_id", "parameter_id"] if c in df.columns]
        df = df[cols].dropna(subset=["observed"]).copy()
        df = df.set_index("observed").sort_index()

        # Resample and aggregate
        if agg not in {"mean", "sum", "min", "max", "median"}:
            raise ValueError(f"Unsupported agg: {agg}")

        # Group by station/variable to avoid mixing streams
        group_keys = [c for c in ["station_id", "parameter_id"] if c in df.columns]

        if group_keys:
            out = (
                df.groupby(group_keys)
                .resample("1D")
                .agg({"value": agg})
                .rename(columns={"value": agg})
                .reset_index()
            )
        else:
            out = df.resample("1D").agg({"value": agg}).rename(columns={"value": agg}).reset_index()

        return out

    def _zscore_anomalies(
        self,
        df: pd.DataFrame,
        z_threshold: float = 3.0,
        rolling: Optional[str] = "7D",
    ) -> pd.DataFrame:
        """Detect anomalies using z-score method.

        Args:
            df: DataFrame with observation data.
            z_threshold: Z-score threshold for anomaly detection.
            rolling: Rolling window for calculating mean/std (e.g., '7D' for 7 days).

        Returns:
            DataFrame containing detected anomalies with z-scores.
        """
        if df is None or df.empty or "value" not in df.columns:
            return pd.DataFrame()

        work = df.dropna(subset=["observed", "value"]).copy()
        work = work.set_index("observed").sort_index()

        if rolling:
            mean = work["value"].rolling(rolling, min_periods=3).mean()
            std = work["value"].rolling(rolling, min_periods=3).std()
        else:
            mean = work["value"].mean()
            std = work["value"].std()

        # Avoid division by zero
        z = (work["value"] - mean) / (std.replace(0, pd.NA) if hasattr(std, "replace") else std)
        work["z"] = z
        anomalies = work[work["z"].abs() >= z_threshold].reset_index()
        return anomalies

    def _completeness(self, df: pd.DataFrame, frequency: str = "1H") -> pd.DataFrame:
        """
        Calculates completeness: actual points vs expected points on a regular grid.
        """
        if df is None or df.empty:
            return pd.DataFrame()

        work = df.dropna(subset=["observed"]).copy()
        work = work.set_index("observed").sort_index()

        # Determine the full expected index
        start = work.index.min().floor(frequency)
        end = work.index.max().ceil(frequency)
        full_index = pd.date_range(start=start, end=end, freq=frequency, tz="UTC")

        group_keys = [c for c in ["station_id", "parameter_id"] if c in work.columns]
        if group_keys:
            parts = []
            for keys, g in work.groupby(group_keys):
                present = g.index.snap(frequency).unique()
                expected = len(full_index)
                actual = len(pd.Index(full_index).intersection(present))
                completeness = 100.0 * actual / expected if expected else 0.0
                row = dict(zip(group_keys, keys if isinstance(keys, tuple) else (keys,)))
                row.update({"expected": expected, "actual": actual, "completeness_pct": completeness})
                parts.append(row)
            return pd.DataFrame(parts)
        else:
            present = work.index.snap(frequency).unique()
            expected = len(full_index)
            actual = len(pd.Index(full_index).intersection(present))
            completeness = 100.0 * actual / expected if expected else 0.0
            return pd.DataFrame(
                [{"expected": expected, "actual": actual, "completeness_pct": completeness}]
            )

    def plotter(self, df: pd.DataFrame):
        """Create and display a time series plot of observation data.

        Args:
            df: DataFrame with 'observed' and 'value' columns to plot.
        """
        # Using graph_objects
        fig = go.Figure([go.Scatter(x=df["observed"], y=df["value"])])
        fig.show()
