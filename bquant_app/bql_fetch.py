from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple, Iterable
import pandas as pd


@dataclass
class TimeSeriesData:
    cb_close: pd.Series
    udly_close: pd.Series
    ud_delta: pd.Series


def _ensure_series(df: pd.DataFrame, value_col: str = "value") -> pd.Series:
    if isinstance(df, pd.Series):
        return df
    # Expect a tidy frame with [date, value] or an index of dates
    if "date" in df.columns and value_col in df.columns:
        out = df.set_index("date").sort_index()[value_col].copy()
        out.index = pd.to_datetime(out.index)
        return out
    # Fallback: if index is already date-like and there's a single column
    if df.shape[1] == 1:
        col = df.columns[0]
        out = df[col].copy()
        out.index = pd.to_datetime(out.index)
        return out
    raise ValueError("Cannot coerce DataFrame to a date-indexed Series. Provide tidy [date, value].")


def derive_underlying_from_cb(cb_ticker: str) -> str:
    """Derive the common underlying ticker from a convertible via BQL.

    Uses `cv_common_ticker_exch()` as provided by the user.
    Returns a string ticker (e.g., "TICK US Equity").
    """
    try:
        import bql
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(
            "BQL runtime not available. Run in BQuant with Bloomberg BQL."
        ) from exc

    bq = bql.Service()
    fn = bql.Function("cv_common_ticker_exch")
    req = bql.Request(cb_ticker, fn)
    res = bq.execute(req)
    df = res[0].df()
    # Expect a single string value
    val = df["value"].iloc[0]
    if not isinstance(val, str) or not val:
        raise ValueError("cv_common_ticker_exch() returned empty/invalid underlying ticker.")
    return val


def fetch_timeseries_with_bql(
    cb_ticker: str,
    udly_ticker: Optional[str],
    start: str,
    end: str,
    freq: str = "BUSINESS_DAYS",
) -> TimeSeriesData:
    """
    Fetch time series for CB close, underlying close, and CB ud_delta using BQL.

    Notes:
    - This function relies on the BQuant/BQL runtime. Replace the field/function
      names below if your environment uses different aliases.
    - If `udly_ticker` is None, you may adapt this function to derive the
      underlying from the CB security via a BQL field.
    """
    try:
        import bql
    except Exception as exc:  # pragma: no cover - informative error in non-BQL envs
        raise RuntimeError(
            "BQL runtime not available. Run this in BQuant or install Bloomberg's bql package."
        ) from exc

    bq = bql.Service()

    # Helper to request a simple price time series
    def _px_ts(sec: str, field: str) -> pd.Series:
        dates = bql.Function("range", start, end, freq)  # e.g., 'BUSINESS_DAYS'
        px = bql.Function(field, dates)  # e.g., px_last(range(...))
        req = bql.Request(sec, px)
        res = bq.execute(req)
        df = res[0].df()
        return _ensure_series(df)

    # Convertible close
    cb_close = _px_ts(cb_ticker, "px_last")

    # Underlying close (either provided or derived)
    if not udly_ticker:
        udly_ticker = derive_underlying_from_cb(cb_ticker)
    udly_close = _px_ts(udly_ticker, "px_last")

    # CB delta time series (field provided by user: ud_delta)
    # If ud_delta is available as a time series field, the same style works; otherwise
    # adapt this to your environment.
        dates = bql.Function("range", start, end, freq)
        delta_fn = bql.Function("ud_delta", dates)
        delta_req = bql.Request(cb_ticker, delta_fn)
        delta_res = bq.execute(delta_req)
        ud_delta = _ensure_series(delta_res[0].df())

    return TimeSeriesData(cb_close=cb_close, udly_close=udly_close, ud_delta=ud_delta)

def compute_nuke_with_bql_function_single(
    cb_ticker: str,
    anchor_cb_price: float,
    anchor_udly_price: float,
    input_udly_price: float,
) -> float:
    """Call BQL nuke function for a single input underlying price.

    Uses: nuke_dollar_neutral_price(
            nuke_anchor_bond_price(anchor_cb_price),
            nuke_anchor_underlying_price(anchor_udly_price),
            nuke_input_underlying_price(input_udly_price))
    Returns a float.
    """
    import bql  # rely on BQuant runtime
    bq = bql.Service()

    fn = bql.Function(
        "nuke_dollar_neutral_price",
        bql.Function("nuke_anchor_bond_price", float(anchor_cb_price)),
        bql.Function("nuke_anchor_underlying_price", float(anchor_udly_price)),
        bql.Function("nuke_input_underlying_price", float(input_udly_price)),
    )
    req = bql.Request(cb_ticker, fn)
    res = bq.execute(req)
    df = res[0].df()
    return float(df["value"].iloc[0])


def compute_nuke_series_with_bql(
    cb_ticker: str,
    udly_close: pd.Series,
    anchor_cb_price: float,
    anchor_udly_price: float,
) -> pd.Series:
    """Attempt a vectorized BQL nuke over the given date index; fallback to per-date calls.

    Parameters
    - cb_ticker: convertible identifier for BQL context
    - udly_close: series of input underlying prices (indexed by date)
    - anchor_cb_price: CB(T0)
    - anchor_udly_price: U(T0)
    """
    try:
        import bql
        bq = bql.Service()

        # Try to build a vectorized expression using the underlying PX time series as input
        # Note: Some BQL deployments accept numeric literals in functions; adjust if needed.
        input_series = udly_close.sort_index()
        # To use the existing series directly, request the same range and map by date.
        start = input_series.index[0].strftime("%Y-%m-%d")
        end = input_series.index[-1].strftime("%Y-%m-%d")
        dates = bql.Function("range", start, end, "BUSINESS_DAYS")
        udly_ts_fn = bql.Function("px_last", dates)

        nuke_fn = bql.Function(
            "nuke_dollar_neutral_price",
            bql.Function("nuke_anchor_bond_price", float(anchor_cb_price)),
            bql.Function("nuke_anchor_underlying_price", float(anchor_udly_price)),
            bql.Function("nuke_input_underlying_price", udly_ts_fn),
        )
        req = bql.Request(cb_ticker, nuke_fn)
        res = bq.execute(req)
        series_vec = _ensure_series(res[0].df())
        # Align to provided index
        series_vec = series_vec.reindex(input_series.index).dropna()
        if not series_vec.empty:
            return series_vec
        # If empty, fall back
        raise RuntimeError("Vectorized BQL nuke returned empty series; falling back.")
    except Exception:
        # Fallback: per-date single computations (slower but robust)
        values = {}
        for dt_idx, u in udly_close.sort_index().items():
            try:
                nuke_val = compute_nuke_with_bql_function_single(
                    cb_ticker=cb_ticker,
                    anchor_cb_price=anchor_cb_price,
                    anchor_udly_price=anchor_udly_price,
                    input_udly_price=float(u),
                )
                values[pd.Timestamp(dt_idx)] = nuke_val
            except Exception:
                # Skip on failure for a given date
                continue
        if not values:
            raise RuntimeError("Unable to compute nuke series via BQL (both vectorized and fallback failed).")
        return pd.Series(values).sort_index()
