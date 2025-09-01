from __future__ import annotations

from dataclasses import dataclass
import os
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
    if not isinstance(df, pd.DataFrame) or df.empty:
        raise ValueError("Cannot coerce to Series: empty or invalid DataFrame.")

    # Case-insensitive handling for 'date'
    cols = list(df.columns)
    lower_map = {c.lower(): c for c in cols}
    date_key = next((c for c in cols if c.lower() == "date"), None)

    # If explicit [date, value]
    if date_key and value_col in cols:
        out = df.set_index(date_key).sort_index()[value_col].copy()
        out.index = pd.to_datetime(out.index)
        return out

    # If we have a date column and exactly one non-meta column, pick it
    if date_key:
        meta_like = {date_key, "security", "SECURITY", "ticker", "TICKER"}
        candidates = [c for c in cols if c not in meta_like]
        # If multiple, prefer 'value' or first numeric column
        if len(candidates) == 1:
            picked = candidates[0]
        else:
            if value_col in candidates:
                picked = value_col
            else:
                numeric = [c for c in candidates if pd.api.types.is_numeric_dtype(df[c])]
                picked = numeric[0] if numeric else (candidates[0] if candidates else None)
        if picked:
            out = df.set_index(date_key).sort_index()[picked].copy()
            out.index = pd.to_datetime(out.index)
            return out

    # Fallback: date-like index and single value column
    if df.shape[1] == 1:
        col = df.columns[0]
        out = df[col].copy()
        out.index = pd.to_datetime(out.index)
        return out

    raise ValueError(
        "Cannot coerce DataFrame to a date-indexed Series. Provide tidy [date, value]."
    )


def _get_bql_service():
    """Return (bql_module, bql_service) or raise a clear error if not Bloomberg BQL.

    Uses the modern namespaced API (`bq.data`, `bq.func`). Also surfaces a helpful
    error if a third‑party `bql` package is shadowing Bloomberg's BQL runtime.
    """
    try:
        import bql  # type: ignore
    except Exception as exc:
        raise RuntimeError(
            "Bloomberg BQL runtime not available (cannot import 'bql'). Run in BQuant environment."
        ) from exc

    if not hasattr(bql, "Service") or not hasattr(bql, "Request"):
        mod_path = getattr(bql, "__file__", "<unknown>")
        raise RuntimeError(
            f"Invalid 'bql' module (no Service/Request). Found at {mod_path}.\n"
            "You may have installed an unrelated 'bql' from PyPI. In BQuant, you do not need to pip install 'bql'.\n"
            "Fix: uninstall the PyPI 'bql' (e.g., `%pip uninstall -y bql`) and restart, or run this code inside Bloomberg BQuant."
        )

    try:
        bq = bql.Service()
    except Exception as exc:
        raise RuntimeError("Unable to construct bql.Service(); check BQuant/BQL runtime.") from exc

    # Basic sanity: modern API should expose namespaced accessors
    for attr in ("data", "func", "execute"):
        if not hasattr(bq, attr):
            raise RuntimeError(
                f"bql.Service() missing attribute '{attr}'. Your BQL runtime may be incompatible."
            )

    return bql, bq


def _has_nuke_funcs(bq) -> bool:
    # In this environment, the nuke function is exposed under bq.data with keyword args
    return hasattr(bq.data, "nuke_dollar_neutral_price")


def _hedge_model() -> str:
    return os.getenv("BQL_NUKE_HEDGE_MODEL", "Delta")


def derive_underlying_from_cb(cb_ticker: str) -> str:
    """Derive the common underlying ticker from a convertible via BQL.

    Uses `cv_common_ticker_exch()` (data or func); falls back to legacy Function.
    Returns a string ticker (e.g., "TICK US Equity").
    """
    bql, bq = _get_bql_service()
    if hasattr(bq.data, "cv_common_ticker_exch"):
        expr = bq.data.cv_common_ticker_exch()
    elif hasattr(bq.func, "cv_common_ticker_exch"):
        expr = bq.func.cv_common_ticker_exch()
    else:
        raise RuntimeError(
            "BQL does not expose 'cv_common_ticker_exch' in this environment. "
            "Please provide the underlying ticker explicitly."
        )
    req = bql.Request(cb_ticker, {"value": expr})
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
    bql, bq = _get_bql_service()

    # Helper to request a simple time series (data item or function)
    def _ts(sec: str, data_item_name: str) -> pd.Series:
        # Use two-parameter range per BQL guide (default business days)
        dates = bq.func.range(start, end)
        di = None
        if hasattr(bq.data, data_item_name):
            di = getattr(bq.data, data_item_name)(dates=dates)
        elif hasattr(bq.func, data_item_name):
            # Some items are exposed as functions
            try:
                di = getattr(bq.func, data_item_name)(dates=dates)
            except TypeError:
                di = getattr(bq.func, data_item_name)(dates)
        else:
            raise RuntimeError(
                f"Unknown BQL item '{data_item_name}' (neither bq.data nor bq.func)."
            )
        req = bql.Request(sec, {"value": di})
        res = bq.execute(req)
        df = res[0].df()
        return _ensure_series(df)

    # Convertible close
    cb_close = _ts(cb_ticker, "px_last")

    # Underlying close (either provided or derived)
    if not udly_ticker:
        udly_ticker = derive_underlying_from_cb(cb_ticker)
    udly_close = _ts(udly_ticker, "px_last")

    # CB delta time series (field provided by user: ud_delta)
    # If ud_delta is available as a time series field, the same style works; otherwise
    # adapt this to your environment.
    dates = bq.func.range(start, end)
    # Robust resolution for ud_delta: try configurable item name(s). If series fails, try scalar and broadcast.
    try:
        # Candidate names: env override first, then common fallbacks
        env_name = os.getenv("BQL_DELTA_ITEM", "ud_delta").strip()
        candidates = []
        if env_name:
            candidates.append(env_name)
        for n in ("ud_delta", "delta", "conv_delta", "cb_delta"):
            if n not in candidates:
                candidates.append(n)

        delta_expr = None
        chosen_name = None
        for name in candidates:
            if hasattr(bq.data, name):
                delta_expr = getattr(bq.data, name)(dates=dates)
                chosen_name = name
                break
            if hasattr(bq.func, name):
                try:
                    delta_expr = getattr(bq.func, name)(dates=dates)
                except TypeError:
                    delta_expr = getattr(bq.func, name)(dates)
                chosen_name = name
                break
        if delta_expr is None:
            raise RuntimeError("No BQL time series for a delta item was found.")

        delta_item = {"value": delta_expr}
        delta_req = bql.Request(cb_ticker, delta_item)
        delta_res = bq.execute(delta_req)
        ud_delta = _ensure_series(delta_res[0].df())
    except Exception:
        # Scalar fallback (as-of), then broadcast over cb_close index
        scalar_expr = None
        env_name = os.getenv("BQL_DELTA_ITEM", "ud_delta").strip()
        candidates = []
        if env_name:
            candidates.append(env_name)
        for n in ("ud_delta", "delta", "conv_delta", "cb_delta"):
            if n not in candidates:
                candidates.append(n)

        for name in candidates:
            if hasattr(bq.data, name):
                try:
                    scalar_expr = getattr(bq.data, name)()
                    break
                except Exception:
                    pass
            if hasattr(bq.func, name):
                try:
                    scalar_expr = getattr(bq.func, name)()
                    break
                except Exception:
                    pass
        if scalar_expr is None:
            raise RuntimeError(
                "No BQL data item or function for a delta (scalar or time series). "
                "Set a fixed delta in the UI or define env var BQL_DELTA_ITEM."
            )

        scalar_req = bql.Request(cb_ticker, {"value": scalar_expr})
        scalar_res = bq.execute(scalar_req)
        scalar_df = scalar_res[0].df()
        if "value" not in scalar_df.columns or scalar_df.empty:
            raise ValueError("Unable to retrieve 'ud_delta' as time series or scalar from BQL.")
        scalar_val = float(scalar_df["value"].iloc[0])
        # Broadcast constant delta across cb_close index
        ud_delta = pd.Series(scalar_val, index=cb_close.index).rename("ud_delta")

    return TimeSeriesData(cb_close=cb_close, udly_close=udly_close, ud_delta=ud_delta)

def compute_nuke_with_bql_function_single(
    cb_ticker: str,
    anchor_cb_price: float,
    anchor_udly_price: float,
    input_udly_price: float,
    anchor_delta: Optional[float] = None,
) -> float:
    """Call BQL nuke function for a single input underlying price.

    Uses data.nuke_dollar_neutral_price with keyword args:
    - nuke_input_underlying_price
    - nuke_anchor_bond_price
    - nuke_anchor_underlying_price
    - delta_hedge_cv_model (default: "Delta" or env BQL_NUKE_HEDGE_MODEL)
    Returns a float.
    """
    bql, bq = _get_bql_service()
    if not _has_nuke_funcs(bq):
        raise RuntimeError("BQL nuke function is unavailable in this environment.")

    # Build kwargs according to environment
    kwargs = dict(
        nuke_input_underlying_price=float(input_udly_price),
        delta_hedge_cv_model=_hedge_model(),
        nuke_anchor_bond_price=float(anchor_cb_price),
        nuke_anchor_underlying_price=float(anchor_udly_price),
    )
    # Optional: pass explicit delta if param name is provided via env
    delta_arg = os.getenv("BQL_NUKE_DELTA_ARG", "").strip()
    if delta_arg:
        if anchor_delta is None:
            raise RuntimeError(
                "BQL_NUKE_DELTA_ARG is set but no anchor_delta provided."
            )
        kwargs[delta_arg] = float(anchor_delta)

    fn = bq.data.nuke_dollar_neutral_price(**kwargs)
    req = bql.Request(cb_ticker, {"value": fn})
    res = bq.execute(req)
    df = res[0].df()
    return float(df["value"].iloc[0])


def compute_nuke_series_with_bql(
    cb_ticker: str,
    udly_close: pd.Series,
    anchor_cb_price: float,
    anchor_udly_price: float,
    anchor_delta: Optional[float] = None,
) -> pd.Series:
    """Attempt a vectorized BQL nuke over the given date index; fallback to per-date calls.

    Parameters
    - cb_ticker: convertible identifier for BQL context
    - udly_close: series of input underlying prices (indexed by date)
    - anchor_cb_price: CB(T0)
    - anchor_udly_price: U(T0)
    """
    try:
        bql, bq = _get_bql_service()
        if not _has_nuke_funcs(bq):
            raise RuntimeError("BQL nuke function is unavailable in this environment.")

        # Try to build a vectorized expression using the underlying PX time series as input
        # Note: Some BQL deployments accept numeric literals in functions; adjust if needed.
        input_series = udly_close.sort_index()
        # To use the existing series directly, request the same range and map by date.
        start = input_series.index[0].strftime("%Y-%m-%d")
        end = input_series.index[-1].strftime("%Y-%m-%d")
        dates = bq.func.range(start, end)
        udly_ts_item = bq.data.px_last(dates=dates)

        kwargs = dict(
            nuke_input_underlying_price=udly_ts_item,
            delta_hedge_cv_model=_hedge_model(),
            nuke_anchor_bond_price=float(anchor_cb_price),
            nuke_anchor_underlying_price=float(anchor_udly_price),
        )
        delta_arg = os.getenv("BQL_NUKE_DELTA_ARG", "").strip()
        if delta_arg:
            if anchor_delta is None:
                raise RuntimeError(
                    "BQL_NUKE_DELTA_ARG is set but no anchor_delta provided."
                )
            kwargs[delta_arg] = float(anchor_delta)

        nuke_fn = bq.data.nuke_dollar_neutral_price(**kwargs)
        req = bql.Request(cb_ticker, {"value": nuke_fn})
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
        # If nuke function is absent, bail out early to let caller choose linear method
        bql, bq = _get_bql_service()
        if not _has_nuke_funcs(bq):
            raise RuntimeError("BQL nuke function is unavailable; use linear method.")
        values = {}
        for dt_idx, u in udly_close.sort_index().items():
            try:
                nuke_val = compute_nuke_with_bql_function_single(
                    cb_ticker=cb_ticker,
                    anchor_cb_price=anchor_cb_price,
                    anchor_udly_price=anchor_udly_price,
                    input_udly_price=float(u),
                    anchor_delta=anchor_delta,
                )
                values[pd.Timestamp(dt_idx)] = nuke_val
            except Exception:
                # Skip on failure for a given date
                continue
        if not values:
            raise RuntimeError("Unable to compute nuke series via BQL (both vectorized and fallback failed).")
        return pd.Series(values).sort_index()
