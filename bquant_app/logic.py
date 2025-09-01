from __future__ import annotations

import pandas as pd
from typing import Optional, Literal


def compute_dollar_neutral(
    cb_close: pd.Series,
    udly_close: pd.Series,
    ud_delta: pd.Series,
    anchor_date: Optional[pd.Timestamp] = None,
    method: Literal["delta", "external_nuke"] = "delta",
    nuke_series: Optional[pd.Series] = None,
    delta_override: Optional[float] = None,
    use_oldest_delta: bool = False,
) -> pd.DataFrame:
    """
    Compute the dollar-neutral (DN) variation time series for a convertible bond.

    Definitions (daily close based):
    - Let anchor date be T0 (typically the first date in the range).
    - CB0 = CB(T0), U0 = U(T0), D0 = ud_delta(T0)
    - For each date t: nuke(t) = CB0 + D0 * (U(t) - U0)
    - DN(t) = nuke(t) - CB(t)

    Parameters
    - cb_close: pd.Series of convertible close prices indexed by date
    - udly_close: pd.Series of underlying close prices indexed by date
    - ud_delta: pd.Series of convertible delta (field: ud_delta) indexed by date
    - anchor_date: optional explicit anchor date; if None, uses first common date
    - method: currently only "delta" (linear reprice using anchor delta)

    Returns
    - pd.DataFrame with columns: [cb_close, udly_close, ud_delta, nuke, dn]
    """

    if method not in {"delta", "external_nuke"}:
        raise ValueError("Unsupported method. Use 'delta' or 'external_nuke'.")

    # Align on common dates and ensure sorted index
    df = (
        pd.concat(
            {
                "cb_close": cb_close,
                "udly_close": udly_close,
                "ud_delta": ud_delta,
            },
            axis=1,
        )
        .dropna()
        .sort_index()
    )

    if df.empty:
        raise ValueError("No overlapping dates between cb_close, udly_close and ud_delta.")

    if anchor_date is None:
        anchor_date = df.index[0]
    else:
        # normalize to Timestamp if string/date was passed
        anchor_date = pd.Timestamp(anchor_date)
        if anchor_date not in df.index:
            # pick the next available date on/after anchor_date
            pos = df.index.searchsorted(anchor_date)
            if pos >= len(df.index):
                raise ValueError("Anchor date is after the last available data point.")
            anchor_date = df.index[pos]

    CB0 = df.at[anchor_date, "cb_close"]
    U0 = df.at[anchor_date, "udly_close"]
    if delta_override is not None:
        D0 = float(delta_override)
    elif use_oldest_delta:
        D0 = float(df["ud_delta"].iloc[0])
    else:
        D0 = float(df.at[anchor_date, "ud_delta"])

    if method == "external_nuke":
        if nuke_series is None:
            raise ValueError("method='external_nuke' requires nuke_series to be provided.")
        # Align external nuke series
        df["nuke"] = nuke_series.reindex(df.index)
    else:
        # Linear nuke using anchor delta
        df["nuke"] = CB0 + D0 * (df["udly_close"] - U0)
    df["dn"] = df["nuke"] - df["cb_close"]

    return df


def explain_method() -> str:
    """Short textual explanation of the DN method implemented here."""
    return (
        "DN(t) = [CB(T0) + ud_delta(T0) * (U(t) - U(T0))] - CB(t), "
        "with T0 as the anchor date."
    )
