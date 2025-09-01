from __future__ import annotations

import pandas as pd
import matplotlib.pyplot as plt

from .bql_fetch import fetch_timeseries_with_bql, compute_nuke_series_with_bql
from .logic import compute_dollar_neutral


def plot_dn_static(
    cb_ticker: str,
    udly_ticker: str | None,
    start: str,
    end: str,
    anchor_date: str | pd.Timestamp | None = None,
    method: str = "BQL nuke",  # or "Delta (linéaire)"
    delta_override: float | None = None,
    use_oldest_delta: bool = False,
    show_cb_reference: bool = True,
):
    """
    Compute and plot Dollar-Neutral (DN) as a static Matplotlib figure (no JS).

    Parameters
    - cb_ticker: convertible bond identifier
    - udly_ticker: underlying identifier (None -> derive via BQL cv_common_ticker_exch())
    - start/end: YYYY-MM-DD strings
    - anchor_date: anchor date (None -> first common date)
    - method: "BQL nuke" or "Delta (linéaire)"
    - delta_override: if set, uses this fixed delta for linear method
    - use_oldest_delta: if True, uses oldest delta in range for linear method
    - show_cb_reference: overlay CB close for reference

    Returns (fig, ax, df) where df includes columns: cb_close, udly_close, ud_delta, nuke, dn
    """

    # Fetch series via BQL (range over business days by default)
    ts = fetch_timeseries_with_bql(cb_ticker=cb_ticker, udly_ticker=udly_ticker, start=start, end=end)

    # Base: compute linear DN for alignment and fallback
    df = compute_dollar_neutral(
        cb_close=ts.cb_close,
        udly_close=ts.udly_close,
        ud_delta=ts.ud_delta,
        anchor_date=anchor_date,
        method="delta",
        delta_override=delta_override,
        use_oldest_delta=use_oldest_delta,
    )

    # BQL nuke path
    actual_anchor = pd.Timestamp(anchor_date) if anchor_date else df.index[0]
    method_note = "Delta (linéaire)"
    if method.strip().lower().startswith("bql"):
        try:
            CB0 = float(df.at[actual_anchor, "cb_close"])
            U0 = float(df.at[actual_anchor, "udly_close"])
            nuke_series = compute_nuke_series_with_bql(
                cb_ticker=cb_ticker,
                udly_close=df["udly_close"],
                anchor_cb_price=CB0,
                anchor_udly_price=U0,
            )
            df = compute_dollar_neutral(
                cb_close=ts.cb_close,
                udly_close=ts.udly_close,
                ud_delta=ts.ud_delta,
                anchor_date=actual_anchor,
                method="external_nuke",
                nuke_series=nuke_series,
            )
            method_note = "BQL nuke"
        except Exception:
            method_note = "Delta (linéaire — fallback)"

    # Plot static
    fig, ax = plt.subplots(figsize=(10, 4))
    df["dn"].plot(ax=ax, lw=2, label=f"DN ({method_note})")
    if show_cb_reference:
        ax2 = ax.twinx()
        df["cb_close"].plot(ax=ax2, lw=1, color="#888", alpha=0.6, label="CB close")
        ax2.set_ylabel("CB close")
        # Manage legends
        lines1, labels1 = ax.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax.legend(lines1 + lines2, labels1 + labels2, loc="upper left")
    else:
        ax.legend(loc="upper left")

    ax.set_title(f"Dollar-Neutral — {cb_ticker} (Business Days)\nAnchor: {pd.Timestamp(actual_anchor).date()}")
    ax.set_ylabel("DN")
    fig.tight_layout()
    return fig, ax, df


def plot_dn_static_from_series(
    cb_close: pd.Series,
    udly_close: pd.Series,
    ud_delta: pd.Series,
    anchor_date: str | pd.Timestamp | None = None,
    method: str = "Delta (linéaire)",
    delta_override: float | None = None,
    use_oldest_delta: bool = False,
    show_cb_reference: bool = True,
):
    """Static DN plot without any BQL dependency using pre-fetched series.

    Provide price/delta series (date-indexed). Useful if BQL is unavailable or blocked.
    """
    df = compute_dollar_neutral(
        cb_close=cb_close,
        udly_close=udly_close,
        ud_delta=ud_delta,
        anchor_date=anchor_date,
        method="delta" if not method.lower().startswith("external") else "external_nuke",
        delta_override=delta_override,
        use_oldest_delta=use_oldest_delta,
    )

    # Plot
    fig, ax = plt.subplots(figsize=(10, 4))
    df["dn"].plot(ax=ax, lw=2, label="DN")
    if show_cb_reference:
        ax2 = ax.twinx()
        df["cb_close"].plot(ax=ax2, lw=1, color="#888", alpha=0.6, label="CB close")
        ax2.set_ylabel("CB close")
        lines1, labels1 = ax.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax.legend(lines1 + lines2, labels1 + labels2, loc="upper left")
    else:
        ax.legend(loc="upper left")

    actual_anchor = (pd.to_datetime(anchor_date) if anchor_date is not None else df.index[0])
    ax.set_title(f"Dollar-Neutral (Static) — Anchor: {pd.Timestamp(actual_anchor).date()}")
    ax.set_ylabel("DN")
    fig.tight_layout()
    return fig, ax, df


def example_static():
    """Convenience example for quick manual check."""
    cb = "DE000A4DFHL5 Corp"
    start = "2024-01-01"
    end = pd.Timestamp.today().date().isoformat()
    fig, ax, df = plot_dn_static(cb_ticker=cb, udly_ticker=None, start=start, end=end)
    return fig
