from __future__ import annotations

import datetime as dt
import pandas as pd
import panel as pn
import hvplot.pandas  # noqa: F401 - registers hvplot

from .logic import compute_dollar_neutral
from .bql_fetch import fetch_timeseries_with_bql, compute_nuke_series_with_bql


pn.extension("tabulator")


def _default_dates(days: int = 60):
    end = dt.date.today()
    start = end - dt.timedelta(days=days)
    return start, end


class DNApp:
    def __init__(self):
        start, end = _default_dates(90)

        # Inputs
        self.cb_ticker = pn.widgets.TextInput(name="CB Ticker", placeholder="e.g., TICKER Corp 2.5 2028", value="")
        self.udly_ticker = pn.widgets.TextInput(name="Underlying Ticker", placeholder="e.g., TICK US Equity (optional)", value="")
        self.date_range = pn.widgets.DatetimeRangePicker(name="Date Range", start=dt.date(2000, 1, 1), end=dt.date.today(), value=(start, end))
        self.anchor_date = pn.widgets.DatetimeInput(name="Anchor Date (optional)", value=None)
        self.method = pn.widgets.Select(name="Méthode", options=["BQL nuke", "Delta (linéaire)"], value="BQL nuke")
        self.delta_override = pn.widgets.FloatInput(name="Delta fixe (optionnel)", value=None, step=0.01, start=-5.0, end=5.0)
        self.use_oldest_delta = pn.widgets.Checkbox(name="Utiliser le delta le plus ancien", value=False)
        self.run_btn = pn.widgets.Button(name="Compute", button_type="primary")

        # Outputs
        self.status = pn.pane.Markdown("", sizing_mode="stretch_width")
        self.table = pn.widgets.Tabulator(pd.DataFrame(), height=280, sizing_mode="stretch_width")
        self.plot_pane = pn.pane.HoloViews(sizing_mode="stretch_both")

        self.run_btn.on_click(self._on_run)

    def _on_run(self, _):
        cb = self.cb_ticker.value.strip()
        ud = self.udly_ticker.value.strip() or None
        (start, end) = self.date_range.value
        anchor_dt = self.anchor_date.value
        method = self.method.value
        delta_override = self.delta_override.value
        use_oldest_delta = self.use_oldest_delta.value

        if not cb:
            self.status.object = "Veuillez saisir le ticker de l'obligation convertible."
            self.status.styles = {"color": "#b00"}
            return

        start_s = pd.Timestamp(start).strftime("%Y-%m-%d")
        end_s = pd.Timestamp(end).strftime("%Y-%m-%d")

        try:
            self.status.object = f"Récupération BQL: {cb} / {ud or '(derive)'} de {start_s} à {end_s}…"
            self.status.styles = {"color": "#555"}
            # Business days by default
            ts = fetch_timeseries_with_bql(cb_ticker=cb, udly_ticker=ud, start=start_s, end=end_s, freq="BUSINESS_DAYS")

            # Base alignment + linear fallback
            df = compute_dollar_neutral(
                cb_close=ts.cb_close,
                udly_close=ts.udly_close,
                ud_delta=ts.ud_delta,
                anchor_date=anchor_dt,
                method="delta",
                delta_override=delta_override,
                use_oldest_delta=use_oldest_delta,
            )

            actual_anchor = pd.Timestamp(anchor_dt) if anchor_dt else df.index[0]

            if method == "BQL nuke":
                try:
                    CB0 = df.at[actual_anchor, "cb_close"]
                    U0 = df.at[actual_anchor, "udly_close"]
                    nuke_series = compute_nuke_series_with_bql(
                        cb_ticker=cb,
                        udly_close=df["udly_close"],
                        anchor_cb_price=float(CB0),
                        anchor_udly_price=float(U0),
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
                except Exception as bql_exc:
                    method_note = f"Delta (fallback, BQL nuke échec: {type(bql_exc).__name__})"
            else:
                method_note = "Delta (linéaire)"

            self.table.value = df.round(6).reset_index().rename(columns={"index": "date"})

            # Plot DN time series
            dn_plot = df["dn"].hvplot(line_width=2, title="Dollar-Neutral (DN)", ylabel="DN", xlabel="Date")
            cb_plot = df["cb_close"].hvplot(line_width=1, color="#888", alpha=0.6, ylabel="Price")
            self.plot_pane.object = (dn_plot * cb_plot).opts(legend_position="top_left")

            self.status.object = (
                f"OK. Méthode: {method_note} | Dates: {df.index[0].date()} → {df.index[-1].date()} | "
                f"Anchor: {pd.Timestamp(actual_anchor).date()}"
            )
            self.status.styles = {"color": "#0a0"}

        except Exception as exc:
            self.status.object = f"Erreur: {type(exc).__name__}: {exc}"
            self.status.styles = {"color": "#b00"}

    def view(self):
        controls = pn.Row(
            pn.Column(
                self.cb_ticker,
                self.udly_ticker,
                self.date_range,
                self.anchor_date,
                self.method,
                self.delta_override,
                self.use_oldest_delta,
                self.run_btn,
                sizing_mode="stretch_width",
            ),
        )
        return pn.Column(
            pn.pane.Markdown("# DN Convertible App"),
            controls,
            self.status,
            pn.Row(self.plot_pane, sizing_mode="stretch_both"),
            pn.Spacer(height=8),
            self.table,
            sizing_mode="stretch_both",
        )


def main():
    app = DNApp()
    return app.view()


if __name__ == "__main__":
    # Serve with: panel serve bquant_app/app.py --show
    pn.extension()
    pn.serve({"/": main}, show=True)
