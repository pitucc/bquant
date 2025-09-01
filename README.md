BQuant DN Convertible App
=========================

Objectif
--------
- Afficher sur un graphique les variations "dollar neutral" (DN) d’une obligation convertible sur une période choisie.
- Méthode (linéaire, ancrée sur T0): nuke(t) = CB(T0) + ud_delta(T0) * (U(t) - U(T0)) ; DN(t) = nuke(t) - CB(t).

Contenu
-------
- `bquant_app/logic.py`: fonction de calcul DN à partir de séries Pandas.
- `bquant_app/bql_fetch.py`: accès BQL pour séries, dérivation du sous-jacent et nuke.
- `bquant_app/app.py`: mini-app Panel (JS) — nécessite jupyter_bokeh/JS autorisé.
- `bquant_app/static_app.py`: tracé statique Matplotlib (sans JS) pour notebook JupyterLab restreint.

Utilisation dans BQuant
-----------------------
1. Ouvrir un Notebook BQuant avec accès BQL.
2. Installer les dépendances si nécessaire (Panel, hvplot) dans l’environnement BQuant.
3. Exécuter/servir l’app via Panel, par exemple:

   ```python
   import panel as pn
   from bquant_app.app import main
   pn.extension()
   pn.serve({"/": main}, show=True)
   ```

4. Renseigner:
   - CB Ticker: identifiant de l’obligation convertible
   - Underlying Ticker: identifiant du sous-jacent (laisser vide pour dériver via `cv_common_ticker_exch()`)
   - Date Range: plage d’analyse
   - Anchor Date (optionnel): par défaut, le premier jour dispo
   - Méthode: "BQL nuke" (par défaut) ou "Delta (linéaire)"
   - Delta fixe (optionnel): valeur qui remplace `ud_delta` pour tout le range
   - Utiliser le delta le plus ancien: force l’usage de `ud_delta` au plus vieux jour du range

Notes BQL
---------
- API utilisée: namespaces modernes `bq.data.*` et `bq.func.*` (conformément au guide HTML BQL).
- Exemples: `bq.func.range(...)`, `bq.data.px_last(dates=...)`, `bq.data.ud_delta(dates=...)`, `bq.data.cv_common_ticker_exch()`.
- Fonctions « nuke »: `bq.func.nuke_dollar_neutral_price(...)`, `bq.func.nuke_anchor_bond_price(...)`, `bq.func.nuke_anchor_underlying_price(...)`, `bq.func.nuke_input_underlying_price(...)`.
- Le champ delta de la convertible est `ud_delta`.
- Les champs de prix utilisés sont `px_last` pour CB et sous-jacent.
- Sous-jacent dérivé automatiquement via `cv_common_ticker_exch()` si non renseigné.
- L’app tente un calcul vectorisé via BQL; si indisponible, elle bascule sur un fallback (delta linéaire) ou, selon l’environnement, des appels BQL unitaires plus lents.
- Les séries sont récupérées en Business Days (jours ouvrés). Adaptez le `freq` si votre environnement BQL utilise une autre clé.

Affichage statique (sans JS)
----------------------------
Si JupyterLab bloque le JavaScript, utilisez le module statique (Matplotlib) :

```python
import pandas as pd
from bquant_app.static_app import plot_dn_static

cb = "DE000A4DFHL5 Corp"
start, end = "2024-01-01", pd.Timestamp.today().date().isoformat()

# Méthode BQL nuke (fallback delta si indisponible), ancre = premier jour commun
fig, ax, df = plot_dn_static(
    cb_ticker=cb,
    udly_ticker=None,   # dérivation auto via cv_common_ticker_exch()
    start=start,
    end=end,
    anchor_date=None,
    method="BQL nuke", # ou "Delta (linéaire)"
    delta_override=None,
    use_oldest_delta=False,
    show_cb_reference=True,
)
fig
```

Ce tracé ne dépend pas de Bokeh/Panel/JS et s’affiche dans un notebook JupyterLab restreint.

Problème courant: mauvais module `bql` (PyPI) vs Bloomberg BQL
----------------------------------------------------------------
Si vous voyez des erreurs liées à `bql` (e.g., `AttributeError` sur `Service`, `Request`, `data` ou `func`), votre environnement charge probablement un package `bql` tiers (PyPI) au lieu du runtime Bloomberg.

- Vérifiez le module chargé et les attributs clés:
  ```python
  import bql; print(bql.__file__)
  bq = bql.Service()
  print("has data:", hasattr(bq, "data"), ", has func:", hasattr(bq, "func"), ", has execute:", hasattr(bq, "execute"))
  ```
- Si le chemin pointe vers un site-packages tiers ou que `data/func` manquent, désinstallez le `bql` PyPI:
  ```python
  %pip uninstall -y bql
  ```
  Redémarrez le kernel, puis utilisez un environnement Bloomberg BQuant où `bql` est fourni nativement.
- Le code détecte ce cas et lève une erreur explicite avec guidance.

Test rapide (BQuant)
--------------------
Exécutez ce test dans un Notebook BQuant pour valider l’accès BQL et les requêtes:

```python
from bquant_app.bql_fetch import fetch_timeseries_with_bql, compute_nuke_series_with_bql
import pandas as pd

cb = "<CB TICKER ICI>"             # e.g., "DE000A4DFHL5 Corp"
ud = None                           # laisser None pour dérivation via cv_common_ticker_exch()
start, end = "2024-01-01", pd.Timestamp.today().date().isoformat()

ts = fetch_timeseries_with_bql(cb_ticker=cb, udly_ticker=ud, start=start, end=end, freq="BUSINESS_DAYS")
print("CB series head:\n", ts.cb_close.head())
print("UDLY series head:\n", ts.udly_close.head())
print("UD delta head:\n", ts.ud_delta.head())

# Optionnel: test de la fonction nuke BQL (anchor = premier jour commun)
CB0 = ts.cb_close.dropna().iloc[0]
U0 = ts.udly_close.dropna().iloc[0]
nuke_series = compute_nuke_series_with_bql(cb_ticker=cb, udly_close=ts.udly_close, anchor_cb_price=float(CB0), anchor_udly_price=float(U0))
print("Nuke head:\n", nuke_series.head())
```

Sans BQL, vous pouvez tracer via des séries pré-chargées:
```python
from bquant_app.static_app import plot_dn_static_from_series
# Fournir des Series date-indexées: cb_close, udly_close, ud_delta
fig, ax, df = plot_dn_static_from_series(cb_close, udly_close, ud_delta, anchor_date=None)
```

Exigences
---------
- Bloomberg BQL accessible dans l’environnement pour `bql_fetch.py`.
- Python 3.9+ recommandé ; pandas, panel, hvplot.

Limites et extensions
---------------------
- Deux méthodes disponibles: BQL nuke (prioritaire) et delta linéaire (fallback). Vous pouvez étendre pour supporter d’autres modèles (gamma, etc.).
