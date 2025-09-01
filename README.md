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
- Le champ delta de la convertible est `ud_delta`.
- Les champs de prix utilisés sont `px_last` pour CB et sous-jacent.
- Sous-jacent dérivé automatiquement via `cv_common_ticker_exch()` si non renseigné.
- Nuke BQL: `nuke_dollar_neutral_price(nuke_anchor_bond_price(), nuke_anchor_underlying_price(), nuke_input_underlying_price())`.
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

Exigences
---------
- Bloomberg BQL accessible dans l’environnement pour `bql_fetch.py`.
- Python 3.9+ recommandé ; pandas, panel, hvplot.

Limites et extensions
---------------------
- Deux méthodes disponibles: BQL nuke (prioritaire) et delta linéaire (fallback). Vous pouvez étendre pour supporter d’autres modèles (gamma, etc.).
