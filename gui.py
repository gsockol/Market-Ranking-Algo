"""
gui.py — HVLP Global Gym Market Scoring Tool
=============================================
Desktop GUI built on Python's built-in tkinter (no extra install needed).

Three modes
-----------
▶ Run All Countries
    Full 17-variable pipeline for all 20 preloaded countries.
    Shows ranked table with tier colour bands and per-category contributions.
    Enables Excel download and HTML dashboard (opens in default browser).

📊 Single Country Scorecard
    Select any preloaded country from the dropdown.
    Displays a detailed scorecard: category headers, per-variable breakdown
    (raw value / normalised score / weight / contribution / data source).

➕ Add New Country
    Enter a country name + 7 required CSV inputs.
    Appends to the 20-country dataset, reruns the full pipeline, and shows
    the new country's scorecard with its global rank.

Usage
-----
    python gui.py
"""

import os
import shutil
import sys
import threading
import webbrowser
from pathlib import Path

import numpy as np
import pandas as pd
import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext, ttk

# ---------------------------------------------------------------------------
# Allow running from repo root without installing the package
# ---------------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).parent))

import config as cfg
from src.calculator import calculate_derived_metrics
from src.commentary import generate_commentary
from src.dashboard import generate_dashboard
from src.exporter import export_excel
from src.fetcher import fetch_all_external_data
from src.ingestor import ingest_csv
from src.normalizer import normalize_all
from src.override_loader import load_yaml_overrides, merge_overrides
from src.scorer import compute_scores
from src.weighter import build_weight_matrix

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CSV_PATH = "input_data.csv"
OUTPUT_DIR = "output"
YAML_PATH = "overrides/manual_inputs.yaml"

PRELOADED_COUNTRIES = sorted(cfg.COUNTRY_ISO3_MAP.keys())

CATEGORY_LABELS = {
    "market_opportunity":   "Market Opportunity",
    "penetration_headroom": "Penetration Headroom",
    "operational_risk":     "Operational Risk",
    "cost_structure":       "Cost Structure",
    "demand_indicators":    "Demand Indicators",
}

VAR_LABELS = {
    "opportunity_usd_m":      "Opportunity ($M)",
    "potential_market_size":  "Potential Market Size ($M)",
    "gym_membership_cagr":    "Gym CAGR (%)",
    "penetration_headroom":   "Penetration Headroom",
    "concentration":          "Concentration (000s/gym)",
    "ease_of_doing_business": "Ease of Doing Business",
    "political_stability":    "Political Stability",
    "inflation_rate":         "Inflation Rate (%)",
    "currency_volatility":    "Currency Volatility",
    "rule_of_law":            "Rule of Law",
    "financing_accessibility":"Financing Accessibility",
    "corporate_tax_rate":     "Corporate Tax Rate (%)",
    "labor_cost_index":       "Labour Cost Index",
    "real_estate_cost_index": "Real Estate Cost Index",
    "working_age_population_pct": "Working Age Population (15–64%)",
    "middle_class_pct":       "Middle Class (%)",
    "avg_gym_spend_pct_gdp":  "Avg Gym Spend as % GDP",
}

# World countries for Add New Country dialog (curated list)
WORLD_COUNTRIES = sorted([
    "Afghanistan", "Albania", "Algeria", "Angola", "Argentina", "Armenia",
    "Australia", "Azerbaijan", "Bahrain", "Bangladesh", "Belarus", "Bolivia",
    "Bosnia and Herzegovina", "Botswana", "Bulgaria", "Cambodia", "Cameroon",
    "Canada", "China", "Croatia", "Cuba", "Cyprus", "Czech Republic",
    "Denmark", "Ecuador", "Egypt", "El Salvador", "Estonia", "Ethiopia",
    "Finland", "Georgia", "Ghana", "Greece", "Guatemala", "Hong Kong",
    "Hungary", "Iceland", "Iran", "Iraq", "Ireland", "Israel", "Jordan",
    "Kazakhstan", "Kenya", "Kuwait", "Latvia", "Lebanon", "Lithuania",
    "Luxembourg", "Malaysia", "Malta", "Mexico", "Moldova", "Mongolia",
    "Morocco", "Mozambique", "Myanmar", "Nepal", "New Zealand", "Nigeria",
    "Norway", "Oman", "Pakistan", "Panama", "Paraguay", "Peru", "Qatar",
    "Romania", "Russia", "Saudi Arabia", "Senegal", "Serbia", "Singapore",
    "Slovakia", "Slovenia", "South Africa", "Spain", "Sri Lanka", "Sweden",
    "Syria", "Taiwan", "Tanzania", "Tunisia", "Uganda", "Ukraine",
    "United Arab Emirates", "Uruguay", "Uzbekistan", "Venezuela", "Vietnam",
    "Zambia", "Zimbabwe",
])

EXTRA_ISO3 = {
    "Australia": "AUS", "Canada": "CAN", "China": "CHN", "Spain": "ESP",
    "Sweden": "SWE", "Norway": "NOR", "Denmark": "DNK", "Finland": "FIN",
    "Greece": "GRC", "Czech Republic": "CZE", "Hungary": "HUN",
    "Romania": "ROU", "Bulgaria": "BGR", "Croatia": "HRV", "Slovakia": "SVK",
    "Slovenia": "SVN", "Estonia": "EST", "Latvia": "LVA", "Lithuania": "LTU",
    "Ireland": "IRL", "Luxembourg": "LUX", "Malta": "MLT", "Cyprus": "CYP",
    "Iceland": "ISL", "Serbia": "SRB", "Ukraine": "UKR", "Russia": "RUS",
    "Kazakhstan": "KAZ", "Vietnam": "VNM", "Malaysia": "MYS",
    "Singapore": "SGP", "New Zealand": "NZL", "Pakistan": "PAK",
    "Nigeria": "NGA", "South Africa": "ZAF", "Egypt": "EGY",
    "Saudi Arabia": "SAU", "United Arab Emirates": "ARE", "Israel": "ISR",
    "Morocco": "MAR", "Mexico": "MEX", "Argentina": "ARG", "Peru": "PER",
}


# ---------------------------------------------------------------------------
# Pipeline orchestrator (shared by all three GUI modes)
# ---------------------------------------------------------------------------

def run_pipeline(extra_rows=None, log_fn=None):
    """
    Execute the full 10-step scoring pipeline.

    Parameters
    ----------
    extra_rows : list[dict] | None
        Additional country rows to append to the preloaded CSV dataset.
    log_fn : callable | None
        Called with a status string at each major step (for GUI progress).

    Returns
    -------
    tuple: (scores_df, full_df, normalized_df, weight_matrix, audit, commentary)
    """
    def _log(msg):
        if log_fn:
            log_fn(msg)

    _log("Step 1 — Ingesting CSV …")
    df = ingest_csv(CSV_PATH, cfg.CSV_COLUMN_MAP)

    if extra_rows:
        extra_df = pd.DataFrame(extra_rows)
        df = pd.concat([df, extra_df], ignore_index=True)

    countries = df["country"].tolist()

    _log(f"Step 2 — Computing derived metrics for {len(countries)} countries …")
    df = calculate_derived_metrics(df, cfg.DUES_INCREASE_PCT)

    _log("Step 3 — Fetching external data (World Bank / OECD / Trading Economics) …")
    external_data = fetch_all_external_data(
        countries=countries,
        country_iso3_map=cfg.COUNTRY_ISO3_MAP,
        wb_indicators=cfg.WB_INDICATORS,
        oecd_country_codes=cfg.OECD_COUNTRY_CODES,
        te_api_key=cfg.TRADING_ECONOMICS_API_KEY,
        cache_dir=cfg.CACHE_DIR,
        ttl_hours=cfg.CACHE_EXPIRY_HOURS,
    )

    _log("Step 4 — Loading YAML overrides …")
    yaml_overrides = load_yaml_overrides(YAML_PATH)

    _log("Step 5 — Merging all data sources …")
    scored_vars = list(cfg.WEIGHTS.keys())
    df, audit = merge_overrides(df, external_data, yaml_overrides, scored_vars)

    _log("Step 6 — Normalising (min-max, active dataset scope) …")
    normalized_df = normalize_all(df, scored_vars, cfg.INVERTED_VARIABLES, cfg.USA_BASELINE)

    _log("Step 7 — Building per-country weight matrix (Rules 1–3) …")
    availability = {
        row["country"]: {var: pd.notna(row.get(var)) for var in scored_vars}
        for _, row in df.iterrows()
    }
    weight_matrix = build_weight_matrix(
        countries=countries,
        availability_matrix=availability,
        base_weights=cfg.WEIGHTS,
        rule1_cfg=cfg.RULE1_MISSING_CAGR,
        rule2_cfg=cfg.RULE2_MISSING_CONCENTRATION,
        categories=cfg.VARIABLE_CATEGORIES,
    )

    _log("Step 8 — Computing composite scores …")
    scores_df = compute_scores(
        normalized_df=normalized_df,
        weight_matrix=weight_matrix,
        categories=cfg.VARIABLE_CATEGORIES,
        tier_thresholds=cfg.TIER_THRESHOLDS,
        tier_labels=cfg.TIER_LABELS,
    )

    _log("Step 9 — Generating commentary …")
    commentary = generate_commentary(
        scores_df=scores_df,
        full_df=df,
        normalized_df=normalized_df,
        weight_matrix=weight_matrix,
        audit=audit,
        categories=cfg.VARIABLE_CATEGORIES,
        inverted_variables=cfg.INVERTED_VARIABLES,
    )

    _log("Step 10 — Writing HTML dashboard and Excel workbook …")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    generate_dashboard(
        scores_df=scores_df, full_df=df, normalized_df=normalized_df,
        weight_matrix=weight_matrix, audit=audit, commentary=commentary,
        categories=cfg.VARIABLE_CATEGORIES, base_weights=cfg.WEIGHTS,
        tier_colors=cfg.TIER_COLORS, output_dir=OUTPUT_DIR,
        filename=cfg.DASHBOARD_FILENAME,
    )
    export_excel(
        scores_df=scores_df, full_df=df, normalized_df=normalized_df,
        weight_matrix=weight_matrix, audit=audit,
        categories=cfg.VARIABLE_CATEGORIES, base_weights=cfg.WEIGHTS,
        output_dir=OUTPUT_DIR, filename=cfg.EXCEL_FILENAME,
    )

    _log("Done.")
    return scores_df, df, normalized_df, weight_matrix, audit, commentary


# ---------------------------------------------------------------------------
# Scorecard text builder
# ---------------------------------------------------------------------------

def build_scorecard_text(country, scores_df, full_df, normalized_df,
                         weight_matrix, audit):
    """Return a formatted multi-line scorecard string for display."""
    row = scores_df[scores_df["country"] == country]
    if row.empty:
        return f"No scorecard data found for: {country}"
    row = row.iloc[0]

    rank  = int(row["rank"])
    score = float(row["composite_score"])
    tier  = row["tier"]
    total = len(scores_df)

    country_idx = full_df[full_df["country"] == country].index
    if country_idx.empty:
        return f"Country '{country}' not found in dataset."
    pos = list(full_df.index).index(country_idx[0])
    norm_row = normalized_df.iloc[pos]
    full_row = full_df[full_df["country"] == country].iloc[0]
    weights  = weight_matrix.get(country, {})
    ctry_audit = audit.get(country, {})

    lines = []
    sep = "═" * 80
    lines.append(sep)
    lines.append(f"  SCORECARD: {country.upper()}")
    lines.append(sep)
    lines.append(
        f"  Global Rank: {rank} / {total}   │   "
        f"Composite Score: {score:.1f} / 100   │   {tier}"
    )
    lines.append("")

    cat_base_wts = {
        cat: sum(cfg.WEIGHTS.get(v, 0.0) for v in vlist)
        for cat, vlist in cfg.VARIABLE_CATEGORIES.items()
    }

    header = (
        f"  {'Variable':<30} {'Raw Value':>12} {'Norm':>6} "
        f"{'Wt%':>5} {'Contrib':>8}  Source"
    )
    divider = "  " + "─" * 76

    for cat_key, var_list in cfg.VARIABLE_CATEGORIES.items():
        cat_label  = CATEGORY_LABELS.get(cat_key, cat_key)
        cat_score  = float(row.get(f"contrib_{cat_key}", 0.0))
        cat_base   = cat_base_wts[cat_key] * 100
        lines.append(f"  ┌─ {cat_label.upper()} "
                     f"(base weight {cat_base:.0f}%)  →  contribution: {cat_score:.2f} pts")
        lines.append(header)
        lines.append(divider)

        for var in var_list:
            label = VAR_LABELS.get(var, var)
            raw   = full_row.get(var, np.nan)
            norm  = norm_row.get(var, np.nan)
            w     = weights.get(var, 0.0)
            src   = ctry_audit.get(var, "")

            raw_str  = f"{raw:,.2f}"  if pd.notna(raw)  else "—"
            norm_str = f"{float(norm)*100:.1f}" if pd.notna(norm) else "—"
            contrib  = w * (float(norm) if pd.notna(norm) else 0.0) * 100

            lines.append(
                f"  {label:<30} {raw_str:>12} {norm_str:>6} "
                f"{w*100:>4.1f}% {contrib:>7.2f}  {src}"
            )

        lines.append("")

    lines.append(sep)
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Add New Country dialog
# ---------------------------------------------------------------------------

class AddCountryDialog(tk.Toplevel):
    def __init__(self, parent):
        super().__init__(parent)
        self.title("Add New Country")
        self.geometry("560x600")
        self.resizable(False, False)
        self.grab_set()
        self.result = None
        self._build()

    def _build(self):
        pad = {"padx": 14, "pady": 5}

        ttk.Label(self, text="Add New Country to Analysis",
                  font=("", 13, "bold")).pack(pady=(18, 4))
        ttk.Label(self, text="The model will score this country alongside the "
                             "20 preloaded countries.",
                  foreground="gray").pack(pady=(0, 12))

        form = ttk.Frame(self, padding=(16, 0))
        form.pack(fill=tk.BOTH, expand=True)

        fields = [
            ("Country name",                    "country",              "str"),
            ("ISO 3-letter code (e.g. VNM)",    "iso3",                 "str_opt"),
            ("Market Size ($M)",                "market_size_m",        "float"),
            ("Current Penetration (e.g. 0.05)", "current_penetration_pct", "float"),
            ("Future Penetration  (e.g. 0.15)", "future_penetration_pct",  "float"),
            ("Population (millions)",           "population_m",         "float"),
            ("Concentration (000s per gym)",    "concentration",        "float"),
            ("GDP per Capita ($)",              "gdp_per_capita",       "float"),
            ("Gym CAGR % (blank = use Rule 1)", "gym_membership_cagr",  "float_opt"),
        ]

        self._vars = {}
        for r, (label, key, dtype) in enumerate(fields):
            ttk.Label(form, text=label, anchor=tk.W).grid(
                row=r, column=0, sticky=tk.W, **pad)
            var = tk.StringVar()
            entry = ttk.Entry(form, textvariable=var, width=22)
            entry.grid(row=r, column=1, sticky=tk.EW, **pad)
            self._vars[key] = (var, dtype)

        form.columnconfigure(1, weight=1)

        btn_frame = ttk.Frame(self, padding=(14, 10))
        btn_frame.pack(fill=tk.X, side=tk.BOTTOM)
        ttk.Button(btn_frame, text="Score Country",
                   command=self._submit).pack(side=tk.RIGHT, padx=4)
        ttk.Button(btn_frame, text="Cancel",
                   command=self.destroy).pack(side=tk.RIGHT)

    def _submit(self):
        data = {}
        for key, (var, dtype) in self._vars.items():
            raw = var.get().strip()
            if dtype == "str":
                if not raw:
                    messagebox.showwarning("Missing Field", f"Please enter: {key}",
                                           parent=self)
                    return
                data[key] = raw
            elif dtype == "str_opt":
                data[key] = raw.upper() if raw else ""
            elif dtype == "float":
                try:
                    data[key] = float(raw)
                except ValueError:
                    messagebox.showwarning("Invalid Input",
                                           f"Enter a number for: {key}", parent=self)
                    return
            elif dtype == "float_opt":
                data[key] = float(raw) if raw else None

        self.result = data
        self.destroy()


# ---------------------------------------------------------------------------
# Main application window
# ---------------------------------------------------------------------------

class HVLPApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("HVLP Global Gym Market Scoring Tool")
        self.root.geometry("1280x820")
        self.root.minsize(1000, 640)

        # Pipeline state
        self._scores_df      = None
        self._full_df        = None
        self._normalized_df  = None
        self._weight_matrix  = None
        self._audit          = None

        self._build_ui()

    # -----------------------------------------------------------------------
    # UI construction
    # -----------------------------------------------------------------------

    def _build_ui(self):
        # ── Toolbar ─────────────────────────────────────────────────────────
        toolbar = ttk.Frame(self.root, padding=(10, 8))
        toolbar.pack(fill=tk.X, side=tk.TOP)

        ttk.Button(toolbar, text="▶  Run All Countries",
                   command=self._run_all_thread,
                   style="Accent.TButton"
                   ).pack(side=tk.LEFT, padx=(0, 10))

        ttk.Separator(toolbar, orient=tk.VERTICAL).pack(
            side=tk.LEFT, fill=tk.Y, padx=6)

        ttk.Label(toolbar, text="Single Country:").pack(side=tk.LEFT)
        self.country_var = tk.StringVar(value=PRELOADED_COUNTRIES[0])
        ttk.Combobox(
            toolbar, textvariable=self.country_var,
            values=PRELOADED_COUNTRIES, width=18, state="readonly",
        ).pack(side=tk.LEFT, padx=6)
        ttk.Button(toolbar, text="📊 Scorecard",
                   command=self._single_scorecard).pack(side=tk.LEFT, padx=(0, 10))

        ttk.Separator(toolbar, orient=tk.VERTICAL).pack(
            side=tk.LEFT, fill=tk.Y, padx=6)
        ttk.Button(toolbar, text="➕ Add New Country",
                   command=self._add_country_dialog).pack(side=tk.LEFT, padx=6)

        # Right-side action buttons
        self._excel_btn = ttk.Button(
            toolbar, text="⬇  Save Excel",
            command=self._save_excel, state=tk.DISABLED)
        self._excel_btn.pack(side=tk.RIGHT, padx=4)

        self._dash_btn = ttk.Button(
            toolbar, text="🌐  Open Dashboard",
            command=self._open_dashboard, state=tk.DISABLED)
        self._dash_btn.pack(side=tk.RIGHT, padx=4)

        # ── Status bar ───────────────────────────────────────────────────────
        status_bar = ttk.Frame(self.root, padding=(8, 2))
        status_bar.pack(fill=tk.X, side=tk.BOTTOM)
        self._status_var = tk.StringVar(value="Ready — click ▶ Run All Countries to begin.")
        ttk.Label(status_bar, textvariable=self._status_var,
                  anchor=tk.W).pack(side=tk.LEFT, fill=tk.X, expand=True)
        self._progress = ttk.Progressbar(status_bar, mode="indeterminate", length=180)
        self._progress.pack(side=tk.RIGHT)

        # ── Notebook tabs ────────────────────────────────────────────────────
        self._nb = ttk.Notebook(self.root)
        self._nb.pack(fill=tk.BOTH, expand=True, padx=10, pady=(4, 0))

        # Tab 1: Rankings table
        rankings_frame = ttk.Frame(self._nb)
        self._nb.add(rankings_frame, text="  Rankings  ")
        self._build_rankings_tab(rankings_frame)

        # Tab 2: Scorecard
        scorecard_frame = ttk.Frame(self._nb)
        self._nb.add(scorecard_frame, text="  Scorecard  ")
        self._scorecard_text = scrolledtext.ScrolledText(
            scorecard_frame, font=("Courier New", 10),
            state=tk.DISABLED, wrap=tk.NONE, bg="#f7f7f7",
        )
        self._scorecard_text.pack(fill=tk.BOTH, expand=True)

        # Tab 3: Log
        log_frame = ttk.Frame(self._nb)
        self._nb.add(log_frame, text="  Log  ")
        self._log_text = scrolledtext.ScrolledText(
            log_frame, font=("Courier New", 9),
            state=tk.DISABLED, bg="#1e1e1e", fg="#d4d4d4",
        )
        self._log_text.pack(fill=tk.BOTH, expand=True)

    def _build_rankings_tab(self, parent):
        cat_keys = list(cfg.VARIABLE_CATEGORIES.keys())
        cat_short = {
            "market_opportunity":   "Mkt Opp.",
            "penetration_headroom": "Pen. HR",
            "operational_risk":     "Op. Risk",
            "cost_structure":       "Cost",
            "demand_indicators":    "Demand",
        }
        cols = ("Rank", "Country", "Score", "Tier") + tuple(
            cat_short.get(k, k) for k in cat_keys
        )
        col_widths = {
            "Rank": 50, "Country": 145, "Score": 70, "Tier": 185,
            "Mkt Opp.": 80, "Pen. HR": 70, "Op. Risk": 75, "Cost": 60, "Demand": 70,
        }

        self._tree = ttk.Treeview(parent, columns=cols, show="headings", height=28)
        for col in cols:
            self._tree.heading(col, text=col,
                               command=lambda c=col: self._sort_tree(c))
            self._tree.column(col, width=col_widths.get(col, 80), anchor=tk.CENTER)

        self._tree.tag_configure("tier1", background="#d1fae5")  # green
        self._tree.tag_configure("tier2", background="#dbeafe")  # blue
        self._tree.tag_configure("tier3", background="#fef3c7")  # amber
        self._tree.tag_configure("tier4", background="#fee2e2")  # red

        vsb = ttk.Scrollbar(parent, orient=tk.VERTICAL, command=self._tree.yview)
        self._tree.configure(yscrollcommand=vsb.set)
        vsb.pack(side=tk.RIGHT, fill=tk.Y)

        hsb = ttk.Scrollbar(parent, orient=tk.HORIZONTAL, command=self._tree.xview)
        self._tree.configure(xscrollcommand=hsb.set)
        hsb.pack(side=tk.BOTTOM, fill=tk.X)

        self._tree.pack(fill=tk.BOTH, expand=True)

    # -----------------------------------------------------------------------
    # Mode 1 — Run All Countries
    # -----------------------------------------------------------------------

    def _run_all_thread(self):
        self._start_busy("Running analysis for all countries …")
        threading.Thread(target=self._run_all_worker, daemon=True).start()

    def _run_all_worker(self):
        try:
            result = run_pipeline(log_fn=self._log)
            self.root.after(0, lambda: self._run_all_done(result))
        except Exception as exc:
            self.root.after(0, lambda: self._show_error(str(exc)))

    def _run_all_done(self, result):
        (self._scores_df, self._full_df, self._normalized_df,
         self._weight_matrix, self._audit, _) = result
        self._populate_rankings()
        self._excel_btn.config(state=tk.NORMAL)
        self._dash_btn.config(state=tk.NORMAL)
        self._nb.select(0)
        self._stop_busy(
            f"✅ Complete — {len(self._scores_df)} countries scored."
        )

    # -----------------------------------------------------------------------
    # Mode 2 — Single Country Scorecard
    # -----------------------------------------------------------------------

    def _single_scorecard(self):
        country = self.country_var.get()
        if not country:
            messagebox.showwarning("No Country", "Please select a country.")
            return
        if self._scores_df is None:
            self._start_busy(f"Running analysis to score {country} …")
            threading.Thread(
                target=self._scorecard_worker, args=(country,), daemon=True
            ).start()
        else:
            self._show_scorecard(country)

    def _scorecard_worker(self, country):
        try:
            result = run_pipeline(log_fn=self._log)
            (self._scores_df, self._full_df, self._normalized_df,
             self._weight_matrix, self._audit, _) = result
            self.root.after(0, lambda: self._scorecard_ready(country))
        except Exception as exc:
            self.root.after(0, lambda: self._show_error(str(exc)))

    def _scorecard_ready(self, country):
        self._populate_rankings()
        self._excel_btn.config(state=tk.NORMAL)
        self._dash_btn.config(state=tk.NORMAL)
        self._stop_busy("Ready.")
        self._show_scorecard(country)

    def _show_scorecard(self, country):
        text = build_scorecard_text(
            country, self._scores_df, self._full_df,
            self._normalized_df, self._weight_matrix, self._audit,
        )
        self._scorecard_text.config(state=tk.NORMAL)
        self._scorecard_text.delete("1.0", tk.END)
        self._scorecard_text.insert("1.0", text)
        self._scorecard_text.config(state=tk.DISABLED)
        self._nb.select(1)

    # -----------------------------------------------------------------------
    # Mode 3 — Add New Country
    # -----------------------------------------------------------------------

    def _add_country_dialog(self):
        dlg = AddCountryDialog(self.root)
        self.root.wait_window(dlg)
        if dlg.result is None:
            return
        data = dlg.result
        country = data["country"]
        iso3    = data.pop("iso3", "") or ""

        # Register new country's ISO3 so the fetcher can call APIs for it
        if iso3:
            cfg.COUNTRY_ISO3_MAP[country] = iso3
            cfg.OECD_COUNTRY_CODES[country] = iso3 if len(iso3) == 3 else None

        row = {
            "country":                country,
            "market_size_m":          data["market_size_m"],
            "current_penetration_pct": data["current_penetration_pct"],
            "future_penetration_pct": data["future_penetration_pct"],
            "population_m":           data["population_m"],
            "concentration":          data["concentration"],
            "gdp_per_capita":         data["gdp_per_capita"],
        }
        if data.get("gym_membership_cagr") is not None:
            row["gym_membership_cagr"] = data["gym_membership_cagr"]

        self._start_busy(f"Adding and scoring {country} …")
        threading.Thread(
            target=self._add_country_worker, args=(country, row), daemon=True
        ).start()

    def _add_country_worker(self, country, row):
        try:
            result = run_pipeline(extra_rows=[row], log_fn=self._log)
            (self._scores_df, self._full_df, self._normalized_df,
             self._weight_matrix, self._audit, _) = result
            self.root.after(0, lambda: self._add_country_done(country))
        except Exception as exc:
            self.root.after(0, lambda: self._show_error(str(exc)))

    def _add_country_done(self, country):
        self._populate_rankings()
        self._excel_btn.config(state=tk.NORMAL)
        self._dash_btn.config(state=tk.NORMAL)
        self._stop_busy(f"✅ {country} added and scored.")
        self._show_scorecard(country)

    # -----------------------------------------------------------------------
    # Rankings table helpers
    # -----------------------------------------------------------------------

    def _populate_rankings(self):
        for item in self._tree.get_children():
            self._tree.delete(item)

        cat_contrib_cols = [
            f"contrib_{k}" for k in cfg.VARIABLE_CATEGORIES.keys()
        ]

        for _, row in self._scores_df.iterrows():
            tier_label = row.get("tier", "")
            if "1" in tier_label:
                tag = "tier1"
            elif "2" in tier_label:
                tag = "tier2"
            elif "3" in tier_label:
                tag = "tier3"
            else:
                tag = "tier4"

            contribs = [f"{row.get(c, 0.0):.1f}" for c in cat_contrib_cols]
            self._tree.insert("", "end", tags=(tag,), values=(
                int(row["rank"]),
                row["country"],
                f"{row['composite_score']:.1f}",
                tier_label,
                *contribs,
            ))

    def _sort_tree(self, col):
        items = [(self._tree.set(i, col), i) for i in self._tree.get_children("")]
        try:
            items.sort(key=lambda x: float(x[0]))
        except ValueError:
            items.sort(key=lambda x: x[0])
        for idx, (_, item) in enumerate(items):
            self._tree.move(item, "", idx)

    # -----------------------------------------------------------------------
    # Output actions
    # -----------------------------------------------------------------------

    def _save_excel(self):
        src = Path(OUTPUT_DIR) / cfg.EXCEL_FILENAME
        if not src.exists():
            messagebox.showerror("File Not Found", "Run the analysis first.")
            return
        dest = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel workbook", "*.xlsx")],
            initialfile=cfg.EXCEL_FILENAME,
        )
        if dest:
            shutil.copy(src, dest)
            self._log(f"Excel saved → {dest}")

    def _open_dashboard(self):
        path = Path(OUTPUT_DIR) / cfg.DASHBOARD_FILENAME
        if not path.exists():
            messagebox.showerror("File Not Found", "Run the analysis first.")
            return
        webbrowser.open(path.resolve().as_uri())
        self._log(f"Dashboard opened in browser → {path}")

    # -----------------------------------------------------------------------
    # Status / logging helpers
    # -----------------------------------------------------------------------

    def _start_busy(self, msg):
        self._status_var.set(msg)
        self._progress.start(10)
        self._log(msg)

    def _stop_busy(self, msg):
        self._progress.stop()
        self._status_var.set(msg)

    def _log(self, msg):
        self.root.after(0, lambda: self._append_log(msg))

    def _append_log(self, msg):
        self._log_text.config(state=tk.NORMAL)
        self._log_text.insert(tk.END, f"{msg}\n")
        self._log_text.see(tk.END)
        self._log_text.config(state=tk.DISABLED)

    def _show_error(self, msg):
        self._stop_busy(f"Error — see Log tab for details.")
        self._log(f"ERROR: {msg}")
        messagebox.showerror("Error", msg)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    root = tk.Tk()
    try:
        # Attempt to enable a modern theme if available
        style = ttk.Style(root)
        for theme in ("clam", "alt", "default"):
            if theme in style.theme_names():
                style.theme_use(theme)
                break
    except Exception:
        pass
    HVLPApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
