"""
src/dashboard.py
================
Generates a self-contained HTML dashboard (no external CDN dependencies).

Layout
------
  Header      — title, run metadata, model summary
  Summary bar — 4 stat cards (# countries, top market, # Tier 1, avg score)
  Rankings    — sortable table with score bars, tier badges, category mini-bars
  Detail rows — expandable per-country panels with variable breakdown table,
                data source labels, weight adjustments, raw + normalised values
  Footer      — methodology notes
"""

import html
import math
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_VAR_LABELS = {
    "opportunity_usd_m":        "Opportunity ($M)",
    "potential_market_size":    "Potential Market Size ($M)",
    "gym_membership_cagr":      "Gym Membership CAGR",
    "penetration_headroom":     "Penetration Headroom",
    "concentration":            "Concentration (inhab/gym, 000s)",
    "ease_of_doing_business":   "Ease of Doing Business",
    "political_stability":      "Political Stability Index",
    "inflation_rate":           "Inflation Rate (inv.)",
    "currency_volatility":      "Currency Volatility (inv.)",
    "rule_of_law":              "Rule of Law",
    "financing_accessibility": "Ease of Financing (GFDD)",
    "corporate_tax_rate":       "Corporate Tax Rate (inv.)",
    "labor_cost_index":         "Labour Cost Index (inv.)",
    "real_estate_cost_index":   "Real Estate Cost Index (inv.)",
    "youth_population_pct":       "Youth / Working Age Population % (15–64)",
    "middle_class_pct":         "Middle Class %",
    "avg_gym_spend_pct_gdp":    "Avg Gym Spend as % of GDP",
}

_CAT_LABELS = {
    "market_opportunity":   "Market Opportunity",
    "penetration_headroom": "Penetration Headroom",
    "operational_risk":     "Operational Risk",
    "cost_structure":       "Cost Structure",
    "demand_indicators":    "Demand Indicators",
}

_CAT_COLORS = {
    "market_opportunity":   "#3b82f6",
    "penetration_headroom": "#8b5cf6",
    "operational_risk":     "#f59e0b",
    "cost_structure":       "#ef4444",
    "demand_indicators":    "#10b981",
}

_SOURCE_BADGES = {
    "csv_derived":      ('<span class="badge badge-csv">CSV / Derived</span>', "Computed from CSV inputs"),
    "api":              ('<span class="badge badge-api">API</span>', "World Bank / OECD / Trading Economics"),
    "manual_yaml":      ('<span class="badge badge-manual">Manual YAML</span>', "overrides/manual_inputs.yaml"),
    "manual_prompt":    ('<span class="badge badge-manual">Manual (prompt)</span>', "Interactive terminal input"),
    "manual_input":     ('<span class="badge badge-manual">Manual Input</span>', "User-entered via widget/GUI"),
    "defaulted_to_zero":('<span class="badge badge-csv">Defaulted 0</span>', "No CAGR data — defaulted to 0.0"),
    "missing":          ('<span class="badge badge-missing">Missing</span>', "No data — weight redistributed"),
}


def _tier_color(tier: str) -> str:
    if "Tier 1" in tier:
        return "#7c3aed"   # purple — outperforming
    if "Tier 2" in tier:
        return "#22c55e"   # green  — competitive
    if "Tier 3" in tier:
        return "#3b82f6"   # blue   — developing
    if "Tier 4" in tier:
        return "#f59e0b"   # amber  — headwinds
    return "#ef4444"       # red    — high risk


def _fmt(val, decimals=2, suffix=""):
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return "—"
    return f"{val:,.{decimals}f}{suffix}"


def _score_bar(score: float, color: str, width_px: int = 120) -> str:
    # Scale against 200 so USA (100) sits at 50%; scores > 100 extend further.
    pct = max(0, min(200, score))
    filled = round(pct / 200 * width_px)
    return (
        f'<div class="score-bar-wrap" style="width:{width_px}px">'
        f'<div class="score-bar-fill" style="width:{filled}px;background:{color}"></div>'
        f'</div>'
    )


def _cat_mini_bars(score_row: pd.Series, categories: dict) -> str:
    parts = []
    for cat_key, cat_cfg in categories.items():
        contrib_key = f"contrib_{cat_key}"
        contrib = score_row.get(contrib_key, 0.0) or 0.0
        color = _CAT_COLORS.get(cat_key, "#6b7280")
        label = _CAT_LABELS.get(cat_key, cat_key)
        bar_w = round(contrib / 60 * 60)   # 60pts = full bar width
        bar_w = max(0, min(60, bar_w))
        parts.append(
            f'<div class="mini-bar-row" title="{label}: {contrib:.1f}pts">'
            f'<div class="mini-bar-fill" style="width:{bar_w}px;background:{color}"></div>'
            f'<span class="mini-bar-label">{contrib:.1f}</span>'
            f'</div>'
        )
    return "".join(parts)


# ---------------------------------------------------------------------------
# Detail panel per country
# ---------------------------------------------------------------------------

def _detail_panel(
    country: str,
    score_row: pd.Series,
    full_row: pd.Series,
    norm_row: pd.Series,
    weights: dict,
    audit: dict,
    commentary: dict,
    categories: dict,
    base_weights: dict,
) -> str:
    country_audit = audit.get(country, {})
    comm = commentary.get(country, {})

    # Build variable rows grouped by category
    cat_blocks = []
    for cat_key, cat_cfg in categories.items():
        vars_in_cat = cat_cfg if isinstance(cat_cfg, list) else cat_cfg.get("variables", [])
        cat_label = _CAT_LABELS.get(cat_key, cat_key)
        cat_color = _CAT_COLORS.get(cat_key, "#6b7280")

        rows_html = []
        for var in vars_in_cat:
            label = _VAR_LABELS.get(var, var)
            raw_val = full_row.get(var, np.nan)
            norm_val = norm_row.get(var, np.nan)
            w = weights.get(var, 0.0)
            base_w = base_weights.get(var, 0.0)
            src = country_audit.get(var, "missing")
            badge_html, _ = _SOURCE_BADGES.get(src, ("", ""))
            contrib = (norm_val * w * 100) if pd.notna(norm_val) else 0.0

            # Weight adjustment indicator
            if abs(w - base_w) > 1e-6:
                w_str = f'<span class="weight-adj">{w*100:.1f}%</span> <span class="weight-base">(base {base_w*100:.1f}%)</span>'
            elif w == 0.0:
                w_str = '<span class="weight-zero">0% (zeroed)</span>'
            else:
                w_str = f'{w*100:.1f}%'

            raw_str = _fmt(raw_val, 2) if pd.notna(raw_val) else "—"
            norm_str = f"{norm_val:.3f}" if pd.notna(norm_val) else "—"
            contrib_str = f"{contrib:.2f}pts" if pd.notna(norm_val) and w > 0 else "—"

            rows_html.append(
                f'<tr>'
                f'<td class="var-label">{html.escape(label)}</td>'
                f'<td>{badge_html}</td>'
                f'<td class="num">{raw_str}</td>'
                f'<td class="num">{norm_str}</td>'
                f'<td class="num">{w_str}</td>'
                f'<td class="num contrib">{contrib_str}</td>'
                f'</tr>'
            )

        cat_contrib = score_row.get(f"contrib_{cat_key}", 0.0) or 0.0
        cat_blocks.append(
            f'<div class="cat-block">'
            f'<div class="cat-block-header" style="border-left:4px solid {cat_color}">'
            f'<span class="cat-block-label">{html.escape(cat_label)}</span>'
            f'<span class="cat-block-contrib">{cat_contrib:.1f} pts</span>'
            f'</div>'
            f'<table class="var-table"><thead><tr>'
            f'<th>Variable</th><th>Source</th><th>Raw Value</th>'
            f'<th>USA-Norm (×100)</th><th>Weight</th><th>Contribution</th>'
            f'</tr></thead><tbody>'
            + "".join(rows_html)
            + '</tbody></table></div>'
        )

    commentary_html = (
        f'<div class="commentary">'
        f'<p class="comm-drivers">📊 {html.escape(comm.get("drivers", ""))}</p>'
        f'<p class="comm-risks">⚠️ {html.escape(comm.get("risks", ""))}</p>'
        f'<p class="comm-notes">🗒️ {html.escape(comm.get("data_notes", ""))}</p>'
        f'</div>'
    )

    # Global Rank Banner — transparent, all text #9600fa, rank and score same class
    rank_val = score_row.get("rank", "—")
    score_val = score_row.get("composite_score", 0.0)
    total_countries = score_row.get("_total", "")
    tier_val = score_row.get("tier", "")
    rank_banner_html = (
        f'<div class="rank-banner">'
        f'<span class="rank-banner-text">Global Rank: </span>'
        f'<span class="rank-banner-emphasis">{rank_val}</span>'
        f'<span class="rank-banner-text"> &nbsp;|&nbsp; Score: </span>'
        f'<span class="rank-banner-emphasis">{score_val:.1f}</span>'
        f'<span class="rank-banner-text"> &nbsp;|&nbsp; {html.escape(str(tier_val))}</span>'
        f'</div>'
    )

    return (
        f'<tr class="detail-row" id="detail-{html.escape(country)}" style="display:none">'
        f'<td colspan="10">'
        f'<div class="detail-panel">'
        + rank_banner_html
        + commentary_html
        + "".join(cat_blocks)
        + '</div></td></tr>'
    )


# ---------------------------------------------------------------------------
# Main table rows
# ---------------------------------------------------------------------------

def _main_row(
    score_row: pd.Series,
    full_row: pd.Series,
    norm_row: pd.Series,
    weights: dict,
    audit: dict,
    commentary: dict,
    categories: dict,
    base_weights: dict,
) -> str:
    country = score_row["country"]
    score = score_row["composite_score"]
    tier = score_row["tier"]
    rank = score_row["rank"]
    tc = _tier_color(tier)

    # Data completeness: % of scored variables with actual data
    all_vars = list(base_weights.keys())
    country_audit = audit.get(country, {})
    missing_count = sum(1 for v in all_vars if country_audit.get(v) == "missing")
    completeness = round((len(all_vars) - missing_count) / len(all_vars) * 100)
    comp_color = "#22c55e" if completeness >= 90 else "#f59e0b" if completeness >= 70 else "#ef4444"

    row_html = (
        f'<tr class="main-row" onclick="toggleDetail(\'{html.escape(country)}\')">'
        f'<td class="rank-cell">#{rank}</td>'
        f'<td class="country-cell"><strong>{html.escape(country)}</strong></td>'
        f'<td class="score-cell">'
        f'<span class="score-num">{score:.1f}</span>'
        + _score_bar(score, tc)
        + '</td>'
        f'<td><span class="tier-badge" style="background:{tc}">{html.escape(tier)}</span></td>'
        f'<td class="cat-bars">' + _cat_mini_bars(score_row, categories) + '</td>'
        f'<td><span class="completeness" style="color:{comp_color}">{completeness}%</span></td>'
        f'<td class="expand-cell">▼</td>'
        f'</tr>'
    )

    detail_html = _detail_panel(
        country, score_row, full_row, norm_row,
        weights, audit, commentary, categories, base_weights
    )

    return row_html + detail_html


# ---------------------------------------------------------------------------
# CSS
# ---------------------------------------------------------------------------

_CSS = """
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
       background: #f8fafc; color: #000000; font-size: 14px; }
a { color: #3b82f6; }

/* Header */
.header { background: #290241; color: #FAEEFF; padding: 24px 32px; }
.header h1 { font-size: 22px; font-weight: 700; letter-spacing: -0.5px; color: #FAEEFF; }
.header .subtitle { font-size: 12px; color: #d6b4f5; margin-top: 4px; }

/* Summary cards */
.summary-bar { display: flex; gap: 16px; padding: 20px 32px; flex-wrap: wrap; }
.stat-card { background: #fff; border: 1px solid #e2e8f0; border-radius: 10px;
             padding: 16px 20px; flex: 1; min-width: 160px; }
.stat-card .stat-val { font-size: 28px; font-weight: 700; color: #0f172a; }
.stat-card .stat-lbl { font-size: 11px; color: #64748b; margin-top: 2px; text-transform: uppercase;
                        letter-spacing: 0.5px; }

/* Table container */
.table-wrap { padding: 0 32px 40px; overflow-x: auto; }
table.rankings { width: 100%; border-collapse: collapse; background: #fff;
                 border: 1px solid #e2e8f0; border-radius: 10px; overflow: hidden; }
table.rankings th { background: #9600fa; font-size: 11px; text-transform: uppercase;
                    letter-spacing: 0.5px; color: #FAEEFF; padding: 10px 14px;
                    text-align: left; border-bottom: 2px solid #7a00cc; cursor: pointer; }
table.rankings th:hover { background: #7a00cc; }
tr.main-row { cursor: pointer; transition: background 0.15s; }
tr.main-row:hover { background: #f8fafc; }
tr.main-row td { padding: 10px 14px; border-bottom: 1px solid #f1f5f9; vertical-align: middle; }
.rank-cell { font-weight: 700; color: #9600fa; width: 48px; }
.country-cell { min-width: 130px; }
.country-cell strong { color: #290241; }
.score-cell { white-space: nowrap; }
.score-num { font-size: 20px; font-weight: 700; margin-right: 8px; color: #9600fa; }
.score-bar-wrap { display: inline-block; height: 8px; background: #e2e8f0;
                  border-radius: 4px; vertical-align: middle; }
.score-bar-fill { height: 8px; border-radius: 4px; }
.tier-badge { display: inline-block; padding: 3px 10px; border-radius: 20px;
              color: #fff; font-size: 11px; font-weight: 600; white-space: nowrap; }
.cat-bars { min-width: 90px; }
.mini-bar-row { display: flex; align-items: center; gap: 4px; margin-bottom: 2px; }
.mini-bar-fill { height: 6px; border-radius: 3px; min-width: 2px; }
.mini-bar-label { font-size: 10px; color: #64748b; min-width: 26px; }
.completeness { font-size: 12px; font-weight: 600; }
.expand-cell { color: #94a3b8; font-size: 12px; width: 24px; text-align: center; }

/* Detail panel */
tr.detail-row td { padding: 0; border-bottom: 2px solid #e2e8f0; background: #f8fafc; }
.detail-panel { padding: 20px 28px; }

/* Global Rank Banner — transparent, all text #9600fa */
.rank-banner { background: transparent; padding: 8px 0 14px; border: none; }
.rank-banner-text { color: #9600fa; font-size: 14px; }
.rank-banner-emphasis { color: #9600fa; font-weight: 700; font-size: 16px; }
.commentary { background: #eff6ff; border-left: 4px solid #3b82f6;
              border-radius: 4px; padding: 12px 16px; margin-bottom: 16px; }
.commentary p { margin-bottom: 4px; font-size: 13px; line-height: 1.5; }
.comm-drivers { color: #1e40af; }
.comm-risks   { color: #92400e; }
.comm-notes   { color: #374151; }
.cat-block { margin-bottom: 16px; }
.cat-block-header { display: flex; justify-content: space-between; align-items: center;
                    padding: 6px 12px; background: #fff; margin-bottom: 4px;
                    border-radius: 4px; border: 1px solid #e2e8f0; }
.cat-block-label { font-weight: 600; font-size: 13px; color: #290241; }
.cat-block-contrib { font-size: 12px; color: #290241; font-weight: 600; }
table.var-table { width: 100%; border-collapse: collapse; font-size: 12px; }
table.var-table th { background: #9600fa; padding: 6px 10px; text-align: left;
                     font-size: 10px; text-transform: uppercase; color: #FAEEFF;
                     letter-spacing: 0.4px; }
table.var-table td { padding: 6px 10px; border-bottom: 1px solid #f1f5f9; }
table.var-table td.num { text-align: right; font-family: monospace; }
table.var-table td.contrib { font-weight: 600; color: #0f172a; }
.var-label { color: #000000; }
.weight-adj { color: #d97706; font-weight: 600; }
.weight-base { color: #94a3b8; font-size: 10px; }
.weight-zero { color: #ef4444; font-size: 11px; }

/* Badges — dark text for readability */
.badge { display: inline-block; padding: 1px 7px; border-radius: 10px;
         font-size: 10px; font-weight: 600; }
.badge-csv    { background: transparent; color: #1d4ed8; border: 1px solid #1d4ed8; }
.badge-api    { background: transparent; color: #065f46; border: 1px solid #065f46; }
.badge-manual { background: transparent; color: #92400e; border: 1px solid #92400e; }
.badge-missing{ background: transparent; color: #991b1b; border: 1px solid #991b1b; }

/* Legend */
.legend { display: flex; gap: 16px; padding: 0 32px 12px; flex-wrap: wrap; }
.legend-item { display: flex; align-items: center; gap: 6px; font-size: 12px; color: #64748b; }
.legend-dot { width: 12px; height: 12px; border-radius: 50%; }

/* Footer */
.footer { background: #290241; color: #d6b4f5; padding: 20px 32px; font-size: 11px; line-height: 1.8; }
.footer strong { color: #FAEEFF; }

/* Sort arrow */
th.sort-asc::after { content: " ↑"; }
th.sort-desc::after { content: " ↓"; }
"""

_JS = """
function toggleDetail(country) {
    var row = document.getElementById('detail-' + country);
    if (!row) return;
    row.style.display = (row.style.display === 'none' || row.style.display === '') ? 'table-row' : 'none';
}

// Column sorting
var sortState = {col: null, asc: false};
function sortTable(colIdx) {
    var table = document.getElementById('rankTable');
    var tbody = table.querySelector('tbody');
    var allRows = Array.from(tbody.querySelectorAll('tr'));
    // pair main-row with its detail-row
    var pairs = [];
    for (var i = 0; i < allRows.length; i++) {
        if (allRows[i].classList.contains('main-row')) {
            var detail = allRows[i + 1] && allRows[i + 1].classList.contains('detail-row')
                         ? allRows[i + 1] : null;
            pairs.push({main: allRows[i], detail: detail});
        }
    }
    var asc = (sortState.col === colIdx) ? !sortState.asc : true;
    sortState = {col: colIdx, asc: asc};

    pairs.sort(function(a, b) {
        var av = a.main.children[colIdx] ? a.main.children[colIdx].innerText.trim() : '';
        var bv = b.main.children[colIdx] ? b.main.children[colIdx].innerText.trim() : '';
        var an = parseFloat(av.replace(/[^0-9.-]/g, ''));
        var bn = parseFloat(bv.replace(/[^0-9.-]/g, ''));
        if (!isNaN(an) && !isNaN(bn)) return asc ? an - bn : bn - an;
        return asc ? av.localeCompare(bv) : bv.localeCompare(av);
    });

    // Update sort indicators
    document.querySelectorAll('#rankTable th').forEach(function(th, i) {
        th.classList.remove('sort-asc', 'sort-desc');
        if (i === colIdx) th.classList.add(asc ? 'sort-asc' : 'sort-desc');
    });

    pairs.forEach(function(pair) {
        tbody.appendChild(pair.main);
        if (pair.detail) tbody.appendChild(pair.detail);
    });
}
"""


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def generate_dashboard(
    scores_df: pd.DataFrame,
    full_df: pd.DataFrame,
    normalized_df: pd.DataFrame,
    weight_matrix: dict,
    audit: dict,
    commentary: dict,
    categories: dict,
    base_weights: dict,
    tier_colors: dict,
    output_dir: str,
    filename: str,
) -> str:
    """
    Write a self-contained HTML file and return its path.

    Parameters
    ----------
    scores_df     : output of scorer.compute_scores
    full_df       : merged data DataFrame (raw values)
    normalized_df : output of normalizer.normalize_all
    weight_matrix : {country: {var: weight}}
    audit         : {country: {var: source_label}}
    commentary    : {country: {drivers, risks, data_notes}}
    categories    : config.VARIABLE_CATEGORIES
    base_weights  : config.WEIGHTS
    tier_colors   : config.TIER_COLORS
    output_dir    : e.g. "output"
    filename      : e.g. "hvlp_market_ranking.html"
    """
    out_path = Path(output_dir)
    out_path.mkdir(exist_ok=True)
    filepath = out_path / filename

    # Build country → full_df row mapping
    country_to_full_idx = {
        row["country"]: i for i, row in full_df.iterrows()
    }

    # ---- Summary stats ------------------------------------------------
    n_countries = len(scores_df)
    top_country = scores_df.iloc[0]["country"] if n_countries > 0 else "—"
    avg_score = scores_df["composite_score"].mean()
    n_tier1 = (scores_df["tier"].str.contains("1")).sum()
    run_date = date.today().strftime("%B %d, %Y")

    summary_html = f"""
    <div class="summary-bar">
      <div class="stat-card">
        <div class="stat-val">{n_countries}</div>
        <div class="stat-lbl">Countries Analysed</div>
      </div>
      <div class="stat-card">
        <div class="stat-val">{html.escape(top_country)}</div>
        <div class="stat-lbl">Top-Ranked Market</div>
      </div>
      <div class="stat-card">
        <div class="stat-val">{n_tier1}</div>
        <div class="stat-lbl">Tier 1 Markets</div>
      </div>
      <div class="stat-card">
        <div class="stat-val">{avg_score:.1f}</div>
        <div class="stat-lbl">Avg Composite Score</div>
      </div>
    </div>
    """

    # ---- Legend -------------------------------------------------------
    legend_html = '<div class="legend">'
    for t, color in tier_colors.items():
        label_key = int(t) if isinstance(t, int) else t
        legend_html += (
            f'<div class="legend-item">'
            f'<div class="legend-dot" style="background:{color}"></div>'
            f'<span>Tier {t}</span>'
            f'</div>'
        )
    legend_html += "</div>"

    # ---- Table rows ---------------------------------------------------
    rows_html = ""
    for _, score_row in scores_df.iterrows():
        country = score_row["country"]
        idx = country_to_full_idx.get(country)
        if idx is None:
            continue
        full_row = full_df.iloc[idx]
        norm_row = normalized_df.iloc[idx]
        weights = weight_matrix.get(country, {})

        rows_html += _main_row(
            score_row, full_row, norm_row, weights,
            audit, commentary, categories, base_weights
        )

    # ---- Category bar legend for mini-bars ----------------------------
    cat_legend = "".join(
        f'<div class="legend-item">'
        f'<div class="legend-dot" style="background:{_CAT_COLORS.get(k,"#6b7280")}"></div>'
        f'<span>{_CAT_LABELS.get(k, k)}</span>'
        f'</div>'
        for k in categories
    )

    # ---- Full HTML ----------------------------------------------------
    page = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>HVLP Global Gym Market Opportunity Rankings</title>
<style>{_CSS}</style>
</head>
<body>

<div class="header">
  <h1>HVLP Global Gym Market Opportunity Model</h1>
  <div class="subtitle">
    Run date: {run_date} &nbsp;|&nbsp;
    Model: 17-variable weighted composite &nbsp;|&nbsp;
    Countries: {n_countries} &nbsp;|&nbsp;
    Scoring model: USA benchmark = 100
  </div>
</div>

{summary_html}

<div class="legend" style="padding:0 32px 4px">
  <span style="font-size:11px;color:#64748b;font-weight:600;text-transform:uppercase;
               letter-spacing:0.5px;margin-right:8px">Score bars by category:</span>
  {cat_legend}
</div>

<div class="table-wrap">
<table class="rankings" id="rankTable">
<thead>
<tr>
  <th onclick="sortTable(0)">Rank</th>
  <th onclick="sortTable(1)">Country</th>
  <th onclick="sortTable(2)">Score</th>
  <th onclick="sortTable(3)">Tier</th>
  <th>Category Breakdown</th>
  <th onclick="sortTable(5)">Data%</th>
  <th></th>
</tr>
</thead>
<tbody>
{rows_html}
</tbody>
</table>
</div>

<div class="footer">
  <strong>Methodology</strong><br>
  17 variables across 5 categories: Market Opportunity (35%), Penetration Headroom (10%),
  Operational Risk (25%), Cost Structure (10%), Demand Indicators (15%).<br>
  All variables normalised against USA benchmark (USA = 100). Scores > 100 indicate outperformance vs the USA; scores < 100 indicate underperformance.
  Inverted variables: Inflation Rate, Currency Volatility, Corporate Tax Rate,
  Labour Cost Index, Real Estate Cost Index.<br>
  Conditional rules: Rule 1 (CAGR missing → Opportunity 25%, Potential 15%);
  Rule 2 (Concentration missing → Penetration Headroom 10%);
  Rule 3 (other missing → proportional redistribution within category).<br>
  Financing Accessibility: World Bank GFDD composite (3 indicators, min-max normalised
  per-component, then averaged).<br>
  <strong>Data sources:</strong> World Bank WDI, World Bank WGI, World Bank GFDD,
  OECD Tax Database 2024, ILO/OECD Labour Cost estimates, Numbeo Property Index 2024,
  Pew Research / World Bank PovcalNet (middle class estimates).<br>
  ⚠ Ease of Doing Business (IC.BUS.EASE.XQ) was discontinued by the World Bank in 2021;
  last available data is 2019/2020.  Update via manual override when B-READY data is published.
</div>

<script>{_JS}</script>
</body>
</html>
"""

    filepath.write_text(page, encoding="utf-8")
    return str(filepath)
