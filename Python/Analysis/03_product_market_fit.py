"""
03_product_market_fit.py
NestKraft — Product Export Scorecard + Market Fit Analysis
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec
import seaborn as sns
from sqlalchemy import create_engine
import os, sys, warnings
warnings.filterwarnings('ignore')

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              '..', 'Data Generation'))
from config import CONNECTION_STRING

engine   = create_engine(CONNECTION_STRING)
OUT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        '..', '..', 'Outputs')
os.makedirs(OUT_PATH, exist_ok=True)

print("\n" + "="*65)
print("   NESTKRAFT — PRODUCT EXPORT SCORECARD")
print("="*65)

df   = pd.read_sql("SELECT * FROM vw_product_export_scorecard", engine)
cat  = pd.read_sql("SELECT * FROM vw_category_mix", engine)
comp = pd.read_sql("SELECT * FROM vw_competitor_benchmark", engine)

# ── Product ranking ───────────────────────────────────────────
df_sorted = df.sort_values("total_revenue_usd",
                            ascending=False).reset_index(drop=True)
df_sorted.index += 1

print("\n📊 TOP 15 PRODUCTS BY EXPORT REVENUE\n")
print(f"{'#':<4} {'Product':<38} {'Cat':<12} {'Rev $':<12} "
      f"{'Margin%':<10} {'Markets':<9} {'Tag'}")
print("-"*100)
for idx, row in df_sorted.head(15).iterrows():
    print(f"{idx:<4} {str(row['product_name'])[:37]:<38} "
          f"{row['category']:<12} "
          f"${row['total_revenue_usd']:>10,.0f} "
          f"{row['avg_margin_pct']:>9.1f}% "
          f"{row['markets_present']:>8} "
          f"  {row['export_viability_tag']}")

# ── Category mix ──────────────────────────────────────────────
print("\n\n📦 CATEGORY REVENUE MIX\n")
print(f"{'Category':<14} {'Revenue $':<14} {'Share%':<9} "
      f"{'Margin%':<10} {'Products':<10} {'Markets'}")
print("-"*65)
for _, row in cat.iterrows():
    print(f"{row['category']:<14} "
          f"${row['total_revenue_usd']:>12,.0f} "
          f"{row['revenue_share_pct']:>8.1f}% "
          f"{row['avg_margin_pct']:>9.1f}% "
          f"{row['product_count']:>9} "
          f"{row['markets_served']:>7}")

# ── Star products ─────────────────────────────────────────────
stars = df_sorted[df_sorted["export_viability_tag"].str.contains("Star", na=False)]
growth = df_sorted[df_sorted["export_viability_tag"].str.contains("Growth", na=False)]
review = df_sorted[df_sorted["export_viability_tag"].str.contains("Review", na=False)]

print(f"\n\n⭐ STAR PRODUCTS ({len(stars)} products):")
for _, r in stars.iterrows():
    print(f"   • {r['product_name'][:45]:<47} "
          f"Rev: ${r['total_revenue_usd']:>10,.0f} | "
          f"Margin: {r['avg_margin_pct']:.1f}% | "
          f"Markets: {r['markets_present']}")

print(f"\n📈 GROWTH PRODUCTS ({len(growth)} products):")
for _, r in growth.head(5).iterrows():
    print(f"   • {r['product_name'][:45]:<47} "
          f"Rev: ${r['total_revenue_usd']:>10,.0f} | "
          f"Margin: {r['avg_margin_pct']:.1f}%")

print(f"\n⚠️  REVIEW PRODUCTS ({len(review)} products):")
for _, r in review.iterrows():
    print(f"   • {r['product_name'][:45]:<47} "
          f"Margin: {r['avg_margin_pct']:.1f}% | "
          f"Markets: {r['markets_present']}")

# ── Pricing benchmark insights ────────────────────────────────
overpriced   = comp[comp["pricing_recommendation"].str.contains("Overpriced",  na=False)]
underpriced  = comp[comp["pricing_recommendation"].str.contains("Underpriced", na=False)]
optimal      = comp[comp["pricing_recommendation"].str.contains("Optimally",   na=False)]

print(f"\n\n💰 PRICING INTELLIGENCE SUMMARY")
print(f"   Overpriced combinations  : {len(overpriced):,}")
print(f"   Underpriced combinations : {len(underpriced):,}")
print(f"   Optimally priced         : {len(optimal):,}")

if len(underpriced) > 0:
    print(f"\n   🔴 TOP 5 UNDERPRICED (leaving money on table):")
    for _, r in underpriced.nlargest(5,"price_gap_pct").iterrows():
        print(f"      • {r['country']:<14} | {r['product_name'][:30]:<32} | "
              f"Our: ${r['our_avg_price_usd']:.2f} | "
              f"Mkt: ${r['avg_market_price_usd']:.2f} | "
              f"Gap: {r['price_gap_pct']:.1f}%")

# ════════════════════════════════════════════════════════════════
# CHARTS
# ════════════════════════════════════════════════════════════════
DARK = '#0F1117'
plt.rcParams.update({
    'figure.facecolor': DARK, 'axes.facecolor': DARK,
    'axes.edgecolor':   '#2E2E3E', 'text.color': '#E0E0E0',
    'axes.labelcolor':  '#E0E0E0', 'xtick.color': '#B0B0B0',
    'ytick.color':      '#B0B0B0', 'grid.color':  '#1E1E2E',
})

fig = plt.figure(figsize=(22, 15))
fig.suptitle("NestKraft — Product Export Intelligence",
             fontsize=18, fontweight='bold', color='white', y=0.98)
gs = GridSpec(2, 3, figure=fig, hspace=0.42, wspace=0.35)
ax1 = fig.add_subplot(gs[0, :2])
ax2 = fig.add_subplot(gs[0, 2])
ax3 = fig.add_subplot(gs[1, 0])
ax4 = fig.add_subplot(gs[1, 1])
ax5 = fig.add_subplot(gs[1, 2])

# Chart 1: Top 15 products revenue bar
top15 = df_sorted.head(15)
TAG_COLORS = {
    "⭐ Star Export Product": "#00E676",
    "📈 Growth Product":      "#FFD740",
    "✅ Viable — Optimize":   "#40C4FF",
    "⚠️ Review — Low Margin": "#FF5252",
}
bar_colors = [TAG_COLORS.get(str(t), "#888")
              for t in top15["export_viability_tag"]]
bars = ax1.barh(top15["product_name"].str[:35],
                top15["total_revenue_usd"],
                color=bar_colors, edgecolor='none', height=0.7)
ax1.set_title("Top 15 Products by Export Revenue",
              fontsize=13, fontweight='bold', pad=10)
ax1.set_xlabel("Total Revenue (USD)")
ax1.invert_yaxis()
ax1.grid(axis='x', alpha=0.3)
for bar, val in zip(bars, top15["total_revenue_usd"]):
    ax1.text(val+100000, bar.get_y()+bar.get_height()/2,
             f'${val/1e6:.1f}M',
             va='center', fontsize=8, color='white')

# Chart 2: Margin vs revenue scatter (bubble = markets)
viability_colors = [TAG_COLORS.get(str(t), "#888")
                    for t in df_sorted["export_viability_tag"]]
sizes = (df_sorted["markets_present"] / df_sorted["markets_present"].max()) * 400 + 50
ax2.scatter(df_sorted["avg_margin_pct"],
            df_sorted["total_revenue_usd"],
            s=sizes, c=viability_colors,
            alpha=0.85, edgecolors='white', linewidth=0.4)
ax2.set_xlabel("Avg Gross Margin %")
ax2.set_ylabel("Total Revenue USD")
ax2.set_title("Margin vs Revenue\n(bubble = markets)",
              fontsize=12, fontweight='bold', pad=10)
ax2.grid(alpha=0.3)
ax2.axvline(50, color='#FFD740', linestyle='--', alpha=0.5)

# Chart 3: Category revenue pie
cat_colors_pie = ["#00E676","#40C4FF","#FFD740",
                   "#FF9100","#EA80FC","#FF5252","#69F0AE"]
wedges, texts, autotexts = ax3.pie(
    cat["total_revenue_usd"],
    labels=cat["category"],
    autopct='%1.1f%%',
    colors=cat_colors_pie[:len(cat)],
    startangle=90,
    pctdistance=0.82,
    textprops={'color':'white','fontsize':8})
for at in autotexts:
    at.set_fontsize(7.5)
    at.set_color('white')
ax3.set_title("Revenue Mix by Category",
              fontsize=12, fontweight='bold', pad=10)

# Chart 4: Price index distribution
ax4.hist(comp["avg_price_index"].dropna(),
         bins=30, color="#40C4FF", edgecolor='none', alpha=0.85)
ax4.axvline(85,  color='#FF5252',  linestyle='--',
            alpha=0.8, label='Underpriced (<85)')
ax4.axvline(110, color='#FFD740',  linestyle='--',
            alpha=0.8, label='Overpriced (>110)')
ax4.axvline(100, color='#00E676',  linestyle='-',
            alpha=0.6, label='Market Parity (100)')
ax4.set_xlabel("Our Price Index vs Competitors")
ax4.set_ylabel("Count")
ax4.set_title("Price Index Distribution\n(vs Competitor Average)",
              fontsize=12, fontweight='bold', pad=10)
ax4.legend(fontsize=8, framealpha=0.3)
ax4.grid(alpha=0.3)

# Chart 5: Category margin comparison
cat_sorted = cat.sort_values("avg_margin_pct", ascending=True)
colors5 = ["#00E676" if x>=55 else
           "#FFD740" if x>=45 else
           "#FF5252" for x in cat_sorted["avg_margin_pct"]]
ax5.barh(cat_sorted["category"], cat_sorted["avg_margin_pct"],
         color=colors5, edgecolor='none', height=0.6)
ax5.set_title("Avg Gross Margin\nby Category",
              fontsize=12, fontweight='bold', pad=10)
ax5.set_xlabel("Avg Gross Margin %")
ax5.axvline(55, color='#00E676', linestyle='--', alpha=0.5)
ax5.grid(axis='x', alpha=0.3)
for i, (_, row) in enumerate(cat_sorted.iterrows()):
    ax5.text(row.avg_margin_pct+0.3, i,
             f"{row.avg_margin_pct:.1f}%",
             va='center', fontsize=8.5, color='white')

plt.tight_layout(rect=[0, 0, 1, 0.96])
path = os.path.join(OUT_PATH, "03_product_market_fit.png")
plt.savefig(path, dpi=150, bbox_inches='tight', facecolor=DARK)
plt.show()
print(f"\n✅ Chart saved: {path}")

df_sorted.to_csv(os.path.join(OUT_PATH, "product_scorecard.csv"), index=False)
print("✅ product_scorecard.csv saved")
print("\n" + "="*65 + "\n")