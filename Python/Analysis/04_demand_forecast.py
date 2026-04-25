"""
04_demand_forecast.py
NestKraft — Demand Trend Analysis + Seasonality Intelligence
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec
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
print("   NESTKRAFT — DEMAND FORECAST & SEASONALITY")
print("="*65)

# ── Load data ─────────────────────────────────────────────────
demand  = pd.read_sql("SELECT * FROM vw_demand_intelligence", engine)
seasonal= pd.read_sql("SELECT * FROM vw_seasonal_intelligence", engine)
orders  = pd.read_sql("""
    SELECT
        fiscal_year, month_num, month_name, quarter,
        SUM(total_fob_value_usd)  AS revenue_usd,
        COUNT(order_id)           AS total_orders,
        SUM(quantity_units)       AS total_units,
        AVG(gross_margin_pct)     AS avg_margin
    FROM fact_export_orders
    WHERE order_status != 'Cancelled'
    GROUP BY fiscal_year, month_num, month_name, quarter
    ORDER BY fiscal_year, month_num
""", engine)

# ── Monthly revenue trend ─────────────────────────────────────
print("\n📊 MONTHLY REVENUE TREND\n")
print(f"{'Year':<6} {'Month':<12} {'Revenue USD':>14} "
      f"{'Orders':>8} {'Margin%':>9} {'Season'}")
print("-"*60)
for _, row in orders.iterrows():
    month_n = int(row["month_num"])
    season = ("🎄 Peak"    if month_n in [11,12,1]  else
              "📦 Pre-Peak" if month_n in [9,10]     else
              "☀️ Slow"     if month_n in [6,7,8]    else
              "➡️  Normal")
    print(f"{int(row['fiscal_year']):<6} "
          f"{row['month_name']:<12} "
          f"${row['revenue_usd']:>13,.0f} "
          f"{int(row['total_orders']):>8,} "
          f"{row['avg_margin']:>8.1f}% "
          f"  {season}")

# ── Top growing markets ───────────────────────────────────────
print("\n\n🚀 TOP 10 FASTEST GROWING MARKETS (YoY Search Growth)\n")
top_growth = (demand.groupby(["country","region"])
              .agg(avg_yoy_growth = ("avg_yoy_growth_pct","mean"),
                   avg_trends     = ("avg_trends_score",  "mean"),
                   total_demand   = ("total_demand_units","sum"),
                   total_search   = ("total_search_volume","sum"))
              .reset_index()
              .sort_values("avg_yoy_growth", ascending=False)
              .head(10))
for i, (_, row) in enumerate(top_growth.iterrows(), 1):
    print(f"  {i:>2}. {row['country']:<16} | "
          f"YoY Growth: {row['avg_yoy_growth']:>6.1f}% | "
          f"Trends Score: {row['avg_trends']:>5.1f} | "
          f"Demand Units: {row['total_demand']:>8,}")

# ── Seasonality peaks ─────────────────────────────────────────
print("\n\n📅 SEASONAL PEAK ANALYSIS BY MARKET (Top 5 Markets)\n")
top5_mkts = (seasonal.groupby("country")["total_revenue_usd"]
             .sum().nlargest(5).index.tolist())
for mkt in top5_mkts:
    mkt_data = (seasonal[seasonal["country"]==mkt]
                .groupby(["month_num","month_name"])
                ["total_revenue_usd"].sum()
                .reset_index()
                .sort_values("total_revenue_usd", ascending=False))
    top3_months = mkt_data.head(3)["month_name"].tolist()
    bottom3     = mkt_data.tail(3)["month_name"].tolist()
    print(f"   {mkt:<16} "
          f"Peak: {', '.join(top3_months):<30} "
          f"Slow: {', '.join(bottom3)}")

# ── Category demand growth ────────────────────────────────────
print("\n\n📦 DEMAND GROWTH BY CATEGORY\n")
cat_demand = (demand.groupby("category")
              .agg(avg_yoy_growth = ("avg_yoy_growth_pct","mean"),
                   avg_trends     = ("avg_trends_score",  "mean"),
                   total_demand   = ("total_demand_units","sum"))
              .reset_index()
              .sort_values("avg_yoy_growth", ascending=False))
for _, row in cat_demand.iterrows():
    bar_len = int(row["avg_yoy_growth"] / 2)
    bar     = "█" * min(bar_len, 30)
    print(f"   {row['category']:<14} {bar:<32} "
          f"{row['avg_yoy_growth']:.1f}% YoY")

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
fig.suptitle("NestKraft — Demand Forecast & Seasonality Intelligence",
             fontsize=18, fontweight='bold', color='white', y=0.98)
gs = GridSpec(2, 3, figure=fig, hspace=0.42, wspace=0.35)
ax1 = fig.add_subplot(gs[0, :])
ax2 = fig.add_subplot(gs[1, 0])
ax3 = fig.add_subplot(gs[1, 1])
ax4 = fig.add_subplot(gs[1, 2])

# Chart 1: Revenue trend line — all 3 years
YEAR_COLORS = {2022:"#40C4FF", 2023:"#FFD740", 2024:"#00E676"}
for yr in sorted(orders["fiscal_year"].unique()):
    yr_data = orders[orders["fiscal_year"]==yr].sort_values("month_num")
    ax1.plot(yr_data["month_num"],
             yr_data["revenue_usd"],
             marker='o', linewidth=2.2,
             color=YEAR_COLORS.get(int(yr),"#888"),
             label=str(int(yr)), markersize=5)
    # Shade peak months
    for _, row in yr_data.iterrows():
        if row["month_num"] in [11, 12, 1]:
            ax1.axvspan(row["month_num"]-0.4,
                        row["month_num"]+0.4,
                        alpha=0.08, color='#00E676')

ax1.set_xticks(range(1, 13))
ax1.set_xticklabels(["Jan","Feb","Mar","Apr","May","Jun",
                     "Jul","Aug","Sep","Oct","Nov","Dec"])
ax1.set_ylabel("Monthly Revenue (USD)")
ax1.set_title("Monthly Revenue Trend 2022-2024 "
              "(Green shading = Peak Season)",
              fontsize=13, fontweight='bold', pad=10)
ax1.legend(fontsize=10, framealpha=0.3)
ax1.grid(alpha=0.3)

# Chart 2: Seasonality index heatmap
monthly_avg = (orders.groupby("month_num")["revenue_usd"]
               .mean().reset_index())
monthly_avg["norm"] = (monthly_avg["revenue_usd"] /
                       monthly_avg["revenue_usd"].mean())
month_names = ["Jan","Feb","Mar","Apr","May","Jun",
               "Jul","Aug","Sep","Oct","Nov","Dec"]
colors2 = ["#00E676" if x>=1.1 else
           "#FFD740" if x>=0.95 else
           "#FF5252" for x in monthly_avg["norm"]]
bars2 = ax2.bar(month_names, monthly_avg["norm"],
                color=colors2, edgecolor='none')
ax2.axhline(1.0, color='white', linestyle='--',
            alpha=0.4, label='Average (1.0)')
ax2.set_title("Seasonality Index\n(>1 = Above Average)",
              fontsize=12, fontweight='bold', pad=10)
ax2.set_ylabel("Seasonality Index")
ax2.tick_params(axis='x', rotation=45)
ax2.legend(fontsize=8, framealpha=0.3)
ax2.grid(axis='y', alpha=0.3)
for bar, val in zip(bars2, monthly_avg["norm"]):
    ax2.text(bar.get_x()+bar.get_width()/2,
             val+0.01, f'{val:.2f}',
             ha='center', fontsize=7.5,
             color='white', fontweight='bold')

# Chart 3: Top 10 growing markets
top10g = (demand.groupby("country")
          .agg(avg_yoy=("avg_yoy_growth_pct","mean"))
          .reset_index()
          .sort_values("avg_yoy", ascending=True)
          .tail(10))
growth_colors = ["#00E676" if x>20 else
                 "#FFD740" if x>10 else
                 "#FF9100" for x in top10g["avg_yoy"]]
ax3.barh(top10g["country"], top10g["avg_yoy"],
         color=growth_colors, edgecolor='none', height=0.7)
ax3.axvline(0, color='white', alpha=0.3)
ax3.set_title("YoY Demand Growth\nby Market (%)",
              fontsize=12, fontweight='bold', pad=10)
ax3.set_xlabel("YoY Search Growth %")
ax3.grid(axis='x', alpha=0.3)
for i, (_, row) in enumerate(top10g.iterrows()):
    ax3.text(row.avg_yoy+0.3, i,
             f"{row.avg_yoy:.1f}%",
             va='center', fontsize=8, color='white')

# Chart 4: Category demand growth bar
cat_d = (demand.groupby("category")
         .agg(avg_yoy=("avg_yoy_growth_pct","mean"),
              total_d=("total_demand_units","sum"))
         .reset_index()
         .sort_values("avg_yoy", ascending=False))
cat_colors4 = ["#00E676","#40C4FF","#FFD740",
                "#FF9100","#EA80FC","#FF5252","#69F0AE"][:len(cat_d)]
ax4.bar(cat_d["category"], cat_d["avg_yoy"],
        color=cat_colors4, edgecolor='none', alpha=0.88)
ax4.set_title("Demand Growth\nby Category (YoY %)",
              fontsize=12, fontweight='bold', pad=10)
ax4.set_ylabel("YoY Growth %")
ax4.tick_params(axis='x', rotation=30)
ax4.grid(axis='y', alpha=0.3)
ax4.axhline(0, color='white', alpha=0.3)
for i, (_, row) in enumerate(cat_d.iterrows()):
    ax4.text(i, row.avg_yoy+0.3,
             f"{row.avg_yoy:.1f}%",
             ha='center', fontsize=8,
             color='white', fontweight='bold')

plt.tight_layout(rect=[0, 0, 1, 0.96])
path = os.path.join(OUT_PATH, "04_demand_forecast.png")
plt.savefig(path, dpi=150, bbox_inches='tight', facecolor=DARK)
plt.show()
print(f"\n✅ Chart saved: {path}")

orders.to_csv(os.path.join(OUT_PATH, "monthly_revenue_trend.csv"), index=False)
demand.to_csv(os.path.join(OUT_PATH, "demand_intelligence.csv"), index=False)
print("✅ monthly_revenue_trend.csv saved")
print("✅ demand_intelligence.csv saved")
print("\n" + "="*65 + "\n")