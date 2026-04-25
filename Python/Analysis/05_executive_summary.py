"""
05_executive_summary.py
NestKraft — Executive Summary Intelligence Report
Combines all insights into one final dashboard
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec
import matplotlib.patches as mpatches
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
print("   NESTKRAFT — EXECUTIVE SUMMARY REPORT")
print("="*65)

# ── Load all key views ────────────────────────────────────────
summary  = pd.read_sql("SELECT * FROM vw_master_summary",       engine)
revenue  = pd.read_sql("SELECT * FROM vw_executive_revenue",    engine)
markets  = pd.read_sql("SELECT * FROM vw_market_entry_readiness", engine)
products = pd.read_sql("SELECT * FROM vw_product_export_scorecard", engine)
channels = pd.read_sql("SELECT * FROM vw_channel_performance",  engine)
payment  = pd.read_sql("SELECT * FROM vw_payment_risk",         engine)
returns  = pd.read_sql("SELECT * FROM vw_returns_analysis",     engine)
delay    = pd.read_sql("SELECT * FROM vw_shipment_delay_analysis", engine)

# ── Master KPIs ───────────────────────────────────────────────
s = summary.iloc[0]
print(f"""
╔══════════════════════════════════════════════════════════╗
║         NESTKRAFT — MASTER KPI DASHBOARD                ║
╠══════════════════════════════════════════════════════════╣
║  Total Revenue       : ${s['total_revenue_usd']:>15,.0f} USD      ║
║  Revenue (INR)       : ₹{s['total_revenue_cr_inr']:>12,.1f} Crore        ║
║  Avg Gross Margin    : {s['avg_gross_margin_pct']:>14.1f}%           ║
║  Total Orders        : {s['total_orders']:>15,}           ║
║  Total Units Exported: {s['total_units_exported']:>15,}           ║
║  Active Buyers       : {s['total_buyers']:>15,}           ║
║  Markets Active      : {s['markets_active']:>15,}           ║
║  Products Exported   : {s['products_exported']:>15,}           ║
╠══════════════════════════════════════════════════════════╣
║  Overdue Orders      : {s['overdue_orders']:>15,}           ║
║  Overdue Value       : ${s['overdue_value_usd']:>15,.0f} USD      ║
║  Total Shipments     : {s['total_shipments']:>15,}           ║
║  Avg Transit Days    : {s['avg_transit_days']:>14.1f} days         ║
║  Total Demurrage     : ${s['total_demurrage_usd']:>15,.0f} USD      ║
║  Total Returns       : {s['total_returns']:>15,}           ║
║  Return Loss         : ${s['total_return_loss_usd']:>15,.0f} USD      ║
╚══════════════════════════════════════════════════════════╝
""")

# ── YoY Revenue Growth ────────────────────────────────────────
print("📈 YEAR-OVER-YEAR REVENUE GROWTH\n")
yr_rev = (revenue.groupby("fiscal_year")
          .agg(revenue    = ("total_revenue_usd",    "sum"),
               orders     = ("total_orders",         "sum"),
               avg_margin = ("avg_gross_margin_pct", "mean"))
          .reset_index())
for i, row in yr_rev.iterrows():
    if i == 0:
        growth = "—"
    else:
        prev   = yr_rev.iloc[i-1]["revenue"]
        growth = f"+{(row['revenue']-prev)/prev*100:.1f}%"
    print(f"   {int(row['fiscal_year'])} | "
          f"Revenue: ${row['revenue']:>14,.0f} | "
          f"Orders: {int(row['orders']):>7,} | "
          f"Margin: {row['avg_margin']:.1f}% | "
          f"Growth: {growth}")

# ── Top 5 markets ─────────────────────────────────────────────
print("\n\n🌍 TOP 5 MARKETS BY REVENUE\n")
top5_mkts = (markets.sort_values("total_revenue_usd", ascending=False)
             .head(5)[["country","region","total_revenue_usd",
                        "avg_margin_pct","unique_buyers",
                        "delay_rate_pct","weighted_priority_score"]])
for i, (_, row) in enumerate(top5_mkts.iterrows(), 1):
    print(f"   {i}. {row['country']:<16} "
          f"Rev: ${row['total_revenue_usd']:>12,.0f} | "
          f"Margin: {row['avg_margin_pct']:.1f}% | "
          f"Buyers: {int(row['unique_buyers']):>3} | "
          f"Delay: {row['delay_rate_pct']:.1f}% | "
          f"Score: {row['weighted_priority_score']:.2f}")

# ── Top 5 products ────────────────────────────────────────────
print("\n\n⭐ TOP 5 PRODUCTS BY REVENUE\n")
top5_prods = (products.sort_values("total_revenue_usd", ascending=False)
              .head(5)[["product_name","category","total_revenue_usd",
                         "avg_margin_pct","markets_present",
                         "export_viability_tag"]])
for i, (_, row) in enumerate(top5_prods.iterrows(), 1):
    print(f"   {i}. {str(row['product_name'])[:38]:<40} "
          f"Rev: ${row['total_revenue_usd']:>12,.0f} | "
          f"Margin: {row['avg_margin_pct']:.1f}% | "
          f"Markets: {row['markets_present']}")

# ── Channel performance ───────────────────────────────────────
print("\n\n📡 CHANNEL PERFORMANCE SUMMARY\n")
ch_summary = (channels.groupby("order_channel")
              .agg(revenue       = ("total_revenue_usd",  "sum"),
                   avg_margin    = ("avg_margin_pct",     "mean"),
                   avg_aov       = ("avg_order_value_usd","mean"),
                   repeat_rate   = ("repeat_rate_pct",    "mean"),
                   cancel_rate   = ("cancellation_rate_pct","mean"))
              .reset_index()
              .sort_values("revenue", ascending=False))
print(f"{'Channel':<18} {'Revenue':>14} {'Margin%':>9} "
      f"{'AOV':>8} {'Repeat%':>9} {'Cancel%':>9}")
print("-"*70)
for _, row in ch_summary.iterrows():
    print(f"{row['order_channel']:<18} "
          f"${row['revenue']:>13,.0f} "
          f"{row['avg_margin']:>8.1f}% "
          f"${row['avg_aov']:>7.0f} "
          f"{row['repeat_rate']:>8.1f}% "
          f"{row['cancel_rate']:>8.1f}%")

# ── Payment risk ──────────────────────────────────────────────
print("\n\n⚠️  PAYMENT RISK SUMMARY\n")
high_risk = payment[payment["payment_risk_flag"].str.contains("High", na=False)]
print(f"   High risk markets     : {len(high_risk)}")
print(f"   Total overdue value   : "
      f"${payment['overdue_value_usd'].sum():,.0f} USD")
print(f"   Avg DSO across markets: "
      f"{payment['avg_dso_days'].mean():.1f} days")
if len(high_risk) > 0:
    print(f"\n   🔴 High Risk Markets:")
    for _, r in high_risk.iterrows():
        print(f"      • {r['country']:<16} "
              f"Overdue: ${r['overdue_value_usd']:>10,.0f} | "
              f"Avg Delay: {r['avg_delay_days']:.1f} days")

# ── Returns summary ───────────────────────────────────────────
print("\n\n📦 RETURNS INTELLIGENCE\n")
total_rev   = s["total_revenue_usd"]
total_loss  = returns["total_return_loss_usd"].sum()
print(f"   Total return loss     : ${total_loss:,.0f} USD")
print(f"   Loss as % of revenue  : {total_loss/total_rev*100:.2f}%")
top_ret_rsn = (returns.groupby("top_return_reason")
               ["total_returns"].sum()
               .sort_values(ascending=False).head(3))
print(f"\n   Top return reasons:")
for rsn, cnt in top_ret_rsn.items():
    print(f"      • {rsn:<35} {cnt:,} returns")

# ── Shipment delay summary ────────────────────────────────────
print("\n\n🚢 SHIPMENT DELAY INTELLIGENCE\n")
delay_mkt = (delay.groupby("country")
             .agg(total_ships   = ("total_shipments",    "sum"),
                  delayed_ships = ("delayed_shipments",  "sum"),
                  total_dem     = ("total_demurrage_usd","sum"))
             .reset_index())
delay_mkt["delay_pct"] = (delay_mkt["delayed_ships"] /
                           delay_mkt["total_ships"] * 100).round(2)
top3_delay = delay_mkt.nlargest(3, "delay_pct")
print(f"   Overall delay rate    : "
      f"{delay_mkt['delayed_ships'].sum()/delay_mkt['total_ships'].sum()*100:.1f}%")
print(f"   Total demurrage cost  : "
      f"${delay_mkt['total_dem'].sum():,.0f} USD")
print(f"\n   Most delayed markets:")
for _, r in top3_delay.iterrows():
    print(f"      • {r['country']:<16} "
          f"Delay Rate: {r['delay_pct']:.1f}% | "
          f"Demurrage: ${r['total_dem']:>8,.0f}")

# ════════════════════════════════════════════════════════════════
# EXECUTIVE DASHBOARD — 6 CHARTS
# ════════════════════════════════════════════════════════════════
DARK = '#0F1117'
plt.rcParams.update({
    'figure.facecolor': DARK, 'axes.facecolor': DARK,
    'axes.edgecolor':   '#2E2E3E', 'text.color': '#E0E0E0',
    'axes.labelcolor':  '#E0E0E0', 'xtick.color': '#B0B0B0',
    'ytick.color':      '#B0B0B0', 'grid.color':  '#1E1E2E',
})

fig = plt.figure(figsize=(24, 16))
fig.suptitle("NestKraft D2C — Executive Export Intelligence Dashboard",
             fontsize=20, fontweight='bold', color='white', y=0.98)
gs = GridSpec(2, 3, figure=fig, hspace=0.42, wspace=0.35)
ax1 = fig.add_subplot(gs[0, 0])
ax2 = fig.add_subplot(gs[0, 1])
ax3 = fig.add_subplot(gs[0, 2])
ax4 = fig.add_subplot(gs[1, 0])
ax5 = fig.add_subplot(gs[1, 1])
ax6 = fig.add_subplot(gs[1, 2])

# Chart 1: KPI scorecard as text boxes
ax1.axis('off')
kpis = [
    ("Total Revenue",    f"${s['total_revenue_usd']/1e9:.2f}B USD"),
    ("Gross Margin",     f"{s['avg_gross_margin_pct']:.1f}%"),
    ("Total Orders",     f"{s['total_orders']:,}"),
    ("Active Markets",   f"{s['markets_active']}"),
    ("Active Buyers",    f"{s['total_buyers']}"),
    ("Avg Transit",      f"{s['avg_transit_days']:.1f} days"),
    ("Total Returns",    f"{s['total_returns']:,}"),
    ("Demurrage Cost",   f"${s['total_demurrage_usd']/1e6:.2f}M"),
]
kpi_colors = ["#00E676","#00E676","#40C4FF","#40C4FF",
               "#FFD740","#FFD740","#FF9100","#FF9100"]
for i, ((label, value), color) in enumerate(zip(kpis, kpi_colors)):
    row_n = i // 2
    col_n = i % 2
    ax1.text(col_n*0.52+0.02, 1 - row_n*0.26 - 0.05,
             label, fontsize=9, color='#B0B0B0',
             transform=ax1.transAxes)
    ax1.text(col_n*0.52+0.02, 1 - row_n*0.26 - 0.14,
             value, fontsize=14, fontweight='bold',
             color=color, transform=ax1.transAxes)
ax1.set_title("Master KPI Summary",
              fontsize=13, fontweight='bold', pad=10)

# Chart 2: Revenue by year — grouped bar
yr_rev_sorted = yr_rev.sort_values("fiscal_year")
yr_colors = ["#40C4FF","#FFD740","#00E676"]
bars2 = ax2.bar([str(int(y)) for y in yr_rev_sorted["fiscal_year"]],
                yr_rev_sorted["revenue"],
                color=yr_colors[:len(yr_rev_sorted)],
                edgecolor='none', width=0.5)
ax2.set_title("Revenue by Year",
              fontsize=13, fontweight='bold', pad=10)
ax2.set_ylabel("Revenue (USD)")
ax2.grid(axis='y', alpha=0.3)
for bar, val in zip(bars2, yr_rev_sorted["revenue"]):
    ax2.text(bar.get_x()+bar.get_width()/2,
             val*1.01, f'${val/1e6:.0f}M',
             ha='center', fontsize=9,
             color='white', fontweight='bold')

# Chart 3: Top 8 markets by revenue
top8_m = (markets.sort_values("total_revenue_usd", ascending=True)
          .tail(8))
m_colors = plt.cm.RdYlGn(
    np.linspace(0.3, 0.9, len(top8_m)))
ax3.barh(top8_m["country"], top8_m["total_revenue_usd"],
         color=m_colors, edgecolor='none', height=0.7)
ax3.set_title("Top 8 Markets\nby Revenue",
              fontsize=12, fontweight='bold', pad=10)
ax3.set_xlabel("Revenue (USD)")
ax3.grid(axis='x', alpha=0.3)
for i, (_, row) in enumerate(top8_m.iterrows()):
    ax3.text(row.total_revenue_usd*1.01, i,
             f'${row.total_revenue_usd/1e6:.1f}M',
             va='center', fontsize=8, color='white')

# Chart 4: Channel revenue + margin
ch_sorted = ch_summary.sort_values("revenue", ascending=False)
x4 = np.arange(len(ch_sorted))
ax4b = ax4.twinx()
bars4 = ax4.bar(x4, ch_sorted["revenue"],
                color="#40C4FF", alpha=0.75,
                edgecolor='none', label="Revenue")
line4 = ax4b.plot(x4, ch_sorted["avg_margin"],
                  color="#00E676", marker='o',
                  linewidth=2, markersize=6,
                  label="Margin %")
ax4.set_xticks(x4)
ax4.set_xticklabels(ch_sorted["order_channel"],
                    rotation=35, ha='right', fontsize=8)
ax4.set_ylabel("Revenue (USD)", color="#40C4FF")
ax4b.set_ylabel("Gross Margin %", color="#00E676")
ax4b.tick_params(colors="#00E676")
ax4.set_title("Channel Revenue & Margin",
              fontsize=12, fontweight='bold', pad=10)
ax4.grid(axis='y', alpha=0.3)

# Chart 5: Payment status donut
pay_agg = payment[["paid_on_time","paid_late",
                    "pending","overdue"]].sum()
pay_labels = ["Paid On Time","Paid Late","Pending","Overdue"]
pay_colors = ["#00E676","#FFD740","#40C4FF","#FF5252"]
pay_vals   = [pay_agg["paid_on_time"], pay_agg["paid_late"],
              pay_agg["pending"],      pay_agg["overdue"]]
wedges, texts, autotexts = ax5.pie(
    pay_vals, labels=pay_labels,
    colors=pay_colors, autopct='%1.1f%%',
    startangle=90, pctdistance=0.78,
    wedgeprops=dict(width=0.55),
    textprops={'color':'white','fontsize':8.5})
for at in autotexts:
    at.set_fontsize(8)
    at.set_color('white')
ax5.set_title("Payment Status Distribution",
              fontsize=12, fontweight='bold', pad=10)

# Chart 6: Return loss by market (top 10)
ret_mkt = (returns.groupby("country")["total_return_loss_usd"]
           .sum().reset_index()
           .sort_values("total_return_loss_usd", ascending=True)
           .tail(10))
ax6.barh(ret_mkt["country"],
         ret_mkt["total_return_loss_usd"],
         color="#FF5252", edgecolor='none', height=0.7, alpha=0.88)
ax6.set_title("Return Loss by Market\n(Top 10 USD)",
              fontsize=12, fontweight='bold', pad=10)
ax6.set_xlabel("Return Loss (USD)")
ax6.grid(axis='x', alpha=0.3)
for i, (_, row) in enumerate(ret_mkt.iterrows()):
    ax6.text(row.total_return_loss_usd*1.01, i,
             f'${row.total_return_loss_usd:,.0f}',
             va='center', fontsize=7.5, color='white')

plt.tight_layout(rect=[0, 0, 1, 0.96])
path = os.path.join(OUT_PATH, "05_executive_summary.png")
plt.savefig(path, dpi=150, bbox_inches='tight', facecolor=DARK)
plt.show()
print(f"\n✅ Chart saved: {path}")

# ── Save master summary CSV ───────────────────────────────────
summary.to_csv(os.path.join(OUT_PATH, "master_summary.csv"), index=False)
yr_rev.to_csv(os.path.join(OUT_PATH,  "yearly_revenue.csv"), index=False)
ch_summary.to_csv(os.path.join(OUT_PATH, "channel_performance.csv"), index=False)
print("✅ master_summary.csv saved")
print("✅ yearly_revenue.csv saved")
print("✅ channel_performance.csv saved")

print("\n" + "="*65)
print("   EXECUTIVE SUMMARY COMPLETE")
print("   All 5 Python scripts done ✅")
print("="*65 + "\n")