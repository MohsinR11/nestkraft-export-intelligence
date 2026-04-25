"""
02_landed_cost_engine.py
NestKraft — Landed Cost Engine + Margin Waterfall Analysis
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
print("   NESTKRAFT — LANDED COST ENGINE")
print("="*65)

df = pd.read_sql("SELECT * FROM vw_landed_cost_analysis", engine)

# ── Market level summary ──────────────────────────────────────
mkt = df.groupby(["country","region"]).agg(
    avg_fob               = ("avg_fob_per_unit_usd",         "mean"),
    avg_freight           = ("avg_freight_per_unit_usd",     "mean"),
    avg_insurance         = ("avg_insurance_per_unit_usd",   "mean"),
    avg_customs           = ("avg_customs_per_unit_usd",     "mean"),
    avg_port              = ("avg_port_per_unit_usd",        "mean"),
    avg_last_mile         = ("avg_lastmile_per_unit_usd",    "mean"),
    avg_demurrage         = ("avg_demurrage_per_unit_usd",   "mean"),
    avg_total_lc          = ("avg_total_lc_per_unit_usd",    "mean"),
    avg_lc_fob_ratio      = ("avg_lc_fob_ratio",             "mean"),
    avg_exporter_margin   = ("avg_exporter_margin_pct",      "mean"),
    total_demurrage       = ("total_demurrage_usd",          "sum"),
    total_shipments       = ("total_shipments",              "sum"),
).reset_index().round(4)

mkt = mkt.sort_values("avg_exporter_margin", ascending=False).reset_index(drop=True)
mkt.index += 1

# ── Print market table ────────────────────────────────────────
print("\n📊 LANDED COST BY MARKET (Per Unit)\n")
print(f"{'#':<4} {'Country':<16} {'FOB $':<9} {'Freight':<9} "
      f"{'Customs':<9} {'LC/FOB':<8} {'Ex.Margin%'}")
print("-"*70)
for idx, row in mkt.iterrows():
    flag = "🟢" if row.avg_exporter_margin > 70 else \
           "🟡" if row.avg_exporter_margin > 60 else "🔴"
    print(f"{idx:<4} {row['country']:<16} "
          f"${row.avg_fob:<8.2f} "
          f"${row.avg_freight:<8.4f} "
          f"${row.avg_customs:<8.4f} "
          f"{row.avg_lc_fob_ratio:<8.4f} "
          f"{flag} {row.avg_exporter_margin:.2f}%")

# ── Category summary ──────────────────────────────────────────
cat = df.groupby("category").agg(
    avg_fob             = ("avg_fob_per_unit_usd",       "mean"),
    avg_total_lc        = ("avg_total_lc_per_unit_usd",  "mean"),
    avg_lc_fob_ratio    = ("avg_lc_fob_ratio",           "mean"),
    avg_exporter_margin = ("avg_exporter_margin_pct",    "mean"),
    freight_pct         = ("freight_pct_of_lc",          "mean"),
    customs_pct         = ("customs_pct_of_lc",          "mean"),
    lastmile_pct        = ("lastmile_pct_of_lc",         "mean"),
).reset_index().round(2)

print("\n\n📦 COST BREAKDOWN BY CATEGORY\n")
print(f"{'Category':<14} {'FOB $':<9} {'LC $':<9} "
      f"{'LC/FOB':<9} {'Freight%':<10} {'Customs%':<10} "
      f"{'LastMile%':<11} {'Ex.Margin%'}")
print("-"*85)
for _, row in cat.iterrows():
    print(f"{row['category']:<14} "
          f"${row.avg_fob:<8.2f} "
          f"${row.avg_total_lc:<8.4f} "
          f"{row.avg_lc_fob_ratio:<9.4f} "
          f"{row.freight_pct:<10.1f} "
          f"{row.customs_pct:<10.1f} "
          f"{row.lastmile_pct:<11.1f} "
          f"{row.avg_exporter_margin:.2f}%")

# ── Demurrage hotspots ────────────────────────────────────────
print("\n\n⚠️  TOP 5 DEMURRAGE HOTSPOTS\n")
dem = mkt.nlargest(5, "total_demurrage")
for _, row in dem.iterrows():
    print(f"   • {row['country']:<18} "
          f"Total: ${row.total_demurrage:>10,.2f} | "
          f"Shipments: {row.total_shipments:>6,} | "
          f"Avg/Ship: ${row.avg_demurrage:.2f}")

# ── High cost routes ──────────────────────────────────────────
print("\n\n🔴 TOP 10 HIGH COST ROUTES\n")
top_lc = df.nlargest(10, "avg_lc_fob_ratio")[
    ["country","category","product_name",
     "avg_fob_per_unit_usd","avg_total_lc_per_unit_usd",
     "avg_lc_fob_ratio","avg_exporter_margin_pct"]]
for i, (_, row) in enumerate(top_lc.iterrows(), 1):
    print(f"  {i:>2}. {row['country']:<16} | {row['category']:<12} | "
          f"{row['product_name'][:28]:<30} | "
          f"LC/FOB: {row['avg_lc_fob_ratio']:.4f} | "
          f"Ex.Margin: {row['avg_exporter_margin_pct']:.1f}%")

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
fig.suptitle("NestKraft — Landed Cost & Margin Intelligence",
             fontsize=18, fontweight='bold', color='white', y=0.98)
gs = GridSpec(2, 3, figure=fig, hspace=0.42, wspace=0.35)
ax1 = fig.add_subplot(gs[0, :2])
ax2 = fig.add_subplot(gs[0, 2])
ax3 = fig.add_subplot(gs[1, 0])
ax4 = fig.add_subplot(gs[1, 1])
ax5 = fig.add_subplot(gs[1, 2])

# Chart 1: Stacked cost components by market
top10 = mkt.head(10)
comps  = ["avg_freight","avg_insurance","avg_customs",
          "avg_port","avg_last_mile"]
clabels= ["Freight","Insurance","Customs","Port","Last Mile"]
ccolors= ["#40C4FF","#69F0AE","#FFD740","#FF9100","#EA80FC"]
bottom = np.zeros(len(top10))
for col, lbl, clr in zip(comps, clabels, ccolors):
    ax1.bar(top10["country"], top10[col], bottom=bottom,
            label=lbl, color=clr, alpha=0.88)
    bottom += top10[col].values
ax1.set_title("Landed Cost Components by Market (Per Unit USD)",
              fontsize=13, fontweight='bold', pad=10)
ax1.set_ylabel("Cost per Unit (USD)")
ax1.legend(fontsize=9, framealpha=0.3, loc='upper right')
ax1.tick_params(axis='x', rotation=35)
ax1.grid(axis='y', alpha=0.3)

# Chart 2: Exporter margin by market
margin_colors = ["#00E676" if x>70 else
                 "#FFD740" if x>60 else
                 "#FF5252" for x in mkt["avg_exporter_margin"]]
ax2.barh(mkt["country"], mkt["avg_exporter_margin"],
         color=margin_colors, edgecolor='none', height=0.7)
ax2.set_title("Exporter Net\nMargin % by Market",
              fontsize=12, fontweight='bold', pad=10)
ax2.set_xlabel("Exporter Margin %")
ax2.invert_yaxis()
ax2.axvline(70, color='#00E676', linestyle='--', alpha=0.4)
ax2.grid(axis='x', alpha=0.3)
for i, (_, row) in enumerate(mkt.iterrows()):
    ax2.text(row.avg_exporter_margin+0.2, i-1,
             f"{row.avg_exporter_margin:.1f}%",
             va='center', fontsize=7.5, color='white')

# Chart 3: LC/FOB by category
cat_colors = ["#40C4FF","#69F0AE","#FFD740","#FF9100",
               "#EA80FC","#FF5252","#00E676"][:len(cat)]
ax3.bar(cat["category"], cat["avg_lc_fob_ratio"],
        color=cat_colors[:len(cat)], edgecolor='none')
ax3.set_title("LC/FOB Ratio\nby Category",
              fontsize=12, fontweight='bold', pad=10)
ax3.set_ylabel("LC / FOB Ratio")
ax3.axhline(1.0, color='#FF5252', linestyle='--',
            alpha=0.6, label='Break Even (1.0)')
ax3.tick_params(axis='x', rotation=25)
ax3.legend(fontsize=8, framealpha=0.3)
ax3.grid(axis='y', alpha=0.3)
for i, (_, row) in enumerate(cat.iterrows()):
    ax3.text(i, row.avg_lc_fob_ratio+0.002,
             f"{row.avg_lc_fob_ratio:.3f}",
             ha='center', fontsize=8.5,
             color='white', fontweight='bold')

# Chart 4: Margin waterfall (avg per unit)
avg_fob  = df["avg_fob_per_unit_usd"].mean()
avg_fr   = df["avg_freight_per_unit_usd"].mean()
avg_ins  = df["avg_insurance_per_unit_usd"].mean()
avg_cus  = df["avg_customs_per_unit_usd"].mean()
avg_port = df["avg_port_per_unit_usd"].mean()
avg_lm   = df["avg_lastmile_per_unit_usd"].mean()
avg_net  = avg_fob - avg_fr - avg_ins - avg_cus - avg_port - avg_lm

wf_labels = ["FOB","Freight","Insurance","Customs",
             "Port","Last Mile","Net LC"]
wf_values = [avg_fob, -avg_fr, -avg_ins, -avg_cus,
             -avg_port, -avg_lm, avg_net]
wf_colors = ["#00E676","#FF5252","#FF5252","#FF5252",
             "#FF5252","#FF5252","#40C4FF"]

running = 0
bottoms_wf = []
for i, v in enumerate(wf_values):
    if i == 0 or i == len(wf_values)-1:
        bottoms_wf.append(0)
    else:
        bottoms_wf.append(running + v if v < 0 else running)
    running += v if i < len(wf_values)-1 else 0

ax4.bar(wf_labels, [abs(v) for v in wf_values],
        bottom=bottoms_wf, color=wf_colors,
        edgecolor='white', linewidth=0.4, width=0.6)
ax4.set_title("Cost Waterfall\n(Avg Per Unit USD)",
              fontsize=12, fontweight='bold', pad=10)
ax4.set_ylabel("USD per Unit")
ax4.tick_params(axis='x', rotation=30)
ax4.grid(axis='y', alpha=0.3)
for i, (lbl, val) in enumerate(zip(wf_labels, wf_values)):
    ax4.text(i, abs(val)+bottoms_wf[i]+0.005,
             f'${abs(val):.3f}',
             ha='center', va='bottom',
             fontsize=7.5, color='white')

# Chart 5: Demurrage by market
dem5 = mkt.nlargest(10, "total_demurrage")
ax5.bar(dem5["country"], dem5["total_demurrage"],
        color="#FF9100", edgecolor='none', alpha=0.9)
ax5.set_title("Total Demurrage\nby Market (USD)",
              fontsize=12, fontweight='bold', pad=10)
ax5.set_ylabel("Demurrage USD")
ax5.tick_params(axis='x', rotation=40)
ax5.grid(axis='y', alpha=0.3)
for i, (_, row) in enumerate(dem5.iterrows()):
    ax5.text(i, row.total_demurrage*1.02,
             f"${row.total_demurrage:,.0f}",
             ha='center', fontsize=7, color='white')

plt.tight_layout(rect=[0, 0, 1, 0.96])
path = os.path.join(OUT_PATH, "02_landed_cost_engine.png")
plt.savefig(path, dpi=150, bbox_inches='tight', facecolor=DARK)
plt.show()
print(f"\n✅ Chart saved: {path}")

df.to_csv(os.path.join(OUT_PATH, "landed_cost_analysis.csv"), index=False)
print("✅ landed_cost_analysis.csv saved")
print("\n" + "="*65 + "\n")