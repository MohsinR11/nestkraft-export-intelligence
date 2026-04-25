"""
01_market_scoring.py
NestKraft - Market Entry Readiness Scoring Model
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
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
print("   NESTKRAFT - MARKET ENTRY READINESS SCORING")
print("="*65)

df = pd.read_sql("SELECT * FROM vw_market_entry_readiness", engine)

# ── Weighted scoring ──────────────────────────────────────────
WEIGHTS = {
    "home_decor_demand_score":     0.25,
    "indian_craft_affinity_score": 0.20,
    "market_entry_ease_score":     0.18,
    "ecommerce_maturity_score":    0.17,
    "regulatory_ease_score":       0.12,
    "gdp_score":                   0.08,
}
df["gdp_score"] = np.round(
    (df["gdp_per_capita_usd"] - df["gdp_per_capita_usd"].min()) /
    (df["gdp_per_capita_usd"].max() - df["gdp_per_capita_usd"].min()) * 9 + 1, 2)

df["final_score"] = sum(df[col]*w for col, w in WEIGHTS.items())
df["final_score"] = df["final_score"].round(2)

df["revenue_potential"] = np.round(
    df["market_size_usd_mn"] / df["market_size_usd_mn"].max() * 10, 2)

df["priority_score"] = np.round(
    df["final_score"] * 0.65 + df["revenue_potential"] * 0.35, 2)

df["entry_tier"] = pd.cut(
    df["final_score"],
    bins=[0, 6.5, 7.5, 8.5, 10],
    labels=["Tier 4 - Avoid", "Tier 3 - Monitor",
            "Tier 2 - Plan Entry", "Tier 1 - Enter Now"])

df_sorted = df.sort_values("priority_score", ascending=False).reset_index(drop=True)
df_sorted.index += 1

# ── Print ranking ─────────────────────────────────────────────
print("\n📊 MARKET PRIORITY RANKING\n")
print(f"{'#':<4} {'Country':<16} {'Region':<16} {'Score':<8} "
      f"{'Priority':<10} {'Tier':<22} {'Mkt Size'}")
print("-"*90)
for idx, row in df_sorted.iterrows():
    print(f"{idx:<4} {row['country']:<16} {row['region']:<16} "
          f"{row['final_score']:<8} {row['priority_score']:<10} "
          f"{str(row['entry_tier']):<22} ${row['market_size_usd_mn']:,.0f}M")

# ── Key insights ──────────────────────────────────────────────
print("\n" + "-"*65)
tier1 = df_sorted[df_sorted["entry_tier"] == "Tier 1 - Enter Now"]
tier2 = df_sorted[df_sorted["entry_tier"] == "Tier 2 - Plan Entry"]

print("\n✅ TIER 1 - ENTER NOW:")
for _, r in tier1.iterrows():
    print(f"   • {r['country']:<16} Score: {r['final_score']} | "
          f"Demand: {r['home_decor_demand_score']} | "
          f"Craft Affinity: {r['indian_craft_affinity_score']} | "
          f"Customs: {r['avg_customs_duty_pct']}%")

print("\n📋 TIER 2 - PLAN IN 6-12 MONTHS:")
for _, r in tier2.iterrows():
    print(f"   • {r['country']:<16} Score: {r['final_score']} | "
          f"Market Size: ${r['market_size_usd_mn']:,.0f}M")

print(f"\n👑 BEST REVENUE POTENTIAL : "
      f"{df_sorted.nlargest(1,'market_size_usd_mn')['country'].values[0]}")
print(f"🎯 BEST CRAFT AFFINITY    : "
      f"{df_sorted.nlargest(1,'indian_craft_affinity_score')['country'].values[0]}")
print(f"📦 LOWEST CUSTOMS DUTY   : "
      f"{df_sorted.nsmallest(1,'avg_customs_duty_pct')['country'].values[0]}")

# ── Charts ────────────────────────────────────────────────────
DARK = '#0F1117'
plt.rcParams.update({
    'figure.facecolor': DARK, 'axes.facecolor': DARK,
    'axes.edgecolor':   '#2E2E3E', 'text.color': '#E0E0E0',
    'axes.labelcolor':  '#E0E0E0', 'xtick.color': '#B0B0B0',
    'ytick.color':      '#B0B0B0', 'grid.color':  '#1E1E2E',
    'font.family': 'sans-serif',
})
TIER_COLORS = {
    "Tier 1 - Enter Now":  "#00E676",
    "Tier 2 - Plan Entry": "#FFD740",
    "Tier 3 - Monitor":    "#FF9100",
    "Tier 4 - Avoid":      "#FF5252",
}

fig, axes = plt.subplots(2, 2, figsize=(20, 13))
fig.suptitle("NestKraft - Market Entry Intelligence",
             fontsize=18, fontweight='bold', color='white', y=0.98)

# Chart 1: Priority score bar
ax1 = axes[0, 0]
colors = [TIER_COLORS.get(str(t), "#888") for t in df_sorted["entry_tier"]]
bars = ax1.barh(df_sorted["country"], df_sorted["priority_score"],
                color=colors, edgecolor='none', height=0.7)
ax1.set_title("Market Priority Ranking", fontsize=13,
              fontweight='bold', pad=10)
ax1.set_xlabel("Priority Score")
ax1.invert_yaxis()
ax1.axvline(8.0, color='#00E676', linestyle='--', alpha=0.5)
for bar, score in zip(bars, df_sorted["priority_score"]):
    ax1.text(score+0.05, bar.get_y()+bar.get_height()/2,
             f'{score:.2f}', va='center', fontsize=8, color='white')
patches = [mpatches.Patch(color=v, label=k) for k,v in TIER_COLORS.items()]
ax1.legend(handles=patches, fontsize=7, framealpha=0.3, loc='lower right')
ax1.grid(axis='x', alpha=0.3)

# Chart 2: Score breakdown top 5
ax2 = axes[0, 1]
top5 = df_sorted.head(5)
cats = ["home_decor_demand_score","indian_craft_affinity_score",
        "market_entry_ease_score","ecommerce_maturity_score",
        "regulatory_ease_score"]
labels = ["Demand","Craft\nAffinity","Entry\nEase","E-comm","Regulatory"]
palette = ["#00E676","#40C4FF","#FFD740","#FF9100","#EA80FC"]
x = np.arange(len(cats))
w = 0.15
for i, (_, row) in enumerate(top5.iterrows()):
    ax2.bar(x+i*w, [row[c] for c in cats], w,
            label=row['country'], color=palette[i], alpha=0.85)
ax2.set_xticks(x+w*2)
ax2.set_xticklabels(labels, fontsize=9)
ax2.set_ylabel("Score (out of 10)")
ax2.set_title("Top 5 Markets — Score Breakdown",
              fontsize=13, fontweight='bold', pad=10)
ax2.set_ylim(0, 12)
ax2.legend(fontsize=8, framealpha=0.3)
ax2.grid(axis='y', alpha=0.3)

# Chart 3: Score vs market size bubble
ax3 = axes[1, 0]
bubble_colors = [TIER_COLORS.get(str(t), "#888") for t in df_sorted["entry_tier"]]
sizes = (df_sorted["market_size_usd_mn"] /
         df_sorted["market_size_usd_mn"].max()) * 1800 + 80
ax3.scatter(df_sorted["final_score"], df_sorted["market_size_usd_mn"],
            s=sizes, c=bubble_colors, alpha=0.85,
            edgecolors='white', linewidth=0.5)
for _, row in df_sorted.iterrows():
    ax3.annotate(row['country'],
                 (row['final_score'], row['market_size_usd_mn']),
                 xytext=(5, 3), textcoords='offset points',
                 fontsize=7.5, color='white', alpha=0.9)
ax3.set_xlabel("Market Entry Score")
ax3.set_ylabel("Market Size (USD Mn)")
ax3.set_title("Score vs Revenue Potential",
              fontsize=13, fontweight='bold', pad=10)
ax3.grid(alpha=0.3)

# Chart 4: Customs + transit heatmap
ax4 = axes[1, 1]
heat = df_sorted[["country","avg_customs_duty_pct",
                   "avg_transit_days_from_india",
                   "composite_score"]].set_index("country")
heat_n = (heat - heat.min()) / (heat.max() - heat.min())
im = ax4.imshow(heat_n.values, cmap='RdYlGn_r',
                aspect='auto', vmin=0, vmax=1)
ax4.set_xticks(range(3))
ax4.set_xticklabels(["Customs %","Transit Days","Mkt Score"], fontsize=9)
ax4.set_yticks(range(len(heat.index)))
ax4.set_yticklabels(heat.index, fontsize=7.5)
ax4.set_title("Market Entry Cost Heatmap\n(Red = Higher Barrier)",
              fontsize=12, fontweight='bold', pad=10)
for i in range(len(heat.index)):
    for j in range(3):
        ax4.text(j, i, f'{heat.values[i,j]:.1f}',
                 ha='center', va='center',
                 fontsize=7.5, color='white', fontweight='bold')
plt.colorbar(im, ax=ax4, shrink=0.8)

plt.tight_layout(rect=[0, 0, 1, 0.96])
path = os.path.join(OUT_PATH, "01_market_scoring.png")
plt.savefig(path, dpi=150, bbox_inches='tight', facecolor=DARK)
plt.show()
print(f"\n✅ Chart saved: {path}")

df_sorted.to_csv(os.path.join(OUT_PATH, "market_scores.csv"), index=False)
print("✅ market_scores.csv saved")
print("\n" + "="*65 + "\n")