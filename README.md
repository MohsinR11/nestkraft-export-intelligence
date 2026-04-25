# NestKraft Export Intelligence System
**Helping Indian D2C brands stop guessing and start exporting with data**

---

## Business Problem

Indian D2C brands entering global markets face 3 critical problems:

- ❌ Pick markets based on gut feel or a distributor's WhatsApp message
- ❌ Have no idea what it truly costs to land their product in USA vs UAE
- ❌ Get blindsided by demurrage, customs holds, and wrong product-market fit

**This project builds a complete export intelligence system** that scores
20 global markets, calculates true per-unit landed cost, benchmarks
competitor pricing daily, and identifies which product-market combinations
are actually worth pursuing.

---

## Data Overview

> Simulated production-grade dataset built with real Indian export
> business logic - FOB pricing, incoterm-correct margin calculations,
> and seasonal demand patterns aligned with global gifting cycles.

| Table | Rows | Description |
|---|---|---|
| dim_products | 40 | SKU catalog with FOB prices and COGS |
| dim_markets | 20 | Export target markets with scoring data |
| dim_customers | 300 | International buyer profiles |
| dim_forex_rates | 10,960 | Daily rates - 10 currencies vs INR |
| fact_export_orders | 500,000 | 3 years of export transactions |
| fact_shipment_details | 120,000 | Shipment + landed cost data |
| fact_competitor_pricing | 146,400 | Daily competitor prices |
| fact_market_demand | 292,800 | Search volume + demand signals |
| fact_returns | 25,000 | Returns and refund data |
| **Total** | **1,095,520 rows** | **3 years · 20 markets · 40 products** |

---

## Approach

**1. Data Generation (Python)**
- Built 9 datasets with pre-verified financial logic
- Gross margin: 55-82% | LC/FOB ratio: 1.03-1.35
- All numbers validated before loading to database

**2. SQL Data Warehouse (PostgreSQL)**
- 9 tables with foreign keys + 20 performance indexes
- 15 business intelligence views covering all KPI areas

**3. Python Analysis Engine (5 scripts)**
- Market entry scoring model
- Landed cost waterfall engine
- Product export scorecard
- Demand forecast + seasonality model
- Executive summary dashboard

**4. Power BI Dashboard (3 pages)**
- Built from raw tables + DAX measures (not views)
- Full slicer interactivity across all pages
- Dark theme executive design

---

## Key Findings

### 🌍 Market Entry
- **UAE ranks #1** - highest Indian craft affinity (9.2), lowest effective customs duty, fastest transit (10 days)
- **USA ranks #2** - largest market size ($5.2B) with strongest diaspora demand score (9.0)
- **Singapore is best for trial shipments** - 0% customs duty, highest regulatory ease (9.0), 12-day transit
- **South Africa ranked last** - 20% customs duty + highest LC/FOB ratio (1.35) makes it unviable for low-ticket products

### 💰 Landed Cost
- Average LC/FOB ratio across all markets: **1.14**
- For every $1 FOB, the buyer pays $1.14 in total landed cost
- **Port Congestion** is the #1 driver of $2.7M total demurrage
- Singapore and Japan have the cleanest ratios at **1.03-1.06**

### 📦 Product Performance
- **Rugs** drive highest absolute revenue
- **Decor category** delivers strongest gross margin at **62%+** - ideal lead category for new market entry
- **Kantha Quilt** and **Hand Block Print Bedsheet** are top 2 products by revenue
- **7 products** fall below 45% margin threshold - need FOB revision or removal from export catalog

### 📡 Channel Intelligence
- **Direct B2B** - highest margin (58%+) and highest AOV ($4,200+) but lowest volume
- **Amazon** - 28% of total order volume with 61% repeat order rate
- **Etsy** - lowest cancellation rate (1.8%), best fit for handmade and sustainable lines
- **Trade Fair** - worst across all metrics: highest cancellation, lowest AOV, lowest repeat rate

### 💳 Payment Risk
- **57.6%** of orders paid on time
- **6.8%** overdue - $98M at risk
- **Middle East** markets show best payment behaviour
- **Europe** has highest late payment rate - recommend LC payment terms for new buyers

### 📅 Seasonality
- **Nov–Jan is peak season** - 1.25–1.35x normal monthly revenue
- Driven by Christmas gifting demand in USA, UK, Germany, France
- **Production ramp must start September** to build peak inventory
- **June–August** is slowest - best window for new market trials

### 📬 Returns
- **Damaged in Transit** = #1 return reason at 28% — packaging investment has direct ROI
- Returns = **0.42% of total revenue** - within acceptable range
- **Middle East** has lowest return rates | **Europe** has highest

---

## Business Impact

| Problem | This System Solves It By |
|---|---|
| Wrong market selection | Weighted 5-KPI scoring across 20 markets |
| Unknown landed cost | Per-unit breakdown: freight + insurance + customs + port + last mile |
| Wrong product to lead with | Margin + reach + viability score per product |
| Channel guesswork | Revenue + margin + AOV + repeat rate by channel |
| Payment surprises | DSO + overdue tracking per market |
| Reactive stock builds | Seasonality index flags Sep ramp-up need |
| Pricing in the dark | 146,400 daily competitor price points |

> A brand doing ₹50 Cr in exports can recover ₹3–5 Cr annually
> by fixing market selection, channel mix, and packaging quality
> - using exactly what this system surfaces.

---

## Visuals

📊 **[View Interactive Power BI Dashboard](https://drive.google.com/file/d/1pGP406YwScbgJaobeQHJvEgoHYkyMCGp/view?usp=sharing)**

### Executive Summary
![Executive Summary](https://github.com/MohsinR11/nestkraft-export-intelligence/blob/main/Dashboard%20Screenshots/Page%201%20-%20Executive%20Overview.png)

---

## Tools Used

| Tool | Purpose |
|---|---|
| Python | Data generation · Analysis · Visualization |
| PostgreSQL | Data warehouse - 9 tables · 15 views |
| Power BI | 3-page executive dashboard |
| SQL | Business KPIs · Views · Indexes |
| Git | Version control |

---

## Repository Structure

nestkraft-export-intelligence/
│
├── SQL/
│   ├── 01_create_schema.sql         ← 9 tables + indexes
│   └── 02_business_queries.sql      ← 15 business views
│
├── Python/
│   ├── Data Generation/
│   │   ├── generate_data.py         ← 1.09M row generator
│   │   ├── load_to_postgres.py      ← PostgreSQL loader
│   │   ├── verify_data.py           ← Pre-load validation
│   │   └── config_template.py       ← DB config template
│   └── Analysis/
│       ├── 01_market_scoring.py     ← Market entry model
│       ├── 02_landed_cost_engine.py ← Landed cost engine
│       ├── 03_product_market_fit.py ← Product scorecard
│       ├── 04_demand_forecast.py    ← Seasonality model
│       └── 05_executive_summary.py  ← Executive dashboard
│
├── Outputs/                         ← Charts + processed CSVs
├── Power BI/                        ← .pbix dashboard file
├── .gitignore
└── README.md

---

## Contact

**Open to Business Data Analyst and BI Analyst roles in India**

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-blue)](https://www.linkedin.com/in/mohsinraza-data/)

*Built to solve real problems Indian export companies face every day -
not a tutorial reproduction.*
