import pandas as pd
import os

raw = r'D:\Projects\End-to-end projects\16. NestKraft Export Intelligence\Data\Raw'

orders = pd.read_csv(os.path.join(raw, 'fact_export_orders.csv'))
print('=== ORDERS CHECK ===')
print(f'Rows              : {len(orders):,}')
print(f'Avg Gross Margin  : {orders["gross_margin_pct"].mean():.1f}%')
print(f'Min Margin        : {orders["gross_margin_pct"].min():.1f}%')
print(f'Max Margin        : {orders["gross_margin_pct"].max():.1f}%')
print(f'Negative Margins  : {(orders["gross_margin_pct"] < 0).sum():,}')
print(f'Avg FOB USD       : ${orders["total_fob_value_usd"].mean():,.0f}')
print(f'Avg COGS INR      : Rs{orders["total_cogs_inr"].mean():,.0f}')

ships = pd.read_csv(os.path.join(raw, 'fact_shipment_details.csv'))
print()
print('=== SHIPMENT CHECK ===')
print(f'Rows              : {len(ships):,}')
print(f'Avg LC/FOB Ratio  : {ships["landed_to_fob_ratio"].mean():.4f}')
print(f'Min LC/FOB        : {ships["landed_to_fob_ratio"].min():.4f}')
print(f'Max LC/FOB        : {ships["landed_to_fob_ratio"].max():.4f}')
print(f'Avg Freight USD   : ${ships["freight_cost_usd"].mean():,.2f}')
print(f'Avg Demurrage USD : ${ships["demurrage_charges_usd"].mean():,.2f}')

rets = pd.read_csv(os.path.join(raw, 'fact_returns.csv'))
print()
print('=== RETURNS CHECK ===')
print(f'Rows              : {len(rets):,}')
print(f'Avg Return Qty    : {rets["return_quantity"].mean():.1f} units')
print(f'Avg Net Loss USD  : ${rets["net_loss_usd"].mean():,.2f}')
print(f'Total Loss USD    : ${rets["net_loss_usd"].sum():,.0f}')