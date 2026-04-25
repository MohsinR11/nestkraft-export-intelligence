-- ============================================================
-- NestKraft Export Intelligence — Business Queries
-- PostgreSQL | 15 Views | Production Grade
-- ============================================================

-- ────────────────────────────────────────────────────────────
-- QUERY 1 · Executive Revenue Dashboard
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW vw_executive_revenue AS
SELECT
    fiscal_year,
    quarter,
    month_num,
    month_name,
    COUNT(order_id)                                        AS total_orders,
    SUM(quantity_units)                                    AS total_units_sold,
    ROUND(SUM(total_fob_value_usd)::NUMERIC, 2)            AS total_revenue_usd,
    ROUND(SUM(total_fob_value_inr)::NUMERIC, 2)            AS total_revenue_inr,
    ROUND(SUM(total_cogs_inr)::NUMERIC, 2)                 AS total_cogs_inr,
    ROUND(SUM(gross_profit_inr)::NUMERIC, 2)               AS total_gross_profit_inr,
    ROUND(AVG(gross_margin_pct)::NUMERIC, 2)               AS avg_gross_margin_pct,
    ROUND(SUM(total_fob_value_usd) /
          NULLIF(COUNT(order_id), 0), 2)                   AS avg_order_value_usd,
    COUNT(CASE WHEN is_repeat_order = 'Y' THEN 1 END)      AS repeat_orders,
    ROUND(COUNT(CASE WHEN is_repeat_order = 'Y'
                     THEN 1 END) * 100.0 /
          NULLIF(COUNT(order_id), 0), 2)                   AS repeat_order_rate_pct,
    COUNT(CASE WHEN order_status  = 'Cancelled'
               THEN 1 END)                                 AS cancelled_orders,
    COUNT(CASE WHEN payment_status = 'Overdue'
               THEN 1 END)                                 AS overdue_payments,
    ROUND(SUM(CASE WHEN payment_status = 'Overdue'
                   THEN total_fob_value_usd ELSE 0
              END)::NUMERIC, 2)                            AS overdue_value_usd,
    COUNT(DISTINCT customer_id)                            AS active_buyers,
    COUNT(DISTINCT market_id)                              AS markets_active
FROM fact_export_orders
GROUP BY fiscal_year, quarter, month_num, month_name
ORDER BY fiscal_year, month_num;

SELECT * FROM vw_executive_revenue;


-- ────────────────────────────────────────────────────────────
-- QUERY 2 · Market Entry Readiness Score
-- Which markets should NestKraft enter first?
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW vw_market_entry_readiness AS
WITH market_revenue AS (
    SELECT
        market_id,
        COUNT(order_id)                                    AS total_orders,
        ROUND(SUM(total_fob_value_usd)::NUMERIC, 2)        AS total_revenue_usd,
        ROUND(AVG(gross_margin_pct)::NUMERIC, 2)           AS avg_margin_pct,
        COUNT(DISTINCT customer_id)                        AS unique_buyers,
        ROUND(AVG(discount_pct)::NUMERIC, 2)               AS avg_discount_pct
    FROM fact_export_orders
    WHERE order_status != 'Cancelled'
    GROUP BY market_id
),
market_logistics AS (
    SELECT
        market_id,
        ROUND(AVG(total_transit_days)::NUMERIC, 1)         AS avg_transit_days,
        ROUND(AVG(landed_to_fob_ratio)::NUMERIC, 4)        AS avg_lc_fob_ratio,
        ROUND(SUM(demurrage_charges_usd)::NUMERIC, 2)      AS total_demurrage_usd,
        ROUND(AVG(CASE WHEN is_delayed = 'Y'
                       THEN 1.0 ELSE 0.0 END)*100, 2)      AS delay_rate_pct
    FROM fact_shipment_details
    GROUP BY market_id
),
market_returns AS (
    SELECT
        market_id,
        COUNT(return_id)                                   AS total_returns,
        ROUND(SUM(net_loss_usd)::NUMERIC, 2)               AS return_loss_usd
    FROM fact_returns
    GROUP BY market_id
)
SELECT
    m.market_id,
    m.country,
    m.region,
    m.currency,
    m.home_decor_demand_score,
    m.market_entry_ease_score,
    m.regulatory_ease_score,
    m.indian_craft_affinity_score,
    m.ecommerce_maturity_score,
    m.avg_customs_duty_pct,
    m.gdp_per_capita_usd,
    m.market_size_usd_mn,
    m.primary_marketplace,
    m.avg_transit_days_from_india,
    m.composite_score,
    m.market_tier,
    -- Weighted priority score
    ROUND((
        m.home_decor_demand_score     * 0.25 +
        m.indian_craft_affinity_score * 0.20 +
        m.market_entry_ease_score     * 0.18 +
        m.ecommerce_maturity_score    * 0.17 +
        m.regulatory_ease_score       * 0.12 +
        (m.gdp_per_capita_usd::NUMERIC/65000*10) * 0.08
    )::NUMERIC, 2)                                         AS weighted_priority_score,
    COALESCE(r.total_orders, 0)                            AS total_orders,
    COALESCE(r.total_revenue_usd, 0)                       AS total_revenue_usd,
    COALESCE(r.avg_margin_pct, 0)                          AS avg_margin_pct,
    COALESCE(r.unique_buyers, 0)                           AS unique_buyers,
    COALESCE(l.avg_transit_days, 0)                        AS avg_transit_days,
    COALESCE(l.avg_lc_fob_ratio, 0)                        AS avg_lc_fob_ratio,
    COALESCE(l.total_demurrage_usd, 0)                     AS total_demurrage_usd,
    COALESCE(l.delay_rate_pct, 0)                          AS delay_rate_pct,
    COALESCE(ret.total_returns, 0)                         AS total_returns,
    COALESCE(ret.return_loss_usd, 0)                       AS return_loss_usd
FROM dim_markets m
LEFT JOIN market_revenue   r   ON m.market_id = r.market_id
LEFT JOIN market_logistics l   ON m.market_id = l.market_id
LEFT JOIN market_returns   ret ON m.market_id = ret.market_id
ORDER BY weighted_priority_score DESC;

SELECT * FROM vw_market_entry_readiness;


-- ────────────────────────────────────────────────────────────
-- QUERY 3 · Product Export Performance Scorecard
-- Which products are star exporters vs margin drains?
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW vw_product_export_scorecard AS
WITH prod_orders AS (
    SELECT
        product_id,
        COUNT(order_id)                                    AS total_orders,
        SUM(quantity_units)                                AS total_units,
        ROUND(SUM(total_fob_value_usd)::NUMERIC, 2)        AS total_revenue_usd,
        ROUND(SUM(gross_profit_inr)::NUMERIC, 2)           AS total_gp_inr,
        ROUND(AVG(gross_margin_pct)::NUMERIC, 2)           AS avg_margin_pct,
        ROUND(AVG(discount_pct)::NUMERIC, 2)               AS avg_discount_pct,
        COUNT(DISTINCT market_id)                          AS markets_present,
        COUNT(DISTINCT customer_id)                        AS unique_buyers,
        COUNT(CASE WHEN order_status  = 'Cancelled'
                   THEN 1 END)                             AS cancellations,
        COUNT(CASE WHEN payment_status = 'Overdue'
                   THEN 1 END)                             AS overdue_count
    FROM fact_export_orders
    GROUP BY product_id
),
prod_logistics AS (
    SELECT
        product_id,
        ROUND(AVG(total_landed_cost_usd /
              NULLIF(s.shipment_id::NUMERIC,0)*0+
              total_landed_cost_usd)::NUMERIC, 2)          AS avg_lc_per_shipment,
        ROUND(AVG(landed_to_fob_ratio)::NUMERIC, 4)        AS avg_lc_fob_ratio,
        ROUND(AVG(exporter_net_margin_usd)::NUMERIC, 2)    AS avg_exporter_margin_usd,
        ROUND(SUM(demurrage_charges_usd)::NUMERIC, 2)      AS total_demurrage_usd,
        ROUND(AVG(CASE WHEN is_delayed='Y'
                       THEN 1.0 ELSE 0.0 END)*100, 2)      AS delay_rate_pct
    FROM fact_shipment_details s
    GROUP BY product_id
),
prod_returns AS (
    SELECT
        product_id,
        COUNT(return_id)                                   AS total_returns,
        ROUND(SUM(net_loss_usd)::NUMERIC, 2)               AS return_loss_usd
    FROM fact_returns
    GROUP BY product_id
),
prod_pricing AS (
    SELECT
        product_id,
        ROUND(AVG(our_price_usd)::NUMERIC, 2)              AS avg_sell_price_usd,
        ROUND(AVG(avg_competitor_price_usd)::NUMERIC, 2)   AS avg_comp_price_usd,
        ROUND(AVG(our_price_index)::NUMERIC, 1)            AS avg_price_index,
        ROUND(AVG(gross_margin_on_our_price_pct)::NUMERIC,2) AS avg_sell_margin_pct
    FROM fact_competitor_pricing
    GROUP BY product_id
)
SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.subcategory,
    p.fob_price_usd,
    p.cogs_inr,
    p.weight_kg,
    p.artisan_cluster,
    p.is_handmade,
    p.is_sustainable,
    o.total_orders,
    o.total_units,
    o.total_revenue_usd,
    o.total_gp_inr,
    o.avg_margin_pct,
    o.avg_discount_pct,
    o.markets_present,
    o.unique_buyers,
    o.cancellations,
    o.overdue_count,
    COALESCE(l.avg_lc_fob_ratio, 0)       AS avg_lc_fob_ratio,
    COALESCE(l.avg_exporter_margin_usd, 0) AS avg_exporter_margin_usd,
    COALESCE(l.total_demurrage_usd, 0)     AS total_demurrage_usd,
    COALESCE(l.delay_rate_pct, 0)          AS delay_rate_pct,
    COALESCE(r.total_returns, 0)           AS total_returns,
    COALESCE(r.return_loss_usd, 0)         AS return_loss_usd,
    COALESCE(pr.avg_sell_price_usd, 0)     AS avg_sell_price_usd,
    COALESCE(pr.avg_comp_price_usd, 0)     AS avg_comp_price_usd,
    COALESCE(pr.avg_price_index, 0)        AS avg_price_index,
    COALESCE(pr.avg_sell_margin_pct, 0)    AS avg_sell_margin_pct,
    CASE
        WHEN o.avg_margin_pct >= 60 AND o.markets_present >= 8
             THEN '⭐ Star Export Product'
        WHEN o.avg_margin_pct >= 50 AND o.markets_present >= 5
             THEN '📈 Growth Product'
        WHEN o.avg_margin_pct >= 35
             THEN '✅ Viable — Optimize'
        ELSE '⚠️ Review — Low Margin'
    END                                    AS export_viability_tag
FROM dim_products p
LEFT JOIN prod_orders   o  ON p.product_id = o.product_id
LEFT JOIN prod_logistics l  ON p.product_id = l.product_id
LEFT JOIN prod_returns   r  ON p.product_id = r.product_id
LEFT JOIN prod_pricing   pr ON p.product_id = pr.product_id
ORDER BY o.total_revenue_usd DESC NULLS LAST;

SELECT * FROM vw_product_export_scorecard;


-- ────────────────────────────────────────────────────────────
-- QUERY 4 · Landed Cost & Margin Analysis
-- Where do logistics eat into exporter margin?
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW vw_landed_cost_analysis AS
SELECT
    m.country,
    m.region,
    p.category,
    p.product_name,
    COUNT(s.shipment_id)                                        AS total_shipments,
    -- Per unit averages (divide totals by quantity)
    ROUND(AVG(o.net_fob_price_usd)::NUMERIC, 4)                AS avg_fob_per_unit_usd,
    ROUND(AVG(s.freight_cost_usd  / NULLIF(o.quantity_units,0))::NUMERIC,4) AS avg_freight_per_unit_usd,
    ROUND(AVG(s.insurance_usd     / NULLIF(o.quantity_units,0))::NUMERIC,4) AS avg_insurance_per_unit_usd,
    ROUND(AVG(s.customs_duty_usd  / NULLIF(o.quantity_units,0))::NUMERIC,4) AS avg_customs_per_unit_usd,
    ROUND(AVG(s.port_handling_usd / NULLIF(o.quantity_units,0))::NUMERIC,4) AS avg_port_per_unit_usd,
    ROUND(AVG(s.last_mile_usd     / NULLIF(o.quantity_units,0))::NUMERIC,4) AS avg_lastmile_per_unit_usd,
    ROUND(AVG(s.demurrage_charges_usd/NULLIF(o.quantity_units,0))::NUMERIC,4) AS avg_demurrage_per_unit_usd,
    ROUND(AVG(s.total_landed_cost_usd/NULLIF(o.quantity_units,0))::NUMERIC,4) AS avg_total_lc_per_unit_usd,
    -- Exporter margin (FOB - freight - insurance only, buyer pays rest under FOB)
    ROUND(AVG(s.exporter_net_margin_usd /
              NULLIF(o.quantity_units,0))::NUMERIC, 4)          AS avg_exporter_margin_per_unit_usd,
    ROUND(AVG(s.exporter_net_margin_usd /
              NULLIF(s.declared_value_usd,0)*100)::NUMERIC, 2)  AS avg_exporter_margin_pct,
    -- LC/FOB ratio
    ROUND(AVG(s.landed_to_fob_ratio)::NUMERIC, 4)              AS avg_lc_fob_ratio,
    -- Cost components as % of total landed
    ROUND(AVG(s.freight_cost_usd /
              NULLIF(s.total_landed_cost_usd,0)*100)::NUMERIC,1) AS freight_pct_of_lc,
    ROUND(AVG(s.customs_duty_usd /
              NULLIF(s.total_landed_cost_usd,0)*100)::NUMERIC,1) AS customs_pct_of_lc,
    ROUND(AVG(s.last_mile_usd /
              NULLIF(s.total_landed_cost_usd,0)*100)::NUMERIC,1) AS lastmile_pct_of_lc,
    ROUND(SUM(s.demurrage_charges_usd)::NUMERIC, 2)             AS total_demurrage_usd,
    CASE
        WHEN AVG(s.landed_to_fob_ratio) > 1.30 THEN 'High Cost Route'
        WHEN AVG(s.landed_to_fob_ratio) > 1.15 THEN 'Moderate Cost'
        ELSE 'Efficient Route'
    END                                                         AS route_efficiency
FROM fact_shipment_details s
JOIN fact_export_orders o ON s.order_id   = o.order_id
JOIN dim_markets        m ON s.market_id  = m.market_id
JOIN dim_products       p ON s.product_id = p.product_id
GROUP BY m.country, m.region, p.category, p.product_name
ORDER BY avg_lc_fob_ratio DESC;

SELECT * FROM vw_landed_cost_analysis;


-- ────────────────────────────────────────────────────────────
-- QUERY 5 · Customer LTV & Buyer Health Scorecard
-- Who are the most valuable export buyers?
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW vw_customer_ltv AS
WITH cust_orders AS (
    SELECT
        customer_id,
        COUNT(order_id)                                    AS total_orders,
        SUM(quantity_units)                                AS total_units,
        ROUND(SUM(total_fob_value_usd)::NUMERIC, 2)        AS total_revenue_usd,
        ROUND(SUM(gross_profit_inr)::NUMERIC, 2)           AS total_gp_inr,
        ROUND(AVG(gross_margin_pct)::NUMERIC, 2)           AS avg_margin_pct,
        ROUND(AVG(discount_pct)::NUMERIC, 2)               AS avg_discount,
        MAX(order_date)                                    AS last_order_date,
        MIN(order_date)                                    AS first_order_date,
        COUNT(DISTINCT product_id)                         AS products_ordered,
        COUNT(DISTINCT market_id)                          AS markets_ordered_from,
        COUNT(CASE WHEN payment_status IN
              ('Overdue','Paid Late') THEN 1 END)          AS late_payments,
        ROUND(AVG(payment_delay_days)::NUMERIC, 1)         AS avg_payment_delay
    FROM fact_export_orders
    WHERE order_status != 'Cancelled'
    GROUP BY customer_id
),
cust_returns AS (
    SELECT
        customer_id,
        COUNT(return_id)                                   AS total_returns,
        ROUND(SUM(net_loss_usd)::NUMERIC, 2)               AS return_loss_usd
    FROM fact_returns
    GROUP BY customer_id
)
SELECT
    c.customer_id,
    c.company_name,
    c.customer_type,
    c.country,
    c.city,
    c.customer_tier,
    c.payment_terms_days,
    c.account_manager,
    o.total_orders,
    o.total_units,
    o.total_revenue_usd,
    o.total_gp_inr,
    o.avg_margin_pct,
    o.avg_discount,
    o.last_order_date,
    o.first_order_date,
    ROUND(DATE_PART('day',
          NOW() - o.last_order_date::TIMESTAMP)::NUMERIC,0) AS days_since_last_order,
    o.products_ordered,
    o.markets_ordered_from,
    o.late_payments,
    o.avg_payment_delay,
    COALESCE(r.total_returns, 0)    AS total_returns,
    COALESCE(r.return_loss_usd, 0)  AS return_loss_usd,
    -- RFM Scores
    CASE
        WHEN DATE_PART('day',NOW()-o.last_order_date::TIMESTAMP) <= 30  THEN 5
        WHEN DATE_PART('day',NOW()-o.last_order_date::TIMESTAMP) <= 60  THEN 4
        WHEN DATE_PART('day',NOW()-o.last_order_date::TIMESTAMP) <= 90  THEN 3
        WHEN DATE_PART('day',NOW()-o.last_order_date::TIMESTAMP) <= 180 THEN 2
        ELSE 1
    END AS recency_score,
    CASE
        WHEN o.total_orders >= 80  THEN 5
        WHEN o.total_orders >= 40  THEN 4
        WHEN o.total_orders >= 20  THEN 3
        WHEN o.total_orders >= 10  THEN 2
        ELSE 1
    END AS frequency_score,
    CASE
        WHEN o.total_revenue_usd >= 300000 THEN 5
        WHEN o.total_revenue_usd >= 100000 THEN 4
        WHEN o.total_revenue_usd >= 50000  THEN 3
        WHEN o.total_revenue_usd >= 10000  THEN 2
        ELSE 1
    END AS monetary_score,
    CASE
        WHEN o.avg_payment_delay > 25       THEN '🔴 High Risk — Payment Issues'
        WHEN COALESCE(r.total_returns,0) > 15 THEN '🟡 High Returns — Review'
        WHEN o.total_revenue_usd > 200000
             AND o.avg_payment_delay <= 5   THEN '🟢 Champion Buyer'
        WHEN o.total_revenue_usd > 80000    THEN '🟢 Loyal Buyer'
        ELSE '⚪ Standard Buyer'
    END AS buyer_health_tag
FROM dim_customers c
LEFT JOIN cust_orders  o ON c.customer_id = o.customer_id
LEFT JOIN cust_returns r ON c.customer_id = r.customer_id
ORDER BY o.total_revenue_usd DESC NULLS LAST;

SELECT * FROM vw_customer_ltv;


-- ────────────────────────────────────────────────────────────
-- QUERY 6 · Shipment Delay & Demurrage Intelligence
-- Which routes and carriers cause most delays?
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW vw_shipment_delay_analysis AS
SELECT
    m.country,
    m.region,
    s.shipping_line,
    s.port_of_loading,
    s.port_of_discharge,
    s.container_type,
    s.delay_reason,
    COUNT(s.shipment_id)                                        AS total_shipments,
    SUM(CASE WHEN s.is_delayed='Y' THEN 1 ELSE 0 END)          AS delayed_shipments,
    ROUND(SUM(CASE WHEN s.is_delayed='Y' THEN 1.0 ELSE 0 END)
          / NULLIF(COUNT(s.shipment_id),0)*100, 2)             AS delay_rate_pct,
    ROUND(AVG(CASE WHEN s.is_delayed='Y'
                   THEN s.delay_days END)::NUMERIC, 1)         AS avg_delay_days,
    ROUND(AVG(s.total_transit_days)::NUMERIC, 1)               AS avg_transit_days,
    ROUND(AVG(s.customs_clearance_days)::NUMERIC, 1)           AS avg_customs_days,
    ROUND(SUM(s.demurrage_charges_usd)::NUMERIC, 2)            AS total_demurrage_usd,
    ROUND(AVG(s.demurrage_charges_usd)::NUMERIC, 2)            AS avg_demurrage_per_shipment,
    ROUND(SUM(s.freight_cost_usd)::NUMERIC, 2)                 AS total_freight_usd,
    CASE
        WHEN SUM(CASE WHEN s.is_delayed='Y' THEN 1.0 ELSE 0 END)
             / NULLIF(COUNT(s.shipment_id),0) > 0.25
             THEN '🔴 Critical — High Delay'
        WHEN SUM(CASE WHEN s.is_delayed='Y' THEN 1.0 ELSE 0 END)
             / NULLIF(COUNT(s.shipment_id),0) > 0.12
             THEN '🟡 Warning — Monitor'
        ELSE '🟢 Performing Well'
    END AS performance_flag
FROM fact_shipment_details s
JOIN dim_markets m ON s.market_id = m.market_id
GROUP BY m.country, m.region, s.shipping_line, s.port_of_loading,
         s.port_of_discharge, s.container_type, s.delay_reason
ORDER BY total_demurrage_usd DESC;

SELECT * FROM vw_shipment_delay_analysis;


-- ────────────────────────────────────────────────────────────
-- QUERY 7 · Competitor Price Benchmarking
-- Are we priced right across markets?
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW vw_competitor_benchmark AS
SELECT
    m.country,
    m.region,
    p.category,
    p.subcategory,
    p.product_name,
    ROUND(AVG(cp.our_price_usd)::NUMERIC, 2)               AS our_avg_price_usd,
    ROUND(AVG(cp.competitor1_price_usd)::NUMERIC, 2)       AS comp1_avg_price_usd,
    ROUND(AVG(cp.competitor2_price_usd)::NUMERIC, 2)       AS comp2_avg_price_usd,
    ROUND(AVG(cp.competitor3_price_usd)::NUMERIC, 2)       AS comp3_avg_price_usd,
    ROUND(AVG(cp.avg_competitor_price_usd)::NUMERIC, 2)    AS avg_market_price_usd,
    ROUND(AVG(cp.our_price_index)::NUMERIC, 1)             AS avg_price_index,
    ROUND(AVG(cp.landed_cost_usd)::NUMERIC, 4)             AS avg_landed_cost_usd,
    ROUND(AVG(cp.gross_margin_on_our_price_pct)::NUMERIC,2) AS avg_gross_margin_pct,
    ROUND((AVG(cp.our_price_usd) -
           AVG(cp.avg_competitor_price_usd))::NUMERIC, 2)  AS price_gap_usd,
    ROUND((AVG(cp.our_price_usd) -
           AVG(cp.avg_competitor_price_usd)) /
           NULLIF(AVG(cp.avg_competitor_price_usd),0)
           * 100::NUMERIC, 2)                              AS price_gap_pct,
    CASE
        WHEN AVG(cp.our_price_index) > 115 THEN '⚠️ Overpriced'
        WHEN AVG(cp.our_price_index) < 85  THEN '⚠️ Underpriced'
        WHEN AVG(cp.our_price_index)
             BETWEEN 95 AND 110             THEN '✅ Optimally Priced'
        ELSE '🟡 Fine Tune'
    END AS pricing_recommendation,
    COUNT(*) AS data_points
FROM fact_competitor_pricing cp
JOIN dim_markets  m ON cp.market_id  = m.market_id
JOIN dim_products p ON cp.product_id = p.product_id
GROUP BY m.country, m.region, p.category,
         p.subcategory, p.product_name
ORDER BY price_gap_pct DESC;

SELECT * FROM vw_competitor_benchmark;


-- ────────────────────────────────────────────────────────────
-- QUERY 8 · Return Rate & Loss Analysis
-- Where are returns hurting export profitability?
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW vw_returns_analysis AS
WITH order_base AS (
    SELECT
        market_id,
        product_id,
        COUNT(order_id)          AS total_orders,
        SUM(quantity_units)      AS total_units_sold,
        SUM(total_fob_value_usd) AS total_revenue_usd
    FROM fact_export_orders
    WHERE order_status = 'Delivered'
    GROUP BY market_id, product_id
)
SELECT
    m.country,
    m.region,
    p.category,
    p.product_name,
    ob.total_orders,
    ob.total_units_sold,
    ROUND(ob.total_revenue_usd::NUMERIC, 2)                 AS total_revenue_usd,
    COUNT(r.return_id)                                      AS total_returns,
    SUM(r.return_quantity)                                  AS total_return_units,
    ROUND(COUNT(r.return_id)*100.0 /
          NULLIF(ob.total_orders,0), 2)                     AS return_rate_pct,
    ROUND(SUM(r.net_loss_usd)::NUMERIC, 2)                  AS total_return_loss_usd,
    ROUND(SUM(r.refund_amount_usd)::NUMERIC, 2)             AS total_refund_usd,
    ROUND(SUM(r.return_shipping_cost_usd)::NUMERIC, 2)      AS total_return_ship_usd,
    ROUND(AVG(r.days_to_return)::NUMERIC, 1)                AS avg_days_to_return,
    MODE() WITHIN GROUP (ORDER BY r.return_reason)          AS top_return_reason,
    ROUND(SUM(r.net_loss_usd) /
          NULLIF(ob.total_revenue_usd,0)*100::NUMERIC, 2)   AS return_loss_pct_of_revenue,
    CASE
        WHEN SUM(r.net_loss_usd)/NULLIF(ob.total_revenue_usd,0) > 0.08
             THEN '🔴 Critical Return Problem'
        WHEN SUM(r.net_loss_usd)/NULLIF(ob.total_revenue_usd,0) > 0.04
             THEN '🟡 Elevated — Investigate'
        ELSE '🟢 Acceptable'
    END AS return_risk_flag
FROM fact_returns r
JOIN dim_markets  m  ON r.market_id  = m.market_id
JOIN dim_products p  ON r.product_id = p.product_id
JOIN order_base   ob ON r.market_id  = ob.market_id
                     AND r.product_id = ob.product_id
GROUP BY m.country, m.region, p.category, p.product_name,
         ob.total_orders, ob.total_units_sold, ob.total_revenue_usd
ORDER BY total_return_loss_usd DESC;

SELECT * FROM vw_returns_analysis;


-- ────────────────────────────────────────────────────────────
-- QUERY 9 · Forex Impact on Export Revenue
-- How much is currency movement affecting INR realisation?
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW vw_forex_impact AS
WITH monthly_fx AS (
    SELECT
        currency_code,
        year,
        month,
        ROUND(AVG(rate_to_inr)::NUMERIC, 4)  AS avg_monthly_rate,
        ROUND(MIN(rate_to_inr)::NUMERIC, 4)  AS min_rate,
        ROUND(MAX(rate_to_inr)::NUMERIC, 4)  AS max_rate,
        ROUND((MAX(rate_to_inr) -
               MIN(rate_to_inr))::NUMERIC, 4) AS monthly_volatility
    FROM dim_forex_rates
    GROUP BY currency_code, year, month
),
monthly_orders AS (
    SELECT
        o.fiscal_year                                      AS year,
        o.month_num                                        AS month,
        m.currency,
        ROUND(SUM(o.total_fob_value_usd)::NUMERIC, 2)     AS revenue_usd,
        ROUND(SUM(o.total_fob_value_inr)::NUMERIC, 2)     AS revenue_inr_actual,
        ROUND(AVG(o.exchange_rate_on_date)::NUMERIC, 4)   AS avg_rate_used
    FROM fact_export_orders o
    JOIN dim_markets m ON o.market_id = m.market_id
    WHERE o.order_status != 'Cancelled'
    GROUP BY o.fiscal_year, o.month_num, m.currency
)
SELECT
    mo.year,
    mo.month,
    mo.currency,
    mo.revenue_usd,
    mo.revenue_inr_actual,
    mo.avg_rate_used,
    fx.avg_monthly_rate                                    AS benchmark_rate,
    fx.monthly_volatility,
    ROUND((mo.revenue_usd *
           fx.avg_monthly_rate)::NUMERIC, 2)               AS revenue_inr_at_benchmark,
    ROUND((mo.revenue_inr_actual -
           mo.revenue_usd * fx.avg_monthly_rate
          )::NUMERIC, 2)                                   AS forex_gain_loss_inr,
    ROUND((mo.revenue_inr_actual -
           mo.revenue_usd * fx.avg_monthly_rate) /
           NULLIF(mo.revenue_usd * fx.avg_monthly_rate,0)
           * 100::NUMERIC, 2)                              AS forex_impact_pct,
    CASE
        WHEN (mo.revenue_inr_actual -
              mo.revenue_usd * fx.avg_monthly_rate) > 0
             THEN 'Forex Gain'
        ELSE 'Forex Loss'
    END AS forex_direction
FROM monthly_orders mo
LEFT JOIN monthly_fx fx
       ON mo.currency = fx.currency_code
      AND mo.year     = fx.year
      AND mo.month    = fx.month
ORDER BY mo.year, mo.month, mo.currency;

SELECT * FROM vw_forex_impact;


-- ────────────────────────────────────────────────────────────
-- QUERY 10 · Channel Performance Analysis
-- Which sales channel drives best margin?
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW vw_channel_performance AS
SELECT
    order_channel,
    fiscal_year,
    quarter,
    COUNT(order_id)                                         AS total_orders,
    SUM(quantity_units)                                     AS total_units,
    ROUND(SUM(total_fob_value_usd)::NUMERIC, 2)             AS total_revenue_usd,
    ROUND(SUM(gross_profit_inr)::NUMERIC, 2)                AS total_gp_inr,
    ROUND(AVG(gross_margin_pct)::NUMERIC, 2)                AS avg_margin_pct,
    ROUND(AVG(discount_pct)::NUMERIC, 2)                    AS avg_discount_pct,
    ROUND(SUM(total_fob_value_usd) /
          NULLIF(COUNT(order_id),0), 2)                     AS avg_order_value_usd,
    COUNT(DISTINCT customer_id)                             AS unique_customers,
    COUNT(DISTINCT market_id)                               AS markets_reached,
    COUNT(CASE WHEN is_repeat_order='Y' THEN 1 END)         AS repeat_orders,
    ROUND(COUNT(CASE WHEN is_repeat_order='Y'
                     THEN 1 END)*100.0 /
          NULLIF(COUNT(order_id),0), 2)                     AS repeat_rate_pct,
    COUNT(CASE WHEN payment_status='Overdue'
               THEN 1 END)                                  AS overdue_orders,
    COUNT(CASE WHEN order_status='Cancelled'
               THEN 1 END)                                  AS cancelled_orders,
    ROUND(COUNT(CASE WHEN order_status='Cancelled'
                     THEN 1 END)*100.0 /
          NULLIF(COUNT(order_id),0), 2)                     AS cancellation_rate_pct
FROM fact_export_orders
GROUP BY order_channel, fiscal_year, quarter
ORDER BY fiscal_year, total_revenue_usd DESC;

SELECT * FROM vw_channel_performance;


-- ────────────────────────────────────────────────────────────
-- QUERY 11 · Demand Intelligence by Market
-- Where is home decor demand growing fastest?
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW vw_demand_intelligence AS
SELECT
    m.country,
    m.region,
    p.category,
    p.product_name,
    d.fiscal_year,
    d.quarter,
    ROUND(AVG(d.google_trends_score)::NUMERIC, 1)           AS avg_trends_score,
    SUM(d.daily_search_volume)                              AS total_search_volume,
    ROUND(AVG(d.yoy_search_growth_pct)::NUMERIC, 2)         AS avg_yoy_growth_pct,
    SUM(d.social_media_mentions)                            AS total_social_mentions,
    ROUND(AVG(d.amazon_bsr)::NUMERIC, 0)                    AS avg_amazon_bsr,
    SUM(d.estimated_demand_units)                           AS total_demand_units,
    ROUND(AVG(d.seasonality_index)::NUMERIC, 3)             AS avg_seasonality_index,
    CASE
        WHEN AVG(d.yoy_search_growth_pct) > 25 THEN '🚀 High Growth'
        WHEN AVG(d.yoy_search_growth_pct) > 10 THEN '📈 Growing'
        WHEN AVG(d.yoy_search_growth_pct) >  0 THEN '➡️ Stable'
        ELSE '📉 Declining'
    END AS demand_classification
FROM fact_market_demand d
JOIN dim_markets  m ON d.market_id  = m.market_id
JOIN dim_products p ON d.product_id = p.product_id
GROUP BY m.country, m.region, p.category,
         p.product_name, d.fiscal_year, d.quarter
ORDER BY avg_yoy_growth_pct DESC;

SELECT * FROM vw_demand_intelligence;


-- ────────────────────────────────────────────────────────────
-- QUERY 12 · Payment Risk & DSO Analysis
-- Which markets are payment risks?
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW vw_payment_risk AS
SELECT
    m.country,
    m.region,
    m.payment_risk_score,
    m.primary_marketplace,
    COUNT(o.order_id)                                       AS total_orders,
    ROUND(SUM(o.total_fob_value_usd)::NUMERIC, 2)           AS total_billed_usd,
    COUNT(CASE WHEN o.payment_status='Paid On Time'
               THEN 1 END)                                  AS paid_on_time,
    COUNT(CASE WHEN o.payment_status='Paid Late'
               THEN 1 END)                                  AS paid_late,
    COUNT(CASE WHEN o.payment_status='Pending'
               THEN 1 END)                                  AS pending,
    COUNT(CASE WHEN o.payment_status='Overdue'
               THEN 1 END)                                  AS overdue,
    ROUND(SUM(CASE WHEN o.payment_status='Overdue'
                   THEN o.total_fob_value_usd ELSE 0
              END)::NUMERIC, 2)                             AS overdue_value_usd,
    ROUND(COUNT(CASE WHEN o.payment_status='Paid On Time'
                     THEN 1 END)*100.0 /
          NULLIF(COUNT(o.order_id),0), 2)                   AS on_time_rate_pct,
    ROUND(AVG(o.payment_delay_days)::NUMERIC, 1)            AS avg_delay_days,
    ROUND(AVG(o.payment_terms_days +
              GREATEST(o.payment_delay_days,0))::NUMERIC,1) AS avg_dso_days,
    CASE
        WHEN AVG(o.payment_delay_days) > 20 THEN '🔴 High Risk'
        WHEN AVG(o.payment_delay_days) > 10 THEN '🟡 Moderate Risk'
        ELSE '🟢 Low Risk'
    END AS payment_risk_flag
FROM fact_export_orders o
JOIN dim_markets m ON o.market_id = m.market_id
GROUP BY m.country, m.region, m.payment_risk_score, m.primary_marketplace
ORDER BY overdue_value_usd DESC;

SELECT * FROM vw_payment_risk;


-- ────────────────────────────────────────────────────────────
-- QUERY 13 · Seasonal Export Intelligence
-- When to ramp production for each market?
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW vw_seasonal_intelligence AS
SELECT
    m.country,
    m.region,
    p.category,
    o.month_num,
    o.month_name,
    o.fiscal_year,
    COUNT(o.order_id)                                       AS total_orders,
    SUM(o.quantity_units)                                   AS total_units,
    ROUND(SUM(o.total_fob_value_usd)::NUMERIC, 2)           AS total_revenue_usd,
    ROUND(AVG(o.gross_margin_pct)::NUMERIC, 2)              AS avg_margin_pct,
    RANK() OVER (
        PARTITION BY m.country, p.category, o.fiscal_year
        ORDER BY SUM(o.total_fob_value_usd) DESC
    )                                                       AS revenue_rank_in_year,
    CASE
        WHEN o.month_num IN (11,12,1)  THEN '🎄 Peak Season'
        WHEN o.month_num IN (9,10)     THEN '📦 Pre-Peak Ramp'
        WHEN o.month_num IN (6,7,8)    THEN '☀️ Summer Slow'
        ELSE '➡️ Normal'
    END AS season_tag
FROM fact_export_orders  o
JOIN dim_markets         m ON o.market_id  = m.market_id
JOIN dim_products        p ON o.product_id = p.product_id
WHERE o.order_status != 'Cancelled'
GROUP BY m.country, m.region, p.category,
         o.month_num, o.month_name, o.fiscal_year
ORDER BY m.country, o.fiscal_year, o.month_num;

SELECT * FROM vw_seasonal_intelligence;


-- ────────────────────────────────────────────────────────────
-- QUERY 14 · Category Revenue Mix & Concentration Risk
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW vw_category_mix AS
WITH total AS (
    SELECT SUM(total_fob_value_usd) AS grand_total
    FROM fact_export_orders
    WHERE order_status != 'Cancelled'
)
SELECT
    p.category,
    p.subcategory,
    COUNT(DISTINCT p.product_id)                            AS product_count,
    COUNT(o.order_id)                                       AS total_orders,
    SUM(o.quantity_units)                                   AS total_units,
    ROUND(SUM(o.total_fob_value_usd)::NUMERIC, 2)           AS total_revenue_usd,
    ROUND(SUM(o.gross_profit_inr)::NUMERIC, 2)              AS total_gp_inr,
    ROUND(AVG(o.gross_margin_pct)::NUMERIC, 2)              AS avg_margin_pct,
    ROUND(SUM(o.total_fob_value_usd) /
          (SELECT grand_total FROM total)*100, 2)           AS revenue_share_pct,
    COUNT(DISTINCT o.market_id)                             AS markets_served,
    COUNT(DISTINCT o.customer_id)                           AS unique_buyers,
    ROUND(POWER(SUM(o.total_fob_value_usd) /
          (SELECT grand_total FROM total)*100, 2)
          ::NUMERIC, 2)                                     AS hhi_contribution
FROM fact_export_orders o
JOIN dim_products p ON o.product_id = p.product_id
WHERE o.order_status != 'Cancelled'
GROUP BY p.category, p.subcategory
ORDER BY total_revenue_usd DESC;

SELECT * FROM vw_category_mix;


-- ────────────────────────────────────────────────────────────
-- QUERY 15 · Master Export Summary (One-page KPI view)
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW vw_master_summary AS
SELECT
    'NestKraft Overall'                                        AS summary_level,
    ROUND(SUM(o.total_fob_value_usd)::NUMERIC, 2)             AS total_revenue_usd,
    ROUND(SUM(o.total_fob_value_inr)/10000000::NUMERIC, 2)    AS total_revenue_cr_inr,
    ROUND(AVG(o.gross_margin_pct)::NUMERIC, 2)                AS avg_gross_margin_pct,
    COUNT(o.order_id)                                         AS total_orders,
    SUM(o.quantity_units)                                     AS total_units_exported,
    COUNT(DISTINCT o.customer_id)                             AS total_buyers,
    COUNT(DISTINCT o.market_id)                               AS markets_active,
    COUNT(DISTINCT o.product_id)                              AS products_exported,
    COUNT(CASE WHEN o.payment_status='Overdue' THEN 1 END)    AS overdue_orders,
    ROUND(SUM(CASE WHEN o.payment_status='Overdue'
                   THEN o.total_fob_value_usd ELSE 0
              END)::NUMERIC, 2)                               AS overdue_value_usd,
    COUNT(DISTINCT s.shipment_id)                             AS total_shipments,
    ROUND(AVG(s.total_transit_days)::NUMERIC, 1)              AS avg_transit_days,
    ROUND(SUM(s.demurrage_charges_usd)::NUMERIC, 2)           AS total_demurrage_usd,
    COUNT(DISTINCT r.return_id)                               AS total_returns,
    ROUND(SUM(r.net_loss_usd)::NUMERIC, 2)                    AS total_return_loss_usd
FROM fact_export_orders   o
LEFT JOIN fact_shipment_details s ON o.order_id = s.order_id
LEFT JOIN fact_returns          r ON o.order_id = r.order_id
WHERE o.order_status != 'Cancelled';

SELECT * FROM vw_master_summary;