-- ============================================================
-- NestKraft Export Intelligence — Database Schema
-- PostgreSQL | 9 Tables | Pre-verified financial logic
-- ============================================================

DROP TABLE IF EXISTS fact_returns             CASCADE;
DROP TABLE IF EXISTS fact_market_demand       CASCADE;
DROP TABLE IF EXISTS fact_competitor_pricing  CASCADE;
DROP TABLE IF EXISTS fact_shipment_details    CASCADE;
DROP TABLE IF EXISTS fact_export_orders       CASCADE;
DROP TABLE IF EXISTS dim_forex_rates          CASCADE;
DROP TABLE IF EXISTS dim_customers            CASCADE;
DROP TABLE IF EXISTS dim_markets              CASCADE;
DROP TABLE IF EXISTS dim_products             CASCADE;

-- ────────────────────────────────────────────────────────────
-- DIM_PRODUCTS
-- ────────────────────────────────────────────────────────────
CREATE TABLE dim_products (
    product_id              VARCHAR(10)   PRIMARY KEY,
    sku_code                VARCHAR(20),
    product_name            VARCHAR(120)  NOT NULL,
    category                VARCHAR(30),
    subcategory             VARCHAR(50),
    brand_name              VARCHAR(50),
    weight_kg               NUMERIC(8,3),
    cogs_inr                NUMERIC(10,2),
    india_mrp_inr           NUMERIC(10,2),
    fob_price_usd           NUMERIC(10,4),
    gross_margin_pct_india  NUMERIC(5,1),
    primary_material        VARCHAR(40),
    is_handmade             CHAR(1),
    is_sustainable          CHAR(1),
    is_export_ready         CHAR(1),
    moq_units               INTEGER,
    monthly_capacity_units  INTEGER,
    lead_time_days          INTEGER,
    shelf_life_months       INTEGER,
    hsn_code                VARCHAR(15),
    marketplace_presence    VARCHAR(40),
    product_launch_date     DATE,
    is_active               CHAR(1),
    artisan_cluster         VARCHAR(30)
);

-- ────────────────────────────────────────────────────────────
-- DIM_MARKETS
-- ────────────────────────────────────────────────────────────
CREATE TABLE dim_markets (
    market_id                   VARCHAR(5)    PRIMARY KEY,
    country                     VARCHAR(50)   NOT NULL,
    region                      VARCHAR(30),
    currency                    VARCHAR(5),
    exchange_rate_to_inr        NUMERIC(10,4),
    home_decor_demand_score     NUMERIC(4,1),
    market_entry_ease_score     NUMERIC(4,1),
    regulatory_ease_score       NUMERIC(4,1),
    indian_craft_affinity_score NUMERIC(4,1),
    ecommerce_maturity_score    NUMERIC(4,1),
    monthly_search_volume_k     INTEGER,
    avg_customs_duty_pct        NUMERIC(5,2),
    amazon_presence             CHAR(1),
    primary_marketplace         VARCHAR(50),
    gdp_per_capita_usd          INTEGER,
    market_size_usd_mn          NUMERIC(10,2),
    population_mn               NUMERIC(8,2),
    avg_transit_days_from_india INTEGER,
    composite_score             NUMERIC(4,2),
    market_tier                 VARCHAR(10),
    vat_gst_pct                 NUMERIC(5,2),
    payment_risk_score          NUMERIC(4,1)
);

-- ────────────────────────────────────────────────────────────
-- DIM_CUSTOMERS
-- ────────────────────────────────────────────────────────────
CREATE TABLE dim_customers (
    customer_id             VARCHAR(10)   PRIMARY KEY,
    company_name            VARCHAR(100),
    customer_type           VARCHAR(50),
    market_id               VARCHAR(5)    REFERENCES dim_markets(market_id),
    country                 VARCHAR(50),
    city                    VARCHAR(50),
    customer_tier           VARCHAR(20),
    credit_limit_usd        NUMERIC(12,2),
    payment_terms_days      INTEGER,
    preferred_categories    VARCHAR(100),
    primary_platform        VARCHAR(50),
    account_manager         VARCHAR(50),
    registration_date       DATE,
    is_active               CHAR(1),
    annual_revenue_band     VARCHAR(20),
    lifetime_orders         INTEGER,
    avg_order_value_usd     NUMERIC(12,2)
);

-- ────────────────────────────────────────────────────────────
-- DIM_FOREX_RATES
-- ────────────────────────────────────────────────────────────
CREATE TABLE dim_forex_rates (
    date              DATE,
    currency_code     VARCHAR(5),
    currency_pair     VARCHAR(10),
    rate_to_inr       NUMERIC(10,4),
    daily_change_pct  NUMERIC(8,4),
    month             INTEGER,
    year              INTEGER,
    quarter           VARCHAR(5),
    PRIMARY KEY (date, currency_code)
);

-- ────────────────────────────────────────────────────────────
-- FACT_EXPORT_ORDERS
-- ────────────────────────────────────────────────────────────
CREATE TABLE fact_export_orders (
    order_id                  BIGINT        PRIMARY KEY,
    order_date                DATE,
    product_id                VARCHAR(10)   REFERENCES dim_products(product_id),
    market_id                 VARCHAR(5)    REFERENCES dim_markets(market_id),
    customer_id               VARCHAR(10)   REFERENCES dim_customers(customer_id),
    quantity_units            INTEGER,
    fob_price_usd_per_unit    NUMERIC(10,4),
    discount_pct              NUMERIC(5,2),
    net_fob_price_usd         NUMERIC(10,4),
    total_fob_value_usd       NUMERIC(14,2),
    exchange_rate_on_date     NUMERIC(10,4),
    total_fob_value_inr       NUMERIC(16,2),
    cogs_inr_per_unit         NUMERIC(10,2),
    total_cogs_inr            NUMERIC(16,2),
    gross_profit_inr          NUMERIC(16,2),
    gross_margin_pct          NUMERIC(7,2),
    payment_terms_days        INTEGER,
    payment_due_date          DATE,
    payment_received_date     DATE,
    payment_delay_days        INTEGER,
    payment_status            VARCHAR(20),
    order_channel             VARCHAR(30),
    order_status              VARCHAR(20),
    incoterm                  VARCHAR(5),
    fiscal_year               INTEGER,
    quarter                   VARCHAR(5),
    month_num                 INTEGER,
    month_name                VARCHAR(15),
    week_num                  INTEGER,
    is_repeat_order           CHAR(1),
    days_since_last_order     INTEGER
);

-- ────────────────────────────────────────────────────────────
-- FACT_SHIPMENT_DETAILS
-- ────────────────────────────────────────────────────────────
CREATE TABLE fact_shipment_details (
    shipment_id               BIGINT        PRIMARY KEY,
    order_id                  BIGINT        REFERENCES fact_export_orders(order_id),
    product_id                VARCHAR(10)   REFERENCES dim_products(product_id),
    market_id                 VARCHAR(5)    REFERENCES dim_markets(market_id),
    customer_id               VARCHAR(10)   REFERENCES dim_customers(customer_id),
    shipment_date             DATE,
    estimated_arrival_date    DATE,
    actual_arrival_date       DATE,
    delay_days                INTEGER,
    is_delayed                CHAR(1),
    shipping_line             VARCHAR(30),
    port_of_loading           VARCHAR(50),
    port_of_discharge         VARCHAR(80),
    container_type            VARCHAR(30),
    hsn_code                  VARCHAR(15),
    declared_value_usd        NUMERIC(14,2),
    freight_cost_usd          NUMERIC(12,2),
    insurance_usd             NUMERIC(12,2),
    cif_value_usd             NUMERIC(14,2),
    customs_duty_usd          NUMERIC(12,2),
    port_handling_usd         NUMERIC(12,2),
    last_mile_usd             NUMERIC(12,2),
    demurrage_charges_usd     NUMERIC(12,2),
    total_landed_cost_usd     NUMERIC(14,2),
    landed_to_fob_ratio       NUMERIC(8,4),
    total_transit_days        INTEGER,
    customs_clearance_days    INTEGER,
    shipment_status           VARCHAR(20),
    delay_reason              VARCHAR(50),
    exporter_net_margin_usd   NUMERIC(14,2)
);

-- ────────────────────────────────────────────────────────────
-- FACT_COMPETITOR_PRICING
-- ────────────────────────────────────────────────────────────
CREATE TABLE fact_competitor_pricing (
    date                            DATE,
    product_id                      VARCHAR(10)  REFERENCES dim_products(product_id),
    market_id                       VARCHAR(5)   REFERENCES dim_markets(market_id),
    our_price_usd                   NUMERIC(10,2),
    competitor1_price_usd           NUMERIC(10,2),
    competitor2_price_usd           NUMERIC(10,2),
    competitor3_price_usd           NUMERIC(10,2),
    avg_competitor_price_usd        NUMERIC(10,2),
    our_price_index                 NUMERIC(6,1),
    price_competitiveness           VARCHAR(20),
    landed_cost_usd                 NUMERIC(10,4),
    gross_margin_on_our_price_pct   NUMERIC(7,2),
    month_num                       INTEGER,
    fiscal_year                     INTEGER,
    PRIMARY KEY (date, product_id, market_id)
);

-- ────────────────────────────────────────────────────────────
-- FACT_MARKET_DEMAND
-- ────────────────────────────────────────────────────────────
CREATE TABLE fact_market_demand (
    date                    DATE,
    product_id              VARCHAR(10)  REFERENCES dim_products(product_id),
    market_id               VARCHAR(5)   REFERENCES dim_markets(market_id),
    google_trends_score     NUMERIC(5,1),
    daily_search_volume     INTEGER,
    yoy_search_growth_pct   NUMERIC(7,2),
    social_media_mentions   INTEGER,
    amazon_bsr              INTEGER,
    estimated_demand_units  INTEGER,
    seasonality_index       NUMERIC(5,3),
    demand_trend            VARCHAR(15),
    month_num               INTEGER,
    fiscal_year             INTEGER,
    quarter                 VARCHAR(5),
    PRIMARY KEY (date, product_id, market_id)
);

-- ────────────────────────────────────────────────────────────
-- FACT_RETURNS
-- ────────────────────────────────────────────────────────────
CREATE TABLE fact_returns (
    return_id                 BIGINT        PRIMARY KEY,
    order_id                  BIGINT        REFERENCES fact_export_orders(order_id),
    product_id                VARCHAR(10)   REFERENCES dim_products(product_id),
    market_id                 VARCHAR(5)    REFERENCES dim_markets(market_id),
    customer_id               VARCHAR(10)   REFERENCES dim_customers(customer_id),
    return_date               DATE,
    return_quantity           INTEGER,
    return_reason             VARCHAR(60),
    return_type               VARCHAR(20),
    refund_amount_usd         NUMERIC(12,2),
    return_shipping_cost_usd  NUMERIC(10,2),
    net_loss_usd              NUMERIC(12,2),
    processing_status         VARCHAR(30),
    days_to_return            INTEGER,
    is_first_return           CHAR(1),
    return_month              INTEGER,
    return_year               INTEGER
);

-- ────────────────────────────────────────────────────────────
-- INDEXES
-- ────────────────────────────────────────────────────────────
CREATE INDEX idx_orders_date     ON fact_export_orders(order_date);
CREATE INDEX idx_orders_product  ON fact_export_orders(product_id);
CREATE INDEX idx_orders_market   ON fact_export_orders(market_id);
CREATE INDEX idx_orders_customer ON fact_export_orders(customer_id);
CREATE INDEX idx_orders_status   ON fact_export_orders(order_status);
CREATE INDEX idx_orders_channel  ON fact_export_orders(order_channel);
CREATE INDEX idx_orders_year     ON fact_export_orders(fiscal_year);
CREATE INDEX idx_ship_date       ON fact_shipment_details(shipment_date);
CREATE INDEX idx_ship_market     ON fact_shipment_details(market_id);
CREATE INDEX idx_ship_delayed    ON fact_shipment_details(is_delayed);
CREATE INDEX idx_ship_line       ON fact_shipment_details(shipping_line);
CREATE INDEX idx_pricing_date    ON fact_competitor_pricing(date);
CREATE INDEX idx_pricing_product ON fact_competitor_pricing(product_id);
CREATE INDEX idx_demand_date     ON fact_market_demand(date);
CREATE INDEX idx_demand_product  ON fact_market_demand(product_id);
CREATE INDEX idx_demand_market   ON fact_market_demand(market_id);
CREATE INDEX idx_returns_product ON fact_returns(product_id);
CREATE INDEX idx_returns_market  ON fact_returns(market_id);
CREATE INDEX idx_forex_date      ON dim_forex_rates(date);
CREATE INDEX idx_forex_currency  ON dim_forex_rates(currency_code);

SELECT 'NestKraft schema created successfully ✅' AS status;