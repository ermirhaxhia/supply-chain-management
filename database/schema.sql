-- ============================================================
-- SUPPLY CHAIN MANAGEMENT
-- Database Schema - PostgreSQL / Supabase
--
-- v2 (2026) — Rindërtuar për t'u përputhur me atë çka kodi
-- REALISHT lexon/shkruan (jo me versionin origjinal të projektit).
-- Për historikun e ndryshimeve shih database/MIGRATION_v2.sql —
-- ky file këtu është "fresh install": e krijon gjithçka nga zero
-- në një projekt bosh Supabase.
-- ============================================================

-- ============================================================
-- GRUP 1: TABELA REFERENCE (8 tabela) — të pandryshuara strukturalisht
-- ============================================================

CREATE TABLE product_categories (
    category_id     VARCHAR(10) PRIMARY KEY,
    category_name   VARCHAR(100) NOT NULL,
    parent_category VARCHAR(50) NOT NULL,
    perishable      BOOLEAN DEFAULT FALSE,
    avg_margin_pct  FLOAT NOT NULL
);

CREATE TABLE suppliers (
    supplier_id       VARCHAR(10) PRIMARY KEY,
    supplier_name     VARCHAR(100) NOT NULL,
    city              VARCHAR(50) NOT NULL,
    latitude          FLOAT NOT NULL,
    longitude         FLOAT NOT NULL,
    avg_lead_days     INT NOT NULL,
    lead_variance     FLOAT NOT NULL,
    defect_rate_pct   FLOAT DEFAULT 0.02,
    payment_terms     INT DEFAULT 30,
    reliability_score FLOAT DEFAULT 85.0
);

CREATE TABLE products (
    product_id      VARCHAR(10) PRIMARY KEY,
    product_name    VARCHAR(100) NOT NULL,
    category_id     VARCHAR(10) NOT NULL REFERENCES product_categories(category_id),
    supplier_id     VARCHAR(10) NOT NULL REFERENCES suppliers(supplier_id),
    unit_price      FLOAT NOT NULL,
    unit_cost       FLOAT NOT NULL,
    weight_kg       FLOAT NOT NULL,
    shelf_life_days INT DEFAULT NULL,
    min_stock       INT NOT NULL,
    max_stock       INT NOT NULL,
    reorder_point   INT NOT NULL,
    reorder_qty     INT NOT NULL,
    abc_class       CHAR(1) DEFAULT 'C',
    xyz_class       CHAR(1) DEFAULT 'X'
);

-- FIX: schema.sql origjinal nuk kishte fare lambda_base/lambda_final/
-- population_factor/size_factor/location_factor, ndërkohë që demand_profile.py,
-- streamlit/app.py, dhe README i presin. Verifikuar drejtpërdrejt kundrejt
-- bazës live (REST API) — kjo është struktura reale.
CREATE TABLE stores (
    store_id          VARCHAR(10) PRIMARY KEY,
    store_name        VARCHAR(100) NOT NULL,
    city              VARCHAR(50) NOT NULL,
    latitude          FLOAT NOT NULL,
    longitude         FLOAT NOT NULL,
    opening_hour      INT DEFAULT 6,
    closing_hour      INT DEFAULT 22,
    size_m2           FLOAT NOT NULL,
    manager_id        VARCHAR(10),
    lambda_base       FLOAT NOT NULL DEFAULT 40.0,
    population_factor FLOAT DEFAULT 1.0,
    size_factor       FLOAT DEFAULT 1.0,
    location_factor   FLOAT DEFAULT 1.0,
    lambda_final      FLOAT NOT NULL DEFAULT 40.0
);

CREATE TABLE warehouses (
    warehouse_id     VARCHAR(10) PRIMARY KEY,
    warehouse_name   VARCHAR(100) NOT NULL,
    type             VARCHAR(20) NOT NULL,
    city             VARCHAR(50) NOT NULL,
    latitude         FLOAT NOT NULL,
    longitude        FLOAT NOT NULL,
    capacity_m3      FLOAT NOT NULL,
    temperature_zone VARCHAR(20) DEFAULT 'Ambient',
    monthly_cost     FLOAT NOT NULL
);

CREATE TABLE vehicles (
    vehicle_id        VARCHAR(10) PRIMARY KEY,
    plate             VARCHAR(20) NOT NULL,
    type              VARCHAR(20) NOT NULL,
    capacity_kg       FLOAT NOT NULL,
    capacity_m3       FLOAT NOT NULL,
    fuel_type         VARCHAR(20) DEFAULT 'Diesel',
    consumption_l_km  FLOAT NOT NULL,
    warehouse_id      VARCHAR(10) NOT NULL REFERENCES warehouses(warehouse_id)
);

CREATE TABLE drivers (
    driver_id       VARCHAR(10) PRIMARY KEY,
    name            VARCHAR(100) NOT NULL,
    warehouse_id    VARCHAR(10) NOT NULL REFERENCES warehouses(warehouse_id),
    status          VARCHAR(20) DEFAULT 'Available'
);

CREATE TABLE routes (
    route_id        VARCHAR(10) PRIMARY KEY,
    warehouse_id    VARCHAR(10) NOT NULL REFERENCES warehouses(warehouse_id),
    store_id        VARCHAR(10) NOT NULL REFERENCES stores(store_id),
    distance_km     FLOAT NOT NULL,
    duration_min    INT NOT NULL,
    road_type       VARCHAR(20) DEFAULT 'Urban'
);

-- ============================================================
-- GRUP 2: TABELA KONTROLLI / KONFIGURIMI (3 tabela) — TË REJA
-- Këto ekzistonin tashmë në DB-në live (kodi i përdor kudo) por
-- nuk ishin kurrë të dokumentuara këtu.
-- ============================================================

-- Lexohet nga demand_profile.get_config() / load_simulation_config()
-- dhe përditësohet nga marketing/purchasing/transport module.
-- id shtohet sepse api/main.py /health bën select("id") mbi këtë tabelë.
CREATE TABLE simulation_config (
    id              SERIAL PRIMARY KEY,
    config_key      VARCHAR(50) UNIQUE NOT NULL,
    config_value    FLOAT NOT NULL DEFAULT 0.0,
    description     TEXT,
    updated_at      TIMESTAMP DEFAULT now()
);

-- Flag "u ekzekutua sot?" për module që duhen 1x/ditë
-- (marketing, transport, purchasing_{store_id}).
-- SHËNIM: kodi aktual (scheduler.py) përdor .update() në vend të
-- upsert — nëse rreshti s'ekziston, update-i nuk bën asgjë (bug).
-- Do rregullohet në fazën e debug-imit; struktura këtu e mbështet
-- UPSERT (key është UNIQUE) sapo të ndreqet kodi.
CREATE TABLE run_log (
    key             VARCHAR(50) PRIMARY KEY,
    last_run        DATE
);

-- Lexohet nga marketing_module.check_upcoming_holidays() për
-- kampanja të dedikuara për festa. holiday_id është PK (jo date),
-- verifikuar kundrejt bazës live.
CREATE TABLE holidays (
    holiday_id      VARCHAR(10) PRIMARY KEY,
    date            DATE NOT NULL,
    name            VARCHAR(100) NOT NULL,
    category_ids    VARCHAR(200)
);

-- ============================================================
-- GRUP 3: TABELA OPERATIVE (8 tabela)
-- ============================================================

-- RISTRUKTURUAR: transaction = header i faturës (produktet individuale
-- shkojnë te sales_hourly). Kolonat e vjetra product_id/quantity/
-- unit_price/discount_pct/total janë hequr sepse sales_module.py nuk
-- i shkruan më — janë zëvendësuar nga modeli me cogs/net_revenue.
CREATE TABLE transactions (
    transaction_id   VARCHAR(20) PRIMARY KEY,
    store_id         VARCHAR(10) NOT NULL REFERENCES stores(store_id),
    timestamp        TIMESTAMP NOT NULL,
    customer_type    VARCHAR(20) DEFAULT 'Normal',
    payment_method   VARCHAR(20) DEFAULT 'Cash',
    promotion_id     VARCHAR(20) DEFAULT NULL,
    total_items      INT NOT NULL,
    revenue          FLOAT NOT NULL,
    discount_amount  FLOAT DEFAULT 0.0,
    net_revenue      FLOAT NOT NULL,
    cogs             FLOAT NOT NULL,
    gross_profit     FLOAT NOT NULL
);

CREATE TABLE inventory_log (
    log_id          VARCHAR(15) PRIMARY KEY,
    store_id        VARCHAR(10) NOT NULL REFERENCES stores(store_id),
    product_id      VARCHAR(10) NOT NULL REFERENCES products(product_id),
    timestamp       TIMESTAMP NOT NULL,
    stock_before    INT NOT NULL,
    stock_after     INT NOT NULL,
    change_reason   VARCHAR(20) NOT NULL
);

CREATE TABLE shipments (
    shipment_id     VARCHAR(15) PRIMARY KEY,
    route_id        VARCHAR(10) NOT NULL REFERENCES routes(route_id),
    vehicle_id      VARCHAR(10) NOT NULL REFERENCES vehicles(vehicle_id),
    driver_id       VARCHAR(10) NOT NULL REFERENCES drivers(driver_id),
    departure_time  TIMESTAMP NOT NULL,
    actual_arrival  TIMESTAMP,
    delay_minutes   INT DEFAULT 0,
    units_delivered INT NOT NULL,
    load_kg         FLOAT NOT NULL,
    fuel_consumed   FLOAT NOT NULL,
    fuel_price      FLOAT NOT NULL,
    transport_cost  FLOAT NOT NULL,
    status          VARCHAR(20) DEFAULT 'In Transit'
);

CREATE TABLE purchase_orders (
    po_id           VARCHAR(15) PRIMARY KEY,
    supplier_id     VARCHAR(10) NOT NULL REFERENCES suppliers(supplier_id),
    product_id      VARCHAR(10) NOT NULL REFERENCES products(product_id),
    warehouse_id    VARCHAR(10) NOT NULL REFERENCES warehouses(warehouse_id),
    order_date      DATE NOT NULL,
    expected_date   DATE NOT NULL,
    actual_date     DATE DEFAULT NULL,
    qty_ordered     INT NOT NULL,
    qty_received    INT DEFAULT 0,
    unit_cost       FLOAT NOT NULL,
    total_cost      FLOAT NOT NULL,
    status          VARCHAR(20) DEFAULT 'Pending'
);

CREATE TABLE warehouse_snapshot (
    snapshot_id      VARCHAR(15) PRIMARY KEY,
    warehouse_id     VARCHAR(10) NOT NULL REFERENCES warehouses(warehouse_id),
    timestamp        TIMESTAMP NOT NULL,
    used_capacity_m3 FLOAT NOT NULL,
    inbound_units    INT DEFAULT 0,
    outbound_units   INT DEFAULT 0,
    labor_hours      FLOAT DEFAULT 0.0,
    orders_processed INT DEFAULT 0
);

CREATE TABLE campaigns (
    campaign_id      VARCHAR(10) PRIMARY KEY,
    campaign_name    VARCHAR(100) NOT NULL,
    type             VARCHAR(20) NOT NULL,
    start_date       DATE NOT NULL,
    end_date         DATE NOT NULL,
    category_id      VARCHAR(10) NOT NULL REFERENCES product_categories(category_id),
    discount_pct     FLOAT NOT NULL,
    cost             FLOAT NOT NULL,
    revenue_lift_pct FLOAT DEFAULT NULL
);

CREATE TABLE fuel_prices (
    date            DATE PRIMARY KEY,
    price_per_liter FLOAT NOT NULL,
    source          VARCHAR(20) DEFAULT 'manual'
);

-- ============================================================
-- GRUP 4: TABELA AGREGAT (8 tabela)
-- ============================================================

-- RISTRUKTURUAR: përputhet me basket_items të sales_module.py
-- dhe me çfarë lexon daily_aggregator.aggregate_sales().
-- RAW — 1 rresht/artikull shportë/orë. Retention e shkurtër
-- (RAW_DATA_RETENTION_DAYS, shih purge_old_raw_data) — buffer para
-- se të shkrihet në sales_hourly_agg, JO arkiv afatgjatë.
CREATE TABLE sales_hourly (
    id                  SERIAL PRIMARY KEY,
    store_id            VARCHAR(10) NOT NULL REFERENCES stores(store_id),
    product_id          VARCHAR(10) NOT NULL REFERENCES products(product_id),
    date                DATE NOT NULL,
    hour                INT NOT NULL,
    units_sold          INT NOT NULL,
    revenue             FLOAT NOT NULL,
    discount_amount     FLOAT DEFAULT 0.0,
    net_revenue         FLOAT NOT NULL,
    cogs                FLOAT NOT NULL,
    gross_profit        FLOAT NOT NULL,
    transactions_count  INT DEFAULT 1
);

-- E RE — 1 rresht/store/ORË (JO/produkt — u provua matematikisht që
-- granulariteti për-produkt prodhon ~3.4 GB/vit, shumë mbi 500MB).
-- Arkivi i vërtetë për ARIMA (1 seri kohore = xhiro/orë), mbahet
-- PËRGJITHMONË. sales_daily/sales_monthly (detaj produkti) ushqehen nga
-- sales_hourly raw (poshtë), jo nga kjo.
CREATE TABLE sales_hourly_agg (
    id                  SERIAL PRIMARY KEY,
    store_id            VARCHAR(10) NOT NULL REFERENCES stores(store_id),
    date                DATE NOT NULL,
    hour                INT NOT NULL,
    transactions_count  INT NOT NULL,
    units_sold          INT NOT NULL,
    revenue             FLOAT NOT NULL,
    discount_amount     FLOAT DEFAULT 0.0,
    net_revenue         FLOAT NOT NULL,
    cogs                FLOAT NOT NULL,
    gross_profit        FLOAT NOT NULL
);

-- RISTRUKTURUAR: përputhet me daily_rows të daily_aggregator.aggregate_sales().
CREATE TABLE sales_daily (
    id                  SERIAL PRIMARY KEY,
    date                DATE NOT NULL,
    store_id            VARCHAR(10) NOT NULL REFERENCES stores(store_id),
    product_id          VARCHAR(10) NOT NULL REFERENCES products(product_id),
    units_sold          INT NOT NULL,
    avg_unit_price      FLOAT DEFAULT 0.0,
    revenue             FLOAT NOT NULL,
    discount_amount     FLOAT DEFAULT 0.0,
    net_revenue         FLOAT NOT NULL,
    cogs                FLOAT NOT NULL,
    gross_profit        FLOAT NOT NULL,
    transactions_count  INT DEFAULT 0
);

-- RISTRUKTURUAR: përputhet me monthly_rows të
-- monthly_aggregator.aggregate_sales_monthly().
CREATE TABLE sales_monthly (
    id                  SERIAL PRIMARY KEY,
    store_id            VARCHAR(10) NOT NULL REFERENCES stores(store_id),
    product_id          VARCHAR(10) NOT NULL REFERENCES products(product_id),
    year                INT NOT NULL,
    month               INT NOT NULL,
    units_sold          INT NOT NULL,
    avg_unit_price      FLOAT DEFAULT 0.0,
    revenue             FLOAT NOT NULL,
    discount_amount     FLOAT DEFAULT 0.0,
    net_revenue         FLOAT NOT NULL,
    cogs                FLOAT NOT NULL,
    gross_profit        FLOAT NOT NULL,
    transactions_count  INT DEFAULT 0
);

-- E RE: transactions → transactions_monthly (funksioni ekziston te
-- monthly_aggregator.py por është shkëputur nga orkestrimi — rilidhet
-- në fazën e debug-imit).
CREATE TABLE transactions_monthly (
    id                     SERIAL PRIMARY KEY,
    store_id               VARCHAR(10) NOT NULL REFERENCES stores(store_id),
    year                   INT NOT NULL,
    month                  INT NOT NULL,
    total_transactions     INT NOT NULL,
    total_items_sold       INT DEFAULT 0,
    avg_basket_value       FLOAT DEFAULT 0.0,
    avg_items_per_basket   FLOAT DEFAULT 0.0,
    cash_count             INT DEFAULT 0,
    card_count             INT DEFAULT 0,
    ewallet_count          INT DEFAULT 0,
    member_count           INT DEFAULT 0,
    normal_count           INT DEFAULT 0,
    total_revenue          FLOAT DEFAULT 0.0,
    total_discount         FLOAT DEFAULT 0.0,
    total_net_revenue      FLOAT DEFAULT 0.0,
    total_cogs             FLOAT DEFAULT 0.0,
    total_gross_profit     FLOAT DEFAULT 0.0
);

CREATE TABLE inventory_daily (
    id              SERIAL PRIMARY KEY,
    date            DATE NOT NULL,
    store_id        VARCHAR(10) NOT NULL REFERENCES stores(store_id),
    product_id      VARCHAR(10) NOT NULL REFERENCES products(product_id),
    avg_stock_level FLOAT NOT NULL,
    min_stock_level INT NOT NULL,
    max_stock_level INT NOT NULL,
    stockout_hours  INT DEFAULT 0,
    expired_units   INT DEFAULT 0,
    restock_count   INT DEFAULT 0
);

-- E RE: inventory_daily → inventory_monthly (funksioni ekziston tashmë
-- te monthly_aggregator.py, tabela thjesht mungonte). total_shrinkage
-- verifikuar kundrejt bazës live — kodi aktual s'e shkruan (DEFAULT 0),
-- mbetet për kompatibilitet me lexues/dashboard të mundshëm.
CREATE TABLE inventory_monthly (
    id              SERIAL PRIMARY KEY,
    store_id        VARCHAR(10) NOT NULL REFERENCES stores(store_id),
    product_id      VARCHAR(10) NOT NULL REFERENCES products(product_id),
    year            INT NOT NULL,
    month           INT NOT NULL,
    avg_stock_level FLOAT NOT NULL,
    min_stock_level INT NOT NULL,
    max_stock_level INT NOT NULL,
    stockout_hours  INT DEFAULT 0,
    expired_units   INT DEFAULT 0,
    restock_count   INT DEFAULT 0,
    total_shrinkage INT DEFAULT 0
);

CREATE TABLE transport_daily (
    id                  SERIAL PRIMARY KEY,
    route_id            VARCHAR(10) NOT NULL REFERENCES routes(route_id),
    date                DATE NOT NULL,
    total_shipments     INT NOT NULL,
    total_units         INT NOT NULL,
    total_cost          FLOAT NOT NULL,
    avg_delay_minutes   FLOAT DEFAULT 0.0,
    on_time_deliveries  INT DEFAULT 0,
    fuel_consumed       FLOAT NOT NULL,
    avg_load_pct        FLOAT NOT NULL
);

CREATE TABLE kpi_monthly (
    id                  SERIAL PRIMARY KEY,
    store_id            VARCHAR(10) NOT NULL REFERENCES stores(store_id),
    year                INT NOT NULL,
    month               INT NOT NULL,
    total_revenue       FLOAT NOT NULL,
    total_cogs          FLOAT NOT NULL,
    gross_margin        FLOAT NOT NULL,
    gross_margin_pct    FLOAT NOT NULL,
    total_transactions  INT NOT NULL,
    avg_basket_value    FLOAT NOT NULL,
    stockout_rate_pct   FLOAT NOT NULL,
    otd_pct             FLOAT NOT NULL,
    avg_lead_time_days  FLOAT NOT NULL,
    transport_cost      FLOAT NOT NULL,
    inventory_cost      FLOAT NOT NULL,
    total_cost          FLOAT NOT NULL,
    net_profit          FLOAT NOT NULL
);

-- ============================================================
-- INDEXES
-- ============================================================

CREATE INDEX idx_transactions_store        ON transactions(store_id);
CREATE INDEX idx_transactions_timestamp    ON transactions(timestamp);
CREATE INDEX idx_inventory_log_store       ON inventory_log(store_id);
CREATE INDEX idx_inventory_log_product     ON inventory_log(product_id);
CREATE INDEX idx_inventory_log_timestamp   ON inventory_log(timestamp);
CREATE INDEX idx_shipments_status          ON shipments(status);
CREATE INDEX idx_sales_hourly_date         ON sales_hourly(date);
CREATE INDEX idx_sales_hourly_store        ON sales_hourly(store_id);
CREATE INDEX idx_sales_hourly_agg_date     ON sales_hourly_agg(date);
CREATE INDEX idx_sales_hourly_agg_store    ON sales_hourly_agg(store_id);
CREATE INDEX idx_sales_daily_date          ON sales_daily(date);
CREATE INDEX idx_sales_monthly_year_month  ON sales_monthly(year, month);
CREATE INDEX idx_inventory_daily_date      ON inventory_daily(date);
CREATE INDEX idx_kpi_monthly_store         ON kpi_monthly(store_id);
CREATE INDEX idx_transactions_monthly_ym   ON transactions_monthly(year, month);
CREATE INDEX idx_inventory_monthly_ym      ON inventory_monthly(year, month);

-- ============================================================
-- ROW LEVEL SECURITY (RLS)
-- ============================================================

ALTER TABLE product_categories   ENABLE ROW LEVEL SECURITY;
ALTER TABLE suppliers            ENABLE ROW LEVEL SECURITY;
ALTER TABLE products             ENABLE ROW LEVEL SECURITY;
ALTER TABLE stores               ENABLE ROW LEVEL SECURITY;
ALTER TABLE warehouses           ENABLE ROW LEVEL SECURITY;
ALTER TABLE vehicles             ENABLE ROW LEVEL SECURITY;
ALTER TABLE drivers              ENABLE ROW LEVEL SECURITY;
ALTER TABLE routes               ENABLE ROW LEVEL SECURITY;
ALTER TABLE simulation_config    ENABLE ROW LEVEL SECURITY;
ALTER TABLE run_log              ENABLE ROW LEVEL SECURITY;
ALTER TABLE holidays             ENABLE ROW LEVEL SECURITY;
ALTER TABLE transactions         ENABLE ROW LEVEL SECURITY;
ALTER TABLE inventory_log        ENABLE ROW LEVEL SECURITY;
ALTER TABLE shipments            ENABLE ROW LEVEL SECURITY;
ALTER TABLE purchase_orders      ENABLE ROW LEVEL SECURITY;
ALTER TABLE warehouse_snapshot   ENABLE ROW LEVEL SECURITY;
ALTER TABLE campaigns            ENABLE ROW LEVEL SECURITY;
ALTER TABLE fuel_prices          ENABLE ROW LEVEL SECURITY;
ALTER TABLE sales_hourly         ENABLE ROW LEVEL SECURITY;
ALTER TABLE sales_hourly_agg     ENABLE ROW LEVEL SECURITY;
ALTER TABLE sales_daily          ENABLE ROW LEVEL SECURITY;
ALTER TABLE sales_monthly        ENABLE ROW LEVEL SECURITY;
ALTER TABLE transactions_monthly ENABLE ROW LEVEL SECURITY;
ALTER TABLE inventory_daily      ENABLE ROW LEVEL SECURITY;
ALTER TABLE inventory_monthly    ENABLE ROW LEVEL SECURITY;
ALTER TABLE transport_daily      ENABLE ROW LEVEL SECURITY;
ALTER TABLE kpi_monthly          ENABLE ROW LEVEL SECURITY;

CREATE POLICY "service_role_all" ON product_categories    FOR ALL USING (true);
CREATE POLICY "service_role_all" ON suppliers             FOR ALL USING (true);
CREATE POLICY "service_role_all" ON products              FOR ALL USING (true);
CREATE POLICY "service_role_all" ON stores                FOR ALL USING (true);
CREATE POLICY "service_role_all" ON warehouses            FOR ALL USING (true);
CREATE POLICY "service_role_all" ON vehicles              FOR ALL USING (true);
CREATE POLICY "service_role_all" ON drivers               FOR ALL USING (true);
CREATE POLICY "service_role_all" ON routes                FOR ALL USING (true);
CREATE POLICY "service_role_all" ON simulation_config     FOR ALL USING (true);
CREATE POLICY "service_role_all" ON run_log               FOR ALL USING (true);
CREATE POLICY "service_role_all" ON holidays              FOR ALL USING (true);
CREATE POLICY "service_role_all" ON transactions          FOR ALL USING (true);
CREATE POLICY "service_role_all" ON inventory_log         FOR ALL USING (true);
CREATE POLICY "service_role_all" ON shipments             FOR ALL USING (true);
CREATE POLICY "service_role_all" ON purchase_orders       FOR ALL USING (true);
CREATE POLICY "service_role_all" ON warehouse_snapshot    FOR ALL USING (true);
CREATE POLICY "service_role_all" ON campaigns             FOR ALL USING (true);
CREATE POLICY "service_role_all" ON fuel_prices           FOR ALL USING (true);
CREATE POLICY "service_role_all" ON sales_hourly          FOR ALL USING (true);
CREATE POLICY "service_role_all" ON sales_hourly_agg      FOR ALL USING (true);
CREATE POLICY "service_role_all" ON sales_daily           FOR ALL USING (true);
CREATE POLICY "service_role_all" ON sales_monthly         FOR ALL USING (true);
CREATE POLICY "service_role_all" ON transactions_monthly  FOR ALL USING (true);
CREATE POLICY "service_role_all" ON inventory_daily       FOR ALL USING (true);
CREATE POLICY "service_role_all" ON inventory_monthly     FOR ALL USING (true);
CREATE POLICY "service_role_all" ON transport_daily       FOR ALL USING (true);
CREATE POLICY "service_role_all" ON kpi_monthly           FOR ALL USING (true);

-- ============================================================
-- SEED: simulation_config — VLERAT DEFAULT
-- Të gjitha "neutrale" (simulim normal, pa event, pa promo).
-- Kërkohen sepse kodi bën .update().eq("config_key", ...) —
-- nëse rreshti nuk ekziston, update-i dështon në heshtje.
-- ============================================================
INSERT INTO simulation_config (config_key, config_value, description) VALUES
    ('simulation_active',      1.0, 'Master on/off switch (lexohet nga streamlit)'),
    ('demand_multiplier',      1.0, 'Multiplier global mbi λ (demand_profile.py)'),
    ('active_event',           0.0, '0=Normal 1=Luftë 2=Pandemi 3=Grevë 4=Thatësirë'),
    ('event_intensity',        0.0, 'Intensiteti i Black Swan event (0-10)'),
    ('fuel_multiplier',        1.0, 'Multiplier mbi çmimin e karburantit'),
    ('transport_disruption',   0.0, 'Shton probabilitet vonese (transport_module.py)'),
    ('transport_delay_min',    0.0, 'Minuta shtesë vonese fikse (Black Swan)'),
    ('promo_active',           0.0, '1 = ka promocion aktiv (vendoset nga marketing_module.py)'),
    ('promo_discount_pct',     0.0, 'Zbritja % e promocionit aktiv'),
    ('promo_demand_lift',      0.0, 'Rritja e kërkesës nga promocioni aktiv'),
    ('lead_time_multiplier',   1.0, 'Multiplier mbi lead time-in e furnizimit'),
    ('reorder_multiplier',     1.0, 'Multiplier mbi sasinë e riporosisë'),
    ('import_price_multiplier',1.0, 'Multiplier mbi koston e importit (Black Swan)');
