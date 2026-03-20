-- ============================================================
-- SUPPLY CHAIN MANAGEMENT
-- Database Schema - PostgreSQL / Supabase
-- ============================================================

-- ============================================================
-- GRUP 1: TABELA REFERENCE (8 tabela)
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

CREATE TABLE stores (
    store_id        VARCHAR(10) PRIMARY KEY,
    store_name      VARCHAR(100) NOT NULL,
    city            VARCHAR(50) NOT NULL,
    latitude        FLOAT NOT NULL,
    longitude       FLOAT NOT NULL,
    opening_hour    INT DEFAULT 6,
    closing_hour    INT DEFAULT 22,
    size_m2         FLOAT NOT NULL,
    manager_id      VARCHAR(10)
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
-- GRUP 2: TABELA OPERATIVE (7 tabela)
-- ============================================================

CREATE TABLE transactions (
    transaction_id  VARCHAR(15) PRIMARY KEY,
    store_id        VARCHAR(10) NOT NULL REFERENCES stores(store_id),
    product_id      VARCHAR(10) NOT NULL REFERENCES products(product_id),
    timestamp       TIMESTAMP NOT NULL,
    quantity        INT NOT NULL,
    unit_price      FLOAT NOT NULL,
    discount_pct    FLOAT DEFAULT 0.0,
    total           FLOAT NOT NULL,
    payment_method  VARCHAR(20) DEFAULT 'Cash',
    promotion_id    VARCHAR(10) DEFAULT NULL,
    customer_type   VARCHAR(20) DEFAULT 'Normal'
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
-- GRUP 3: TABELA AGREGAT (6 tabela)
-- ============================================================

CREATE TABLE sales_hourly (
    id              SERIAL PRIMARY KEY,
    store_id        VARCHAR(10) NOT NULL REFERENCES stores(store_id),
    product_id      VARCHAR(10) NOT NULL REFERENCES products(product_id),
    date            DATE NOT NULL,
    hour            INT NOT NULL,
    transactions    INT NOT NULL,
    units_sold      INT NOT NULL,
    revenue         FLOAT NOT NULL,
    avg_basket      FLOAT NOT NULL,
    discount_total  FLOAT DEFAULT 0.0
);

CREATE TABLE sales_daily (
    id              SERIAL PRIMARY KEY,
    store_id        VARCHAR(10) NOT NULL REFERENCES stores(store_id),
    product_id      VARCHAR(10) NOT NULL REFERENCES products(product_id),
    date            DATE NOT NULL,
    transactions    INT NOT NULL,
    units_sold      INT NOT NULL,
    revenue         FLOAT NOT NULL,
    avg_basket      FLOAT NOT NULL,
    discount_total  FLOAT DEFAULT 0.0,
    stockout_flag   BOOLEAN DEFAULT FALSE
);

CREATE TABLE sales_monthly (
    id              SERIAL PRIMARY KEY,
    store_id        VARCHAR(10) NOT NULL REFERENCES stores(store_id),
    product_id      VARCHAR(10) NOT NULL REFERENCES products(product_id),
    year            INT NOT NULL,
    month           INT NOT NULL,
    transactions    INT NOT NULL,
    units_sold      INT NOT NULL,
    revenue         FLOAT NOT NULL,
    avg_basket      FLOAT NOT NULL,
    discount_total  FLOAT DEFAULT 0.0,
    stockout_days   INT DEFAULT 0
);

CREATE TABLE inventory_daily (
    id              SERIAL PRIMARY KEY,
    store_id        VARCHAR(10) NOT NULL REFERENCES stores(store_id),
    product_id      VARCHAR(10) NOT NULL REFERENCES products(product_id),
    date            DATE NOT NULL,
    avg_stock_level FLOAT NOT NULL,
    min_stock_level INT NOT NULL,
    max_stock_level INT NOT NULL,
    stockout_hours  INT DEFAULT 0,
    expired_units   INT DEFAULT 0,
    restock_count   INT DEFAULT 0
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

CREATE INDEX idx_transactions_store      ON transactions(store_id);
CREATE INDEX idx_transactions_product    ON transactions(product_id);
CREATE INDEX idx_transactions_timestamp  ON transactions(timestamp);
CREATE INDEX idx_inventory_log_store     ON inventory_log(store_id);
CREATE INDEX idx_inventory_log_product   ON inventory_log(product_id);
CREATE INDEX idx_shipments_status        ON shipments(status);
CREATE INDEX idx_sales_hourly_date       ON sales_hourly(date);
CREATE INDEX idx_sales_daily_date        ON sales_daily(date);
CREATE INDEX idx_kpi_monthly_store       ON kpi_monthly(store_id);

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
ALTER TABLE transactions         ENABLE ROW LEVEL SECURITY;
ALTER TABLE inventory_log        ENABLE ROW LEVEL SECURITY;
ALTER TABLE shipments            ENABLE ROW LEVEL SECURITY;
ALTER TABLE purchase_orders      ENABLE ROW LEVEL SECURITY;
ALTER TABLE warehouse_snapshot   ENABLE ROW LEVEL SECURITY;
ALTER TABLE campaigns            ENABLE ROW LEVEL SECURITY;
ALTER TABLE fuel_prices          ENABLE ROW LEVEL SECURITY;
ALTER TABLE sales_hourly         ENABLE ROW LEVEL SECURITY;
ALTER TABLE sales_daily          ENABLE ROW LEVEL SECURITY;
ALTER TABLE sales_monthly        ENABLE ROW LEVEL SECURITY;
ALTER TABLE inventory_daily      ENABLE ROW LEVEL SECURITY;
ALTER TABLE transport_daily      ENABLE ROW LEVEL SECURITY;
ALTER TABLE kpi_monthly          ENABLE ROW LEVEL SECURITY;

CREATE POLICY "service_role_all" ON product_categories  FOR ALL USING (true);
CREATE POLICY "service_role_all" ON suppliers           FOR ALL USING (true);
CREATE POLICY "service_role_all" ON products            FOR ALL USING (true);
CREATE POLICY "service_role_all" ON stores              FOR ALL USING (true);
CREATE POLICY "service_role_all" ON warehouses          FOR ALL USING (true);
CREATE POLICY "service_role_all" ON vehicles            FOR ALL USING (true);
CREATE POLICY "service_role_all" ON drivers             FOR ALL USING (true);
CREATE POLICY "service_role_all" ON routes              FOR ALL USING (true);
CREATE POLICY "service_role_all" ON transactions        FOR ALL USING (true);
CREATE POLICY "service_role_all" ON inventory_log       FOR ALL USING (true);
CREATE POLICY "service_role_all" ON shipments           FOR ALL USING (true);
CREATE POLICY "service_role_all" ON purchase_orders     FOR ALL USING (true);
CREATE POLICY "service_role_all" ON warehouse_snapshot  FOR ALL USING (true);
CREATE POLICY "service_role_all" ON campaigns           FOR ALL USING (true);
CREATE POLICY "service_role_all" ON fuel_prices         FOR ALL USING (true);
CREATE POLICY "service_role_all" ON sales_hourly        FOR ALL USING (true);
CREATE POLICY "service_role_all" ON sales_daily         FOR ALL USING (true);
CREATE POLICY "service_role_all" ON sales_monthly       FOR ALL USING (true);
CREATE POLICY "service_role_all" ON inventory_daily     FOR ALL USING (true);
CREATE POLICY "service_role_all" ON transport_daily     FOR ALL USING (true);
CREATE POLICY "service_role_all" ON kpi_monthly         FOR ALL USING (true);