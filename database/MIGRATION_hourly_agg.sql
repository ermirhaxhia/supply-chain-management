-- ============================================================
-- MIGRATION: sales_hourly_agg — arkiv orësh i përhershëm për ARIMA
-- ============================================================
-- v2: 1 rresht/store/ORË (jo/produkt) — llogaritja tregoi që granulariteti
-- për-produkt do të prodhonte ~3.4 GB/vit, shumë mbi kufirin 500MB.
-- ARIMA bëhet mbi 1 seri kohore (xhiro/orë), jo mbi 7,500 seri të veçanta.
-- sales_daily/sales_monthly (që u duhet detaji i produktit) vazhdojnë të
-- ushqehen nga sales_hourly (raw, retention e shkurtër), jo nga kjo tabelë.
--
-- Nëse e ke ekzekutuar versionin e parë (me product_id), ky DROP+CREATE
-- e zëvendëson pa problem — tabela ishte ende bosh (sapo e krijuam).
--
-- SI TA EKZEKUTOSH: Supabase Dashboard → SQL Editor → ngjit → Run.
-- ============================================================

DROP TABLE IF EXISTS sales_hourly_agg CASCADE;

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

CREATE INDEX idx_sales_hourly_agg_date  ON sales_hourly_agg(date);
CREATE INDEX idx_sales_hourly_agg_store ON sales_hourly_agg(store_id);

ALTER TABLE sales_hourly_agg ENABLE ROW LEVEL SECURITY;

CREATE POLICY "service_role_all" ON sales_hourly_agg FOR ALL USING (true);

-- ============================================================
-- U KRYE. Verifiko me:
--   SELECT count(*) FROM sales_hourly_agg;
-- ============================================================
