-- 01_landing.sql: bronze y silver despues del lote 1 (ingest_date=2026-11-07)
-- Workgroup: lakehouse-wg. Los valores esperados vienen de las pruebas locales.

-- ===== BRONZE (despues de los crawlers) =====
SHOW TABLES IN lakehouse_bronze;  -- raw_customers ... raw_web_events (6 tablas)

SELECT 'raw_customers' AS tabla, count(*) AS filas FROM lakehouse_bronze.raw_customers  -- 5000
UNION ALL SELECT 'raw_products', count(*) FROM lakehouse_bronze.raw_products             -- 500
UNION ALL SELECT 'raw_stores', count(*) FROM lakehouse_bronze.raw_stores                 -- 30
UNION ALL SELECT 'raw_orders', count(*) FROM lakehouse_bronze.raw_orders                 -- 50500
UNION ALL SELECT 'raw_order_items', count(*) FROM lakehouse_bronze.raw_order_items       -- 123988
UNION ALL SELECT 'raw_web_events', count(*) FROM lakehouse_bronze.raw_web_events;        -- 20000

-- pedidos duplicados en bronze (la suciedad intencional)
SELECT count(*) - count(DISTINCT order_id) AS duplicados FROM lakehouse_bronze.raw_orders;  -- 500
SELECT * FROM lakehouse_bronze.raw_web_events LIMIT 5;
DESCRIBE lakehouse_bronze.raw_orders;  -- anotar el tipo de ingest_date

-- ===== SILVER (despues del Job 1 con el lote 1) =====
SELECT 'customers' AS tabla, count(*) AS filas FROM lakehouse_silver.customers  -- 5000
UNION ALL SELECT 'products', count(*) FROM lakehouse_silver.products            -- 500
UNION ALL SELECT 'stores', count(*) FROM lakehouse_silver.stores                -- 30
UNION ALL SELECT 'orders', count(*) FROM lakehouse_silver.orders                -- 50000 (sin duplicados)
UNION ALL SELECT 'order_items', count(*) FROM lakehouse_silver.order_items;     -- 123575 (413 rechazados)

SELECT count(*) - count(DISTINCT order_id) AS dup FROM lakehouse_silver.orders;              -- 0
SELECT count(*) - count(DISTINCT order_item_id) AS dup FROM lakehouse_silver.order_items;    -- 0
SELECT count(*) AS items_huerfanos FROM lakehouse_silver.order_items i
LEFT JOIN lakehouse_silver.orders o ON i.order_id = o.order_id WHERE o.order_id IS NULL;     -- 0
SELECT count(*) AS birth_date_nulas FROM lakehouse_silver.customers WHERE birth_date IS NULL; -- 5

SELECT status, count(*) AS pedidos FROM lakehouse_silver.orders GROUP BY 1 ORDER BY 2 DESC;
-- completed 40092, cancelled 2484, shipped 3966, pending 2468, returned 990 (despues del lote 1)

-- Tras el lote 2: orders 54000, returned 2072, order_items 133393.
-- En CloudWatch (/aws-glue/jobs/output): [QUALITY] order_items: total=123988 rechazados=413
