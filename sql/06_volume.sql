-- Volume and integrity checks.

-- Row count per layer and table
SELECT 'silver' AS layer, 'customers' AS table_name, count(*) AS row_count FROM lakehouse_silver.customers
UNION ALL SELECT 'silver', 'products', count(*) FROM lakehouse_silver.products
UNION ALL SELECT 'silver', 'stores', count(*) FROM lakehouse_silver.stores
UNION ALL SELECT 'silver', 'orders', count(*) FROM lakehouse_silver.orders
UNION ALL SELECT 'silver', 'order_items', count(*) FROM lakehouse_silver.order_items
UNION ALL SELECT 'gold', 'fact_sales', count(*) FROM lakehouse_gold.fact_sales
UNION ALL SELECT 'gold', 'dim_customer', count(*) FROM lakehouse_gold.dim_customer
UNION ALL SELECT 'gold', 'dim_product', count(*) FROM lakehouse_gold.dim_product
UNION ALL SELECT 'gold', 'dim_store', count(*) FROM lakehouse_gold.dim_store
UNION ALL SELECT 'gold', 'dim_geography', count(*) FROM lakehouse_gold.dim_geography
UNION ALL SELECT 'gold', 'dim_date', count(*) FROM lakehouse_gold.dim_date
UNION ALL SELECT 'gold', 'agg_sales_dept_age_quarter', count(*) FROM lakehouse_gold.agg_sales_dept_age_quarter
ORDER BY 1, 2;

-- Files and partitions of the fact table
SELECT count(*) AS files, round(sum(file_size_in_bytes) / 1e6, 1) AS mb FROM "lakehouse_gold"."fact_sales$files";
SELECT * FROM "lakehouse_gold"."fact_sales$partitions" LIMIT 30;

-- Referential integrity: every query must return 0
SELECT count(*) FROM lakehouse_gold.fact_sales f
LEFT JOIN lakehouse_gold.dim_product p ON f.product_key = p.product_key WHERE p.product_key IS NULL;
SELECT count(*) FROM lakehouse_gold.fact_sales f
LEFT JOIN lakehouse_gold.dim_customer c ON f.customer_key = c.customer_key WHERE c.customer_key IS NULL;
SELECT count(*) FROM lakehouse_gold.fact_sales f
LEFT JOIN lakehouse_gold.dim_store s ON f.store_key = s.store_key WHERE s.store_key IS NULL;
SELECT count(*) FROM lakehouse_gold.fact_sales f
LEFT JOIN lakehouse_gold.dim_geography g ON f.geography_key = g.geography_key WHERE g.geography_key IS NULL;
SELECT count(*) FROM lakehouse_gold.fact_sales f
LEFT JOIN lakehouse_gold.dim_date d ON f.date_key = d.date_key WHERE d.date_key IS NULL;
