-- Iceberg capabilities in Athena: time travel, UPDATE, DELETE, maintenance.
-- Replace the <placeholders> before running.

-- Snapshots of silver.orders (one per write)
SELECT snapshot_id, committed_at, operation FROM "lakehouse_silver"."orders$snapshots" ORDER BY committed_at;

-- Time travel: state before the batch 2 upsert vs now
SELECT count(*) FROM lakehouse_silver.orders FOR VERSION AS OF <snapshot_before_the_upsert>;
SELECT count(*) FROM lakehouse_silver.orders FOR TIMESTAMP AS OF TIMESTAMP '2026-11-09 15:00:00 UTC';
SELECT count(*) FROM lakehouse_silver.orders;

-- UPDATE
UPDATE lakehouse_silver.orders SET status = 'returned', updated_at = current_timestamp
WHERE order_id = 12345;

-- Right to be forgotten (GDPR), customer 1234
SELECT customer_key FROM lakehouse_gold.dim_customer WHERE customer_id = 1234;   -- write it down
DELETE FROM lakehouse_gold.fact_sales WHERE customer_key = <noted_value>;
DELETE FROM lakehouse_gold.dim_customer WHERE customer_id = 1234;
DELETE FROM lakehouse_silver.order_items
WHERE order_id IN (SELECT order_id FROM lakehouse_silver.orders WHERE customer_id = 1234);
DELETE FROM lakehouse_silver.orders WHERE customer_id = 1234;
DELETE FROM lakehouse_silver.customers WHERE customer_id = 1234;
SELECT count(*) FROM lakehouse_silver.customers WHERE customer_id = 1234;       -- must be 0

-- Maintenance
OPTIMIZE lakehouse_gold.fact_sales REWRITE DATA USING BIN_PACK;
VACUUM lakehouse_silver.customers;
