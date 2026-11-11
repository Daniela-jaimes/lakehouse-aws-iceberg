-- Business questions on the gold star schema. Run in Athena, workgroup lakehouse-wg.

-- Q1: top 3 departments by net sales in each quarter
WITH v AS (
  SELECT d.year, d.quarter, g.department, SUM(f.net_amount) AS sales
  FROM lakehouse_gold.fact_sales f
  JOIN lakehouse_gold.dim_date d ON f.date_key = d.date_key
  JOIN lakehouse_gold.dim_geography g ON f.geography_key = g.geography_key
  WHERE f.order_status NOT IN ('cancelled', 'returned')
  GROUP BY 1, 2, 3)
SELECT * FROM (SELECT *, RANK() OVER (PARTITION BY year, quarter ORDER BY sales DESC) AS rk FROM v)
WHERE rk <= 3 ORDER BY year, quarter, rk;

-- Q2: sales and average ticket by age group
SELECT c.age_group, COUNT(DISTINCT f.order_id) AS orders, SUM(f.net_amount) AS sales,
       ROUND(SUM(f.net_amount) / COUNT(DISTINCT f.order_id), 0) AS average_ticket
FROM lakehouse_gold.fact_sales f
JOIN lakehouse_gold.dim_customer c ON f.customer_key = c.customer_key
WHERE f.order_status NOT IN ('cancelled', 'returned')
GROUP BY 1 ORDER BY sales DESC;

-- Q3 (manager): leading department by age group and quarter
WITH v AS (
  SELECT d.year, d.quarter, c.age_group, g.department, SUM(f.net_amount) AS sales
  FROM lakehouse_gold.fact_sales f
  JOIN lakehouse_gold.dim_date d ON f.date_key = d.date_key
  JOIN lakehouse_gold.dim_customer c ON f.customer_key = c.customer_key
  JOIN lakehouse_gold.dim_geography g ON f.geography_key = g.geography_key
  WHERE f.order_status NOT IN ('cancelled', 'returned')
  GROUP BY 1, 2, 3, 4)
SELECT year, quarter, age_group, department, sales FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY year, quarter, age_group ORDER BY sales DESC) AS rn
  FROM v)
WHERE rn = 1 ORDER BY year, quarter, age_group;

-- Q3 on the data mart (same answer, no joins)
SELECT year, quarter, age_group, department, net_sales FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY year, quarter, age_group ORDER BY net_sales DESC) AS rn
  FROM lakehouse_gold.agg_sales_dept_age_quarter)
WHERE rn = 1 ORDER BY year, quarter, age_group;

-- Q4 (bonus): categories with the most returns
SELECT p.category, COUNT(*) AS returned_lines, SUM(f.net_amount) AS returned_amount
FROM lakehouse_gold.fact_sales f
JOIN lakehouse_gold.dim_product p ON f.product_key = p.product_key
WHERE f.order_status = 'returned'
GROUP BY 1 ORDER BY returned_lines DESC;
