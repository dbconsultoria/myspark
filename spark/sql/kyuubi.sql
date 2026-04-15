CREATE DATABASE IF NOT EXISTS gold;

-- Registra as tabelas apontando pro caminho
CREATE TABLE IF NOT EXISTS gold.dim_categories USING parquet LOCATION '/data/gold/dim_categories';
CREATE TABLE IF NOT EXISTS gold.dim_products USING parquet LOCATION '/data/gold/dim_products';
CREATE TABLE IF NOT EXISTS gold.dim_customers USING parquet LOCATION '/data/gold/dim_customers';
CREATE TABLE IF NOT EXISTS gold.fact_orders USING parquet LOCATION '/data/gold/fact_orders';

SELECT
    c.customer_name,
    p.product_description,
    DATE_FORMAT(f.order_date, 'dd-MM-yyyy') as date,
    SUM(f.total_quantity) as quantity,
    ROUND(SUM(f.total_sales), 2) as sales
FROM gold.fact_orders f
INNER JOIN gold.dim_customers c
    ON f.customer_code = c.customer_code
    AND c.is_current = true
INNER JOIN gold.dim_products p
    ON f.product_code = p.product_code
    AND p.is_current = true
GROUP BY c.customer_name, p.product_description, DATE_FORMAT(f.order_date, 'dd-MM-yyyy')
ORDER BY date DESC, sales DESC