/*
===============================================================================
Quality Checks - Gold Layer
===============================================================================
Purpose:
    Validates the integrity, consistency, and accuracy of the Gold Layer.
    Checks performed:
    - Uniqueness of surrogate keys in dimension tables
    - Referential integrity between fact and dimension tables
    - Validation of relationships in the data model

Usage:
    Run regularly to ensure the Gold Layer supports reliable analytics.
    Investigate and resolve any discrepancies immediately.
===============================================================================
*/

-- ============================================================================
-- 1. gold.dim_customers
-- ============================================================================
-- Uniqueness of Customer Key
SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- ============================================================================
-- 2. gold.dim_products
-- ============================================================================
-- Uniqueness of Product Key
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

-- ============================================================================
-- 3. gold.fact_sales
-- ============================================================================
-- Referential Integrity: Fact → Dimensions
SELECT 
    f.fact_id,
    f.customer_key,
    f.product_key
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
    ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
    ON p.product_key = f.product_key
WHERE p.product_key IS NULL 
   OR c.customer_key IS NULL;
