/*
===============================================================================
Quality Checks - Silver Layer
===============================================================================
Purpose:
    Performs data quality checks for consistency, accuracy, and standardization 
    across the Silver Layer tables. Checks include:
    - Null or duplicate primary keys
    - Unwanted spaces in string fields
    - Categorical value standardization
    - Invalid date ranges and ordering
    - Logical consistency between numeric fields

Usage:
    Run after loading Silver Layer.
    Investigate and fix any discrepancies before promoting to Gold Layer.
===============================================================================
*/

-- ============================================================================
-- 1. silver.crm_cust_info
-- ============================================================================
-- Primary Key: No NULLs or duplicates
SELECT cst_id, COUNT(*) AS cnt
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- No unwanted spaces in key fields
SELECT cst_key
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key);

-- Check categorical values
SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info;

-- ============================================================================
-- 2. silver.crm_prd_info
-- ============================================================================
-- Primary Key check
SELECT prd_id, COUNT(*) AS cnt
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- No unwanted spaces in product name
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- No NULLs or negative costs
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Categorical standardization
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

-- Date order validation
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- ============================================================================
-- 3. silver.crm_sales_details
-- ============================================================================
-- Invalid date detection
SELECT NULLIF(sls_due_dt, 0) AS sls_due_dt
FROM silver.crm_sales_details
WHERE sls_due_dt <= 0
   OR LEN(sls_due_dt) != 8
   OR sls_due_dt > 20500101
   OR sls_due_dt < 19000101;

-- Order date must not be after shipping/due date
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
   OR sls_order_dt > sls_due_dt;

-- Sales = Quantity × Price
SELECT DISTINCT sls_sales, sls_quantity, sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL
   OR sls_quantity IS NULL
   OR sls_price IS NULL
   OR sls_sales <= 0
   OR sls_quantity <= 0
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

-- ============================================================================
-- 4. silver.erp_cust_az12
-- ============================================================================
-- Birthdate range check
SELECT DISTINCT bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01'
   OR bdate > GETDATE();

-- Gender standardization
SELECT DISTINCT gen
FROM silver.erp_cust_az12;

-- ============================================================================
-- 5. silver.erp_loc_a101
-- ============================================================================
-- Country standardization
SELECT DISTINCT cntry
FROM silver.erp_loc_a101
ORDER BY cntry;

-- ============================================================================
-- 6. silver.erp_px_cat_g1v2
-- ============================================================================
-- No unwanted spaces in category fields
SELECT *
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat)
   OR subcat != TRIM(subcat)
   OR maintenance != TRIM(maintenance);

-- Maintenance value standardization
SELECT DISTINCT maintenance
FROM silver.erp_px_cat_g1v2;
