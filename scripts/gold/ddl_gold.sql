/*
=========================================================
  DESCRIPTION of our script here
=========================================================
*/
--Gold Dim View: dim_customer
CREATE VIEW gold.dim_customer as (--a se observa naming`ul de schema.dim = dimensional table
	SELECT 
		ROW_NUMBER() OVER (ORDER BY cst_id) as customer_key, --DWH PK (a sequence generated in DWH) will serve as connector inside DWH datamodel
		ci.cst_id as customer_id, --Source PK
		ci.cst_key as customer_number,
		ci.cst_firstname as first_name,
		ci.cst_lastname as last_name,
		la.cntry as country,
		ci.cst_material_status as marital_status,
		CASE 
			WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr --CRM is the master for gender info
			ELSE COALESCE(ca.GEN, 'n/a')
		END gender,
		ci.cst_create_date as create_date,
		ca.bdate as birthdate
	FROM silver.crm_cust_info ci
	LEFT JOIN silver.erp_cust_az12 ca
		ON ci.cst_key = ca.cid
	LEFT JOIN silver.erp_loc_a101 la
		ON ci.cst_key = la.cid
)



--Gold Dim View: dim_products
CREATE VIEW gold.dim_products AS(
SELECT 
	ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) as product_key,
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance,
	pn.prd_cost AS cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt AS start_date	
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
	ON pn.cat_id = pc.id
WHERE pn.prd_end_dt is null --filter out all historical data
)


--Create GOLD Fact View: fact_sales
CREATE VIEW gold.fact_sales AS(
SELECT 
	sd.sls_ord_num as order_number,
	pr.product_key,
	cu.customer_key,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt as shipping_date,
	sd.sls_due_dt as due_date,
	sd.sls_sales as sales_amount,
	sd.sls_quantity as quantity,
	sd.sls_price as price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
	ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customer cu
	ON sd.sls_cust_id = cu.customer_id
)
