
CREATE VIEW gold.report_customers as
WITH base_query as(
select 
f.order_number,
f.product_key,
f.order_date,
f.quantity,
c.customer_key,
c.customer_number,
sales_amount,
CONCAT(c.first_name,' ',c.last_name) as customer_name,
DATEDIFF(year, c.birthdate, GETDATE()) as age
FROM gold.fact_sales f
LEFT JOIN gold.dim_customer c
	ON f.customer_key = c.customer_key
WHERE f.order_date is not null
),
customer_aggregation as(
select
customer_key,
customer_number,
customer_name,
age,
count(distinct order_number) as total_orders,
sum(sales_amount) as total_sales,
sum(quantity) as total_quantity,
count(distinct product_key) as total_products,
MAX(order_date) as last_order,
DATEDIFF(month, MIN(order_date), MAX(order_date)) as life_span
FROM base_query
GROUP BY customer_key,customer_number,customer_name,age
)

SELECT
customer_key,
customer_number,
customer_name,
age,
CASE
	WHEN age<20 Then 'Under 20'
	WHEN age between 20 and 29 THEN '20-29'
	WHEN age between 30 and 39 THEN '30-39'
	WHEN age between 40 and 49 THEN '40-49'
	ELSE '50 and above'
END age_group,
CASE
	WHEN life_span>=12 AND total_sales > 5000 THEN 'VIP'
	WHEN life_span >=12 AND total_sales <= 5000 THEN 'Regular'
	Else 'New'
END as customer_segment,
total_orders,
total_sales,
total_quantity,
total_products,
last_order,
life_span,
DATEDIFF(month, last_order,GETDATE()) as recency,
CASE
	WHEN total_orders = 0 THEN 0
	ELSE total_sales/total_orders
END avg_order_value,
CASE
	WHEN life_span = 0 THEN total_sales
	ELSE total_sales/life_span
END avg_monthly_spending
FROM customer_aggregation
