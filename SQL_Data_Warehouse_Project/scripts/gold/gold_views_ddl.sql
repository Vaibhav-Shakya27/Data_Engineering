/*
The script bilds views for gold layer that pulls data rom silver layer tables.
This develops the following views:
  gold.product
  gold.customers
  gold.sales
*/

DROP VIEW gold.dim_customers;

CREATE VIEW gold.dim_customers 
as (select 
ROW_NUMBER() over (order by ci.cst_id ) as customer_key,
ci.cst_id as customer_id,
ci.cst_key as customer_number,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
ci.cst_marital_status as marital_status,
CASE 
	WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr
	ELSE COALESCE(ca.gen,'Unknown')
END as gender,
la.cntry as country,
ca.bdate as birth_date,
ci.cst_create_date as create_date



from silver.crm_cust_info ci

left join silver.erp_cust_az12 ca
on  ci.cst_key = ca.cid 

left join silver.erp_loc_a101 la
on ci.cst_key = la.cid
)
;


DROP VIEW gold.dim_product;

CREATE VIEW gold.dim_product
as (
select 
ROW_NUMBER() OVER (ORDER BY pr.prd_start_dt, pr.prd_key) as product_key,
pr.prd_id as product_id,
pr.cat_id as category_id ,
pr.prd_key as product_number,
pr.prd_nm as product_name,
pr.prd_cost as product_cost,
pr.prd_line as product_line ,
pr.prd_start_dt as product_start_date,
ec.cat as product_category,
ec.subcat as product_subcategory,
ec.maintainence 
from silver.crm_prd_info pr

left join silver.erp_px_cat_g1v2 ec
on pr.cat_id = ec.id

where pr.prd_end_dt is null)
;


DROP VIEW gold.fact_sales;
CREATE VIEW gold.fact_sales
as (
SELECT 
sd.sls_ord_num as order_number,
pr.product_key,
dc.customer_key,
sd.sls_order_dt as order_date,
sd.sls_ship_dt as shipping_date,
sd.sls_due_dt as due_date,
sd.sls_sales as sales,
sd.sls_quantity as quantity,
sd.sls_price as price
from silver.crm_sales_details sd

left join gold.dim_product pr
on sd.sls_prd_key = pr.product_number

left join gold.dim_customers dc
on sd.sls_cust_id = dc.customer_id 
)
;
