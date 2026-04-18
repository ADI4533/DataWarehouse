-- loading cleaned and transormed data into silver.crm_cust_info

insert into silver.crm_cust_info(
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date)
select 
cst_id,
cst_key,
trim(cst_firstname) as cst_firstname,
trim(cst_lastname) as cst_lastname,
case 
    when upper(trim(cst_marital_status)) = 'M' then 'Married'
    when upper(trim(cst_marital_status)) = 'S' then 'Single'
    else 'n/a'
end cst_marital_status, -- normalize and standardization
case 
    when upper(trim(cst_gndr)) = 'F' then 'Female'
    when upper(trim(cst_gndr)) = 'M' then 'Male'
    else 'n/a'
end cst_gndr, -- normalize and standardization
cst_create_date 
from (
select *
from(
Select *,
row_number() over (partition by cst_id order by cst_create_date desc) as flag_last
from bronze.crm_cust_info
)t -- removed duplicates and retained the latest data 
where flag_last = 1) f

-- cleaned and loaded sliver.crm_sales_details table
insert into silver.crm_sales_details(
sls_ord_num ,
sls_prd_key ,
sls_cust_id ,
sls_order_dt,
sls_ship_dt ,
sls_due_dt ,
sls_sales ,
sls_quantity,
sls_price 
)

select 
sls_ord_num,
sls_prd_key,
sls_cust_id,
case 
when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
else cast(cast(sls_order_dt as varchar)as date)
end sls_order_dt,
case 
when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
else cast(cast(sls_ship_dt as varchar)as date)
end sls_ship_dt,
case 
when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
else cast(cast(sls_due_dt as varchar)as date)
end sls_due_dt,
case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity*abs(sls_price) 
then sls_quantity*abs(sls_price) 
else sls_sales
end as sls_sales,
sls_quantity,
case when sls_price is null or sls_price <=0 
then sls_sales/nullif(sls_quantity,0)
else sls_price 
end as sls_price
from bronze.crm_sales_details
