1)Number of stores and in which country:
  
SELECT country_code, 
COUNT (*) FROM dim_store_details
GROUP BY country_code	

2)Which locations have the most stores:
  
SELECT locality, 
COUNT (*) FROM dim_store_details
GROUP BY locality
ORDER BY COUNT(*) DESC;

3)Which months produced the largest amount of sales:
  
select 	dim_date_times.month, 
ROUND(SUM(orders_table.product_quantity*dim_products.product_price)) AS total_revenue
FROM orders_table
JOIN dim_date_times on  orders_table.date_uuid = dim_date_times.date_uuid
JOIN dim_products on  orders_table.product_code = dim_products.product_code
GROUP BY dim_date_times.month
ORDER BY SUM(orders_table.product_quantity*dim_products.product_price) DESC;

4)How many sales are coming from online:
  
SELECT 	COUNT (orders_table.product_quantity) AS numbers_of_sales,SUM(orders_table.product_quantity) AS product_quantity_count,
	CASE 
		WHEN dim_store_details.store_code = 'WEB-1388012W' THEN 'Web'
		ELSE 'Offline'
		END AS product_location
FROM orders_table
JOIN dim_date_times ON  orders_table.date_uuid = dim_date_times.date_uuid
JOIN dim_products ON  orders_table.product_code = dim_products.product_code
JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
GROUP BY product_location
ORDER BY SUM(orders_table.product_quantity) ASC;

5)What percentage of sales come through each type of store?

SELECT 	dim_store_details.store_type, 
SUM (orders_table.product_quantity*dim_products.product_price) AS revenue,
SUM(100.0*orders_table.product_quantity*dim_products.product_price)/(SUM(SUM(orders_table.product_quantity*dim_products.product_price)) OVER ()) AS percentage_total
FROM orders_table
JOIN dim_date_times ON  orders_table.date_uuid = dim_date_times.date_uuid
JOIN dim_products ON  orders_table.product_code = dim_products.product_code
JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
GROUP BY dim_store_details.store_type
ORDER BY percentage_total DESC;

6)Which month in each year produced the highest cost of sales?

SELECT 
    dim_date_times.year,
    dim_date_times.month, 
    ROUND(SUM(orders_table.product_quantity * dim_products.product_price)) AS revenue
FROM 
    orders_table
JOIN 
    dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
JOIN 
    dim_products ON orders_table.product_code = dim_products.product_code
JOIN 
    dim_store_details ON orders_table.store_code = dim_store_details.store_code
GROUP BY 
    dim_date_times.year, 
    dim_date_times.month
ORDER BY 
    SUM(orders_table.product_quantity * dim_products.product_price) DESC;

7)What is our staff headcount?

select sum(dim_store_details.staff_numbers) as total_staff_numbers, dim_store_details.country_code
from dim_store_details
group by 	dim_store_details.country_code

8)Which German store type is selling the most?

SELECT 
    ROUND(SUM(dim_products.product_price * orders_table.product_quantity)::numeric, 2) AS total_sales,
    dim_store_details.store_type,
    dim_store_details.country_code
FROM 
    orders_table
JOIN 
    dim_store_details ON orders_table.store_code = dim_store_details.store_code
JOIN
    dim_products ON orders_table.product_code = dim_products.product_code
WHERE
    dim_store_details.country_code = 'DE'
GROUP BY 
    dim_store_details.store_type, dim_store_details.country_code
ORDER BY 
    total_sales ASC;

9)How quickly is the company making sales?

WITH OrderedSales AS (
    SELECT
        ddt.year,
        LEAD(TO_TIMESTAMP(ddt.year || '-' || ddt.month || '-' || ddt.day || ' ' || ddt.timestamp, 'YYYY-MM-DD HH24:MI:SS'))
        OVER (PARTITION BY ddt.year ORDER BY TO_TIMESTAMP(ddt.year || '-' || ddt.month || '-' || ddt.day || ' ' || ddt.timestamp, 'YYYY-MM-DD HH24:MI:SS')) 
        - TO_TIMESTAMP(ddt.year || '-' || ddt.month || '-' || ddt.day || ' ' || ddt.timestamp, 'YYYY-MM-DD HH24:MI:SS') AS time_to_next_sale
    FROM
        orders_table ot
    JOIN dim_date_times ddt ON ot.date_uuid = ddt.date_uuid
),
AverageTime AS (
    SELECT
        year,
        AVG(time_to_next_sale) AS avg_time
    FROM OrderedSales
    GROUP BY year
)
SELECT
    year,
    '"hours": ' || EXTRACT(HOUR FROM avg_time) || ', "minutes": ' || EXTRACT(MINUTE FROM avg_time) || ', "seconds": ' || EXTRACT(SECOND FROM avg_time) || ', "milliseconds": ' || (EXTRACT(MILLISECOND FROM avg_time) % 1000)::int AS actual_time_taken
FROM AverageTime
ORDER BY year DESC;


