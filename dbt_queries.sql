---------------  Creation Of Raw Tables ---------------------
CREATE TABLE fact_sales_raw (
  order_id INT,
  product_id INT,
  date_id DATE,
  region_id INT,
  quantity INT,
  amount DECIMAL(10, 2)
);

CREATE TABLE products_raw (
  ASIN INT,
  category STRING,
  style STRING,
  size STRING,
  sku STRING
);

CREATE TABLE dates_raw (
  date_id DATE,
  date DATE
);

CREATE TABLE shipment_region_raw (
  regionID INT,
  city STRING,
  state STRING,
  country STRING,
  postal_code STRING
);

CREATE TABLE orders_raw (
  order_id INT,
  courier_status STRING,
  sales_channel STRING,
  fulfilled_by STRING
);


-- models/dim_product.sql

WITH product_raw AS (
    SELECT
        ASIN as product_id,
		SKU as product_name,
        category,
        style,
		Size
    FROM {{ source('sales', 'products_raw') }}
)

SELECT
    product_id,
    product_name,
    category,
    style,
	size
FROM product_raw

-- models/dim_date.sql

WITH date_raw AS (
    SELECT
        date_id,
        date
    FROM {{ source('sales', 'dates_raw') }}
)

SELECT
    date_id,
    date,
    EXTRACT(YEAR FROM date) AS year,
	EXTRACT(QUARTER FROM date) AS qtr,
    EXTRACT(MONTH FROM date) AS month,
    CASE
        WHEN EXTRACT(MONTH FROM date) IN (12, 1, 2) THEN 'Winter'
        WHEN EXTRACT(MONTH FROM date) IN (3, 4, 5) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM date) IN (6, 7, 8) THEN 'Summer'
        ELSE 'Autumn'
    END AS season
FROM date_raw

-- models/dim_shipment_region.sql

WITH location_raw AS (
    SELECT
        region_id,
		city,
		state,
		country,
		postal_code
    FROM {{ source('sales', 'shipment_region_raw') }}
)

SELECT
    region_id,
    city,
    state,
    country,
	postal_code
FROM location_raw


-- models/dim_order.sql

WITH order_raw AS (
    SELECT
        order_id,
        status,
        sales_channel,
        fulfilled_by
    FROM {{ source('sales', 'orders_raw') }}
)

SELECT
    order_id,
    status,
	sales_channel,
    fulfilled_by
FROM order_raw


-- models/fact_sales.sql

WITH 
day AS (
    SELECT
        date_id,
		date,
		year,
		qtr,
		month,
		season
    FROM {{ ref('dim_date') }}
),
product AS (
    SELECT
        product_id,
		product_name,
		category,
		style,
		size
    FROM {{ ref('dim_product') }}
),
location AS (
    SELECT
        region_id,
		city,
		state,
		country,
		postal_code
    FROM {{ ref('dim_shipment_region') }}
),
order_data AS (
    SELECT
        order_id,
        status,
        sales_channel,
        fulfilled_by
    FROM {{ ref('dim_order') }}
)
SELECT
    fs.order_id,
    fs.product_id,
    fs.date_id,
	d.year,
	d.qtr,
	d.month,
	d.season,
    fs.location_id,
    fs.quantity,
    fs.amount,
    p.product_name,
    p.category,
    p.style,
    l.city,
    l.state,
    l.country,
    o.status,
    o.fulfilled_by,
    o.sales_channel
FROM {{ source('sales', 'fact_sales_raw') }} fs
JOIN day d on fs.date_id = day.date_id
JOIN product p ON fs.product_id = p.product_id
JOIN location l ON fs.location_id = l.location_id
JOIN order_data o ON fs.order_id = o.order_id
