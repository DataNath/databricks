-- Databricks notebook source
CREATE TABLE IF NOT EXISTS nathansandbox.superstore_data.all_returned_orders AS

WITH raw_orders AS (
  SELECT *
  FROM nathansandbox.superstore_data.orders
),

return_data AS (
  SELECT `Order ID`
  FROM nathansandbox.superstore_data.returns
)

SELECT
  `Row ID` AS row_id,
  a.`Order ID` AS order_id,
  `Order Date` AS order_date,
  `Ship Date` AS ship_date,
  `Country/Region` AS country_region,
  City,
  `Product ID` AS product_id,
  `Product Name` AS product_name,
  Sales,
  Quantity,
  Discount,
  Profit
FROM raw_orders a
INNER JOIN return_data b ON a.`Order ID` = b.`Order ID`
