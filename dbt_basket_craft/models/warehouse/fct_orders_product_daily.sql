WITH fct_orders_product_daily AS (
   SELECT
       DATE(o.order_date) AS order_day,
       o.primary_product_id AS product_id,
      
       -- Order metrics
       COUNT(DISTINCT o.order_id) AS total_orders,
       SUM(o.items_purchased) AS total_items_purchased,
      
       -- Financial metrics
       ROUND(SUM(o.price_usd)::numeric, 2) AS total_revenue_usd,
       ROUND(SUM(o.cogs_usd)::numeric, 2) AS total_cost_usd,
       ROUND(SUM(o.price_usd - o.cogs_usd)::numeric, 2) AS total_profit_usd,
       ROUND(AVG(o.price_usd)::numeric, 2) AS avg_order_value_usd,
      
       -- User metrics
       COUNT(DISTINCT o.user_id) AS unique_customers_count,
       COUNT(DISTINCT o.website_session_id) AS unique_sessions_count,
      
       -- Calculated metrics
       ROUND((SUM(o.items_purchased) / COUNT(DISTINCT o.order_id))::numeric, 2) AS avg_items_per_order,
       ROUND((SUM(o.price_usd - o.cogs_usd) / SUM(o.price_usd) * 100)::numeric, 2) AS profit_margin_pct,


       -- Metadata
       CURRENT_TIMESTAMP AS loaded_at
   FROM {{ ref('stg_orders') }} o
   GROUP BY
       DATE(o.order_date),
       o.primary_product_id
   ORDER BY
       order_day,
       product_id
)
SELECT *
FROM fct_orders_product_daily