WITH raw_orders AS (
   SELECT * FROM {{ source('basket_craft', 'orders') }}
),
stg_orders AS (
   SELECT
       order_id,
       created_at AS order_date,
       website_session_id,
       user_id,
       primary_product_id,
       items_purchased,
       price_usd,       
       cogs_usd,
       CURRENT_TIMESTAMP AS loaded_at
   FROM raw_orders
)
SELECT * FROM stg_orders