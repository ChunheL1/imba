DROP TABLE IF EXISTS JR_IMDA.IMBA_DWM.up_features;
CREATE TABLE JR_IMDA.IMBA_DWM.up_features AS (
    SELECT user_id, 
        product_id,
        Count(*) AS up_orders,
        Min(order_number) AS up_first_order, 
        Max(order_number) AS up_last_order, 
        Avg(add_to_cart_order) AS up_average_cart_position
    FROM JR_IMDA.IMBA_ODS.ORDERS_PRIOR GROUP BY user_id,
    product_id)
