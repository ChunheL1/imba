DROP TABLE IF EXISTS JR_IMDA.IMBA_DWM.USER_FEATURE_2;
CREATE TABLE JR_IMDA.IMBA_DWM.USER_FEATURE_2 as
    SELECT user_id,
        Count(*) AS user_total_products,
        Count(DISTINCT product_id) AS user_distinct_products ,
        Sum(CASE WHEN reordered = 1 THEN 1 ELSE 0 END) / Cast(Sum(CASE WHEN order_number > 1 THEN 1 ELSE 0 END) AS DOUBLE)AS user_reorder_ratio
    FROM JR_IMDA.IMBA_ODS.ORDERS_PRIOR  GROUP BY user_id;