DROP TABLE IF EXISTS JR_IMDA.IMBA_DWM.prd_features;
CREATE TABLE JR_IMDA.IMBA_DWM.prd_features AS (
    select 
        product_id,
        Count(*) AS prod_orders,
        Sum(reordered) AS prod_reorders,
        Sum(CASE WHEN product_seq_time = 1 THEN 1 ELSE 0 END) AS prod_first_orders,
        Sum(CASE WHEN product_seq_time = 2 THEN 1 ELSE 0 END) AS prod_second_orders
    from 
        (select * , 
            rank() over (partition by user_id , product_id order by order_number ) as product_seq_time 
        from JR_IMDA.IMBA_ODS.ORDERS_PRIOR ) 
    group by product_id
    
   )
