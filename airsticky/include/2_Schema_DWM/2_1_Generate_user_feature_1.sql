DROP TABLE IF EXISTS JR_IMDA.IMBA_DWM.USER_FEATURE_1;
CREATE TABLE JR_IMDA.IMBA_DWM.USER_FEATURE_1 as
select user_id,
        max(order_number) as max_order_number,
        sum(days_since_prior_order) as totall_days_prior_order,
        avg(days_since_prior_order) as avg_days_priro_order
    from JR_IMDA.IMBA_DB.ORDERS
    group by user_id;