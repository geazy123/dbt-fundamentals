with orders as (
    select * from {{ ref('stg_orders')}}
),

payments as (
    select * from {{ ref('stg_payments')}}
),

order_payments as (
    select
    order_id,
    sum(case when order_status = 'success' then order_amount end) as order_amount
    from payments
    group by 1
),

fact_orders as(
    select 
    orders.order_id,
    orders.customer_id,
    orders.order_date,
    coalesce(order_payments.order_amount, 0) as order_amount
     from orders LEFT JOIN order_payments using (order_id)
)

select * from fact_orders