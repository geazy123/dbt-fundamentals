with customers as(
    select * from {{ ref('stg_customers')}}
),

customer_order_info as (
    select
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders,
        sum(order_amount) as ltv
    from {{ ref('fct_orders')}}
    group by 1
),

final as (
    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_order_info.first_order_date,
        customer_order_info.most_recent_order_date,
        coalesce(customer_order_info.number_of_orders, 0) as number_of_orders,
        customer_order_info.ltv
    from customers
    left join customer_order_info using (customer_id)
)

select * from final