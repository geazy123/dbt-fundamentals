{%- set payment_methods = ['credit_card','bank_transfer','coupon','gift_card'] -%}

with payments as (
    select * from {{ ref('stg_payments')}}
),

pivoted as (
    select
    order_id,
    {% for payment_method in payment_methods -%}
    SUM(case when payment_method = '{{ payment_method }}' then order_amount else 0 end) as {{ payment_method }}_amount
    {%- if not loop.last -%}
    ,
    {% endif %}
    {%- endfor %}

    from payments
    where order_status = 'success'
    group by 1
    )

select * from pivoted