with payments as (
    SELECT 
    CREATED as payment_date,
    ORDERID as order_id,
    PAYMENTMETHOD as payment_method,
    STATUS as order_status,
    AMOUNT/100 as order_amount
     FROM {{ source('stripe', 'payments') }}
)

select * from payments