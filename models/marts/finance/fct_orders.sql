with payment as (
    select
        *
    from {{ref('stg_stripe__payments')}}
),

orders as (
    SELECT
      *
    from {{ref('stg_jaffle_shop__orders')}}
),

order_payments as (
    select
        order_id,
        sum (case when payment_status = 'success' then amount end) as amount

    from payment
    group by 1
),

 final as (

    select
        orders.order_id,
        orders.customer_id,
        orders.order_date,
        coalesce (order_payments.amount, 0) as amount

    from orders
    left join order_payments using (order_id)
)

select * from final