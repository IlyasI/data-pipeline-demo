/*
    Orders fact table.

    Joins orders with their payment totals and customer information to create
    the core analytical fact table. Each row represents a single order with
    its associated payment and customer context.

    Grain: one row per order.
*/

with orders as (
    select * from {{ ref('stg_orders') }}
),

payments as (
    select * from {{ ref('stg_payments') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

-- Aggregate payments to the order level
order_payments as (
    select
        order_id,

        sum(amount_dollars)                             as total_amount_dollars,
        count(*)                                        as payment_count,
        count(distinct payment_method)                  as distinct_payment_methods,

        -- Payment method breakdown
        sum(case when payment_method = 'credit_card'
            then amount_dollars else 0 end)             as credit_card_amount,
        sum(case when payment_method = 'coupon'
            then amount_dollars else 0 end)             as coupon_amount,
        sum(case when payment_method = 'bank_transfer'
            then amount_dollars else 0 end)             as bank_transfer_amount,
        sum(case when payment_method = 'gift_card'
            then amount_dollars else 0 end)             as gift_card_amount

    from payments
    group by order_id
),

final as (
    select
        -- Primary key
        orders.order_id,

        -- Foreign keys
        orders.customer_id,

        -- Order attributes
        orders.order_status,
        orders.ordered_at,

        -- Customer context (denormalized for query convenience)
        customers.first_name                            as customer_first_name,
        customers.last_name                             as customer_last_name,

        -- Payment metrics
        coalesce(order_payments.total_amount_dollars, 0)
                                                        as order_total_dollars,
        coalesce(order_payments.payment_count, 0)       as payment_count,
        order_payments.distinct_payment_methods,

        -- Payment method breakdown
        coalesce(order_payments.credit_card_amount, 0)  as credit_card_amount,
        coalesce(order_payments.coupon_amount, 0)       as coupon_amount,
        coalesce(order_payments.bank_transfer_amount, 0)
                                                        as bank_transfer_amount,
        coalesce(order_payments.gift_card_amount, 0)    as gift_card_amount,

        -- Derived flags
        case
            when order_payments.coupon_amount > 0 then true
            else false
        end                                             as used_coupon,

        case
            when orders.order_status in ('returned', 'return_pending') then true
            else false
        end                                             as is_returned,

        -- Metadata
        orders._loaded_at

    from orders
    left join order_payments on orders.order_id = order_payments.order_id
    left join customers on orders.customer_id = customers.customer_id
)

select * from final
