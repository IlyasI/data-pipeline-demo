/*
    Customer dimension table.

    Combines customer attributes with aggregated order and payment data
    to create a single wide table for analytics. This follows the standard
    dimensional modeling pattern where we denormalize for query performance.

    Grain: one row per customer.
*/

with customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

payments as (
    select * from {{ ref('stg_payments') }}
),

-- Aggregate order-level metrics per customer
customer_orders as (
    select
        customer_id,

        count(distinct order_id)                        as total_orders,
        min(ordered_at)                                 as first_order_at,
        max(ordered_at)                                 as most_recent_order_at,

        -- Order status breakdown
        count(distinct case
            when order_status = 'completed' then order_id
        end)                                            as completed_orders,
        count(distinct case
            when order_status in ('returned', 'return_pending') then order_id
        end)                                            as returned_orders

    from orders
    group by customer_id
),

-- Aggregate payment-level metrics per customer
customer_payments as (
    select
        orders.customer_id,

        sum(payments.amount_dollars)                    as lifetime_spend_dollars,
        count(distinct payments.payment_method)         as distinct_payment_methods,

        -- Most common payment method
        mode() within group (
            order by payments.payment_method
        )                                               as preferred_payment_method

    from payments
    inner join orders on payments.order_id = orders.order_id
    group by orders.customer_id
),

final as (
    select
        -- Primary key
        customers.customer_id,

        -- Customer attributes
        customers.first_name,
        customers.last_name,
        customers.first_name || ' ' || customers.last_name  as full_name,

        -- Order metrics
        coalesce(customer_orders.total_orders, 0)       as total_orders,
        coalesce(customer_orders.completed_orders, 0)   as completed_orders,
        coalesce(customer_orders.returned_orders, 0)    as returned_orders,
        customer_orders.first_order_at,
        customer_orders.most_recent_order_at,

        -- Payment metrics
        coalesce(customer_payments.lifetime_spend_dollars, 0)
                                                        as lifetime_spend_dollars,
        customer_payments.distinct_payment_methods,
        customer_payments.preferred_payment_method,

        -- Derived
        case
            when customer_orders.total_orders is null then 'inactive'
            when customer_orders.total_orders = 1 then 'new'
            when customer_orders.total_orders between 2 and 4 then 'active'
            else 'loyal'
        end                                             as customer_tier,

        -- Metadata
        customers._loaded_at

    from customers
    left join customer_orders on customers.customer_id = customer_orders.customer_id
    left join customer_payments on customers.customer_id = customer_payments.customer_id
)

select * from final
