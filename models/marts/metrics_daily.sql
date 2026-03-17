/*
    Daily business metrics rollup.

    Aggregates key business metrics at a daily grain for dashboards and
    trend analysis. This is the go-to table for "how is the business doing?"
    questions.

    Grain: one row per day.
*/

with orders as (
    select * from {{ ref('fct_orders') }}
),

-- Generate a date spine to ensure we have rows for days with zero orders.
-- In production, this would reference a date dimension table.
date_spine as (
    select distinct ordered_at as date_day
    from orders
    where ordered_at is not null
),

daily_orders as (
    select
        ordered_at                                      as date_day,

        -- Volume
        count(distinct order_id)                        as total_orders,
        count(distinct customer_id)                     as unique_customers,

        -- Revenue
        sum(order_total_dollars)                        as total_revenue_dollars,
        avg(order_total_dollars)                        as avg_order_value_dollars,
        median(order_total_dollars)                     as median_order_value_dollars,

        -- Order status breakdown
        count(distinct case
            when order_status = 'completed' then order_id
        end)                                            as completed_orders,
        count(distinct case
            when order_status = 'placed' then order_id
        end)                                            as placed_orders,
        count(distinct case
            when order_status = 'shipped' then order_id
        end)                                            as shipped_orders,
        count(distinct case
            when is_returned then order_id
        end)                                            as returned_orders,

        -- Payment methods
        count(distinct case
            when used_coupon then order_id
        end)                                            as orders_with_coupon,
        sum(coupon_amount)                              as total_coupon_discount_dollars,

        -- Derived rates
        round(
            count(distinct case when is_returned then order_id end)::numeric
            / nullif(count(distinct order_id), 0)
            * 100, 2
        )                                               as return_rate_pct

    from orders
    where ordered_at is not null
    group by ordered_at
),

final as (
    select
        -- Primary key
        ds.date_day,

        -- Metrics (default to zero for days with no activity)
        coalesce(d.total_orders, 0)                     as total_orders,
        coalesce(d.unique_customers, 0)                 as unique_customers,
        coalesce(d.total_revenue_dollars, 0)            as total_revenue_dollars,
        coalesce(d.avg_order_value_dollars, 0)          as avg_order_value_dollars,
        coalesce(d.median_order_value_dollars, 0)       as median_order_value_dollars,
        coalesce(d.completed_orders, 0)                 as completed_orders,
        coalesce(d.placed_orders, 0)                    as placed_orders,
        coalesce(d.shipped_orders, 0)                   as shipped_orders,
        coalesce(d.returned_orders, 0)                  as returned_orders,
        coalesce(d.orders_with_coupon, 0)               as orders_with_coupon,
        coalesce(d.total_coupon_discount_dollars, 0)    as total_coupon_discount_dollars,
        coalesce(d.return_rate_pct, 0)                  as return_rate_pct,

        -- Running totals
        sum(coalesce(d.total_orders, 0)) over (
            order by ds.date_day
            rows between unbounded preceding and current row
        )                                               as cumulative_orders,
        sum(coalesce(d.total_revenue_dollars, 0)) over (
            order by ds.date_day
            rows between unbounded preceding and current row
        )                                               as cumulative_revenue_dollars

    from date_spine ds
    left join daily_orders d on ds.date_day = d.date_day
)

select * from final
order by date_day
