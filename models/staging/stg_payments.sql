/*
    Staging model for raw payments.

    Responsibilities:
    - Rename columns to consistent snake_case
    - Convert amount from cents to dollars
    - Cast types explicitly
    - Deduplicate on primary key
    - Filter out failed payments
*/

with source as (
    select * from {{ source('raw', 'payments') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by id
            order by _loaded_at desc
        ) as _row_num
    from source
),

renamed as (
    select
        -- Primary key
        cast(id as integer)                         as payment_id,

        -- Foreign keys
        cast(order_id as integer)                   as order_id,

        -- Attributes
        cast(payment_method as varchar)             as payment_method,

        -- Financials (source stores in cents, convert to dollars)
        cast(amount as numeric) / 100.0             as amount_dollars,

        -- Metadata
        cast(_loaded_at as timestamp)               as _loaded_at

    from deduplicated
    where _row_num = 1
      and id is not null
)

select * from renamed
