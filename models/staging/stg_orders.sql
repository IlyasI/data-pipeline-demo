/*
    Staging model for raw orders.

    Responsibilities:
    - Rename columns to consistent snake_case
    - Cast types explicitly
    - Filter out test/invalid records
    - Deduplicate on primary key
*/

with source as (
    select * from {{ source('raw', 'orders') }}
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
        cast(id as integer)                     as order_id,

        -- Foreign keys
        cast(user_id as integer)                as customer_id,

        -- Attributes
        cast(status as varchar)                 as order_status,

        -- Timestamps
        cast(order_date as date)                as ordered_at,
        cast(_loaded_at as timestamp)           as _loaded_at

    from deduplicated
    where _row_num = 1
      and id is not null
)

select * from renamed
