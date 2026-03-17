/*
    Staging model for raw customers.

    Responsibilities:
    - Rename columns to consistent snake_case
    - Cast types explicitly
    - Deduplicate on primary key
    - Trim whitespace from string fields
*/

with source as (
    select * from {{ source('raw', 'customers') }}
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
        cast(id as integer)                             as customer_id,

        -- Attributes
        trim(cast(first_name as varchar))               as first_name,
        trim(cast(last_name as varchar))                as last_name,

        -- Metadata
        cast(_loaded_at as timestamp)                   as _loaded_at

    from deduplicated
    where _row_num = 1
      and id is not null
)

select * from renamed
