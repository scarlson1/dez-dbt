with source as (
    select * from {{ source('raw_data', 'yellow_tripdata') }}
),

renamed as (
    select
        -- identifiers (standardized naming for consistency across yellow/green)
        cast(vendorid as integer) as vendor_id,
        cast(ratecodeid as integer) as rate_code_id,
        cast(pulocationid as integer) as pickup_location_id,
        cast(dolocationid as integer) as dropoff_location_id,

        -- timestamps (standardized naming)
        cast(tpep_pickup_datetime as timestamp) as pickup_datetime,  -- tpep = Taxicab Passenger Enhancement Program (yellow taxis)
        cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,

        -- trip info
        store_and_fwd_flag,
        cast(passenger_count as integer) as passenger_count,
        cast(trip_distance as numeric) as trip_distance,

        -- payment info
        cast(fare_amount as numeric) as fare_amount,
        cast(extra as numeric) as extra,
        cast(mta_tax as numeric) as mta_tax,
        cast(tip_amount as numeric) as tip_amount,
        cast(tolls_amount as numeric) as tolls_amount,
        cast(improvement_surcharge as numeric) as improvement_surcharge,
        cast(total_amount as numeric) as total_amount,
        cast(payment_type as integer) as payment_type

    from source
    -- Filter out records with null vendor_id (data quality requirement)
    where vendorid is not null
)

select * from renamed

-- Sample records for dev environment using deterministic date filter
{% if target.name == 'dev' %}
where pickup_datetime >= '2019-01-01' and pickup_datetime < '2019-02-01'
{% endif %}

-- -- source match from sources.yaml (source name and table name)
-- -- rename columns and cast here and order according to category
-- select 
--   -- *
--   -- identifiers
--   cast(vendorid as int) as vendor_id,
--   cast(ratecodeid as int) as rate_code_id,
--   cast(pulocationid as int) as pickup_location_id,
--   cast(dolocationid as int) as dropoff_location_id,

--   -- timestamps
--   cast(tpep_pickup_datetime as timestamp) as pickup_datetime,
--   cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,

--   -- trip info
--   store_and_fwd_flag,
--   cast(passenger_count as int) as passenger_count,
--   cast(trip_distance as float) as trip_distance,
--   1 as trip_type, -- yellow cabs only requested by flagging a cab (no app request)

--   -- payment info
--   cast(fare_amount as numeric) as fare_amount,
--   cast(extra as numeric) as extra,
--   cast(mta_tax as numeric) as mta_tax,
--   cast(tip_amount as numeric) as tip_amount,
--   cast(tolls_amount as numeric) as tolls_amount,
--   cast(improvement_surcharge as numeric) as improvement_surcharge,
--   cast(total_amount as numeric) as total_amount,
--   cast(payment_type as int) as payment_type,
--   0 as ehail_fee,
--   -- congestion_surcharge,
--   -- airport_fee,
-- from {{ source('raw_data', 'yellow_tripdata') }}
-- where vendorid is not null