{{ config(materialized='view') }}
 
with tripdata as 
(
  select *,
    row_number() over(partition by dispatching_base_num, pickup_datetime) as rn
  from {{ source('staging','fhv_tripdata') }}
  where pulocationid is not null or dolocationid is not null
)
select

    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,    
    dispatching_base_num,
    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dropoff_locationid,

    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    cast(sr_flag as numeric) as sr_flag,
    affiliated_base_number
from tripdata
where rn = 1 and EXTRACT(YEAR FROM pickup_datetime) = 2019

-- dbt build --select <model.sql> --vars '{'is_test_run: false}'
{% if var('is_test_run', default=false) %}

  limit 100

{% endif %}