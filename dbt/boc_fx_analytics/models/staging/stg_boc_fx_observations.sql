with raw_tbl as (

  select
    series_id,
    observation_date,
    value,
    ingested_at,
    source
  from {{ source('raw', 'boc_fx_observations') }}

),

clean as (

  select
    cast(raw_tbl.series_id as string) as series_id,
    cast(raw_tbl.observation_date as date) as observation_date,
    cast(raw_tbl.value as float64) as value,
    cast(raw_tbl.ingested_at as timestamp) as ingested_at,
    cast(raw_tbl.source as string) as source
  from raw_tbl
  where raw_tbl.series_id is not null
    and raw_tbl.observation_date is not null
    and raw_tbl.value is not null

)

select * from clean
