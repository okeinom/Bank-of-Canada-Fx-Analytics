with base as (
  select
    observation_date,
    series_id,
    value
  from {{ ref('stg_boc_fx_observations') }}
),

features as (
  select
    observation_date,
    series_id,
    value,

    value - lag(value) over (partition by series_id order by observation_date) as daily_change,
    safe_divide(value, lag(value) over (partition by series_id order by observation_date)) - 1 as pct_change,

    avg(value) over (
      partition by series_id
      order by observation_date
      rows between 6 preceding and current row
    ) as ma_7,

    stddev_pop(value) over (
      partition by series_id
      order by observation_date
      rows between 13 preceding and current row
    ) as rolling_std_14
  from base
)

select * from features
