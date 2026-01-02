select
  observation_date,
  series_id,
  value
from {{ ref('stg_boc_fx_observations') }}