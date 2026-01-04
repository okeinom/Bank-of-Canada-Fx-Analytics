-- Compare FX volatility before vs after 2022
select
  extract(year from observation_date) as year,
  avg(rolling_std_14) as avg_volatility
from analytics.fct_fx_features
group by year
order by year;
