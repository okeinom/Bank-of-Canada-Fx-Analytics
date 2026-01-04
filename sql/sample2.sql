-- Make sure no duplicate dates exist
select series_id, observation_date, count(*)
from analytics.fct_fx_rates_daily
group by 1,2
having count(*) > 1;
                         