select
    active_week
    , dateadd('week', horizon_spend, active_week) as forecast_week
    , scenario
    , case
        when scenario = 'throughQ1-3kMCPD' then 3000
        when scenario = 'planning-1kQ4Q1' then 1000
        when scenario = 'planning-2kQ4Q1' then 2000
        when scenario = 'planning-4kQ4Q1' then 4000
        when scenario = 'planning-5kQ4Q1' then 5000
        when scenario = 'planning-6kQ4Q1' then 6000
        when scenario = 'planning-7kQ4Q1' then 7000
        when scenario = 'planning-8kQ4Q1' then 8000
        when scenario = 'planning-9kQ4Q1' then 9000
        when scenario = 'planning-10kQ4Q1' then 10000
        when scenario = 'planning-11kQ4Q1' then 11000
        when scenario = 'planning-12kQ4Q1' then 12000
        else null
      end as mcpd_scenario
    , submarket_id
    , horizon_spend
    , horizon_conv
    , avg(spending) spending -- one week of spend: avg because the same exists for all conv and ret horizon.
    , sum(applicants) applicants -- one week of spend will generate this many applicants
    , sum(new_dx) new_dx
from martech.dasher.dac_optimizer_granular_acquisition 
where true
  -- and scenario = 'planning-1kQ4Q1'
  -- and submarket_id = 0
  and forecast_week = '2025-10-06'
group by all
order by forecast_week asc