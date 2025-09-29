
-- Actual Spend by Quarter
with submarket_level as (
select
  date_trunc('quarter', fdsa.spend_date) as quarter
  , date_trunc('week', fdsa.spend_date) as week
  , fdsa.submarket_id
  , sum(fdsa.allocated_spend) as weekly_spend
from edw.growth.fact_dasher_spend_allocation as fdsa
join proddb.static.dasher_spending_channel_mapping as m using(channel, subchannel, partner)
left join static.daco_exp_group_split_0331_v2 c on fdsa.submarket_id = c.submarket_id
where 1=1
  and fdsa.spend_date between '2024-01-01' and '2025-06-30'
  and fdsa.submarket_id in (5,7,81)
group by all
)

, global_level as (
select
  date_trunc('quarter', fdsa.spend_date) as quarter
  , date_trunc('week', fdsa.spend_date) as week
  , 0 as submarket_id
  , sum(fdsa.allocated_spend) as weekly_spend
from edw.growth.fact_dasher_spend_allocation as fdsa
join proddb.static.dasher_spending_channel_mapping as m using(channel, subchannel, partner)
left join static.daco_exp_group_split_0331_v2 c on fdsa.submarket_id = c.submarket_id
left join fact_region b on fdsa.submarket_id = b.submarket_id
where 1=1
  and fdsa.spend_date between '2024-01-01' and '2025-06-30'
--   and b.country_id = 1 -- limit to US only for now
group by all
)

, actuals as (
select * from submarket_level
union all
select * from global_level
)

select 
 quarter
 , submarket_id
 , avg(weekly_spend) as avg_weekly_spend
from actuals
group by all
order by quarter asc, submarket_id asc