with wad as (
select 
  date_trunc('week', a.active_date) active_week
  , a.submarket_id
  , count(distinct case when b.age between 18 and 19 then a.dasher_id end) wad_18_to_19
  , count(distinct case when b.age between 18 and 24 then a.dasher_id end) wad_18_to_24
  , count(distinct case when b.age between 25 and 44 then a.dasher_id end) wad_25_to_44
  , count(distinct case when b.age between 45 and 64 then a.dasher_id end) wad_45_to_64
  , count(distinct case when b.age >= 65 then a.dasher_id end) wad_65_above
  , count(distinct a.dasher_id) wad
from dimension_deliveries a 
left join edw.dasher.dimension_dasher_applicants b on b.dasher_id = a.dasher_id
where true
  and a.is_filtered = true
  and a.is_consumer_pickup = false
  and nvl(a.fulfillment_type,'') not in ('virtual','shipping','merchant_fleet')
  and active_week between '2019-01-01' and '2025-08-31'
group by all
)

select 
  submarket_id
  , date_trunc('month', active_week) active_month
  , avg(wad) wad_18plus
  , avg(wad_18_to_19) wad_18_to_19
  , avg(wad_18_to_24) wad_18_to_24
  , avg(wad_25_to_44) wad_25_to_44
  , avg(wad_45_to_64) wad_45_to_64
  , avg(wad_65_above) wad_65_above
from wad
group by all
order by submarket_id asc, active_month asc