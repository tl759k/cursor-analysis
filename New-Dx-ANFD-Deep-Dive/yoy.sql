
create or replace table proddb.static.tbl_lifetime_apps_by_top_100_cities as 

with apps_by_zipcode as (
select
  applied_zip_code
  , applied_submarket_name
  , count(distinct dasher_applicant_id) as apps_18plus
  , count(distinct case when applied_date <= '2024-06-30' then dasher_applicant_id end) as apps_18plus_h1_2024
  , count(distinct case when applied_date <= '2025-12-31' then dasher_applicant_id end) as apps_18plus_h2_2025
from edw.dasher.dimension_dasher_applicants a
left join edw.geo.address b on left(a.applied_zip_code,5) = b.postal_code
where 1=1
  and date_trunc('month', applied_date) between '2019-01-01' and '2025-07-31'
  and applied_country_id = 1 -- limit to US only for now
  and dx_acquisition_allocation_channel = 'Direct'
group by all 
)

, apps_by_city_state as (
select
  case 
    when a.applied_submarket_name in ('Manhattan','Queens','Bronx','Brooklyn','Staten Island') then 'New York'
    else b.locality end 
  as city_name
  , b.administrative_area_level_1 as state_name
  , concat(city_name, ', ', state_name) as city_state
  , sum(apps_18plus) apps_18plus
  , sum(apps_18plus_h1_2024) as apps_18_plus_as_of_h1_2024
  , sum(apps_18plus_h2_2025) as apps_18_plus_as_of_h2_2025
from apps_by_zipcode a 
left join (select distinct postal_code, locality, administrative_area_level_1 from edw.geo.address) b on left(a.applied_zip_code, 5) = b.postal_code
group by all
)

select
  a.city_state
  , apps_18plus
  , apps_18_plus_as_of_h1_2024
  , apps_18_plus_as_of_h2_2025
  , div0(apps_18_plus_as_of_h2_2025, apps_18_plus_as_of_h1_2024) - 1 as yoy
  , b.population
  , div0(a.apps_18plus, b.population) as share_of_18plus_of_population
from apps_by_city_state a
left join abhishah.top_150_us_city_population_census_2023 b on a.city_state = b.city_state
where b.city_rank <= 100
;



grant select on proddb.static.tbl_lifetime_apps_by_top_100_cities to public
;

select * from proddb.static.tbl_lifetime_apps_by_top_100_cities
order by yoy desc

