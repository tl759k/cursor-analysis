-- with store_to_city_mapping as (
-- select distinct
--   sto.store_id
--   , case 
--       when sto.submarket_name in ('Manhattan','Queens','Bronx','Brooklyn','Staten Island') then 'New York'
--       else add.locality end as city_name
--   , add.administrative_area_level_1 as state_name
--   , concat(city_name, ', ',state_name) as city_state
-- from public.dimension_store sto 
-- join edw.geo.address add on sto.address_id = add.id 
-- where sto.is_test = false 
-- )

-- , last_deliver_store_id as (
-- select 
--   dasher_id
--   , store_id
--   , max_by(business_id, created_at) last_deliv_business_id
--   -- , store_name
--   -- , delivery_vehicle_type
--   , max_by(store_id, created_at) last_deliv_store_id
-- from dimension_deliveries 
-- where true
--   and is_filtered = true
--   and is_consumer_pickup = false
--   and nvl(fulfillment_type,'') not in ('virtual','shipping','merchant_fleet')
-- group by all
-- )

-- , dx_mapped_to_msa as (
-- select distinct
--   a.dasher_id
--   , a.last_deliv_store_id
--   , b.city_state
--   , c.msa_title 
--   , c.cbsa_code
--   , c.fips_place
--   , c.fips_state
-- from last_deliver_store_id a
-- left join store_to_city_mapping b on a.last_deliv_store_id = b.store_id
-- left join ericablom.city_msa_crosswalk c on (c.city_name || ', ' || c.state_abbrev) = b.city_state
-- )

-- , monthly_prep as (
-- select
--   b.cbsa_code
--   , b.msa_title
--   -- , b.fips_place
--   -- , b.fips_state
--   -- , b.city_name
--   -- , b.state_abbrev
--   , date_trunc('month', first_dash_date) as applied_month
--   -- lifetime new dx
--   , count(distinct case when a.age between 18 and 24 then a.dasher_id end) dx_cnt_18_to_24
--   , count(distinct case when a.age between 25 and 44 then a.dasher_id end) dx_cnt_25_to_44
--   , count(distinct case when a.age between 45 and 64 then a.dasher_id end) dx_cnt_45_to_64
--   , count(distinct case when a.age >= 65 then a.dasher_id end) dx_cnt_65_above
--   , dx_cnt_18_to_24 + dx_cnt_25_to_44 + dx_cnt_45_to_64 + dx_cnt_65_above as dx_cnt_18plus
-- from edw.dasher.dimension_dasher_applicants a 
-- left join dx_mapped_to_msa b on a.dasher_id = b.dasher_id
-- where 1=1
--   and first_dash_date >= '2019-01-01'
--   and first_dash_date is not null
--   and applied_country_id = 1 -- limit to U.S. only
--   and dx_acquisition_allocation_channel = 'Direct'
-- group by all
-- )

-- select 
--   *
--   , sum(dx_cnt_18plus) over (partition by cbsa_code order by applied_month rows unbounded preceding) as dx_cnt_18plus_cumsum
--   , sum(dx_cnt_18_to_24) over (partition by cbsa_code order by applied_month rows unbounded preceding) as dx_cnt_18_to_24_cumsume
-- from monthly_prep


select 
  * 

from proddb.static.tbl_junk_lifetime_new_dx_by_msa