-- create or replace table proddb.static.tbl_junk_active_dx_by_msa_2024 as

-- with store_to_city_mapping as (
-- select distinct
--   sto.store_id
--  , add.postal_code
--  , row_number() over(partition by sto.store_id order by add.postal_code asc) rn
-- from public.dimension_store sto 
-- join edw.geo.address add on sto.address_id = add.id 
-- where sto.is_test = false 
-- qualify rn = 1
-- )

-- select 
--   c.cbsa
--   , count(distinct case when f.age between 18 and 19 then a.dasher_id end) active_dx_18_to_19
--   , count(distinct case when f.age between 18 and 24 then a.dasher_id end) active_dx_18_to_24  
--   , count(distinct a.dasher_id) active_dx_18plus
--   , d.age18plus_tot
--   , div0(active_dx_18plus, d.age18plus_tot) as active_dx_18plus_share_of_population
--   , div0(active_dx_18_to_24, d.age18plus_tot) as active_dx_18to24_share_of_population
-- from dimension_deliveries a
-- left join store_to_city_mapping b on a.store_id = b.store_id
-- left join proddb.static.tbl_zipcode_cbsa_mapping c on left(b.postal_code,5) = c.zip 
-- left join proddb.static.tbl_us_population_by_cbsa d on c.cbsa = d.cbsa
-- left join edw.dasher.dimension_dasher_applicants f on a.dasher_id = f.dasher_id
-- where true
--   and a.is_filtered = true
--   and a.is_consumer_pickup = false
--   and nvl(a.fulfillment_type,'') not in ('virtual','shipping','merchant_fleet')
--   and a.active_date::date between '2024-01-01' and '2024-12-31'
-- group by all
-- ;

-- grant select on proddb.static.tbl_junk_active_dx_by_msa_2024 to public;


select * from proddb.static.tbl_junk_active_dx_by_msa_2024