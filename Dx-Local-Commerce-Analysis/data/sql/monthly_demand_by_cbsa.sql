-- create or replace table proddb.static.tbl_monthly_demand_by_cbsa as

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
--   date_trunc('month', a.created_at::date) month -- delivery creation month
--   , c.cbsa
--   , count(distinct a.delivery_id) num_delivs
-- from dimension_deliveries a
-- left join store_to_city_mapping b on a.store_id = b.store_id
-- left join proddb.static.tbl_zipcode_cbsa_mapping c on left(b.postal_code,5) = c.zip 
-- where true
--   and a.is_filtered = true
--   and a.is_consumer_pickup = false
--   and nvl(a.fulfillment_type,'') not in ('virtual','shipping','merchant_fleet')
--   and date_trunc('month', a.created_at::date) between '2023-01-01' and '2025-07-31'
-- group by all
-- ;

-- grant select on proddb.static.tbl_monthly_demand_by_cbsa to public;


select * from proddb.static.tbl_monthly_demand_by_cbsa