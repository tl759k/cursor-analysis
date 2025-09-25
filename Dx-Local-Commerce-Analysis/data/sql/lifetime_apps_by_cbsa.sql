-- create or replace table proddb.static.tbl_junk_lifetime_applicants_by_msa as 
-- select
--   b.cbsa
--   , c.msa
--   , c.age18plus_tot
--   , c.age1824_tot
--   -- , c.age2544_tot
--   -- , c.age4564_tot
--   -- , c.age65plus_tot
--   , count(distinct case when age between 18 and 19 then dasher_applicant_id end) apps_18_to_19
--   , count(distinct case when age between 18 and 24 then dasher_applicant_id end) apps_18_to_24
-- --   , count(distinct case when age between 25 and 44 then dasher_applicant_id end) apps_25_to_44
-- --   , count(distinct case when age between 45 and 64 then dasher_applicant_id end) apps_45_to_64
-- --   , count(distinct case when age >= 65 then dasher_applicant_id end) apps_65_above
--   , count(distinct dasher_applicant_id) as apps_18plus
--   , div0(apps_18plus, c.age18plus_tot) as apps_18plus_share_of_population
--   , div0(apps_18_to_24, c.age1824_tot) as apps_18to24_share_of_population
-- from edw.dasher.dimension_dasher_applicants a
-- left join proddb.static.tbl_zipcode_cbsa_mapping b on left(a.applied_zip_code,5) = b.zip 
-- left join proddb.static.tbl_us_population_by_cbsa c on b.cbsa = c.cbsa
-- where 1=1
--   and date_trunc('month', applied_date) between '2019-01-01' and '2025-07-31'
--   and applied_country_id = 1 -- limit to US only for now
--   and dx_acquisition_allocation_channel = 'Direct'
-- group by all 
-- ;

-- grant select on proddb.static.tbl_junk_lifetime_applicants_by_msa to public;

select 
  current_date as Updated_as_of
  , 'Dasher' dataset
  , 'Dasher - lifetime applicants share of population by MSA' description
  , 'MSA' geo_split
  , MSA as geo_dimension
  , 'All' as item_category
  , 'As of' as period
  , '2025-07-31' current_timestamp
  , APPS_18PLUS_SHARE_OF_POPULATION
from proddb.static.tbl_junk_lifetime_applicants_by_msa
where MSA is not null
