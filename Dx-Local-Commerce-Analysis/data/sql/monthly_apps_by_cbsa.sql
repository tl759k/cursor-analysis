
-- create or replace table proddb.static.tbl_junk_monthly_applicants_by_cbsa as 

-- with monthly_apps as (
-- select
--   b.cbsa
--   , date_trunc('month', applied_date) as month
--   , count(distinct dasher_applicant_id) as apps
--   , count(distinct case when age between 18 and 19 then dasher_applicant_id end) apps_18_to_19
--   , count(distinct case when age between 18 and 24 then dasher_applicant_id end) apps_18_to_24
--   , count(distinct case when age between 25 and 44 then dasher_applicant_id end) apps_25_to_44
--   , count(distinct case when age between 45 and 64 then dasher_applicant_id end) apps_45_to_64
--   , count(distinct case when age >= 65 then dasher_applicant_id end) apps_65_above
--   , count(distinct dasher_applicant_id) as apps_18plus
-- from edw.dasher.dimension_dasher_applicants a
-- left join proddb.static.tbl_junk_zipcode_cbsa_mapping b on left(a.applied_zip_code,5) = b.zip 
-- where 1=1
--   and date_trunc('month', applied_date) between '2023-01-01' and '2025-07-31'
--   and applied_country_id = 1 -- limit to US only for now
--   and dx_acquisition_allocation_channel = 'Direct'
-- group by all
-- )

-- select
--   a.*
--   , b.msa
--   , b.age18plus_tot
--   , b.age1824_tot
--   -- , b.age2544_tot
--   -- , b.age4564_tot
--   -- , b.age65plus_tot
--   , div0(a.apps_18plus, b.age18plus_tot) as apps_18plus_share_of_population
--   , div0(a.apps_18_to_24, b.age1824_tot) as apps_18to24_share_of_population
--   -- , div0(a.apps_25_to_44, b.age2544_tot) as apps_25to44_share_of_population
--   -- , div0(a.apps_45_to_64, b.age4564_tot) as apps_45to64_share_of_population
--   -- , div0(a.apps_65_above, b.age65plus_tot) as apps_65above_share_of_population
-- from monthly_apps a
-- left join proddb.static.tbl_us_population_by_cbsa b on a.cbsa = b.cbsa
-- ;

-- grant select on proddb.static.tbl_junk_monthly_applicants_by_cbsa to public;

select * from proddb.static.tbl_junk_monthly_applicants_by_cbsa