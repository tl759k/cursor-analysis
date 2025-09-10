select 
  active_month
  , msa_title
  , cbsa_code
  , sum(case when age_bucket = '1-age_18_to_24' then mad end) as mad_18_to_24
  , sum(case when age_bucket = '1-age_18_to_24' then shift_hours end) as shift_hours_18_to_24
  , sum(case when age_bucket = '1-age_18_to_24' then num_delivs end) as num_delivs_18_to_24
  , div0(sum(case when age_bucket = '1-age_18_to_24' then shift_hours end), sum(case when age_bucket = '1-age_18_to_24' then mad end)) as hours_per_dx_18_to_24
  , div0(sum(case when age_bucket = '1-age_18_to_24' then num_delivs end), sum(case when age_bucket = '1-age_18_to_24' then mad end)) as delivs_per_dx_18_to_24
  , sum(mad) mad
  , sum(shift_hours) shift_hours
  , sum(num_delivs) num_delivs
  , div0(sum(shift_hours), sum(mad)) as hours_per_dx
  , div0(sum(num_delivs), sum(mad)) as delivs_per_dx
from proddb.static.tbl_junk_monthly_active_dx_by_msa
where msa_title is not null
group by all
order by 1 asc