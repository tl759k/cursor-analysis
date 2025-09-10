-- Market Saturation: Lifetime Applicants by MSA
with monthly_applicants_by_age_bucket as (
select
  applied_submarket_id as submarket_id
  , date_trunc('month', first_dash_date) as first_dash_month
  , count(distinct case when age between 18 and 19 then dasher_applicant_id end) new_dx_18_to_19
  , count(distinct case when age between 18 and 24 then dasher_applicant_id end) new_dx_18_to_24
  , count(distinct case when age between 25 and 44 then dasher_applicant_id end) new_dx_25_to_44
  , count(distinct case when age between 45 and 64 then dasher_applicant_id end) new_dx_45_to_64
  , count(distinct case when age >= 65 then dasher_applicant_id end) new_dx_65_above
  , count(distinct dasher_applicant_id) as new_dx_18plus
from edw.dasher.dimension_dasher_applicants
where 1=1
  and first_dash_date between '2019-01-01' and '2025-08-31'
  and applied_country_id = 1 -- limit to U.S.
  and dx_acquisition_allocation_channel = 'Direct' -- organic only
group by all
)

select
  submarket_id
  , first_dash_month
  , new_dx_18_to_19
  , new_dx_18_to_24
  , new_dx_25_to_44
  , new_dx_45_to_64
  , new_dx_65_above
  , new_dx_18plus
  , sum(new_dx_18_to_19) over (partition by submarket_id order by first_dash_month asc rows between unbounded preceding and current row) new_dx_18_to_19_cumsum 
  , sum(new_dx_18_to_24) over (partition by submarket_id order by first_dash_month asc rows between unbounded preceding and current row) new_dx_18_to_24_cumsum
  , sum(new_dx_25_to_44) over (partition by submarket_id order by first_dash_month asc rows between unbounded preceding and current row) new_dx_25_to_44_cumsum
  , sum(new_dx_45_to_64) over (partition by submarket_id order by first_dash_month asc rows between unbounded preceding and current row) new_dx_45_to_64_cumsum
  , sum(new_dx_65_above) over (partition by submarket_id order by first_dash_month asc rows between unbounded preceding and current row) new_dx_65_above_cumsum
  , sum(new_dx_18plus) over (partition by submarket_id order by first_dash_month asc rows between unbounded preceding and current row) new_dx_18plus_cumsum
from monthly_applicants_by_age_bucket