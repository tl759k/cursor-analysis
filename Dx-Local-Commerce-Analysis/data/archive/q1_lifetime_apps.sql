-- Market Saturation: Lifetime Applicants by MSA
with phone_duplicates as ( 
select 
  phone_number
  , min(applied_date) as first_applied_date
  , count(distinct dasher_applicant_id) as dx_count
from edw.dasher.dimension_dasher_applicants 
where 1=1
group by 1
having dx_count > 1 
)

, monthly_applicants_by_age_bucket as (
select
  applied_submarket_id as submarket_id
  , date_trunc('month', applied_date) as applied_month
  , case when (pd.phone_number is null -- No duplicate phone number
          or pd.first_applied_date = a.applied_date) -- OR this is the first application from a phone number
      then 'Non-Dupe' else 'Dupe' end as phone_duplicate_flag
  , count(distinct case when age between 18 and 19 then dasher_applicant_id end) apps_18_to_19
  , count(distinct case when age between 18 and 24 then dasher_applicant_id end) apps_18_to_24
  , count(distinct case when age between 25 and 44 then dasher_applicant_id end) apps_25_to_44
  , count(distinct case when age between 45 and 64 then dasher_applicant_id end) apps_45_to_64
  , count(distinct case when age >= 65 then dasher_applicant_id end) apps_65_above
  , count(distinct dasher_applicant_id) as apps_18plus
from edw.dasher.dimension_dasher_applicants a
left join phone_duplicates pd on pd.phone_number = a.phone_number 
where 1=1
  and applied_date between '2019-01-01' and '2025-08-31'
  and applied_country_id = 1 -- limit to U.S.
  and phone_duplicate_flag = 'Non-Dupe'
  and dx_acquisition_allocation_channel = 'Direct' -- organic only
group by all
)

select
  submarket_id
  , applied_month
  , apps_18_to_19
  , apps_18_to_24
  , apps_25_to_44
  , apps_45_to_64
  , apps_65_above
  , apps_18plus
  , sum(apps_18_to_19) over (partition by submarket_id order by applied_month asc rows between unbounded preceding and current row) apps_18_to_19_cumsum 
  , sum(apps_18_to_24) over (partition by submarket_id order by applied_month asc rows between unbounded preceding and current row) apps_18_to_24_cumsum
  , sum(apps_25_to_44) over (partition by submarket_id order by applied_month asc rows between unbounded preceding and current row) apps_25_to_44_cumsum
  , sum(apps_45_to_64) over (partition by submarket_id order by applied_month asc rows between unbounded preceding and current row) apps_45_to_64_cumsum
  , sum(apps_65_above) over (partition by submarket_id order by applied_month asc rows between unbounded preceding and current row) apps_65_above_cumsum
  , sum(apps_18plus) over (partition by submarket_id order by applied_month asc rows between unbounded preceding and current row) apps_18plus_cumsum
from monthly_applicants_by_age_bucket