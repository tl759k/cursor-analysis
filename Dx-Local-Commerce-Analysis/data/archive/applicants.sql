with all_zipcodes as (
select distinct
  applied_zip_code as applied_zip_code
from edw.dasher.dimension_dasher_applicants
where 1=1
  and date_trunc('month', applied_date) between '2020-01-01' and '2025-07-31'
  and applied_country_id = 1 -- limit to US only for now
)

, week_dates as (
select distinct
  date_trunc('month', first_date_of_week_iso) as reporting_month
from edw.core.dimension_dates
where date_trunc('month', first_date_of_week_iso) between '2020-01-01' and '2025-07-31'
order by reporting_month asc
)

, all_zipcodes_with_all_months as (
select
  applied_zip_code
  , reporting_month
from all_zipcodes
cross join week_dates
)

, applicants_by_zipcode as (
select
  date_trunc('month', applied_date) applied_month
  , applied_submarket_id
  , applied_submarket_name
  , applied_market_id
  , applied_market_name
  , applied_region_id
  , applied_region_name
  , applied_country_id
  , applied_country_name
  , applied_zip_code
  , count(distinct dasher_applicant_id) as applicants
from edw.dasher.dimension_dasher_applicants
where 1=1
  and date_trunc('month', applied_date) between '2020-01-01' and '2025-07-31'
  and applied_country_id = 1 -- limit to US only for now
group by all
)

, new_dx_by_zipcode as (
select
  date_trunc('month', first_dash_date) first_dash_month
  , applied_submarket_id
  , applied_submarket_name
  , applied_market_id
  , applied_market_name
  , applied_region_id
  , applied_region_name
  , applied_country_id
  , applied_country_name
  , applied_zip_code
  , count(distinct dasher_applicant_id) as new_dx
from edw.dasher.dimension_dasher_applicants
where 1=1
  and date_trunc('month', first_dash_date) between '2020-01-01' and '2025-07-31'
  and applied_country_id = 1 -- limit to US only for now
group by all
)

, zipcode_level as (
select
  a.applied_zip_code
  , a.reporting_month
  , b.applied_submarket_id
  , b.applied_submarket_name
  , b.applied_market_id
  , b.applied_market_name
  , b.applied_region_id
  , b.applied_region_name
  , b.applied_country_id
  , b.applied_country_name
  , b.applicants
  , c.new_dx
from all_zipcodes_with_all_months a
left join applicants_by_zipcode b on a.applied_zip_code = b.applied_zip_code and b.applied_month = a.reporting_month
left join new_dx_by_zipcode c on a.applied_zip_code = c.applied_zip_code and c.first_dash_month = a.reporting_month
)

select
  reporting_month
  -- , applied_market_id
  -- , applied_market_name
  -- , applied_submarket_id
  -- , applied_submarket_name
  , sum(applicants) total_applicants
  , sum(new_dx) total_new_dx
  , sum(total_applicants) over (order by reporting_month) as applicants_cumsum
  , sum(total_new_dx) over (order by reporting_month) as new_dx_cumsum
from zipcode_level
group by all
order by reporting_month asc
