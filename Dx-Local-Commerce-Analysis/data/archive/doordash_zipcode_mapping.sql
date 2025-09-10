
select distinct
  applied_zip_code as applied_zip_code
  , applied_submarket_id
  , applied_submarket_name
  , applied_market_id
  , applied_market_name
  , applied_region_id
  , applied_region_name
  , applied_country_id
  , applied_country_name
from edw.dasher.dimension_dasher_applicants
where 1=1
  and date_trunc('quarter', applied_date) between '2020-01-01' and '2025-07-31'
  and applied_country_id = 1 -- limit to US only for now
