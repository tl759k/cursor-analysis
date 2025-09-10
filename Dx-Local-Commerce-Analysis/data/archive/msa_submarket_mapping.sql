-- select distinct cbsa_code, msa_title from  ericablom.city_msa_crosswalk -- checked all population MSA can be mapped to internal source

select 
  fr.submarket_id,
  -- fr.submarket_name,
  trim(split_part(fr.submarket_name, ',', 1)) as sm_name,
  trim(split_part(fr.submarket_name, ',', 2)) as sm_state,  
  b.*
from fact_region fr
left join ericablom.city_msa_crosswalk b on lower(sm_name) = lower(trim(b.city_name)) and lower(sm_state) = lower(trim(b.state_abbrev))
where fr.country_id = 1 and b.city_name is not null

-- any unmapped submarket is not part of a MSA. 