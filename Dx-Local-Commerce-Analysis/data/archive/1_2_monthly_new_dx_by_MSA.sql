with msa_submarket_mapping as (
    select 
      fr.submarket_id,
      trim(split_part(fr.submarket_name, ',', 1)) as sm_name,
      trim(split_part(fr.submarket_name, ',', 2)) as sm_state,  
      b.*
    from fact_region fr
    left join ericablom.city_msa_crosswalk b 
        on lower(sm_name) = lower(trim(b.city_name)) 
       and lower(sm_state) = lower(trim(b.state_abbrev))
    where fr.country_id = 1 
      and b.city_name is not null
)

, monthly_dx as (
    select
        b.submarket_id,
        b.cbsa_code,
        b.msa_title,
        date_trunc('month', a.first_dash_date) as first_dash_month,
        count(distinct case when a.age between 18 and 24 then a.dasher_id end) dx_cnt_18_to_24,
        count(distinct case when a.age between 25 and 44 then a.dasher_id end) dx_cnt_25_to_44,
        count(distinct case when a.age between 45 and 64 then a.dasher_id end) dx_cnt_45_to_64,
        count(distinct case when a.age >= 65 then a.dasher_id end) dx_cnt_65_above,
        count(distinct a.dasher_id) as dx_cnt_18plus
    from edw.dasher.dimension_dasher_applicants a
    left join msa_submarket_mapping b 
        on a.first_dash_submarket_id = b.submarket_id
    where a.first_dash_date is not null
      and a.first_dash_date >= '2024-01-01'
    group by 1,2,3,4
)

, accumulated as (
    select
        submarket_id,
        cbsa_code,
        msa_title,
        first_dash_month,
        dx_cnt_18_to_24,
        dx_cnt_25_to_44,
        dx_cnt_45_to_64,
        dx_cnt_65_above,
        dx_cnt_18plus, 
        sum(dx_cnt_18_to_24) over(partition by submarket_id order by first_dash_month rows unbounded preceding) as cum_dx_18_to_24,
        sum(dx_cnt_25_to_44) over(partition by submarket_id order by first_dash_month rows unbounded preceding) as cum_dx_25_to_44,
        sum(dx_cnt_45_to_64) over(partition by submarket_id order by first_dash_month rows unbounded preceding) as cum_dx_45_to_64,
        sum(dx_cnt_65_above) over(partition by submarket_id order by first_dash_month rows unbounded preceding) as cum_dx_65_above,
        sum(dx_cnt_18plus) over(partition by submarket_id order by first_dash_month rows unbounded preceding) as cum_dx_18plus
    from monthly_dx
)

select * 
from accumulated
order by submarket_id, first_dash_month;
