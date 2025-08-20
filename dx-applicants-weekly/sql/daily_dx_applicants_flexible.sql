-- Daily DX (Dasher) Applicant Counts - Flexible Version
-- Query to count the number of dasher applicants per day for any date range
-- 
-- Instructions:
-- 1. To change the start date, modify the 'start_date' value below
-- 2. To change the end date, modify the 'end_date' value below
-- 3. Current setting: Monday of this week through today

WITH date_range AS (
  SELECT 
    -- Option 1: Monday of this week (current setting)
    DATE_TRUNC('week', CURRENT_DATE()) + INTERVAL '1 day' AS start_date,
    CURRENT_DATE() AS end_date
    
    -- Option 2: Specific date range (uncomment and modify as needed)
    -- '2025-08-19'::DATE AS start_date,
    -- '2025-08-25'::DATE AS end_date
    
    -- Option 3: Last 7 days (uncomment to use)
    -- CURRENT_DATE() - INTERVAL '7 days' AS start_date,
    -- CURRENT_DATE() AS end_date
),

daily_applicants AS (
  SELECT 
    CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', applied_datetime)::DATE AS applied_date,
    COUNT(DISTINCT dasher_applicant_id) AS daily_applicant_count
  FROM edw.dasher.dimension_dasher_applicants dda
  CROSS JOIN date_range dr
  WHERE 
    CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', applied_datetime)::DATE >= dr.start_date
    AND CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', applied_datetime)::DATE <= dr.end_date
  GROUP BY 1
)

SELECT 
  applied_date,
  DAYNAME(applied_date) AS day_of_week,
  daily_applicant_count,
  SUM(daily_applicant_count) OVER (ORDER BY applied_date) AS cumulative_count,
  -- Additional metrics
  AVG(daily_applicant_count) OVER () AS avg_daily_in_period,
  daily_applicant_count - AVG(daily_applicant_count) OVER () AS variance_from_avg
FROM daily_applicants
ORDER BY applied_date; 