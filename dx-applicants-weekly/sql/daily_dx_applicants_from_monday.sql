-- Daily DX (Dasher) Applicant Counts Starting from Monday This Week
-- Query to count the number of dasher applicants per day

WITH this_week_monday AS (
  SELECT 
    DATE_TRUNC('week', CURRENT_DATE()) + INTERVAL '1 day' AS monday_date
    -- DATE_TRUNC('week') gives us Sunday, so we add 1 day to get Monday
),

daily_applicants AS (
  SELECT 
    CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', applied_datetime)::DATE AS applied_date,
    COUNT(DISTINCT dasher_applicant_id) AS daily_applicant_count
  FROM edw.dasher.dimension_dasher_applicants dda
  CROSS JOIN this_week_monday twm
  WHERE 
    CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', applied_datetime)::DATE >= twm.monday_date
    AND CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', applied_datetime)::DATE <= CURRENT_DATE()
  GROUP BY 1
)

SELECT 
  applied_date,
  DAYNAME(applied_date) AS day_of_week,
  daily_applicant_count,
  SUM(daily_applicant_count) OVER (ORDER BY applied_date) AS cumulative_count
FROM daily_applicants
ORDER BY applied_date; 