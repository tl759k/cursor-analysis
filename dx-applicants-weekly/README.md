# DX (Dasher) Applicants Weekly Analysis

This analysis provides daily counts of DX (Dasher) applicants starting from Monday of the current week.

## Files

- `sql/daily_dx_applicants_from_monday.sql` - SQL query for daily DX applicant counts from Monday this week
- `sql/daily_dx_applicants_flexible.sql` - Flexible version that can be easily modified for different date ranges
- `query_dx_applicants.py` - Python script to execute the query and display formatted results

## What is a "DX Applicant"?

DX stands for "Dasher" - DoorDash's delivery drivers. DX applicants are people who have applied to become DoorDash delivery drivers through the application process.

## Data Source

The analysis uses the `edw.dasher.dimension_dasher_applicants` table, which contains records of all dasher applications. The query:

- Converts timestamps from UTC to Pacific Time (America/Los_Angeles)
- Counts distinct `dasher_applicant_id` per day
- Filters for applications from Monday of the current week through today

## Usage

### Run the Analysis
```bash
cd /Users/tl759k/Documents/GitHub/work/cursor-analytics
source venv/bin/activate
python user-analysis/dx-applicants-weekly/query_dx_applicants.py
```

### Modify Date Range
To analyze a different date range, edit `sql/daily_dx_applicants_flexible.sql` and uncomment/modify the date range options.

## Sample Output

```
Daily DX Applicant Counts (Starting from Monday this week)
================================================================================
applied_date day_of_week  daily_applicant_count cumulative_count
  2025-08-19         Tue                   1100             1100

================================================================================
SUMMARY STATISTICS
================================================================================
Total applicants this week: 1,100
Average daily applicants: 1100.0
Highest day: Tue (2025-08-19) with 1,100 applicants
Lowest day: Tue (2025-08-19) with 1,100 applicants
```

## Key Insights from Current Data

- **1,100 DX applicants** applied on Tuesday, August 19, 2025
- This represents all applications received so far this week (starting from Monday)
- Data is timezone-adjusted to Pacific Time for accurate daily counts

## Next Steps

You can extend this analysis by:
- Adding breakdown by acquisition channel (Direct, Paid, Referral)
- Including geographical breakdowns
- Comparing with previous weeks
- Adding conversion rates (applicants to activated dashers) 