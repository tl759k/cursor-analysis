# UH Forecast SLA Dashboard Queries

## Overview
These queries support a dashboard to track UH (Undersupply Hours) forecast SLA performance based on horizon-specific thresholds.

## Key Changes from Original Query

### 1. **Removed Backtesting Logic**
   - All commented-out backtesting CTEs and logic have been removed
   - Query now focuses solely on forecast vs actuals comparison

### 2. **Updated SLA Thresholds (Horizon-Specific)**
   The `within_SLA` calculation now uses different thresholds based on forecast horizon:
   
   ```sql
   case 
     when horizon between 0 and 4 and error_abs < (0.3 / 100) then 1
     when horizon between 5 and 13 and error_abs < (0.45 / 100) then 1
     else 0 
   end as within_SLA
   ```
   
   - **W0 (horizon = 0)**: Error < 0.3%
   - **W1-4 (horizon 1-4)**: Error < 0.3%
   - **W5-13 (horizon 5-13)**: Error < 0.45%

### 3. **Additional SLA Criteria**
   For W0 forecasts, the SLA requires **more than 11 out of 13 weeks** hitting the threshold (calculated in the summary query).

## Files

### 1. `uh_forecast_sla_dashboard.sql`
This is the main query - an updated version of your original query with:
- Backtesting removed
- Horizon-specific SLA thresholds implemented
- Same long-format output for dashboard flexibility

**Use this for:** Detailed time-series analysis, submarket-level breakdowns, and flexible metric pivoting.

### 2. `uh_forecast_sla_summary.sql`
A companion query providing pre-aggregated SLA metrics including:

**Output Views (via `view_type` field):**

- **`Horizon_Summary`**: Overall SLA achievement rates by horizon group
  - Shows total forecasts, SLA hit rate, error statistics
  - Use for high-level KPI cards

- **`Submarket_Performance`**: Submarket-level SLA rates for all horizon groups
  - W0, W1-4, and W5-13 SLA rates per submarket
  - Only includes large submarkets (≥2k deliveries)
  - Use for submarket comparison tables/charts

- **`Weekly_Trend`**: Time-series of SLA achievement
  - Weekly SLA rates by horizon
  - Use for trend line charts

## Dashboard Recommendations

### Key Metrics to Display

1. **Overall SLA Achievement (KPI Cards)**
   - W0: % of forecasts within 0.3%
   - W1-4: % of forecasts within 0.3%
   - W5-13: % of forecasts within 0.45%
   
2. **W0 13-Week Rolling SLA**
   - Calculate: Are ≥11 of last 13 weeks within SLA?
   - Binary pass/fail indicator
   
3. **Trend Charts**
   - Weekly SLA achievement rate over time by horizon
   - Error distribution (median, P90)
   
4. **Submarket Heatmap**
   - Color-code submarkets by SLA achievement
   - Filter to large submarkets only

### Sample Dashboard Queries

**Global W0 SLA Status (Last 13 Weeks):**
```sql
select
  scenario
  , config
  , sum(within_SLA) as weeks_within_sla
  , count(*) as total_weeks
  , case when weeks_within_sla >= 11 then 'PASS' else 'FAIL' end as w0_sla_status
from [use main query output]
where horizon = 0
  and aggregation_level = 'Global'
  and forecast_week >= dateadd('week', -13, current_date)
group by scenario, config
```

**Current Week SLA by Horizon:**
```sql
select
  horizon
  , avg(within_SLA) as sla_rate
  , avg(error_abs_pct) as avg_error_pct
from [use main query output]
where forecast_week = dateadd('week', -1, date_trunc('week', current_date))
  and aggregation_level = 'Global'
group by horizon
order by horizon
```

## Filters for Dashboard

Recommended filters to add:
- **Date Range**: `forecast_created_week`, `forecast_week`
- **Scenario/Config**: For comparing different model versions
- **Aggregation Level**: Global vs Submarket views
- **Horizon Group**: W0, W1-4, W5-13
- **Submarket Size**: Large (≥2k delivs) vs Small

## Notes

- The query filters to `forecast_week <= dateadd('week', -1, current_date)` to only include complete weeks
- Global aggregation uses ideal_online_hours as weights for proper averaging
- Delivery bucket segmentation is preserved for additional analysis
