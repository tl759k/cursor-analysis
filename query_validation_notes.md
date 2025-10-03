# UH Forecast SLA Dashboard - Query Modifications

## Changes Made

### 1. Removed Backtesting Components ✅
- Removed all commented `backtest_pred` CTE
- Removed `actuals_vs_backtest` CTE  
- Removed union with backtesting data in `combined` CTE
- Cleaned up all backtesting references

### 2. Implemented Dynamic SLA Logic ✅
- **W0 (horizon = 0)**: Error threshold < 0.3%, requires 11/13 weeks hitting SLA
- **W1-4 (horizon 1-4)**: Error threshold < 0.3%, requires 100% SLA hit rate
- **W5-13 (horizon 5-13)**: Error threshold < 0.45%, requires 100% SLA hit rate

### 3. Added SLA Tracking Features ✅
- Dynamic `sla_threshold` calculation based on horizon
- Updated `within_SLA` logic to use dynamic thresholds
- Added SLA performance summaries for W0 and W1-13
- Created dashboard-ready output with SLA status indicators

## Key Query Files

### `uh_forecast_sla_dashboard.sql`
- Main query with all original functionality
- Includes dynamic SLA thresholds
- Maintains long format output for flexibility
- Includes commented SLA summary sections

### `uh_forecast_sla_dashboard_summary.sql` 
- Dashboard-focused query
- Provides SLA performance summaries by horizon group
- Includes health score calculations
- Ready for dashboard consumption

## SLA Logic Implementation

```sql
-- Dynamic SLA thresholds
case 
  when horizon = 0 then 0.3 / 100      -- W0: 0.3%
  when horizon between 1 and 4 then 0.3 / 100   -- W1-4: 0.3%  
  when horizon between 5 and 13 then 0.45 / 100  -- W5-13: 0.45%
  else 0.45 / 100
end as sla_threshold

-- SLA Status Logic
case 
  when horizon = 0 and forecasts_hitting_sla >= 11 and total_forecasts >= 13 then 'PASS'
  when horizon = 0 then 'FAIL'
  when horizon between 1 and 13 and sla_hit_rate = 1.0 then 'PASS'
  when horizon between 1 and 13 then 'FAIL'
  else 'N/A'
end as sla_status
```

## Dashboard Outputs

1. **SLA Summary**: Performance by horizon group and submarket
2. **Weekly Trend**: Time series of SLA performance  
3. **Health Score**: Overall forecast quality assessment
4. **Detailed Metrics**: All original metrics plus SLA indicators

## Validation Checklist

- [x] Backtesting code removed
- [x] Dynamic SLA thresholds implemented
- [x] W0 special logic (11/13 weeks) implemented
- [x] W1-4 and W5-13 standard SLA logic implemented
- [x] Dashboard-ready summary created
- [x] Original functionality preserved
- [x] Query structure validated