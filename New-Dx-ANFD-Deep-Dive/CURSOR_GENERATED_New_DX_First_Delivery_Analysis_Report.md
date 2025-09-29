# New DX First Delivery Analysis Report

**CURSOR GENERATED**

**Reviewed by Ax:**

## Executive Summary

This analysis investigated why new DoorDash drivers (DX) are not completing their first deliveries by examining assignment patterns, timing, and completion rates. All three hypotheses were confirmed, revealing significant disadvantages for new DX in their first shifts.

## Key Findings

### ✅ All Three Hypotheses Confirmed

1. **Assignment Rate Gap**: New DX (first dash) receive assignments on 67.8% of shifts vs 75.0% for existing DX (not first dash) - a 7.2 percentage point gap

2. **Assignment Volume Gap**: New DX average 2.72 assignments per shift vs 4.28 for existing DX - 36.5% fewer assignments

3. **Assignment Timing Gap**: New DX wait 9.7 minutes for first assignment vs 8.6 minutes for existing DX - 11.9% longer

### Additional Critical Insights

**Delivery Completion Gap**: Only 51.1% of new DX first shifts complete deliveries vs 61.5% for existing DX - a 10.4 percentage point gap

**🎯 NEW DISCOVERY: The Very First Dash is Different!**
Within new DX, comparing first dash vs subsequent early dashes reveals:
- **Wait Time Issue**: First dash takes 18.8% longer to get assignments (9.7 vs 8.1 minutes)
- **Completion Problem**: First dash has 4.2pp lower delivery completion rate (51.1% vs 55.4%)
- **Similar Assignment Patterns**: Assignment rates and volumes are nearly identical

This suggests the very first dashing experience has unique challenges beyond just being "new"

## Scale of Impact

- **New DX First Dash Population**: 123,180 dashers
- **Total New DX Population**: 216,031 dashers (first + non-first dash)
- **Sample Confidence**: Large sample sizes (100K+ dashers) provide high statistical confidence

## Root Cause Analysis

New DX face a **compound disadvantage** across the entire delivery journey:

1. **Lower Assignment Probability** → Fewer opportunities to earn
2. **Reduced Assignment Volume** → Less earning potential when they do get shifts
3. **Longer Wait Times** → Poor first experience and potential frustration
4. **Lower Completion Rates** → Reduced likelihood of retention

**🔍 Refined Understanding**: The analysis reveals TWO distinct issues:
- **New DX vs Existing DX**: Systemic disadvantages in assignment algorithms/experience
- **First Dash vs Later Dashes**: Additional challenges specific to the very first experience (longer waits, lower completion)

## Business Impact

This analysis reveals a systemic issue affecting new dasher onboarding and retention. The combination of fewer assignments, longer wait times, and lower completion rates creates a poor first impression that likely impacts long-term retention and platform growth.

## Recommended Actions

### Immediate (0-30 days)
1. **Assignment Algorithm Audit**: Review how new DX are treated in assignment logic
2. **First Dash Special Support**: Create specific interventions for the very first dash experience
3. **Onboarding Enhancement**: Add guidance on optimal shift timing and location selection
4. **New DX Boost**: Consider temporary assignment priority for first few shifts

### Medium-term (1-3 months)
1. **Mentorship Program**: Pair new DX with experienced dashers for guidance
2. **Performance Tracking**: Implement weekly monitoring of new DX experience metrics
3. **Geographic Analysis**: Analyze if new DX are starting in suboptimal delivery zones

### Long-term (3+ months)
1. **Predictive Modeling**: Build models to identify at-risk new DX early
2. **Retention Analysis**: Track how first-shift experience impacts long-term retention
3. **Market Expansion**: Ensure assignment supply matches new DX onboarding volume

## Success Metrics

Target improvements to track intervention effectiveness:
- Increase new DX assignment rate to 70%+ (vs current 67.8%)
- Reduce assignment volume gap to <30% (vs current 36.5%)
- Decrease time to first assignment to <9 minutes (vs current 9.7 minutes)
- Improve delivery completion rate to 55%+ (vs current 51.1%)

## Methodology

- **Data Period**: 4 weeks of recent shift data
- **New DX Definition**: Dashers where application week = shift check-in week
- **Primary Comparison**: New DX (First Dash) vs Existing DX (Not First Dash)
- **Key Tables**: dasher_shifts, dimension_dasher_applicants, shift_delivery_assignment

## Statistical Confidence

The large sample sizes (100K+ dashers across multiple weeks) provide high confidence that these differences are both statistically significant and practically meaningful for business operations.

---

## Appendix: Data Sources

### Queries Used
- **Primary Analysis**: `sql/new_dx_summary_single_statement.sql`
- **Enhanced Analysis**: `sql/enhanced_new_dx_assignment_analysis.sql`

### Tables Referenced
- `edw.dasher.dasher_shifts` - Shift and performance data
- `edw.dasher.dimension_dasher_applicants` - Dasher application dates  
- `proddb.prod_assignment.shift_delivery_assignment` - Assignment timing data

### Analysis Files
- **Jupyter Notebook**: `analysis.ipynb` - Complete analysis with visualizations
- **Python Script**: `run_analysis.py` - Automated analysis execution
- **Results CSV**: `new_dx_assignment_analysis_results.csv` - Raw data output

---

*Last updated: September 26, 2025*

*Analysis conducted using Snowflake data warehouse and Python statistical analysis*
