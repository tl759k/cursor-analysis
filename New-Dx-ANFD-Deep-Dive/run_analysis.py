#!/usr/bin/env python3

import sys
import os
sys.path.append('../../utils')

import pandas as pd
import numpy as np
from snowflake_connection import SnowflakeHook

def main():
    print("New DX Assignment Analysis")
    print("="*50)
    
    # Initialize Snowflake connection
    print("Connecting to Snowflake...")
    snowhook = SnowflakeHook()
    
    # Read the summary analysis query
    with open('sql/new_dx_summary_single_statement.sql', 'r') as f:
        summary_query = f.read()
    
    print("Executing summary analysis query...")
    
    # Execute the summary query
    summary_df = snowhook.query_snowflake(summary_query, method='pandas')
    print(f"Query executed successfully! Retrieved {len(summary_df)} rows.")
    
    # Display column names first
    print("\nColumn names:", list(summary_df.columns))
    print("\nFirst few rows:")
    print(summary_df.head())
    
    # Display results
    print("\nSUMMARY ANALYSIS RESULTS:")
    print("="*50)
    
    # Create readable labels (using correct lowercase column names)
    summary_df['dx_type'] = summary_df.apply(lambda row: 
        'New DX (First Dash)' if row['new_dx_l7d'] == 'Y' and row['is_first_dash'] 
        else 'New DX (Not First Dash)' if row['new_dx_l7d'] == 'Y' and not row['is_first_dash']
        else 'Existing DX (First Dash)' if row['new_dx_l7d'] == 'N' and row['is_first_dash']
        else 'Existing DX (Not First Dash)', axis=1)
    
    # Key metrics to analyze (using correct lowercase column names)
    key_metrics = [
        'dx_type', 'total_dashers', 'total_shifts', 
        'pct_shifts_with_assignments', 'avg_assignments_per_shift',
        'pct_shifts_with_deliveries', 'avg_minutes_to_first_assignment'
    ]
    
    print("\nKey Metrics Summary:")
    result_summary = summary_df[key_metrics].round(3)
    print(result_summary.to_string(index=False))
    
    # Hypothesis validation
    print("\n" + "="*60)
    print("HYPOTHESIS VALIDATION")
    print("="*60)
    
    # Separate new DX from existing DX (using correct lowercase column names)
    new_dx_first = summary_df[(summary_df['new_dx_l7d'] == 'Y') & (summary_df['is_first_dash'] == True)]
    new_dx_not_first = summary_df[(summary_df['new_dx_l7d'] == 'Y') & (summary_df['is_first_dash'] == False)]
    existing_dx_first = summary_df[(summary_df['new_dx_l7d'] == 'N') & (summary_df['is_first_dash'] == True)]
    existing_dx_not_first = summary_df[(summary_df['new_dx_l7d'] == 'N') & (summary_df['is_first_dash'] == False)]
    
    def safe_get_value(df, column, default=0):
        """Safely get a value from dataframe, return default if empty"""
        if len(df) > 0:
            return df[column].iloc[0]
        return default
    
    print("\n1. HYPOTHESIS 1: New DX receive fewer assignments after shift check-in")
    print("-" * 70)
    
    new_dx_first_assignment_rate = safe_get_value(new_dx_first, 'pct_shifts_with_assignments')
    existing_dx_not_first_assignment_rate = safe_get_value(existing_dx_not_first, 'pct_shifts_with_assignments')
    
    print(f"New DX (First Dash) assignment rate: {new_dx_first_assignment_rate:.1%}")
    print(f"Existing DX (Not First Dash) assignment rate: {existing_dx_not_first_assignment_rate:.1%}")
    
    if new_dx_first_assignment_rate < existing_dx_not_first_assignment_rate:
        diff = existing_dx_not_first_assignment_rate - new_dx_first_assignment_rate
        print(f"✓ CONFIRMED: New DX have {diff:.1%} lower assignment rate")
    else:
        print("✗ NOT CONFIRMED: New DX do not have lower assignment rates")
    
    print("\n2. HYPOTHESIS 2: New DX receive fewer total assignments during shifts")
    print("-" * 70)
    
    new_dx_first_avg_assignments = safe_get_value(new_dx_first, 'avg_assignments_per_shift')
    existing_dx_not_first_avg_assignments = safe_get_value(existing_dx_not_first, 'avg_assignments_per_shift')
    
    print(f"New DX (First Dash) avg assignments per shift: {new_dx_first_avg_assignments:.2f}")
    print(f"Existing DX (Not First Dash) avg assignments per shift: {existing_dx_not_first_avg_assignments:.2f}")
    
    if new_dx_first_avg_assignments < existing_dx_not_first_avg_assignments:
        diff = existing_dx_not_first_avg_assignments - new_dx_first_avg_assignments
        pct_diff = (diff / existing_dx_not_first_avg_assignments) * 100
        print(f"✓ CONFIRMED: New DX receive {diff:.2f} fewer assignments ({pct_diff:.1f}% less)")
    else:
        print("✗ NOT CONFIRMED: New DX do not receive fewer assignments")
    
    print("\n3. HYPOTHESIS 3: It takes longer for new DX to receive first assignment")
    print("-" * 70)
    
    new_dx_first_time_to_assignment = safe_get_value(new_dx_first, 'avg_minutes_to_first_assignment')
    existing_dx_not_first_time_to_assignment = safe_get_value(existing_dx_not_first, 'avg_minutes_to_first_assignment')
    
    print(f"New DX (First Dash) avg time to first assignment: {new_dx_first_time_to_assignment:.1f} minutes")
    print(f"Existing DX (Not First Dash) avg time to first assignment: {existing_dx_not_first_time_to_assignment:.1f} minutes")
    
    if new_dx_first_time_to_assignment > existing_dx_not_first_time_to_assignment:
        diff = new_dx_first_time_to_assignment - existing_dx_not_first_time_to_assignment
        pct_diff = (diff / existing_dx_not_first_time_to_assignment) * 100
        print(f"✓ CONFIRMED: New DX wait {diff:.1f} minutes longer ({pct_diff:.1f}% more)")
    else:
        print("✗ NOT CONFIRMED: New DX do not wait longer for assignments")
    
    # Additional insights
    print("\n" + "="*60)
    print("ADDITIONAL INSIGHTS")
    print("="*60)
    
    # Delivery completion rates
    new_dx_first_delivery_rate = safe_get_value(new_dx_first, 'pct_shifts_with_deliveries')
    existing_dx_not_first_delivery_rate = safe_get_value(existing_dx_not_first, 'pct_shifts_with_deliveries')
    
    print(f"\nDelivery Completion Rates:")
    print(f"New DX (First Dash): {new_dx_first_delivery_rate:.1%}")
    print(f"Existing DX (Not First Dash): {existing_dx_not_first_delivery_rate:.1%}")
    
    if new_dx_first_delivery_rate < existing_dx_not_first_delivery_rate:
        diff = existing_dx_not_first_delivery_rate - new_dx_first_delivery_rate
        print(f"New DX have {diff:.1%} lower delivery completion rate")
    
    # Save results
    print(f"\nSaving results to CSV...")
    summary_df.to_csv('new_dx_assignment_analysis_results.csv', index=False)
    print("Results saved to: new_dx_assignment_analysis_results.csv")
    
    return summary_df

if __name__ == "__main__":
    results = main()
