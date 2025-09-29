#!/usr/bin/env python3

import sys
import os
sys.path.append('../../utils')

import pandas as pd
import numpy as np
from snowflake_connection import SnowflakeHook

def main():
    print("NEW DX: FIRST DASH vs NON-FIRST DASH COMPARISON")
    print("="*70)
    
    # Initialize Snowflake connection
    snowhook = SnowflakeHook()
    
    # Read the summary analysis query
    with open('sql/new_dx_summary_single_statement.sql', 'r') as f:
        summary_query = f.read()
    
    # Execute the summary query
    summary_df = snowhook.query_snowflake(summary_query, method='pandas')
    
    def safe_get_value(df, column, default=0):
        """Safely get a value from dataframe, return default if empty"""
        if len(df) > 0:
            return df[column].iloc[0]
        return default
    
    # Focus on New DX only
    new_dx_data = summary_df[summary_df['new_dx_l7d'] == 'Y'].copy()
    
    if len(new_dx_data) >= 2:
        new_dx_first_dash = new_dx_data[new_dx_data['is_first_dash'] == True]
        new_dx_not_first_dash = new_dx_data[new_dx_data['is_first_dash'] == False]
        
        print("\n📊 NEW DX COMPARISON: First Dash vs Subsequent Early Dashes")
        print("-" * 70)
        
        # Create detailed comparison
        metrics = {
            'Total Dashers': (
                safe_get_value(new_dx_first_dash, 'total_dashers'),
                safe_get_value(new_dx_not_first_dash, 'total_dashers')
            ),
            'Total Shifts': (
                safe_get_value(new_dx_first_dash, 'total_shifts'),
                safe_get_value(new_dx_not_first_dash, 'total_shifts')
            ),
            'Assignment Rate': (
                safe_get_value(new_dx_first_dash, 'pct_shifts_with_assignments'),
                safe_get_value(new_dx_not_first_dash, 'pct_shifts_with_assignments')
            ),
            'Avg Assignments per Shift': (
                safe_get_value(new_dx_first_dash, 'avg_assignments_per_shift'),
                safe_get_value(new_dx_not_first_dash, 'avg_assignments_per_shift')
            ),
            'Avg Minutes to First Assignment': (
                safe_get_value(new_dx_first_dash, 'avg_minutes_to_first_assignment'),
                safe_get_value(new_dx_not_first_dash, 'avg_minutes_to_first_assignment')
            ),
            'Delivery Completion Rate': (
                safe_get_value(new_dx_first_dash, 'pct_shifts_with_deliveries'),
                safe_get_value(new_dx_not_first_dash, 'pct_shifts_with_deliveries')
            ),
            'Avg Shift Hours': (
                safe_get_value(new_dx_first_dash, 'avg_shift_hours'),
                safe_get_value(new_dx_not_first_dash, 'avg_shift_hours')
            )
        }
        
        # Print comparison table
        print(f"{'Metric':<30} {'First Dash':<15} {'Not First Dash':<15} {'Difference':<15}")
        print("-" * 75)
        
        for metric, (first_val, not_first_val) in metrics.items():
            if 'Rate' in metric and metric != 'Avg':
                # For percentage metrics
                first_str = f"{first_val*100:.1f}%"
                not_first_str = f"{not_first_val*100:.1f}%"
                diff = (first_val - not_first_val) * 100
                diff_str = f"{diff:+.1f}pp"
            elif 'Dashers' in metric or 'Shifts' in metric:
                # For count metrics
                first_str = f"{first_val:,.0f}"
                not_first_str = f"{not_first_val:,.0f}"
                diff = first_val - not_first_val
                diff_str = f"{diff:+,.0f}"
            else:
                # For other metrics
                first_str = f"{first_val:.2f}"
                not_first_str = f"{not_first_val:.2f}"
                diff = first_val - not_first_val
                diff_str = f"{diff:+.2f}"
            
            print(f"{metric:<30} {first_str:<15} {not_first_str:<15} {diff_str:<15}")
        
        print("\n🔍 KEY INSIGHTS:")
        print("-" * 50)
        
        # Assignment rate comparison
        first_assignment_rate = safe_get_value(new_dx_first_dash, 'pct_shifts_with_assignments')
        not_first_assignment_rate = safe_get_value(new_dx_not_first_dash, 'pct_shifts_with_assignments')
        
        diff_pp = (first_assignment_rate - not_first_assignment_rate) * 100
        if abs(diff_pp) > 0.1:  # More than 0.1 percentage points
            if first_assignment_rate > not_first_assignment_rate:
                print(f"✓ First dash has {diff_pp:.1f}pp HIGHER assignment rate")
            else:
                print(f"⚠️  First dash has {abs(diff_pp):.1f}pp LOWER assignment rate")
        else:
            print("• Assignment rates are essentially the same")
        
        # Assignment volume comparison
        first_avg_assignments = safe_get_value(new_dx_first_dash, 'avg_assignments_per_shift')
        not_first_avg_assignments = safe_get_value(new_dx_not_first_dash, 'avg_assignments_per_shift')
        
        diff_assignments = first_avg_assignments - not_first_avg_assignments
        if abs(diff_assignments) > 0.05:  # More than 0.05 assignments
            pct_diff = (diff_assignments / not_first_avg_assignments) * 100
            if first_avg_assignments > not_first_avg_assignments:
                print(f"✓ First dash gets {diff_assignments:.2f} MORE assignments per shift ({pct_diff:.1f}% more)")
            else:
                print(f"⚠️  First dash gets {abs(diff_assignments):.2f} FEWER assignments per shift ({abs(pct_diff):.1f}% less)")
        else:
            print("• Assignment volumes are essentially the same")
        
        # Timing comparison
        first_time = safe_get_value(new_dx_first_dash, 'avg_minutes_to_first_assignment')
        not_first_time = safe_get_value(new_dx_not_first_dash, 'avg_minutes_to_first_assignment')
        
        diff_time = first_time - not_first_time
        if abs(diff_time) > 0.3:  # More than 0.3 minutes
            pct_diff = (diff_time / not_first_time) * 100
            if first_time > not_first_time:
                print(f"⚠️  First dash waits {diff_time:.1f} minutes LONGER for assignments ({pct_diff:.1f}% more)")
            else:
                print(f"✓ First dash waits {abs(diff_time):.1f} minutes LESS for assignments ({abs(pct_diff):.1f}% less)")
        else:
            print("• Assignment timing is essentially the same")
        
        # Delivery completion comparison
        first_delivery_rate = safe_get_value(new_dx_first_dash, 'pct_shifts_with_deliveries')
        not_first_delivery_rate = safe_get_value(new_dx_not_first_dash, 'pct_shifts_with_deliveries')
        
        diff_delivery_pp = (first_delivery_rate - not_first_delivery_rate) * 100
        if abs(diff_delivery_pp) > 0.5:  # More than 0.5 percentage points
            if first_delivery_rate > not_first_delivery_rate:
                print(f"✓ First dash has {diff_delivery_pp:.1f}pp HIGHER delivery completion rate")
            else:
                print(f"⚠️  First dash has {abs(diff_delivery_pp):.1f}pp LOWER delivery completion rate")
        else:
            print("• Delivery completion rates are essentially the same")
        
        # Summary insight
        print(f"\n💡 SUMMARY INSIGHT:")
        print("-" * 50)
        
        significant_differences = []
        
        if abs(diff_pp) > 0.5:
            significant_differences.append(f"assignment rate ({diff_pp:+.1f}pp)")
        if abs(diff_assignments) > 0.1:
            pct_diff = (diff_assignments / not_first_avg_assignments) * 100
            significant_differences.append(f"assignments per shift ({pct_diff:+.1f}%)")
        if abs(diff_time) > 0.5:
            pct_diff = (diff_time / not_first_time) * 100
            significant_differences.append(f"wait time ({pct_diff:+.1f}%)")
        if abs(diff_delivery_pp) > 1.0:
            significant_differences.append(f"delivery completion ({diff_delivery_pp:+.1f}pp)")
        
        if significant_differences:
            print(f"Within new DX, first dash shows meaningful differences in: {', '.join(significant_differences)}")
            print("\nThis suggests the very first experience may be distinct from subsequent early dashes.")
        else:
            print("Within new DX, first dash and subsequent early dashes show similar patterns.")
            print("The main issue appears to be 'new DX vs existing DX' rather than 'first vs later dashes'.")
    
    else:
        print("Insufficient data to compare first dash vs non-first dash within new DX.")

if __name__ == "__main__":
    main()
