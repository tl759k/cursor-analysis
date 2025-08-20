#!/usr/bin/env python3
"""
Query to get daily DX (Dasher) applicant counts starting from Monday this week.

This script executes a SQL query to count the number of dasher applicants per day
starting from Monday of the current week.
"""

import sys
import os
import pandas as pd
from datetime import datetime

# Add the utils directory to the path to import SnowflakeHook
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'utils'))
from snowflake_connection import SnowflakeHook

def main():
    """Execute the DX applicants query and display results."""
    
    # Initialize Snowflake connection
    sf = SnowflakeHook()
    
    # Read the SQL query
    sql_file_path = os.path.join(os.path.dirname(__file__), 'sql', 'daily_dx_applicants_from_monday.sql')
    
    with open(sql_file_path, 'r') as file:
        query = file.read()
    
    print("Executing query to get daily DX applicant counts from Monday this week...")
    print("=" * 80)
    
    try:
        # Execute the query
        df = sf.query_snowflake(query, method='pandas')
        
        if df.empty:
            print("No data found for the specified date range.")
            return
        
        # Display results
        print(f"\nDaily DX Applicant Counts (Starting from Monday this week)")
        print(f"Query executed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)
        
        # Format the dataframe for better display
        df['applied_date'] = pd.to_datetime(df['applied_date']).dt.strftime('%Y-%m-%d')
        
        # Display the results
        print(df.to_string(index=False))
        
        # Summary statistics
        total_applicants = df['daily_applicant_count'].sum()
        avg_daily = df['daily_applicant_count'].mean()
        max_day = df.loc[df['daily_applicant_count'].idxmax()]
        min_day = df.loc[df['daily_applicant_count'].idxmin()]
        
        print("\n" + "=" * 80)
        print("SUMMARY STATISTICS")
        print("=" * 80)
        print(f"Total applicants this week: {total_applicants:,}")
        print(f"Average daily applicants: {avg_daily:.1f}")
        print(f"Highest day: {max_day['day_of_week']} ({max_day['applied_date']}) with {max_day['daily_applicant_count']:,} applicants")
        print(f"Lowest day: {min_day['day_of_week']} ({min_day['applied_date']}) with {min_day['daily_applicant_count']:,} applicants")
        
    except Exception as e:
        print(f"Error executing query: {str(e)}")
        return
    
    finally:
        # Connection is handled automatically by the SnowflakeHook
        pass

if __name__ == "__main__":
    main() 