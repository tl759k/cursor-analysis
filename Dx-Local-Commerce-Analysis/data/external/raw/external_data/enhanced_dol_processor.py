#!/usr/bin/env python3
"""
Enhanced DOL Historical Data Processor

This script specifically processes the DOL historical minimum wage tables
that were successfully scraped but not properly parsed.
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

def scrape_and_process_dol_historical():
    """Scrape and properly process DOL historical minimum wage data"""
    print("🔍 Enhanced DOL Historical Data Processing...")
    
    url = 'https://www.dol.gov/agencies/whd/state/minimum-wage/history'
    
    try:
        # Use requests with proper headers
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=30, verify=False)
        response.raise_for_status()
        
        # Extract all tables using pandas
        tables = pd.read_html(response.text, flavor='lxml')
        
        print(f"✅ Found {len(tables)} tables from DOL historical page")
        
        # Process each table
        all_historical_data = []
        
        for i, table in enumerate(tables):
            print(f"\n📊 Processing Table {i+1}: {table.shape}")
            print(f"Columns: {list(table.columns)}")
            
            # Check if this is a historical wage table
            if 'State or other jurisdiction' in table.columns:
                processed_data = process_dol_historical_table(table, i+1)
                all_historical_data.extend(processed_data)
        
        if all_historical_data:
            df = pd.DataFrame(all_historical_data)
            print(f"\n✅ Successfully processed {len(df)} historical records")
            return df
        else:
            print("❌ No historical data could be processed")
            return pd.DataFrame()
            
    except Exception as e:
        print(f"❌ Error processing DOL historical data: {e}")
        return pd.DataFrame()

def process_dol_historical_table(table, table_num):
    """Process individual DOL historical table"""
    records = []
    
    # Get the state column
    state_col = 'State or other jurisdiction'
    if state_col not in table.columns:
        return records
    
    # Get year columns (exclude the state column)
    year_columns = [col for col in table.columns if col != state_col and str(col).isdigit()]
    
    print(f"  📅 Year columns found: {year_columns}")
    
    # State name mapping
    state_mapping = {
        'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR', 'California': 'CA',
        'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE', 'Florida': 'FL', 'Georgia': 'GA',
        'Hawaii': 'HI', 'Idaho': 'ID', 'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA',
        'Kansas': 'KS', 'Kentucky': 'KY', 'Louisiana': 'LA', 'Maine': 'ME', 'Maryland': 'MD',
        'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN', 'Mississippi': 'MS', 'Missouri': 'MO',
        'Montana': 'MT', 'Nebraska': 'NE', 'Nevada': 'NV', 'New Hampshire': 'NH', 'New Jersey': 'NJ',
        'New Mexico': 'NM', 'New York': 'NY', 'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH',
        'Oklahoma': 'OK', 'Oregon': 'OR', 'Pennsylvania': 'PA', 'Rhode Island': 'RI', 'South Carolina': 'SC',
        'South Dakota': 'SD', 'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT', 'Vermont': 'VT',
        'Virginia': 'VA', 'Washington': 'WA', 'West Virginia': 'WV', 'Wisconsin': 'WI', 'Wyoming': 'WY',
        'District of Columbia': 'DC', 'Washington, D.C.': 'DC', 'D.C.': 'DC'
    }
    
    # Process each row (state)
    for _, row in table.iterrows():
        state_name = str(row[state_col]).strip()
        
        # Clean up state name
        if '(' in state_name:
            state_name = state_name.split('(')[0].strip()
        
        # Skip non-state rows
        if state_name in ['Total', 'Source:', 'Note:', 'Notes:', ''] or pd.isna(state_name):
            continue
        
        # Get state abbreviation
        state_abbr = state_mapping.get(state_name, '')
        
        # Process each year column
        for year_col in year_columns:
            try:
                year = int(year_col)
                wage_value = row[year_col]
                
                # Clean and extract wage
                wage = extract_wage_from_cell(wage_value)
                
                if wage is not None and wage > 0:
                    records.append({
                        'state_name': state_name,
                        'state_abbr': state_abbr,
                        'year': year,
                        'effective_date': f"{year}-01-01",
                        'minimum_wage': wage,
                        'source': f'DOL_Historical_Table_{table_num}',
                        'raw_wage_text': str(wage_value),
                        'table_number': table_num
                    })
                    
            except Exception as e:
                continue
    
    print(f"  ✅ Extracted {len(records)} records from table {table_num}")
    return records

def extract_wage_from_cell(cell_value):
    """Extract wage amount from DOL table cell"""
    if pd.isna(cell_value) or cell_value == '' or str(cell_value).lower() in ['nan', 'none', '--', 'n/a']:
        return None
    
    cell_str = str(cell_value).strip()
    
    # Handle common DOL notations
    if cell_str.lower() in ['same as federal', 'federal', 'fed', 'f']:
        return 7.25  # Current federal minimum wage
    
    if cell_str.lower() in ['no state minimum wage law', 'no law', 'none']:
        return 7.25  # Defaults to federal
    
    # Remove dollar signs, commas, and other characters
    import re
    
    # Look for decimal number pattern
    wage_match = re.search(r'\$?(\d+\.?\d*)', cell_str)
    if wage_match:
        try:
            wage = float(wage_match.group(1))
            # Reasonable range check
            if 1.0 <= wage <= 30.0:
                return wage
        except:
            pass
    
    return None

def create_comprehensive_time_series(df_historical):
    """Create comprehensive monthly/quarterly time series from historical data"""
    print("\n📈 Creating comprehensive time series from historical DOL data...")
    
    if df_historical.empty:
        print("❌ No historical data to process")
        return pd.DataFrame(), pd.DataFrame()
    
    # Get date range
    start_year = 2020  # Focus on recent years for analysis
    end_year = 2025
    
    monthly_data = []
    
    # Get all states
    states = df_historical['state_name'].unique()
    
    for state in states:
        state_data = df_historical[df_historical['state_name'] == state].sort_values('year')
        state_abbr = state_data['state_abbr'].iloc[0] if len(state_data) > 0 else ''
        
        # Create monthly records for each year
        for year in range(start_year, end_year + 1):
            # Find the wage for this year or the most recent year before it
            applicable_wages = state_data[state_data['year'] <= year]
            
            if len(applicable_wages) > 0:
                current_wage = applicable_wages.iloc[-1]['minimum_wage']
            else:
                current_wage = 7.25  # Federal minimum wage fallback
            
            # Create monthly records for this year
            for month in range(1, 13):
                # Only create records up to July 2025
                if year == 2025 and month > 7:
                    break
                # Only create records from 2023 onwards for the target period
                if year < 2023:
                    continue
                    
                date = datetime(year, month, 1)
                
                monthly_data.append({
                    'date': date,
                    'year': year,
                    'month': month,
                    'quarter': f"Q{(month - 1) // 3 + 1}",
                    'state_name': state,
                    'state_abbr': state_abbr,
                    'minimum_wage': current_wage
                })
    
    monthly_df = pd.DataFrame(monthly_data)
    
    # Create quarterly data
    quarterly_df = monthly_df.groupby(['state_name', 'state_abbr', 'year', 'quarter']).agg({
        'minimum_wage': 'last',  # Take last month of quarter
        'date': 'max'
    }).reset_index()
    
    print(f"✅ Created monthly time series: {len(monthly_df)} records")
    print(f"✅ Created quarterly time series: {len(quarterly_df)} records")
    
    return monthly_df, quarterly_df

def main():
    """Main execution function"""
    print("🎯 ENHANCED DOL HISTORICAL DATA PROCESSOR")
    print("=" * 50)
    
    # Step 1: Scrape and process DOL historical data
    historical_df = scrape_and_process_dol_historical()
    
    if not historical_df.empty:
        # Save raw historical data
        historical_df.to_csv('dol_historical_raw_data.csv', index=False)
        print(f"\n💾 Historical raw data saved: dol_historical_raw_data.csv")
        
        # Show sample of historical data
        print("\n📊 Sample Historical Data:")
        sample = historical_df.groupby('state_name').apply(lambda x: x.nlargest(3, 'year')).reset_index(drop=True)
        print(sample[['state_name', 'year', 'minimum_wage', 'source']].head(15).to_string(index=False))
        
        # Create comprehensive time series
        monthly_df, quarterly_df = create_comprehensive_time_series(historical_df)
        
        if not monthly_df.empty:
            # Save time series data
            monthly_df.to_csv('dol_monthly_minimum_wage_by_state.csv', index=False)
            quarterly_df.to_csv('dol_quarterly_minimum_wage_by_state.csv', index=False)
            
            # Create summary
            summary_df = monthly_df.groupby(['state_name', 'state_abbr'])['minimum_wage'].agg([
                'min', 'max', 'mean', 'std'
            ]).reset_index()
            summary_df.columns = ['state_name', 'state_abbr', 'min_wage', 'max_wage', 'avg_wage', 'std_wage']
            summary_df.to_csv('dol_minimum_wage_summary.csv', index=False)
            
            print("\n✅ ENHANCED DOL PROCESSING COMPLETE!")
            print("=" * 50)
            print(f"📊 Historical data points: {len(historical_df)}")
            print(f"📊 Monthly data points: {len(monthly_df)}")
            print(f"📊 Quarterly data points: {len(quarterly_df)}")
            print(f"🗺️  States covered: {monthly_df['state_name'].nunique()}")
            print(f"📅 Historical range: {historical_df['year'].min()}-{historical_df['year'].max()}")
            print(f"📅 Time series range: {monthly_df['date'].min().strftime('%Y-%m-%d')} to {monthly_df['date'].max().strftime('%Y-%m-%d')}")
            
            print("\n📁 Files created:")
            print("  - dol_historical_raw_data.csv (real DOL historical data)")
            print("  - dol_monthly_minimum_wage_by_state.csv")
            print("  - dol_quarterly_minimum_wage_by_state.csv")
            print("  - dol_minimum_wage_summary.csv")
            
            # Show wage ranges
            print(f"\n💰 Wage Analysis:")
            latest_month = monthly_df[monthly_df['date'] == monthly_df['date'].max()]
            print(f"Highest current wage: {latest_month.loc[latest_month['minimum_wage'].idxmax(), 'state_name']} - ${latest_month['minimum_wage'].max():.2f}")
            print(f"Lowest current wage: {latest_month.loc[latest_month['minimum_wage'].idxmin(), 'state_name']} - ${latest_month['minimum_wage'].min():.2f}")
            print(f"Average current wage: ${latest_month['minimum_wage'].mean():.2f}")
            
            return monthly_df, quarterly_df, historical_df
    
    print("❌ No historical data could be processed")
    return None, None, None

if __name__ == "__main__":
    monthly_data, quarterly_data, historical_data = main()
