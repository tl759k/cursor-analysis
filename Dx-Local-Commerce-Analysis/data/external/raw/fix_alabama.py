#!/usr/bin/env python3
"""
Fix Alabama issue by manually adding missing federal minimum wage states
"""

import pandas as pd
import numpy as np
from datetime import datetime

def fix_alabama_data():
    print("🔧 FIXING ALABAMA AND OTHER FEDERAL MINIMUM WAGE STATES...")
    
    # Load existing data
    try:
        existing_data = pd.read_csv('dol_historical_raw_data.csv')
        print(f"📊 Loaded existing data: {len(existing_data)} records")
        print(f"States in existing data: {existing_data['state_name'].nunique()}")
    except:
        existing_data = pd.DataFrame()
        print("❌ No existing data found")
    
    # States that follow federal minimum wage (show "..." in DOL tables)
    federal_wage_states = [
        'Alabama', 'Georgia', 'Idaho', 'Indiana', 'Iowa', 'Kansas', 
        'Kentucky', 'Louisiana', 'Mississippi', 'New Hampshire', 
        'North Carolina', 'North Dakota', 'Oklahoma', 'Pennsylvania', 
        'South Carolina', 'Tennessee', 'Texas', 'Utah', 'Wisconsin', 'Wyoming'
    ]
    
    # State abbreviation mapping
    state_mapping = {
        'Alabama': 'AL', 'Georgia': 'GA', 'Idaho': 'ID', 'Indiana': 'IN', 'Iowa': 'IA',
        'Kansas': 'KS', 'Kentucky': 'KY', 'Louisiana': 'LA', 'Mississippi': 'MS', 
        'New Hampshire': 'NH', 'North Carolina': 'NC', 'North Dakota': 'ND', 
        'Oklahoma': 'OK', 'Pennsylvania': 'PA', 'South Carolina': 'SC', 
        'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT', 'Wisconsin': 'WI', 'Wyoming': 'WY'
    }
    
    # Check which federal wage states are missing
    existing_states = set(existing_data['state_name'].unique()) if not existing_data.empty else set()
    missing_federal_states = [state for state in federal_wage_states if state not in existing_states]
    
    print(f"\n🔍 Missing federal minimum wage states: {missing_federal_states}")
    
    if missing_federal_states:
        # Create records for missing states
        new_records = []
        years = range(2020, 2025)  # Recent years
        
        for state in missing_federal_states:
            state_abbr = state_mapping.get(state, '')
            for year in years:
                new_records.append({
                    'state_name': state,
                    'state_abbr': state_abbr,
                    'year': year,
                    'effective_date': f"{year}-01-01",
                    'minimum_wage': 7.25,  # Federal minimum wage
                    'source': 'Manual_Federal_Wage_Fix',
                    'raw_wage_text': '...',
                    'table_number': 6
                })
        
        print(f"✅ Created {len(new_records)} new records for missing states")
        
        # Combine with existing data
        if not existing_data.empty:
            combined_data = pd.concat([existing_data, pd.DataFrame(new_records)], ignore_index=True)
        else:
            combined_data = pd.DataFrame(new_records)
        
        # Save updated data
        combined_data.to_csv('dol_historical_raw_data_fixed.csv', index=False)
        print(f"💾 Saved fixed data: {len(combined_data)} total records")
        print(f"States covered: {combined_data['state_name'].nunique()}")
        
        # Now create the time series
        create_fixed_time_series(combined_data)
        
        return combined_data
    else:
        print("✅ All federal minimum wage states already present")
        return existing_data

def create_fixed_time_series(df_historical):
    """Create time series from fixed historical data"""
    print("\n📈 Creating time series from fixed data...")
    
    # Create monthly data
    monthly_data = []
    start_year = 2023
    end_year = 2025
    
    states = df_historical['state_name'].unique()
    
    for state in states:
        state_data = df_historical[df_historical['state_name'] == state].sort_values('year')
        state_abbr = state_data['state_abbr'].iloc[0] if len(state_data) > 0 else ''
        
        for year in range(start_year, end_year + 1):
            for month in range(1, 13):
                if year == 2025 and month > 7:
                    break
                    
                # Find applicable wage
                applicable_wages = state_data[state_data['year'] <= year]
                if len(applicable_wages) > 0:
                    current_wage = applicable_wages.iloc[-1]['minimum_wage']
                else:
                    current_wage = 7.25
                
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
        'minimum_wage': 'last',
        'date': 'max'
    }).reset_index()
    
    # Save fixed time series
    monthly_df.to_csv('dol_monthly_minimum_wage_by_state_fixed.csv', index=False)
    quarterly_df.to_csv('dol_quarterly_minimum_wage_by_state_fixed.csv', index=False)
    
    # Create summary
    summary_df = monthly_df.groupby(['state_name', 'state_abbr'])['minimum_wage'].agg([
        'min', 'max', 'mean', 'std'
    ]).reset_index()
    summary_df.columns = ['state_name', 'state_abbr', 'min_wage', 'max_wage', 'avg_wage', 'std_wage']
    summary_df.to_csv('dol_minimum_wage_summary_fixed.csv', index=False)
    
    print(f"✅ Fixed monthly data: {len(monthly_df)} records")
    print(f"✅ Fixed quarterly data: {len(quarterly_df)} records")
    print(f"🗺️  States covered: {monthly_df['state_name'].nunique()}")
    
    # Verify Alabama is included
    alabama_data = monthly_df[monthly_df['state_name'] == 'Alabama']
    print(f"\n🔍 Alabama verification:")
    print(f"Alabama records in monthly data: {len(alabama_data)}")
    if len(alabama_data) > 0:
        print(f"Alabama wage: ${alabama_data['minimum_wage'].iloc[0]:.2f}")
        print("✅ Alabama successfully included!")
    
    return monthly_df, quarterly_df

if __name__ == "__main__":
    print("🎯 ALABAMA FIX SCRIPT")
    print("=" * 30)
    
    fixed_data = fix_alabama_data()
    
    print("\n" + "=" * 50)
    print("✅ ALABAMA FIX COMPLETE!")
    print("Files created:")
    print("  - dol_historical_raw_data_fixed.csv")
    print("  - dol_monthly_minimum_wage_by_state_fixed.csv") 
    print("  - dol_quarterly_minimum_wage_by_state_fixed.csv")
    print("  - dol_minimum_wage_summary_fixed.csv")

