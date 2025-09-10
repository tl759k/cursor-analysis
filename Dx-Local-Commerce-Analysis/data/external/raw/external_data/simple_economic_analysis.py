#!/usr/bin/env python3
"""
Simplified Economic Analysis: Unemployment Rate vs DoorDash Applications

Direct analysis without complex data processing.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import pearsonr
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score
import warnings
warnings.filterwarnings('ignore')

def load_and_merge_data():
    """Load and merge datasets with simple processing"""
    print("🔄 LOADING ECONOMIC DATASETS")
    print("=" * 35)
    
    # 1. Load applications data
    apps_df = pd.read_csv('../df_apps_by_state_output.csv')
    apps_df['date'] = pd.to_datetime(apps_df['month'])
    print(f"✅ Applications: {len(apps_df):,} records")
    
    # 2. Load unemployment data
    unemp_df = pd.read_csv('../bls_state_unemployment.csv')
    unemp_df['month_num'] = unemp_df['period'].str.replace('M', '').astype(int)
    unemp_df['date'] = pd.to_datetime(unemp_df[['year', 'month_num']].rename(columns={'month_num': 'month'}).assign(day=1))
    unemp_df['unemployment_rate'] = unemp_df['value']
    unemp_df['state_name'] = unemp_df['state']
    print(f"✅ Unemployment: {len(unemp_df):,} records")
    
    # 3. Load wage data
    wage_df = pd.read_csv('dol_monthly_minimum_wage_by_state.csv')
    wage_df['date'] = pd.to_datetime(wage_df['date'])
    print(f"✅ Minimum wages: {len(wage_df):,} records")
    
    # 4. Load CPI data
    cpi_df = pd.read_csv('monthly_cpi_by_state.csv')
    cpi_df['date'] = pd.to_datetime(cpi_df['date'])
    print(f"✅ CPI data: {len(cpi_df):,} records")
    
    # Merge datasets
    print("\n🔗 Merging datasets...")
    
    # Start with apps
    merged = apps_df[['state_name', 'date', 'apps_18plus']].copy()
    
    # Merge unemployment
    merged = merged.merge(
        unemp_df[['state_name', 'date', 'unemployment_rate']], 
        on=['state_name', 'date'], 
        how='inner'
    )
    print(f"   After unemployment: {len(merged):,} records")
    
    # Merge wages
    merged = merged.merge(
        wage_df[['state_name', 'date', 'minimum_wage']], 
        on=['state_name', 'date'], 
        how='inner'
    )
    print(f"   After wages: {len(merged):,} records")
    
    # Merge CPI
    merged = merged.merge(
        cpi_df[['state_name', 'date', 'cpi_value']], 
        on=['state_name', 'date'], 
        how='inner'
    )
    print(f"   Final dataset: {len(merged):,} records")
    
    # Add time features
    merged['year'] = merged['date'].dt.year
    merged['month'] = merged['date'].dt.month
    merged['quarter'] = merged['date'].dt.quarter
    
    # Add seasonality
    merged['is_summer'] = merged['month'].isin([6, 7, 8]).astype(int)
    merged['is_winter'] = merged['month'].isin([12, 1, 2]).astype(int)
    
    # Real minimum wage
    baseline_cpi = merged[merged['date'] == merged['date'].min()]['cpi_value'].mean()
    merged['real_minimum_wage'] = merged['minimum_wage'] * (baseline_cpi / merged['cpi_value'])
    
    print(f"\n📊 Final dataset summary:")
    print(f"   States: {merged['state_name'].nunique()}")
    print(f"   Time period: {merged['date'].min().strftime('%Y-%m')} to {merged['date'].max().strftime('%Y-%m')}")
    print(f"   Total observations: {len(merged):,}")
    
    return merged

def analyze_hypothesis(df):
    """Test the main hypothesis"""
    print("\n🎯 HYPOTHESIS TESTING")
    print("=" * 25)
    print("Hypothesis: Higher unemployment → More DoorDash applications")
    
    # Basic correlation
    corr, p_value = pearsonr(df['unemployment_rate'], df['apps_18plus'])
    
    print(f"\n📊 Primary Results:")
    print(f"   Correlation coefficient: {corr:.4f}")
    print(f"   P-value: {p_value:.6f}")
    print(f"   Significance: {'SIGNIFICANT' if p_value < 0.05 else 'NOT SIGNIFICANT'} at α=0.05")
    print(f"   Relationship: {'POSITIVE' if corr > 0 else 'NEGATIVE'}")
    
    # Interpret strength
    abs_corr = abs(corr)
    if abs_corr < 0.1:
        strength = "Very weak"
    elif abs_corr < 0.3:
        strength = "Weak"
    elif abs_corr < 0.5:
        strength = "Moderate"
    elif abs_corr < 0.7:
        strength = "Strong"
    else:
        strength = "Very strong"
    
    print(f"   Strength: {strength}")
    
    return corr, p_value

def regression_analysis(df):
    """Run regression models"""
    print("\n🔬 REGRESSION ANALYSIS")
    print("=" * 25)
    
    # Simple regression
    print("📊 Model 1: Simple Linear Regression")
    X_simple = df[['unemployment_rate']]
    y = df['apps_18plus']
    
    model1 = LinearRegression()
    model1.fit(X_simple, y)
    y_pred1 = model1.predict(X_simple)
    r2_1 = r2_score(y, y_pred1)
    
    print(f"   Coefficient: {model1.coef_[0]:.2f}")
    print(f"   Interpretation: 1% increase in unemployment → {model1.coef_[0]:.1f} more applications")
    print(f"   R-squared: {r2_1:.4f}")
    print(f"   Intercept: {model1.intercept_:.1f}")
    
    # Multiple regression with controls
    print(f"\n📊 Model 2: Multiple Regression with Controls")
    X_multiple = df[['unemployment_rate', 'real_minimum_wage', 'is_summer', 'is_winter']]
    
    model2 = LinearRegression()
    model2.fit(X_multiple, y)
    y_pred2 = model2.predict(X_multiple)
    r2_2 = r2_score(y, y_pred2)
    
    print(f"   Unemployment coefficient: {model2.coef_[0]:.2f}")
    print(f"   Real wage coefficient: {model2.coef_[1]:.2f}")
    print(f"   Summer effect: {model2.coef_[2]:.2f}")
    print(f"   Winter effect: {model2.coef_[3]:.2f}")
    print(f"   R-squared: {r2_2:.4f}")
    
    return model1, model2

def create_visualizations(df, corr):
    """Create key visualizations"""
    print("\n📈 CREATING VISUALIZATIONS")
    print("=" * 30)
    
    plt.figure(figsize=(16, 12))
    
    # 1. Main scatter plot
    plt.subplot(2, 3, 1)
    plt.scatter(df['unemployment_rate'], df['apps_18plus'], alpha=0.6, s=30)
    
    # Regression line
    z = np.polyfit(df['unemployment_rate'], df['apps_18plus'], 1)
    p = np.poly1d(z)
    x_line = np.linspace(df['unemployment_rate'].min(), df['unemployment_rate'].max(), 100)
    plt.plot(x_line, p(x_line), "r--", alpha=0.8, linewidth=2)
    
    plt.xlabel('Unemployment Rate (%)')
    plt.ylabel('DoorDash Applications (18+)')
    plt.title('Unemployment vs Applications')
    plt.grid(True, alpha=0.3)
    plt.text(0.05, 0.95, f'r = {corr:.3f}', transform=plt.gca().transAxes, 
             bbox=dict(boxstyle="round", facecolor='wheat', alpha=0.8))
    
    # 2. Time series trends
    plt.subplot(2, 3, 2)
    monthly_trends = df.groupby('date').agg({
        'unemployment_rate': 'mean',
        'apps_18plus': 'mean'
    })
    
    ax1 = plt.gca()
    line1 = ax1.plot(monthly_trends.index, monthly_trends['unemployment_rate'], 
                     'b-', linewidth=2, label='Unemployment Rate')
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Unemployment Rate (%)', color='b')
    ax1.tick_params(axis='y', labelcolor='b')
    
    ax2 = ax1.twinx()
    line2 = ax2.plot(monthly_trends.index, monthly_trends['apps_18plus'], 
                     'r-', linewidth=2, label='Applications')
    ax2.set_ylabel('Applications', color='r')
    ax2.tick_params(axis='y', labelcolor='r')
    
    plt.title('Time Trends: Unemployment vs Applications')
    plt.xticks(rotation=45)
    
    # 3. State-level correlations
    plt.subplot(2, 3, 3)
    state_corrs = []
    for state in df['state_name'].unique():
        state_data = df[df['state_name'] == state]
        if len(state_data) > 5:
            state_corr, _ = pearsonr(state_data['unemployment_rate'], state_data['apps_18plus'])
            state_corrs.append(state_corr)
    
    plt.hist(state_corrs, bins=15, alpha=0.7, edgecolor='black')
    plt.axvline(np.mean(state_corrs), color='red', linestyle='--', 
                label=f'Mean: {np.mean(state_corrs):.3f}')
    plt.xlabel('Correlation by State')
    plt.ylabel('Number of States')
    plt.title('Distribution of State-Level Correlations')
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    # 4. Correlation matrix
    plt.subplot(2, 3, 4)
    corr_vars = ['apps_18plus', 'unemployment_rate', 'minimum_wage', 'cpi_value']
    corr_matrix = df[corr_vars].corr()
    
    sns.heatmap(corr_matrix, annot=True, cmap='RdBu_r', center=0, 
               square=True, fmt='.3f', cbar_kws={"shrink": .8})
    plt.title('Economic Variables Correlation')
    
    # 5. Seasonality
    plt.subplot(2, 3, 5)
    seasonal = df.groupby('month').agg({
        'apps_18plus': 'mean',
        'unemployment_rate': 'mean'
    })
    
    ax1 = plt.gca()
    bars = ax1.bar(seasonal.index, seasonal['apps_18plus'], alpha=0.7, color='lightblue')
    ax1.set_xlabel('Month')
    ax1.set_ylabel('Avg Applications', color='blue')
    ax1.tick_params(axis='y', labelcolor='blue')
    
    ax2 = ax1.twinx()
    line = ax2.plot(seasonal.index, seasonal['unemployment_rate'], 
                    'ro-', linewidth=2, markersize=6)
    ax2.set_ylabel('Avg Unemployment Rate (%)', color='red')
    ax2.tick_params(axis='y', labelcolor='red')
    
    plt.title('Seasonality Patterns')
    
    # 6. High vs Low unemployment
    plt.subplot(2, 3, 6)
    median_unemp = df['unemployment_rate'].median()
    high_unemp = df[df['unemployment_rate'] > median_unemp]['apps_18plus']
    low_unemp = df[df['unemployment_rate'] <= median_unemp]['apps_18plus']
    
    plt.boxplot([low_unemp, high_unemp], labels=['Low Unemployment', 'High Unemployment'])
    plt.ylabel('Applications (18+)')
    plt.title('Applications by Unemployment Level')
    plt.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig('unemployment_applications_analysis.png', dpi=300, bbox_inches='tight')
    plt.show()
    
    print("✅ Visualization saved as 'unemployment_applications_analysis.png'")

def summary_and_recommendations(df, corr, p_value, models):
    """Generate summary and recommendations"""
    print("\n📋 ANALYSIS SUMMARY & RECOMMENDATIONS")
    print("=" * 45)
    
    print("🎯 KEY FINDINGS:")
    print("-" * 15)
    
    if p_value < 0.05:
        if corr > 0:
            print("✅ HYPOTHESIS CONFIRMED:")
            print(f"   • Statistically significant POSITIVE correlation ({corr:.4f})")
            print(f"   • Higher unemployment rates ARE associated with more DoorDash applications")
            print(f"   • This supports the theory that economic stress drives gig economy participation")
        else:
            print("❌ HYPOTHESIS REJECTED:")
            print(f"   • Statistically significant NEGATIVE correlation ({corr:.4f})")
            print(f"   • Higher unemployment rates are associated with FEWER applications")
            print(f"   • This suggests other factors may dominate the relationship")
    else:
        print("🤔 INCONCLUSIVE RESULTS:")
        print(f"   • No statistically significant relationship found (p={p_value:.4f})")
        print(f"   • The relationship may be more complex or affected by confounding factors")
    
    print(f"\n📊 DATA SUMMARY:")
    print(f"   • Total observations: {len(df):,}")
    print(f"   • States analyzed: {df['state_name'].nunique()}")
    print(f"   • Time period: {df['date'].min().strftime('%Y-%m')} to {df['date'].max().strftime('%Y-%m')}")
    print(f"   • Average unemployment rate: {df['unemployment_rate'].mean():.2f}%")
    print(f"   • Average applications per state-month: {df['apps_18plus'].mean():.0f}")
    
    print(f"\n🔬 MODEL PERFORMANCE:")
    if len(models) >= 2:
        model1, model2 = models
        print(f"   • Simple regression R²: {r2_score(df['apps_18plus'], model1.predict(df[['unemployment_rate']])):.4f}")
        print(f"   • Multiple regression R²: {r2_score(df['apps_18plus'], model2.predict(df[['unemployment_rate', 'real_minimum_wage', 'is_summer', 'is_winter']])):.4f}")
        print(f"   • Unemployment coefficient: {model1.coef_[0]:.2f} applications per 1% unemployment increase")
    
    print(f"\n🚀 BUSINESS IMPLICATIONS:")
    if p_value < 0.05 and corr > 0:
        print("   • Consider increasing recruitment efforts during high unemployment periods")
        print("   • Prepare infrastructure for higher application volumes during economic downturns")
        print("   • Focus on fast onboarding processes during recession periods")
        print("   • Target marketing in areas with rising unemployment")
    elif p_value < 0.05 and corr < 0:
        print("   • Quality over quantity approach during high unemployment")
        print("   • Investigate barriers to application during economic stress")
        print("   • Consider alternative recruitment strategies")
    else:
        print("   • Unemployment alone may not be a reliable predictor")
        print("   • Focus on other economic indicators and demographic factors")
        print("   • Consider regional variations and local economic conditions")
    
    print(f"\n📈 RECOMMENDED NEXT STEPS:")
    print("   1. Age-specific analysis (focus on 18-24 age group)")
    print("   2. Regional/metropolitan area analysis")
    print("   3. Lag effect analysis (how long does unemployment impact last?)")
    print("   4. Include additional economic indicators (job openings, GDP, etc.)")
    print("   5. Seasonal adjustment of unemployment rates")
    print("   6. Analysis of competition effects (market saturation)")

def main():
    """Main analysis execution"""
    print("🎯 ECONOMIC ANALYSIS: UNEMPLOYMENT vs DOORDASH APPLICATIONS")
    print("=" * 65)
    
    # Load and merge data
    df = load_and_merge_data()
    if df is None or len(df) == 0:
        print("❌ Failed to load data")
        return
    
    # Test hypothesis
    corr, p_value = analyze_hypothesis(df)
    
    # Regression analysis
    models = regression_analysis(df)
    
    # Create visualizations
    create_visualizations(df, corr)
    
    # Summary and recommendations
    summary_and_recommendations(df, corr, p_value, models)
    
    print(f"\n✅ ANALYSIS COMPLETE!")
    print("📁 Output files:")
    print("   • unemployment_applications_analysis.png (comprehensive visualization)")
    print("   • Complete statistical results displayed above")
    
    return df, corr, p_value, models

if __name__ == "__main__":
    results = main()
