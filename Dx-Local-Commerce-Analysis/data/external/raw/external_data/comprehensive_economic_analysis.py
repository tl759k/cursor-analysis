#!/usr/bin/env python3
"""
Comprehensive Economic Analysis: Unemployment Rate vs DoorDash Applications

This analysis tests the hypothesis that higher unemployment rates lead to more 
DoorDash applications, especially among young people, while controlling for:
- State-level differences (population, economics, demographics)
- Seasonality effects
- Minimum wage and inflation impacts
- Regional economic variations

Data Sources:
1. DoorDash applications by state/month
2. BLS unemployment rates by state/month  
3. DOL minimum wages by state/month
4. BLS Consumer Price Index by state/month
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Statistical analysis libraries
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score, mean_squared_error
from scipy import stats
from scipy.stats import pearsonr, spearmanr
import statsmodels.api as sm
from statsmodels.formula.api import ols
from statsmodels.stats.diagnostic import het_breuschpagan
from statsmodels.stats.stattools import durbin_watson

# Set up plotting
plt.style.use('default')
sns.set_palette("husl")
plt.rcParams['figure.figsize'] = (12, 8)
plt.rcParams['font.size'] = 10

class EconomicAnalysis:
    def __init__(self):
        self.data = None
        self.results = {}
        self.models = {}
        
    def load_and_merge_data(self):
        """Load and merge all economic datasets"""
        print("🔄 LOADING AND MERGING ECONOMIC DATASETS")
        print("=" * 50)
        
        try:
            # Load datasets
            print("📊 Loading datasets...")
            
            # 1. DoorDash applications
            apps_file = '../df_apps_by_state_output.csv'
            try:
                apps_df = pd.read_csv(apps_file)
                print(f"   ✅ Applications data: {len(apps_df):,} records")
            except FileNotFoundError:
                print(f"   ❌ Applications file not found at {apps_file}")
                # Try alternative location
                apps_df = pd.read_csv('df_apps_by_state_output.csv')
                print(f"   ✅ Applications data (alt path): {len(apps_df):,} records")
            
            # 2. Unemployment rates
            unemp_file = '../bls_state_unemployment.csv'
            try:
                unemp_df = pd.read_csv(unemp_file)
                print(f"   ✅ Unemployment data: {len(unemp_df):,} records")
            except FileNotFoundError:
                print(f"   ❌ Unemployment file not found at {unemp_file}")
                # Try alternative location
                unemp_df = pd.read_csv('bls_state_unemployment.csv')
                print(f"   ✅ Unemployment data (alt path): {len(unemp_df):,} records")
            
            # 3. Minimum wages
            wage_df = pd.read_csv('dol_monthly_minimum_wage_by_state.csv')
            print(f"   ✅ Minimum wage data: {len(wage_df):,} records")
            
            # 4. CPI data
            cpi_df = pd.read_csv('monthly_cpi_by_state.csv')
            print(f"   ✅ CPI data: {len(cpi_df):,} records")
            
            # Standardize column names and formats
            print("\n🔧 Standardizing data formats...")
            
            # Standardize date columns
            for df, name in [(apps_df, 'apps'), (unemp_df, 'unemployment'), (wage_df, 'wages'), (cpi_df, 'cpi')]:
                if 'date' in df.columns:
                    df['date'] = pd.to_datetime(df['date'])
                elif 'month' in df.columns and 'year' in df.columns:
                    df['date'] = pd.to_datetime(df[['year', 'month']].assign(day=1))
                print(f"   ✅ {name}: date standardized")
            
            # Standardize state names
            self.standardize_state_names(apps_df, 'apps')
            self.standardize_state_names(unemp_df, 'unemployment')
            self.standardize_state_names(wage_df, 'wages')
            self.standardize_state_names(cpi_df, 'cpi')
            
            # Merge datasets
            print("\n🔗 Merging datasets...")
            
            # Start with applications data as base
            merged = apps_df.copy()
            
            # Merge unemployment data
            merged = merged.merge(
                unemp_df[['state_name', 'date', 'unemployment_rate']], 
                on=['state_name', 'date'], 
                how='inner'
            )
            print(f"   ✅ After unemployment merge: {len(merged):,} records")
            
            # Merge wage data
            merged = merged.merge(
                wage_df[['state_name', 'date', 'minimum_wage']], 
                on=['state_name', 'date'], 
                how='inner'
            )
            print(f"   ✅ After wage merge: {len(merged):,} records")
            
            # Merge CPI data
            merged = merged.merge(
                cpi_df[['state_name', 'date', 'cpi_value']], 
                on=['state_name', 'date'], 
                how='inner'
            )
            print(f"   ✅ Final merged dataset: {len(merged):,} records")
            
            # Basic data validation
            print(f"\n📈 Data coverage:")
            print(f"   States: {merged['state_name'].nunique()}")
            print(f"   Time period: {merged['date'].min().strftime('%Y-%m')} to {merged['date'].max().strftime('%Y-%m')}")
            print(f"   Months: {merged['date'].nunique()}")
            
            self.data = merged
            return merged
            
        except Exception as e:
            print(f"❌ Error loading data: {e}")
            return None
    
    def standardize_state_names(self, df, dataset_name):
        """Standardize state names across datasets"""
        # Common state name variations
        state_mappings = {
            'DC': 'District of Columbia',
            'Washington DC': 'District of Columbia',
            'D.C.': 'District of Columbia'
        }
        
        if 'state_name' in df.columns:
            df['state_name'] = df['state_name'].replace(state_mappings)
            # Remove territories for consistent analysis
            us_states = df[~df['state_name'].isin(['Guam', 'Puerto Rico', 'U.S. Virgin Islands', 'Federal'])]
            print(f"     {dataset_name}: {len(us_states)} records for US states/DC")
        
    def engineer_features(self):
        """Create additional features for analysis"""
        print("\n🛠️  FEATURE ENGINEERING")
        print("=" * 30)
        
        df = self.data.copy()
        
        # Time-based features
        df['year'] = df['date'].dt.year
        df['month'] = df['date'].dt.month
        df['quarter'] = df['date'].dt.quarter
        
        # Seasonality indicators
        df['is_summer'] = df['month'].isin([6, 7, 8]).astype(int)
        df['is_winter'] = df['month'].isin([12, 1, 2]).astype(int)
        df['is_holiday_season'] = df['month'].isin([11, 12]).astype(int)
        
        # Economic indicators
        # Real minimum wage (adjusted for inflation)
        baseline_cpi = df[df['date'] == df['date'].min()]['cpi_value'].mean()
        df['real_minimum_wage'] = df['minimum_wage'] * (baseline_cpi / df['cpi_value'])
        
        # Unemployment rate categories
        df['high_unemployment'] = (df['unemployment_rate'] > df['unemployment_rate'].median()).astype(int)
        
        # State-level economic context (using unemployment and wage levels)
        state_medians = df.groupby('state_name').agg({
            'unemployment_rate': 'median',
            'minimum_wage': 'median',
            'cpi_value': 'median'
        }).add_suffix('_state_median')
        
        df = df.merge(state_medians, left_on='state_name', right_index=True)
        
        # Relative indicators (state performance vs its own baseline)
        df['unemployment_relative'] = df['unemployment_rate'] - df['unemployment_rate_state_median']
        df['wage_relative'] = df['minimum_wage'] - df['minimum_wage_state_median']
        
        # Log transformations for skewed variables
        df['log_apps'] = np.log1p(df['apps_18plus'])  # log(1+x) to handle zeros
        
        # Lag variables (previous month effects)
        df = df.sort_values(['state_name', 'date'])
        df['unemployment_rate_lag1'] = df.groupby('state_name')['unemployment_rate'].shift(1)
        df['apps_18plus_lag1'] = df.groupby('state_name')['apps_18plus'].shift(1)
        
        print("✅ Created features:")
        print(f"   • Seasonality indicators (summer, winter, holiday)")
        print(f"   • Real minimum wage (inflation-adjusted)")
        print(f"   • State-relative unemployment and wage measures")
        print(f"   • Log-transformed applications")
        print(f"   • Lagged variables (1-month)")
        
        self.data = df
        return df
    
    def exploratory_analysis(self):
        """Comprehensive exploratory data analysis"""
        print("\n📊 EXPLORATORY DATA ANALYSIS")
        print("=" * 35)
        
        df = self.data
        
        # Basic statistics
        print("📈 Descriptive Statistics:")
        desc_stats = df[['apps_18plus', 'unemployment_rate', 'minimum_wage', 'cpi_value']].describe()
        print(desc_stats.round(2))
        
        # Correlation analysis
        print("\n🔗 Correlation Matrix:")
        corr_vars = ['apps_18plus', 'unemployment_rate', 'minimum_wage', 'real_minimum_wage', 'cpi_value']
        correlation_matrix = df[corr_vars].corr()
        print(correlation_matrix.round(3))
        
        # Key hypothesis test: unemployment vs applications
        unemployment_app_corr, p_value = pearsonr(df['unemployment_rate'].dropna(), 
                                                 df.loc[df['unemployment_rate'].notna(), 'apps_18plus'])
        
        print(f"\n🎯 KEY HYPOTHESIS TEST:")
        print(f"   Unemployment Rate vs Applications Correlation: {unemployment_app_corr:.4f}")
        print(f"   P-value: {p_value:.6f}")
        print(f"   Significance: {'SIGNIFICANT' if p_value < 0.05 else 'NOT SIGNIFICANT'} at α=0.05")
        
        # Store key results
        self.results['basic_correlation'] = {
            'correlation': unemployment_app_corr,
            'p_value': p_value,
            'significant': p_value < 0.05
        }
        
        # State-level analysis
        print(f"\n🗺️  State-Level Patterns:")
        state_stats = df.groupby('state_name').agg({
            'apps_18plus': ['mean', 'std'],
            'unemployment_rate': ['mean', 'std'],
            'minimum_wage': 'mean'
        }).round(2)
        
        # Find states with highest unemployment-application correlation
        state_correlations = []
        for state in df['state_name'].unique():
            state_data = df[df['state_name'] == state]
            if len(state_data) > 3:  # Need minimum observations
                corr, p_val = pearsonr(state_data['unemployment_rate'], state_data['apps_18plus'])
                state_correlations.append({
                    'state': state,
                    'correlation': corr,
                    'p_value': p_val,
                    'observations': len(state_data)
                })
        
        state_corr_df = pd.DataFrame(state_correlations).sort_values('correlation', ascending=False)
        print(f"\nTop 10 states by unemployment-application correlation:")
        print(state_corr_df.head(10)[['state', 'correlation', 'p_value']].round(4))
        
        self.results['state_correlations'] = state_corr_df
        
        return correlation_matrix
    
    def statistical_modeling(self):
        """Run comprehensive statistical models"""
        print("\n🔬 STATISTICAL MODELING")
        print("=" * 25)
        
        df = self.data.dropna(subset=['apps_18plus', 'unemployment_rate', 'minimum_wage', 'cpi_value'])
        
        # Model 1: Simple OLS Regression
        print("📊 Model 1: Simple OLS Regression")
        print("-" * 35)
        
        X1 = df[['unemployment_rate']]
        y = df['apps_18plus']
        
        model1 = LinearRegression()
        model1.fit(X1, y)
        y_pred1 = model1.predict(X1)
        
        r2_1 = r2_score(y, y_pred1)
        mse_1 = mean_squared_error(y, y_pred1)
        
        print(f"   Coefficient (Unemployment Rate): {model1.coef_[0]:.2f}")
        print(f"   Intercept: {model1.intercept_:.2f}")
        print(f"   R-squared: {r2_1:.4f}")
        print(f"   RMSE: {np.sqrt(mse_1):.2f}")
        
        # Model 2: Multiple Regression with Controls
        print("\n📊 Model 2: Multiple Regression with Economic Controls")
        print("-" * 55)
        
        control_vars = ['unemployment_rate', 'real_minimum_wage', 'cpi_value', 
                       'is_summer', 'is_winter', 'is_holiday_season']
        
        X2 = df[control_vars]
        
        model2 = LinearRegression()
        model2.fit(X2, y)
        y_pred2 = model2.predict(X2)
        
        r2_2 = r2_score(y, y_pred2)
        mse_2 = mean_squared_error(y, y_pred2)
        
        print(f"   Coefficients:")
        for var, coef in zip(control_vars, model2.coef_):
            print(f"     {var}: {coef:.3f}")
        print(f"   Intercept: {model2.intercept_:.2f}")
        print(f"   R-squared: {r2_2:.4f}")
        print(f"   RMSE: {np.sqrt(mse_2):.2f}")
        
        # Model 3: Fixed Effects Model (using statsmodels)
        print("\n📊 Model 3: State Fixed Effects Model")
        print("-" * 40)
        
        # Create state dummies
        df_model = df.copy()
        df_model = pd.get_dummies(df_model, columns=['state_name'], prefix='state')
        
        # Fixed effects regression formula
        state_cols = [col for col in df_model.columns if col.startswith('state_')]
        formula_vars = ['unemployment_rate', 'real_minimum_wage', 'cpi_value', 
                       'is_summer', 'is_winter'] + state_cols[:-1]  # Drop one state for reference
        
        formula = f"apps_18plus ~ {' + '.join(formula_vars)}"
        
        try:
            model3 = ols(formula, data=df_model).fit()
            
            print(f"   Unemployment Rate Coefficient: {model3.params['unemployment_rate']:.3f}")
            print(f"   Standard Error: {model3.bse['unemployment_rate']:.3f}")
            print(f"   t-statistic: {model3.tvalues['unemployment_rate']:.3f}")
            print(f"   p-value: {model3.pvalues['unemployment_rate']:.6f}")
            print(f"   R-squared: {model3.rsquared:.4f}")
            print(f"   Adjusted R-squared: {model3.rsquared_adj:.4f}")
            
            # Diagnostic tests
            print(f"\n   📋 Diagnostic Tests:")
            
            # Heteroskedasticity test
            lm_stat, lm_p, fvalue, f_p = het_breuschpagan(model3.resid, model3.model.exog)
            print(f"     Breusch-Pagan test p-value: {lm_p:.6f}")
            print(f"     Heteroskedasticity: {'Present' if lm_p < 0.05 else 'Not detected'}")
            
            # Durbin-Watson test for autocorrelation
            dw_stat = durbin_watson(model3.resid)
            print(f"     Durbin-Watson statistic: {dw_stat:.3f}")
            print(f"     Autocorrelation: {'Possible' if dw_stat < 1.5 or dw_stat > 2.5 else 'Not detected'}")
            
            self.models['fixed_effects'] = model3
            
        except Exception as e:
            print(f"   ❌ Fixed effects model failed: {e}")
            model3 = None
        
        # Model 4: Log-Linear Model
        print("\n📊 Model 4: Log-Linear Model (Percentage Effects)")
        print("-" * 50)
        
        X4 = df[['unemployment_rate', 'real_minimum_wage', 'is_summer', 'is_winter']]
        y_log = df['log_apps']
        
        model4 = LinearRegression()
        model4.fit(X4, y_log)
        y_pred4 = model4.predict(X4)
        
        r2_4 = r2_score(y_log, y_pred4)
        
        print(f"   Unemployment Rate Coefficient: {model4.coef_[0]:.4f}")
        print(f"   Interpretation: 1% increase in unemployment → {(np.exp(model4.coef_[0]) - 1)*100:.2f}% change in applications")
        print(f"   R-squared: {r2_4:.4f}")
        
        # Store models
        self.models['simple'] = model1
        self.models['multiple'] = model2
        self.models['log_linear'] = model4
        
        # Model comparison
        print(f"\n📊 MODEL COMPARISON")
        print(f"-" * 20)
        print(f"{'Model':<25} {'R-squared':<12} {'Unemployment Coef':<18}")
        print(f"-" * 55)
        print(f"{'Simple OLS':<25} {r2_1:<12.4f} {model1.coef_[0]:<18.3f}")
        print(f"{'Multiple Regression':<25} {r2_2:<12.4f} {model2.coef_[0]:<18.3f}")
        if model3:
            print(f"{'Fixed Effects':<25} {model3.rsquared:<12.4f} {model3.params['unemployment_rate']:<18.3f}")
        print(f"{'Log-Linear':<25} {r2_4:<12.4f} {model4.coef_[0]:<18.4f}")
        
        # Store results
        self.results['models'] = {
            'simple_ols': {'r2': r2_1, 'coef': model1.coef_[0], 'rmse': np.sqrt(mse_1)},
            'multiple_reg': {'r2': r2_2, 'coef': model2.coef_[0], 'rmse': np.sqrt(mse_2)},
            'log_linear': {'r2': r2_4, 'coef': model4.coef_[0]},
            'fixed_effects': {'r2': model3.rsquared, 'coef': model3.params['unemployment_rate']} if model3 else None
        }
        
        return self.models
    
    def robustness_checks(self):
        """Perform robustness checks and sensitivity analysis"""
        print("\n🔍 ROBUSTNESS CHECKS")
        print("=" * 22)
        
        df = self.data.dropna(subset=['apps_18plus', 'unemployment_rate'])
        
        # 1. Outlier analysis
        print("📊 Outlier Analysis:")
        
        # Identify outliers using IQR method
        Q1_apps = df['apps_18plus'].quantile(0.25)
        Q3_apps = df['apps_18plus'].quantile(0.75)
        IQR_apps = Q3_apps - Q1_apps
        outlier_threshold = Q3_apps + 1.5 * IQR_apps
        
        outliers = df[df['apps_18plus'] > outlier_threshold]
        print(f"   Applications outliers (>Q3+1.5*IQR): {len(outliers)} observations")
        
        if len(outliers) > 0:
            print(f"   Outlier states: {outliers['state_name'].value_counts().head().to_dict()}")
        
        # Test correlation without outliers
        df_no_outliers = df[df['apps_18plus'] <= outlier_threshold]
        corr_no_outliers, p_no_outliers = pearsonr(df_no_outliers['unemployment_rate'], 
                                                   df_no_outliers['apps_18plus'])
        
        print(f"   Correlation without outliers: {corr_no_outliers:.4f} (p={p_no_outliers:.4f})")
        
        # 2. Subsample analysis
        print(f"\n📊 Subsample Analysis:")
        
        # High unemployment states
        high_unemp_states = df[df['unemployment_rate'] > df['unemployment_rate'].quantile(0.75)]
        if len(high_unemp_states) > 10:
            corr_high, p_high = pearsonr(high_unemp_states['unemployment_rate'], 
                                        high_unemp_states['apps_18plus'])
            print(f"   High unemployment periods: r={corr_high:.4f}, p={p_high:.4f}")
        
        # Different time periods
        recent_data = df[df['date'] >= '2024-01-01']
        if len(recent_data) > 10:
            corr_recent, p_recent = pearsonr(recent_data['unemployment_rate'], 
                                           recent_data['apps_18plus'])
            print(f"   Recent period (2024+): r={corr_recent:.4f}, p={p_recent:.4f}")
        
        # 3. Alternative specifications
        print(f"\n📊 Alternative Specifications:")
        
        # Spearman correlation (rank-based, robust to outliers)
        spearman_corr, spearman_p = spearmanr(df['unemployment_rate'], df['apps_18plus'])
        print(f"   Spearman correlation: {spearman_corr:.4f} (p={spearman_p:.4f})")
        
        # Relative measures
        if 'unemployment_relative' in df.columns:
            rel_corr, rel_p = pearsonr(df['unemployment_relative'].dropna(), 
                                      df.loc[df['unemployment_relative'].notna(), 'apps_18plus'])
            print(f"   Relative unemployment vs apps: {rel_corr:.4f} (p={rel_p:.4f})")
        
        # Store robustness results
        self.results['robustness'] = {
            'outliers_count': len(outliers),
            'correlation_no_outliers': corr_no_outliers,
            'spearman_correlation': spearman_corr,
            'spearman_p_value': spearman_p
        }
        
        return self.results['robustness']
    
    def create_visualizations(self):
        """Create comprehensive visualizations"""
        print("\n📈 CREATING VISUALIZATIONS")
        print("=" * 30)
        
        df = self.data
        
        # Figure 1: Main relationship scatter plot
        plt.figure(figsize=(15, 10))
        
        # Subplot 1: Unemployment vs Applications
        plt.subplot(2, 3, 1)
        plt.scatter(df['unemployment_rate'], df['apps_18plus'], alpha=0.6, s=30)
        
        # Add regression line
        z = np.polyfit(df['unemployment_rate'].dropna(), 
                      df.loc[df['unemployment_rate'].notna(), 'apps_18plus'], 1)
        p = np.poly1d(z)
        plt.plot(df['unemployment_rate'].sort_values(), 
                p(df['unemployment_rate'].sort_values()), "r--", alpha=0.8, linewidth=2)
        
        plt.xlabel('Unemployment Rate (%)')
        plt.ylabel('DoorDash Applications (18+)')
        plt.title('Unemployment Rate vs DoorDash Applications')
        plt.grid(True, alpha=0.3)
        
        # Add correlation text
        corr_coef = self.results['basic_correlation']['correlation']
        plt.text(0.05, 0.95, f'r = {corr_coef:.3f}', transform=plt.gca().transAxes, 
                bbox=dict(boxstyle="round", facecolor='wheat', alpha=0.8))
        
        # Subplot 2: Time series by state (sample)
        plt.subplot(2, 3, 2)
        sample_states = ['California', 'Texas', 'New York', 'Florida']
        for state in sample_states:
            if state in df['state_name'].values:
                state_data = df[df['state_name'] == state].sort_values('date')
                plt.plot(state_data['date'], state_data['unemployment_rate'], 
                        label=state, linewidth=2, alpha=0.8)
        
        plt.xlabel('Date')
        plt.ylabel('Unemployment Rate (%)')
        plt.title('Unemployment Rate Trends (Sample States)')
        plt.legend()
        plt.xticks(rotation=45)
        plt.grid(True, alpha=0.3)
        
        # Subplot 3: Applications by state
        plt.subplot(2, 3, 3)
        for state in sample_states:
            if state in df['state_name'].values:
                state_data = df[df['state_name'] == state].sort_values('date')
                plt.plot(state_data['date'], state_data['apps_18plus'], 
                        label=state, linewidth=2, alpha=0.8)
        
        plt.xlabel('Date')
        plt.ylabel('DoorDash Applications (18+)')
        plt.title('Application Trends (Sample States)')
        plt.legend()
        plt.xticks(rotation=45)
        plt.grid(True, alpha=0.3)
        
        # Subplot 4: Correlation heatmap
        plt.subplot(2, 3, 4)
        corr_vars = ['apps_18plus', 'unemployment_rate', 'minimum_wage', 'cpi_value']
        corr_matrix = df[corr_vars].corr()
        
        sns.heatmap(corr_matrix, annot=True, cmap='RdBu_r', center=0, 
                   square=True, fmt='.3f', cbar_kws={"shrink": .8})
        plt.title('Economic Variables Correlation')
        
        # Subplot 5: Seasonality analysis
        plt.subplot(2, 3, 5)
        monthly_stats = df.groupby('month').agg({
            'apps_18plus': 'mean',
            'unemployment_rate': 'mean'
        })
        
        ax1 = plt.gca()
        ax1.bar(monthly_stats.index, monthly_stats['apps_18plus'], alpha=0.7, color='skyblue')
        ax1.set_xlabel('Month')
        ax1.set_ylabel('Avg Applications', color='blue')
        ax1.tick_params(axis='y', labelcolor='blue')
        
        ax2 = ax1.twinx()
        ax2.plot(monthly_stats.index, monthly_stats['unemployment_rate'], 
                color='red', marker='o', linewidth=2)
        ax2.set_ylabel('Avg Unemployment Rate (%)', color='red')
        ax2.tick_params(axis='y', labelcolor='red')
        
        plt.title('Seasonality: Applications vs Unemployment')
        
        # Subplot 6: State-level correlation distribution
        plt.subplot(2, 3, 6)
        state_corrs = self.results.get('state_correlations', pd.DataFrame())
        if not state_corrs.empty:
            plt.hist(state_corrs['correlation'], bins=15, alpha=0.7, edgecolor='black')
            plt.axvline(state_corrs['correlation'].mean(), color='red', 
                       linestyle='--', label=f'Mean: {state_corrs["correlation"].mean():.3f}')
            plt.xlabel('Unemployment-Application Correlation by State')
            plt.ylabel('Number of States')
            plt.title('Distribution of State-Level Correlations')
            plt.legend()
            plt.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig('economic_analysis_comprehensive.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        print("✅ Comprehensive visualization saved as 'economic_analysis_comprehensive.png'")
        
        # Figure 2: Model results visualization
        plt.figure(figsize=(12, 8))
        
        # Model coefficients comparison
        plt.subplot(2, 2, 1)
        models = self.results.get('models', {})
        model_names = []
        coefficients = []
        
        for name, model_data in models.items():
            if model_data and 'coef' in model_data:
                model_names.append(name.replace('_', ' ').title())
                coefficients.append(model_data['coef'])
        
        if model_names:
            bars = plt.bar(model_names, coefficients, alpha=0.7)
            plt.ylabel('Unemployment Rate Coefficient')
            plt.title('Model Comparison: Unemployment Coefficients')
            plt.xticks(rotation=45)
            plt.grid(True, alpha=0.3)
            
            # Color bars based on sign
            for bar, coef in zip(bars, coefficients):
                bar.set_color('green' if coef > 0 else 'red')
        
        # R-squared comparison
        plt.subplot(2, 2, 2)
        r_squared_values = []
        for name, model_data in models.items():
            if model_data and 'r2' in model_data:
                r_squared_values.append(model_data['r2'])
        
        if model_names and r_squared_values:
            plt.bar(model_names, r_squared_values, alpha=0.7, color='lightblue')
            plt.ylabel('R-squared')
            plt.title('Model Comparison: Explained Variance')
            plt.xticks(rotation=45)
            plt.grid(True, alpha=0.3)
        
        # Residuals plot (if available)
        plt.subplot(2, 2, 3)
        if 'multiple' in self.models:
            model = self.models['multiple']
            X = df[['unemployment_rate', 'real_minimum_wage', 'cpi_value', 
                   'is_summer', 'is_winter', 'is_holiday_season']].dropna()
            y = df.loc[X.index, 'apps_18plus']
            y_pred = model.predict(X)
            residuals = y - y_pred
            
            plt.scatter(y_pred, residuals, alpha=0.6)
            plt.axhline(y=0, color='red', linestyle='--')
            plt.xlabel('Predicted Applications')
            plt.ylabel('Residuals')
            plt.title('Residuals vs Fitted Values')
            plt.grid(True, alpha=0.3)
        
        # State-level effects (if available)
        plt.subplot(2, 2, 4)
        state_effects = df.groupby('state_name')['apps_18plus'].mean().sort_values(ascending=False)
        top_states = state_effects.head(10)
        
        plt.barh(range(len(top_states)), top_states.values, alpha=0.7)
        plt.yticks(range(len(top_states)), top_states.index)
        plt.xlabel('Average Applications')
        plt.title('Top 10 States by Average Applications')
        plt.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig('model_results_analysis.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        print("✅ Model results visualization saved as 'model_results_analysis.png'")
    
    def generate_summary_report(self):
        """Generate comprehensive summary report"""
        print("\n📋 COMPREHENSIVE ANALYSIS SUMMARY")
        print("=" * 40)
        
        # Basic stats
        df = self.data
        basic_corr = self.results.get('basic_correlation', {})
        models = self.results.get('models', {})
        robustness = self.results.get('robustness', {})
        
        print("🎯 HYPOTHESIS TEST RESULTS:")
        print("-" * 30)
        print(f"Primary Hypothesis: Higher unemployment → More DoorDash applications")
        print()
        print(f"📊 Key Findings:")
        if basic_corr:
            corr = basic_corr['correlation']
            p_val = basic_corr['p_value']
            significant = basic_corr['significant']
            
            print(f"   • Unemployment-Application Correlation: {corr:.4f}")
            print(f"   • Statistical Significance: {'YES' if significant else 'NO'} (p={p_val:.6f})")
            print(f"   • Direction: {'POSITIVE' if corr > 0 else 'NEGATIVE'} relationship")
            print(f"   • Strength: {self.interpret_correlation_strength(corr)}")
        
        print(f"\n🔬 MODEL RESULTS:")
        print("-" * 20)
        if 'multiple_reg' in models:
            multi_model = models['multiple_reg']
            print(f"   • Multiple Regression R²: {multi_model['r2']:.4f}")
            print(f"   • Unemployment Coefficient: {multi_model['coef']:.3f}")
            print(f"     → 1% increase in unemployment = {multi_model['coef']:.1f} more applications")
        
        if 'fixed_effects' in models and models['fixed_effects']:
            fe_model = models['fixed_effects']
            print(f"   • Fixed Effects R²: {fe_model['r2']:.4f}")
            print(f"   • Unemployment Coefficient: {fe_model['coef']:.3f}")
            print(f"     → Controls for state-specific factors")
        
        if 'log_linear' in models:
            log_model = models['log_linear']
            pct_effect = (np.exp(log_model['coef']) - 1) * 100
            print(f"   • Log-Linear Model: {pct_effect:+.2f}% change per 1% unemployment increase")
        
        print(f"\n🔍 ROBUSTNESS CHECKS:")
        print("-" * 25)
        if robustness:
            print(f"   • Outliers identified: {robustness.get('outliers_count', 'N/A')}")
            if 'correlation_no_outliers' in robustness:
                print(f"   • Correlation without outliers: {robustness['correlation_no_outliers']:.4f}")
            if 'spearman_correlation' in robustness:
                print(f"   • Spearman (rank) correlation: {robustness['spearman_correlation']:.4f}")
        
        print(f"\n📈 DATA COVERAGE:")
        print("-" * 20)
        print(f"   • States analyzed: {df['state_name'].nunique()}")
        print(f"   • Time period: {df['date'].min().strftime('%Y-%m')} to {df['date'].max().strftime('%Y-%m')}")
        print(f"   • Total observations: {len(df):,}")
        print(f"   • Average applications per state-month: {df['apps_18plus'].mean():.1f}")
        print(f"   • Average unemployment rate: {df['unemployment_rate'].mean():.2f}%")
        
        # Economic interpretation
        print(f"\n💡 ECONOMIC INTERPRETATION:")
        print("-" * 30)
        
        if basic_corr and basic_corr['correlation'] > 0 and basic_corr['significant']:
            print("✅ HYPOTHESIS CONFIRMED:")
            print("   • Higher unemployment rates are associated with more DoorDash applications")
            print("   • This supports the theory that economic stress drives gig economy participation")
            print("   • Young adults (18+) appear to turn to delivery work during unemployment")
            
        elif basic_corr and basic_corr['correlation'] < 0 and basic_corr['significant']:
            print("❌ HYPOTHESIS REJECTED:")
            print("   • Higher unemployment rates are associated with FEWER DoorDash applications")
            print("   • Possible explanations:")
            print("     - Economic downturns reduce overall demand for delivery services")
            print("     - Competition for gig work increases, making it harder to get approved")
            print("     - People may lack resources (car, phone) needed for delivery work")
            
        else:
            print("🤔 INCONCLUSIVE RESULTS:")
            print("   • No statistically significant relationship found")
            print("   • Possible explanations:")
            print("     - Other factors may be more important (wages, demographics, etc.)")
            print("     - Relationship may be non-linear or vary by region/time")
            print("     - Data limitations or measurement issues")
        
        # Control variables insights
        if 'multiple' in self.models:
            print(f"\n🎛️  CONTROL VARIABLES EFFECTS:")
            print("-" * 35)
            model = self.models['multiple']
            vars_names = ['unemployment_rate', 'real_minimum_wage', 'cpi_value', 
                         'is_summer', 'is_winter', 'is_holiday_season']
            
            for var, coef in zip(vars_names, model.coef_):
                effect_direction = "increases" if coef > 0 else "decreases"
                print(f"   • {var.replace('_', ' ').title()}: {effect_direction} applications ({coef:+.2f})")
        
        return self.results
    
    def interpret_correlation_strength(self, correlation):
        """Interpret correlation strength"""
        abs_corr = abs(correlation)
        if abs_corr < 0.1:
            return "Very weak"
        elif abs_corr < 0.3:
            return "Weak"
        elif abs_corr < 0.5:
            return "Moderate"
        elif abs_corr < 0.7:
            return "Strong"
        else:
            return "Very strong"
    
    def recommend_next_steps(self):
        """Provide recommendations for next steps"""
        print(f"\n🚀 RECOMMENDED NEXT STEPS")
        print("=" * 30)
        
        basic_corr = self.results.get('basic_correlation', {})
        
        print("🔬 ANALYTICAL IMPROVEMENTS:")
        print("   1. Age-specific analysis:")
        print("      • Break down by age groups (18-25, 26-35, etc.)")
        print("      • Test if young adults are more responsive to unemployment")
        print("   2. Regional analysis:")
        print("      • Urban vs rural unemployment effects")
        print("      • Regional economic conditions")
        print("   3. Temporal analysis:")
        print("      • Lag effects (how long does unemployment impact last?)")
        print("      • Seasonal unemployment patterns")
        
        print(f"\n📊 ADDITIONAL DATA TO COLLECT:")
        print("   1. Demographic data:")
        print("      • Population by age group by state")
        print("      • Educational attainment levels")
        print("      • Income distribution")
        print("   2. Economic indicators:")
        print("      • GDP by state")
        print("      • Cost of living indices")
        print("      • Job openings data (JOLTS)")
        print("   3. DoorDash specific:")
        print("      • Approval rates for new dashers")
        print("      • Market saturation metrics")
        print("      • Competition from other gig platforms")
        
        print(f"\n🎯 BUSINESS IMPLICATIONS:")
        if basic_corr and basic_corr.get('significant'):
            if basic_corr['correlation'] > 0:
                print("   • Target recruitment during high unemployment periods")
                print("   • Prepare for increased applicant volume during recessions")
                print("   • Adjust onboarding capacity based on economic indicators")
            else:
                print("   • Focus on quality over quantity during economic downturns")
                print("   • Consider alternative recruitment strategies during high unemployment")
                print("   • Investigate barriers to application during tough economic times")
        
        print(f"\n🔮 ADVANCED ANALYTICS:")
        print("   1. Machine learning models:")
        print("      • Random forest for non-linear relationships")
        print("      • Time series forecasting for application volume")
        print("   2. Causal inference:")
        print("      • Instrumental variables approach")
        print("      • Difference-in-differences analysis")
        print("   3. Geographic analysis:")
        print("      • County-level data for finer granularity")
        print("      • Spatial correlation analysis")
        
        print(f"\n📈 MONITORING DASHBOARD:")
        print("   • Real-time unemployment vs application tracking")
        print("   • Early warning system for economic downturns")
        print("   • State-specific recruitment strategy recommendations")

def main():
    """Main analysis execution"""
    print("🎯 COMPREHENSIVE ECONOMIC ANALYSIS")
    print("   Unemployment Rate vs DoorDash Applications")
    print("=" * 60)
    
    # Initialize analysis
    analysis = EconomicAnalysis()
    
    # Step 1: Load and merge data
    merged_data = analysis.load_and_merge_data()
    if merged_data is None:
        print("❌ Failed to load data. Exiting.")
        return
    
    # Step 2: Engineer features
    analysis.engineer_features()
    
    # Step 3: Exploratory analysis
    analysis.exploratory_analysis()
    
    # Step 4: Statistical modeling
    analysis.statistical_modeling()
    
    # Step 5: Robustness checks
    analysis.robustness_checks()
    
    # Step 6: Create visualizations
    analysis.create_visualizations()
    
    # Step 7: Generate summary
    analysis.generate_summary_report()
    
    # Step 8: Recommendations
    analysis.recommend_next_steps()
    
    print(f"\n✅ ANALYSIS COMPLETE!")
    print("📁 Files generated:")
    print("   • economic_analysis_comprehensive.png")
    print("   • model_results_analysis.png")
    print("   • Comprehensive statistical results in console")
    
    return analysis

if __name__ == "__main__":
    analysis_results = main()
