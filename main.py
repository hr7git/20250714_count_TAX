import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import matplotlib.pyplot as plt
import seaborn as sns

# 페이지 설정
st.set_page_config(
    page_title="COVID-19 Impact on Cryptocurrency Portfolio Analysis",
    page_icon="₿",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 사이드바 설정
st.sidebar.title("Navigation")
page = st.sidebar.selectbox("Select Page", 
                           ["Introduction", "Data Overview", "Efficient Frontier Analysis", 
                            "Statistical Testing", "Results & Conclusion"])

# 데이터 정의
sharp_ratio_data = {
    'Portfolio': ['SNP TLT', 'SNP TLT BTC', 'SNP TLT ETH', 'SNP TLT BTC ETH'],
    'Before_COVID': [0.123123, 0.124165, 0.133160, 0.140777],
    'After_COVID': [0.063153, 0.104187, 0.104671, 0.107601],
    'Entire_Period': [0.080768, 0.086321, 0.082167, 0.088174]
}

gmv_data = {
    'Portfolio': ['SNP TLT', 'SNP TLT BTC', 'SNP TLT ETH', 'SNP TLT BTC ETH'],
    'Before_COVID': [0.445380, 0.443802, 0.445191, 0.442624],
    'After_COVID': [0.755785, 0.753847, 0.749474, 0.748057],
    'Entire_Period': [0.625365, 0.625290, 0.623821, 0.622037]
}

hk_test_data = {
    'Asset': ['Bitcoin', 'Ethereum', 'Bitcoin + Ethereum'],
    'Before_COVID_F': [1.5126, 0.7347, 0.2797],
    'Before_COVID_p': [22.16, 48.03, 75.61],
    'After_COVID_F': [2.6444, 5.2355, 10.0053],
    'After_COVID_p': [7.22, 0.57, 0.01],
    'Entire_F': [0.5014, 2.1668, 6.1534],
    'Entire_p': [60.59, 11.52, 0.22]
}

# 메인 컨텐츠
if page == "Introduction":
    st.title("🚀 Impact of COVID-19 on Cryptocurrency Portfolio Performance")
    st.markdown("### Spanning Test of Bitcoin and Ethereum")
    st.markdown("**Author:** Taekyoung Park")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("""
        ## 📊 Research Overview
        
        This study examines whether adding cryptocurrencies (Bitcoin and Ethereum) to traditional portfolios 
        can improve investment performance, particularly focusing on the impact of COVID-19.
        
        ### 🎯 Key Research Questions
        - Can Bitcoin serve as a hedge against inflation from Quantitative Easing?
        - Does cryptocurrency addition expand the efficient frontier?
        - How did COVID-19 affect cryptocurrency's portfolio benefits?
        
        ### 📈 Methodology
        - **Benchmark Portfolio**: S&P 500 Index + Long-term US Treasury Bonds
        - **Test Assets**: Bitcoin (BTC) and Ethereum (ETH)
        - **Analysis Framework**: Huberman and Kandel (1987) Mean-Variance Spanning Test
        - **Time Periods**: 
          - Before COVID-19: 2017/11 ~ 2019/12
          - After COVID-19: 2020/01 ~ 2022/01
        """)
    
    with col2:
        st.markdown("""
        ### 📅 Timeline
        - **2017-2019**: Pre-COVID period
        - **2020**: COVID-19 pandemic begins
        - **2020-2022**: Post-COVID analysis
        
        ### 🔍 Analysis Methods
        - Efficient Frontier Analysis
        - Sharpe Ratio Comparison
        - Global Minimum Variance
        - Statistical Hypothesis Testing
        """)
        
        # 암호화폐 시장 정보
        st.markdown("### 💰 Cryptocurrency Market (Dec 2021)")
        crypto_data = pd.DataFrame({
            'Rank': [1, 2, 3],
            'Name': ['Bitcoin', 'Ethereum', 'Binance Coin'],
            'Market Cap': ['$925.017B', '$476.975B', '$88.777B'],
            'Dominance': ['40.57%', '20.87%', '3.89%']
        })
        st.dataframe(crypto_data, use_container_width=True)

elif page == "Data Overview":
    st.title("📋 Data Overview")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 📊 Benchmark Portfolio Assets")
        benchmark_data = pd.DataFrame({
            'Asset': ['S&P 500 Index (^GSPC)', 'Long-term US Treasury (TLT)'],
            'Description': ['US Stock Market Index', '20+ Year Treasury Bond ETF'],
            'Source': ['Yahoo Finance', 'iShares ETF']
        })
        st.dataframe(benchmark_data, use_container_width=True)
        
        st.markdown("### 🔍 Risk-free Rate (SHY ETF)")
        risk_free_data = pd.DataFrame({
            'Period': ['Entire Period', 'Before COVID-19', 'After COVID-19'],
            'Rate (%)': [0.0064, 0.0110, 0.0022]
        })
        st.dataframe(risk_free_data, use_container_width=True)
    
    with col2:
        st.markdown("### ₿ Test Assets (Cryptocurrencies)")
        crypto_assets = pd.DataFrame({
            'Asset': ['Bitcoin (BTC)', 'Ethereum (ETH)'],
            'Start Date': ['2013-04-28', '2015-08-07'],
            'Analysis Period': ['2017-01-01', '2017-01-01'],
            'Market Cap Rank': [1, 2]
        })
        st.dataframe(crypto_assets, use_container_width=True)
        
        st.markdown("### 📈 Data Characteristics")
        st.markdown("""
        - **High Volatility**: Cryptocurrencies show significantly higher volatility than traditional assets
        - **Low Correlation**: Historical correlation of 0.2~0.3 with traditional portfolios
        - **Profitability**: Exceeds bond returns despite high risk
        - **Diversification Potential**: May improve portfolio efficiency through low correlation
        """)

elif page == "Efficient Frontier Analysis":
    st.title("📈 Efficient Frontier Analysis")
    
    # Sharpe Ratio 분석
    st.markdown("## 🎯 Sharpe Ratio Analysis")
    
    df_sharp = pd.DataFrame(sharp_ratio_data)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Before vs After COVID-19")
        
        fig_sharp = go.Figure()
        
        fig_sharp.add_trace(go.Bar(
            name='Before COVID-19',
            x=df_sharp['Portfolio'],
            y=df_sharp['Before_COVID'],
            marker_color='lightblue'
        ))
        
        fig_sharp.add_trace(go.Bar(
            name='After COVID-19',
            x=df_sharp['Portfolio'],
            y=df_sharp['After_COVID'],
            marker_color='orange'
        ))
        
        fig_sharp.update_layout(
            title='Sharpe Ratio Comparison',
            xaxis_title='Portfolio Type',
            yaxis_title='Sharpe Ratio',
            barmode='group',
            height=400
        )
        
        st.plotly_chart(fig_sharp, use_container_width=True)
    
    with col2:
        st.markdown("### Improvement Percentage")
        
        # 개선 효과 계산
        improvement_data = []
        benchmark_before = df_sharp.iloc[0]['Before_COVID']
        benchmark_after = df_sharp.iloc[0]['After_COVID']
        
        for i in range(1, len(df_sharp)):
            before_imp = ((df_sharp.iloc[i]['Before_COVID'] - benchmark_before) / benchmark_before) * 100
            after_imp = ((df_sharp.iloc[i]['After_COVID'] - benchmark_after) / benchmark_after) * 100
            improvement_data.append({
                'Portfolio': df_sharp.iloc[i]['Portfolio'],
                'Before_COVID_Improvement': before_imp,
                'After_COVID_Improvement': after_imp
            })
        
        df_improvement = pd.DataFrame(improvement_data)
        
        fig_imp = go.Figure()
        
        fig_imp.add_trace(go.Bar(
            name='Before COVID-19',
            x=df_improvement['Portfolio'],
            y=df_improvement['Before_COVID_Improvement'],
            marker_color='lightgreen'
        ))
        
        fig_imp.add_trace(go.Bar(
            name='After COVID-19',
            x=df_improvement['Portfolio'],
            y=df_improvement['After_COVID_Improvement'],
            marker_color='red'
        ))
        
        fig_imp.update_layout(
            title='Sharpe Ratio Improvement (%)',
            xaxis_title='Portfolio Type',
            yaxis_title='Improvement (%)',
            barmode='group',
            height=400
        )
        
        st.plotly_chart(fig_imp, use_container_width=True)
    
    # GMV 분석
    st.markdown("## 🎯 Global Minimum Variance (GMV) Analysis")
    
    df_gmv = pd.DataFrame(gmv_data)
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig_gmv = go.Figure()
        
        fig_gmv.add_trace(go.Bar(
            name='Before COVID-19',
            x=df_gmv['Portfolio'],
            y=df_gmv['Before_COVID'],
            marker_color='lightcoral'
        ))
        
        fig_gmv.add_trace(go.Bar(
            name='After COVID-19',
            x=df_gmv['Portfolio'],
            y=df_gmv['After_COVID'],
            marker_color='darkred'
        ))
        
        fig_gmv.update_layout(
            title='Global Minimum Variance Comparison',
            xaxis_title='Portfolio Type',
            yaxis_title='Variance',
            barmode='group',
            height=400
        )
        
        st.plotly_chart(fig_gmv, use_container_width=True)
    
    with col2:
        st.markdown("### Key Findings")
        st.markdown("""
        #### 📊 Sharpe Ratio Improvements (After COVID-19):
        - **Bitcoin**: 64.98% improvement
        - **Ethereum**: 65.74% improvement  
        - **Bitcoin + Ethereum**: 70.38% improvement
        
        #### 📉 GMV Left Shift (After COVID-19):
        - **Bitcoin**: -0.26% variance reduction
        - **Ethereum**: -0.83% variance reduction
        - **Bitcoin + Ethereum**: -1.02% variance reduction
        
        #### 🔍 Key Insights:
        - COVID-19 significantly enhanced cryptocurrency benefits
        - Combined portfolio shows best performance
        - Risk reduction more pronounced post-COVID
        """)

elif page == "Statistical Testing":
    st.title("🔬 Statistical Testing Results")
    
    st.markdown("## 📊 Huberman-Kandel (HK) Test Results")
    
    df_hk = pd.DataFrame(hk_test_data)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### F-Test Statistics")
        
        fig_f = go.Figure()
        
        fig_f.add_trace(go.Bar(
            name='Before COVID-19',
            x=df_hk['Asset'],
            y=df_hk['Before_COVID_F'],
            marker_color='lightblue'
        ))
        
        fig_f.add_trace(go.Bar(
            name='After COVID-19',
            x=df_hk['Asset'],
            y=df_hk['After_COVID_F'],
            marker_color='orange'
        ))
        
        fig_f.update_layout(
            title='F-Test Statistics',
            xaxis_title='Asset',
            yaxis_title='F-Statistic',
            barmode='group',
            height=400
        )
        
        st.plotly_chart(fig_f, use_container_width=True)
    
    with col2:
        st.markdown("### P-Values (%)")
        
        fig_p = go.Figure()
        
        fig_p.add_trace(go.Bar(
            name='Before COVID-19',
            x=df_hk['Asset'],
            y=df_hk['Before_COVID_p'],
            marker_color='lightgreen'
        ))
        
        fig_p.add_trace(go.Bar(
            name='After COVID-19',
            x=df_hk['Asset'],
            y=df_hk['After_COVID_p'],
            marker_color='red'
        ))
        
        # 5% 유의수준 라인 추가
        fig_p.add_hline(y=5, line_dash="dash", line_color="black", 
                       annotation_text="5% Significance Level")
        
        fig_p.update_layout(
            title='P-Values (Lower is Better)',
            xaxis_title='Asset',
            yaxis_title='P-Value (%)',
            barmode='group',
            height=400
        )
        
        st.plotly_chart(fig_p, use_container_width=True)
    
    st.markdown("## 📈 Statistical Test Summary")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("### Before COVID-19")
        st.markdown("""
        - **Bitcoin**: p-value = 22.16% ❌
        - **Ethereum**: p-value = 48.03% ❌
        - **Combined**: p-value = 75.61% ❌
        
        *No significant improvement*
        """)
    
    with col2:
        st.markdown("### After COVID-19")
        st.markdown("""
        - **Bitcoin**: p-value = 7.22% ❌
        - **Ethereum**: p-value = 0.57% ✅
        - **Combined**: p-value = 0.01% ✅
        
        *Significant improvement!*
        """)
    
    with col3:
        st.markdown("### Entire Period")
        st.markdown("""
        - **Bitcoin**: p-value = 60.59% ❌
        - **Ethereum**: p-value = 11.52% ❌
        - **Combined**: p-value = 0.22% ✅
        
        *Combined portfolio significant*
        """)
    
    st.markdown("## 🔍 Step-Down Test Analysis")
    st.markdown("""
    ### Key Findings from Step-Down Tests:
    
    #### After COVID-19 Period:
    - **Ethereum**: 
      - Step 1 (α = 0): p-value = 8.97%
      - Step 2 (β = 1|α = 0): p-value = 0.63% ✅
      - **Conclusion**: GMV shift effect is more significant than tangency portfolio improvement
    
    - **Bitcoin + Ethereum**:
      - Step 1 (α = 0): p-value = 7.66%
      - Step 2 (β = 1|α = 0): p-value = 0.01% ✅
      - **Conclusion**: Global minimum variance left shift is the primary driver of improvement
    
    #### 📊 Interpretation:
    - **GMV Effect > Sharpe Ratio Effect**: The left shift of global minimum variance point contributes more to portfolio improvement than the increase in maximum Sharpe ratio
    - **Risk Reduction**: Adding cryptocurrencies primarily reduces portfolio risk rather than increasing returns
    """)

elif page == "Results & Conclusion":
    st.title("🎯 Results & Conclusion")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("## 📊 Key Findings")
        
        st.markdown("""
        ### 🚀 COVID-19 Impact on Cryptocurrency Benefits
        
        #### Before COVID-19 (2017-2019):
        - Limited portfolio improvement effects
        - No statistically significant spanning test results
        - Cryptocurrency benefits not yet realized
        
        #### After COVID-19 (2020-2022):
        - **Dramatic improvement in portfolio performance**
        - **Ethereum**: 65.74% Sharpe ratio improvement
        - **Bitcoin + Ethereum**: 70.38% Sharpe ratio improvement
        - **Strong statistical significance** (p < 0.01)
        
        ### 📈 Portfolio Efficiency Analysis
        
        #### Sharpe Ratio Improvements:
        - Bitcoin: 0.85% → 64.98% improvement
        - Ethereum: 8.15% → 65.74% improvement
        - Combined: 14.34% → 70.38% improvement
        
        #### Risk Reduction (GMV Left Shift):
        - Bitcoin: -0.35% → -0.26% variance reduction
        - Ethereum: -0.04% → -0.83% variance reduction
        - Combined: -0.62% → -1.02% variance reduction
        """)
    
    with col2:
        st.markdown("## 🎯 Summary Statistics")
        
        # 요약 통계
        summary_stats = pd.DataFrame({
            'Metric': ['Sharpe Ratio Improvement', 'GMV Variance Reduction', 'Statistical Significance'],
            'Before COVID': ['Low (0.85-14.34%)', 'Minimal (0.04-0.62%)', 'None (p > 5%)'],
            'After COVID': ['High (65-70%)', 'Significant (0.26-1.02%)', 'Strong (p < 1%)']
        })
        
        st.dataframe(summary_stats, use_container_width=True)
        
        st.markdown("### 🏆 Best Performing Portfolio")
        st.metric("Bitcoin + Ethereum", "70.38%", "Sharpe Ratio Improvement")
        st.metric("Risk Reduction", "1.02%", "GMV Left Shift")
        st.metric("Statistical Significance", "0.01%", "P-value")
    
    st.markdown("## 🔬 Statistical Evidence")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        ### 📊 Huberman-Kandel Test Results
        
        **Null Hypothesis**: New assets do not expand investment opportunities
        
        #### After COVID-19:
        - **Ethereum**: F = 5.24, p = 0.57% ✅
        - **Bitcoin + Ethereum**: F = 10.01, p = 0.01% ✅
        - **Conclusion**: Reject null hypothesis
        
        #### Statistical Interpretation:
        - Adding cryptocurrencies **significantly expands** the efficient frontier
        - Portfolio improvement is **statistically robust**
        - Benefits are **primarily post-COVID phenomenon**
        """)
    
    with col2:
        st.markdown("""
        ### 🎯 Step-Down Test Insights
        
        **Key Discovery**: GMV effect dominates tangency portfolio effect
        
        #### Primary Driver of Improvement:
        - **Global Minimum Variance** left shift
        - **Risk reduction** more important than return enhancement
        - **Diversification benefits** through low correlation
        
        #### Practical Implications:
        - Cryptocurrencies act as **diversifiers** rather than return enhancers
        - **Risk management** is the primary benefit
        - **Portfolio efficiency** improved through correlation benefits
        """)
    
    st.markdown("## 🚀 Conclusion")
    
    st.markdown("""
    ### 🎯 Main Conclusions
    
    1. **COVID-19 as a Catalyst**: The pandemic significantly enhanced the portfolio benefits of cryptocurrencies
    
    2. **Statistical Evidence**: Strong statistical evidence (p < 0.01) supports the addition of cryptocurrencies to traditional portfolios post-COVID
    
    3. **Risk Reduction Focus**: The primary benefit comes from risk reduction (GMV left shift) rather than return enhancement
    
    4. **Combined Portfolio Superiority**: Bitcoin + Ethereum combination provides the best risk-adjusted returns
    
    5. **Diversification Benefits**: Low correlation with traditional assets makes cryptocurrencies effective diversifiers
    
    ### 🔮 Future Research Directions
    
    1. **Heteroskedasticity Analysis**: Examine how time-varying volatility affects portfolio improvement significance
    
    2. **Constraint Analysis**: Study the impact of portfolio weight constraints (0-1 bounds) on regression results
    
    3. **Asset Weight Optimization**: Investigate optimal cryptocurrency allocation in constrained portfolios
    
    4. **Extended Time Series**: Analyze longer-term effects beyond the immediate post-COVID period
    
    ### 💡 Practical Investment Implications
    
    - **Portfolio Managers**: Consider cryptocurrency allocation for risk reduction
    - **Institutional Investors**: COVID-19 changed the investment landscape for digital assets
    - **Risk Management**: Cryptocurrencies can improve portfolio efficiency through diversification
    - **Timing Matters**: The benefits of cryptocurrency inclusion are period-dependent
    """)
    
    # 마무리 메시지
    st.success("📈 This analysis provides strong evidence that COVID-19 fundamentally changed the role of cryptocurrencies in portfolio optimization, transforming them from speculative assets to valuable diversifiers.")

# 사이드바 추가 정보
st.sidebar.markdown("---")
st.sidebar.markdown("### 📊 Research Info")
st.sidebar.markdown("""
**Author**: Taekyoung Park  
**Study Period**: 2017-2022  
**Methodology**: Huberman-Kandel Spanning Test  
**Assets**: Bitcoin, Ethereum, S&P 500, US Treasury
""")

st.sidebar.markdown("### 🔑 Key Metrics")
st.sidebar.markdown("""
- **Sharpe Ratio**: Risk-adjusted return measure
- **GMV**: Global Minimum Variance
- **HK Test**: Mean-variance spanning test
- **Step-Down Test**: Decomposition analysis
""")

st.sidebar.markdown("### 📈 Period Analysis")
st.sidebar.markdown("""
- **Before COVID**: 2017/11 - 2019/12
- **After COVID**: 2020/01 - 2022/01
- **Entire Period**: 2017/11 - 2022/01
""")
