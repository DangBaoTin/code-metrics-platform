import streamlit as st
import pandas as pd
import plotly.express as px
import numpy as np
import time

# --- 1. Page Configuration ---
st.set_page_config(page_title="Code Metrics Data Platform", layout="wide", initial_sidebar_state="expanded")

# --- 2. Sidebar Setup ---
st.sidebar.header("⚙️ Control Panel")

st.sidebar.subheader("Simulation Controls")
if st.sidebar.button("▶ Generate Fake Submissions (Background)"):
    st.sidebar.success("Simulator running in background...")
    # In Phase 2, this can trigger a subprocess running your generate_logs.py

st.sidebar.subheader("Dashboard Views")
view = st.sidebar.radio(
    "Select View:",
    ("Live Leaderboard", "System Latency Alerts", "Dropout Risk Monitor", "Batch Analytics"),
    index=3 # Defaults to Batch Analytics to match your screenshot
)

st.sidebar.markdown("---")
st.sidebar.subheader("System Status")
st.sidebar.markdown("🟢 **Kafka:** Connected")
st.sidebar.markdown("🟢 **Cassandra:** Connected")
st.sidebar.markdown("🟢 **Spark:** Idle")

# --- 3. Main Dashboard Routing ---
st.title("Code Metrics Data Platform")

# ---------------------------------------------------------
# VIEW 1: BATCH ANALYTICS (The view from your screenshot)
# ---------------------------------------------------------
if view == "Batch Analytics":
    st.header("📊 Batch Analytics & BI")
    st.markdown("Insights generated from nightly Spark SQL ETL pipelines.")
    
    # Create two columns for the layout
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Student Engagement Score (Top 10)")
        
        # Generate mock data mimicking your screenshot
        users = [f"u_{np.random.randint(100, 999)}" for _ in range(10)]
        scores = np.sort(np.random.randint(400, 950, 10))[::-1]
        df_engagement = pd.DataFrame({"User ID": users, "Score (Login Time + Solved)": scores})
        
        # Plotly Bar Chart
        fig = px.bar(df_engagement, x="User ID", y="Score (Login Time + Solved)", 
                     color="Score (Login Time + Solved)", color_continuous_scale="Blues")
        fig.update_layout(xaxis_title="", yaxis_title="Engagement Score", margin=dict(l=0, r=0, t=0, b=0))
        st.plotly_chart(fig, use_container_width=True)
        
    with col2:
        st.subheader("Difficulty Heatmap (Fail Rates)")
        
        # Generate mock data mimicking your screenshot table
        problems = [f"p_{i}" for i in [42, 17, 88, 5, 99, 23, 11]]
        attempts = [4502, 3810, 2100, 1850, 1200, 950, 420]
        fail_rates = [85.4, 72.1, 68.9, 45.2, 32.1, 15.4, 8.2]
        categories = ["Hard", "Hard", "Hard", "Medium", "Medium", "Easy", "Easy"]
        
        df_heatmap = pd.DataFrame({
            "Problem ID": problems,
            "Total Attempts": attempts,
            "Fail Rate (%)": fail_rates,
            "Category": categories
        })
        
        # Apply Pandas styling to create the heatmap effect on the Fail Rate column
        st.dataframe(
            df_heatmap.style.background_gradient(subset=["Fail Rate (%)"], cmap="Reds"),
            use_container_width=True,
            hide_index=True
        )

# ---------------------------------------------------------
# VIEW 2: LIVE LEADERBOARD
# ---------------------------------------------------------
elif view == "Live Leaderboard":
    st.header("🏆 Live Leaderboard (Real-Time)")
    st.markdown("Updated continuously via Spark Streaming from the Kafka `raw_submissions` topic.")
    
    # 1. Create an empty placeholder container
    table_placeholder = st.empty()
    
    # 2. Create a loop to simulate live data arriving every second
    # (Note: In your final version, this loop will query Cassandra instead of faking data)
    for i in range(300): # Will run for 5 minutes (300 seconds)
        # Generate slightly changing mock data
        df_live = pd.DataFrame({
            "Rank": [1, 2, 3, 4, 5],
            "User ID": ["u_999", "u_452", "u_112", "u_877", "u_231"],
            "Total Solved": [142 + i, 138 + (i // 2), 135 + (i % 3), 120, 119],
            "Recent Status": np.random.choice(["Pass", "Runtime Error", "Time Limit Exceeded"], 5)
        })
        
        # 3. Inject the new dataframe into the placeholder
        table_placeholder.dataframe(df_live, use_container_width=True, hide_index=True)
        
        # 4. Pause for 1 second before the loop restarts
        time.sleep(1)

# ---------------------------------------------------------
# VIEW 3: SYSTEM LATENCY ALERTS
# ---------------------------------------------------------
elif view == "System Latency Alerts":
    st.header("⚡ System Latency Alerts")
    st.markdown("Monitoring code execution delays and compiler health in real-time.")
    
    # Simulate a sudden spike in execution time
    st.error("⚠️ CRITICAL: Latency spike detected on Compiler Node 3 (Avg execution > 5000ms)")
    
    time_idx = pd.date_range(start=pd.Timestamp.now() - pd.Timedelta(minutes=5), periods=60, freq="5S")
    latency = np.random.normal(150, 20, 60)
    latency[-5:] = [400, 1200, 3500, 5200, 4800] # Inject a spike at the end of the graph
    
    df_latency = pd.DataFrame({"Time": time_idx, "Execution Time (ms)": latency})
    fig2 = px.line(df_latency, x="Time", y="Execution Time (ms)", title="Average Execution Time (Last 5 Minutes)")
    fig2.add_hline(y=3000, line_dash="dash", line_color="red", annotation_text="Critical Timeout Threshold")
    st.plotly_chart(fig2, use_container_width=True)

# ---------------------------------------------------------
# VIEW 4: DROPOUT RISK MONITOR
# ---------------------------------------------------------
elif view == "Dropout Risk Monitor":
    st.header("🚨 Dropout Risk Monitor")
    st.markdown("Machine Learning predictions based on historical engagement and failure rates.")
    
    df_risk = pd.DataFrame({
        "User ID": ["u_105", "u_442", "u_911"],
        "Risk Level": ["High", "High", "Medium"],
        "Primary Factor": ["Consecutive Failures (Week 1)", "Zero Logins in 7 Days", "Low Engagement Score"],
        "Suggested Action": ["Send Tutor Email", "Send Reminder", "Adjust Difficulty to Easy"]
    })
    
    # Highlight high-risk rows
    def highlight_risk(val):
        color = 'red' if val == 'High' else 'orange' if val == 'Medium' else 'green'
        return f'color: {color}; font-weight: bold'
        
    st.dataframe(df_risk.style.map(highlight_risk, subset=['Risk Level']), use_container_width=True, hide_index=True)