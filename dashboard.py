import streamlit as st
import pandas as pd
from google.cloud import bigquery
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/fraud-detection-project/gcp-key.json"

st.set_page_config(
    page_title="Fraud Detection Platform",
    page_icon="🛡️",
    layout="wide"
)

# ── CSS ──────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

* { font-family: 'Inter', sans-serif; }

.stApp { background: #f4f6f9; }

.header-container {
    background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
    padding: 40px;
    border-radius: 16px;
    margin-bottom: 30px;
    color: white;
}

.header-title {
    font-size: 2.2rem;
    font-weight: 700;
    color: white;
    margin: 0;
    letter-spacing: -0.5px;
}

.header-subtitle {
    font-size: 1rem;
    color: #a0aec0;
    margin-top: 6px;
}

.kpi-card {
    background: white;
    border-radius: 12px;
    padding: 24px;
    text-align: center;
    box-shadow: 0 1px 3px rgba(0,0,0,0.08);
    border-top: 3px solid #e53e3e;
    height: 100%;
}

.kpi-value {
    font-size: 1.9rem;
    font-weight: 700;
    color: #1a202c;
    line-height: 1.2;
}

.kpi-label {
    font-size: 0.78rem;
    color: #718096;
    text-transform: uppercase;
    letter-spacing: 1.2px;
    margin-top: 6px;
    font-weight: 500;
}

.kpi-delta {
    font-size: 0.82rem;
    color: #e53e3e;
    font-weight: 600;
    margin-top: 4px;
}

.section-title {
    font-size: 1.1rem;
    font-weight: 600;
    color: #1a202c;
    margin-bottom: 4px;
}

.section-subtitle {
    font-size: 0.82rem;
    color: #718096;
    margin-bottom: 16px;
}

.chart-card {
    background: white;
    border-radius: 12px;
    padding: 24px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.08);
}

div[data-testid="stMetric"] {
    background: white;
    border-radius: 12px;
    padding: 20px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.08);
}

footer { visibility: hidden; }
</style>
""", unsafe_allow_html=True)

# ── DATA ─────────────────────────────────────────────────────────────────
client = bigquery.Client(project="fraud-detection-project-491323")

@st.cache_data(ttl=300)
def load_data():
    df_cat = client.query("""
        SELECT * FROM `fraud-detection-project-491323.fraud_detection.fraud_by_category`
        ORDER BY fraud_rate DESC
    """).to_dataframe()

    df_state = client.query("""
        SELECT * FROM `fraud-detection-project-491323.fraud_detection.fraud_by_state`
        ORDER BY total_frauds DESC
    """).to_dataframe()

    df_time = client.query("""
        SELECT date, SUM(total_frauds) as total_frauds,
               SUM(total_transactions) as total_transactions
        FROM `fraud-detection-project-491323.fraud_detection.fraud_by_time`
        GROUP BY date ORDER BY date
    """).to_dataframe()

    df_merchant = client.query("""
        SELECT * FROM `fraud-detection-project-491323.fraud_detection.fraud_by_merchant`
        ORDER BY total_frauds DESC LIMIT 10
    """).to_dataframe()

    df_age = client.query("""
        SELECT * FROM `fraud-detection-project-491323.fraud_detection.fraud_by_age_gender`
        ORDER BY age_group
    """).to_dataframe()

    return df_cat, df_state, df_time, df_merchant, df_age

with st.spinner(""):
    df_cat, df_state, df_time, df_merchant, df_age = load_data()

total_tx = int(df_cat['total_transactions'].sum())
total_fraud = int(df_cat['total_frauds'].sum())
total_amt = float(df_cat['total_amount'].sum())
fraud_rate = round(total_fraud / total_tx * 100, 2)

# ── HEADER ───────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="header-container">
    <p class="header-title">🛡️ Fraud Detection Platform</p>
    <p class="header-subtitle">
        Real-time payment fraud analytics &nbsp;|&nbsp;
        1,296,675 transactions monitored &nbsp;|&nbsp;
        Pipeline: Kafka → Spark → BigQuery → dbt
    </p>
</div>
""", unsafe_allow_html=True)

# ── KPIs ─────────────────────────────────────────────────────────────────
k1, k2, k3, k4 = st.columns(4)

with k1:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-value">{total_tx:,}</div>
        <div class="kpi-label">Total Transactions</div>
        <div class="kpi-delta">Fully processed</div>
    </div>""", unsafe_allow_html=True)

with k2:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-value">{total_fraud:,}</div>
        <div class="kpi-label">Fraudulent Cases</div>
        <div class="kpi-delta">Detected & flagged</div>
    </div>""", unsafe_allow_html=True)

with k3:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-value">{fraud_rate}%</div>
        <div class="kpi-label">Fraud Rate</div>
        <div class="kpi-delta">Across all categories</div>
    </div>""", unsafe_allow_html=True)

with k4:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-value">${total_amt/1e6:.1f}M</div>
        <div class="kpi-label">Total Volume</div>
        <div class="kpi-delta">Transaction amount</div>
    </div>""", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── TILE 1 — CATEGORICAL ─────────────────────────────────────────────────
st.markdown('<div class="section-title">📊 Tile 1 — Fraud Rate by Merchant Category</div>', unsafe_allow_html=True)
st.markdown('<div class="section-subtitle">Distribution of fraud rate across merchant categories — partitioned & clustered BigQuery table</div>', unsafe_allow_html=True)

fig1 = px.bar(
    df_cat.sort_values("fraud_rate", ascending=True),
    x="fraud_rate",
    y="category",
    orientation="h",
    color="fraud_rate",
    color_continuous_scale=[[0, "#fed7d7"], [0.5, "#fc8181"], [1, "#c53030"]],
    text="fraud_rate",
    labels={"fraud_rate": "Fraud Rate (%)", "category": "Merchant Category"},
)
fig1.update_traces(
    texttemplate="<b>%{text:.2f}%</b>",
    textposition="outside",
    marker_line_width=0
)
fig1.update_layout(
    height=380,
    plot_bgcolor="white",
    paper_bgcolor="white",
    coloraxis_showscale=False,
    margin=dict(l=20, r=80, t=10, b=20),
    xaxis=dict(showgrid=True, gridcolor="#f0f0f0", title="Fraud Rate (%)"),
    yaxis=dict(showgrid=False, title=""),
    font=dict(family="Inter", size=12),
)
st.plotly_chart(fig1, use_container_width=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── TILE 2 — TEMPORAL ────────────────────────────────────────────────────
st.markdown('<div class="section-title">📅 Tile 2 — Fraud Trends Over Time</div>', unsafe_allow_html=True)
st.markdown('<div class="section-subtitle">Daily evolution of fraudulent transactions — temporal distribution across the dataset period</div>', unsafe_allow_html=True)

fig2 = make_subplots(specs=[[{"secondary_y": True}]])

fig2.add_trace(go.Scatter(
    x=df_time["date"],
    y=df_time["total_frauds"],
    name="Daily Frauds",
    fill="tozeroy",
    fillcolor="rgba(197,48,48,0.08)",
    line=dict(color="#c53030", width=2.5),
    mode="lines"
), secondary_y=False)

fig2.add_trace(go.Scatter(
    x=df_time["date"],
    y=df_time["total_transactions"],
    name="Total Transactions",
    line=dict(color="#3182ce", width=1.5, dash="dot"),
    opacity=0.6
), secondary_y=True)

fig2.update_layout(
    height=350,
    plot_bgcolor="white",
    paper_bgcolor="white",
    legend=dict(
        orientation="h",
        yanchor="bottom", y=1.02,
        xanchor="right", x=1,
        font=dict(size=11)
    ),
    margin=dict(l=20, r=20, t=30, b=20),
    font=dict(family="Inter", size=12),
    hovermode="x unified"
)
fig2.update_xaxes(showgrid=True, gridcolor="#f0f0f0", title="Date")
fig2.update_yaxes(title_text="Frauds", secondary_y=False, showgrid=True, gridcolor="#f0f0f0")
fig2.update_yaxes(title_text="Transactions", secondary_y=True, showgrid=False)
st.plotly_chart(fig2, use_container_width=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── ROW 3 — MAP + MERCHANTS ──────────────────────────────────────────────
col_map, col_merch = st.columns([1.1, 0.9])

with col_map:
    st.markdown('<div class="section-title">🗺️ Geographic Distribution — Fraud by State</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Total fraudulent transactions per US state</div>', unsafe_allow_html=True)

    fig3 = px.choropleth(
        df_state,
        locations="state",
        locationmode="USA-states",
        color="total_frauds",
        scope="usa",
        color_continuous_scale=[[0, "#fff5f5"], [0.5, "#fc8181"], [1, "#c53030"]],
        labels={"total_frauds": "Total Frauds"},
        hover_data={"fraud_rate": True, "total_transactions": True}
    )
    fig3.update_layout(
        height=320,
        margin=dict(l=0, r=0, t=0, b=0),
        paper_bgcolor="white",
        geo=dict(bgcolor="white", lakecolor="white"),
        coloraxis_colorbar=dict(title="Frauds", thickness=12)
    )
    st.plotly_chart(fig3, use_container_width=True)

with col_merch:
    st.markdown('<div class="section-title">🏪 Top 10 Fraudulent Merchants</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Merchants with highest fraud case count</div>', unsafe_allow_html=True)

    fig4 = px.bar(
        df_merchant.sort_values("total_frauds"),
        x="total_frauds",
        y="merchant",
        orientation="h",
        color="fraud_rate",
        color_continuous_scale=[[0, "#fed7d7"], [1, "#c53030"]],
        labels={"total_frauds": "Frauds", "merchant": ""},
        text="total_frauds"
    )
    fig4.update_traces(
        texttemplate="%{text}",
        textposition="outside",
        marker_line_width=0
    )
    fig4.update_layout(
        height=320,
        plot_bgcolor="white",
        paper_bgcolor="white",
        coloraxis_showscale=False,
        margin=dict(l=10, r=60, t=10, b=20),
        font=dict(family="Inter", size=11),
        xaxis=dict(showgrid=True, gridcolor="#f0f0f0"),
        yaxis=dict(showgrid=False)
    )
    st.plotly_chart(fig4, use_container_width=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── ROW 4 — AGE/GENDER ───────────────────────────────────────────────────
col_age, col_gen = st.columns(2)

with col_age:
    st.markdown('<div class="section-title">🎂 Fraud Rate by Age Group</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Vulnerability by customer age segment</div>', unsafe_allow_html=True)

    df_age_grp = df_age.groupby("age_group").agg(
        total_frauds=("total_frauds","sum"),
        total_transactions=("total_transactions","sum")
    ).reset_index()
    df_age_grp["fraud_rate"] = (df_age_grp["total_frauds"] / df_age_grp["total_transactions"] * 100).round(2)
    df_age_grp["age_label"] = df_age_grp["age_group"].apply(lambda x: f"{int(x)}s")

    fig5 = px.bar(
        df_age_grp,
        x="age_label", y="fraud_rate",
        color="fraud_rate",
        color_continuous_scale=[[0,"#fed7d7"],[1,"#c53030"]],
        text="fraud_rate",
        labels={"age_label":"Age Group","fraud_rate":"Fraud Rate (%)"}
    )
    fig5.update_traces(texttemplate="%{text:.2f}%", textposition="outside", marker_line_width=0)
    fig5.update_layout(
        height=280, plot_bgcolor="white", paper_bgcolor="white",
        coloraxis_showscale=False,
        margin=dict(l=10,r=10,t=10,b=20),
        font=dict(family="Inter",size=12),
        xaxis=dict(showgrid=False),
        yaxis=dict(showgrid=True,gridcolor="#f0f0f0")
    )
    st.plotly_chart(fig5, use_container_width=True)

with col_gen:
    st.markdown('<div class="section-title">👤 Fraud Distribution by Gender</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Fraud cases split by customer gender</div>', unsafe_allow_html=True)

    df_gen = df_age.groupby("gender").agg(
        total_frauds=("total_frauds","sum")
    ).reset_index()

    fig6 = px.pie(
        df_gen,
        values="total_frauds",
        names="gender",
        color_discrete_sequence=["#c53030","#3182ce"],
        hole=0.55
    )
    fig6.update_traces(
        textposition="outside",
        textinfo="percent+label",
        pull=[0.03,0.03]
    )
    fig6.update_layout(
        height=280,
        paper_bgcolor="white",
        margin=dict(l=20,r=20,t=20,b=20),
        font=dict(family="Inter",size=12),
        showlegend=False
    )
    st.plotly_chart(fig6, use_container_width=True)

# ── FOOTER ────────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown("""
<div style='text-align:center; color:#a0aec0; font-size:0.78rem; padding:10px 0'>
    🛡️ <b>Fraud Detection Platform</b> &nbsp;|&nbsp;
    Data Warehouse: <b>BigQuery</b> (partitioned + clustered) &nbsp;|&nbsp;
    Transforms: <b>dbt</b> &nbsp;|&nbsp;
    Pipeline: <b>Apache Kafka + Spark Streaming</b> &nbsp;|&nbsp;
    IaC: <b>Terraform</b>
</div>
""", unsafe_allow_html=True)