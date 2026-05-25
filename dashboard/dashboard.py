"""
Dashboard — podglad alertow w czasie rzeczywistym.
Czyta pliki CSV generowane przez Spark Streaming.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import os
import glob
import time

st.set_page_config(
    page_title="Fraud Monitor",
    page_icon="🛡️",
    layout="wide",
)

ALERTS_PATH = os.getenv("ALERTS_CSV_PATH", "/data/alerts.csv")


@st.cache_data(ttl=5)
def load_alerts() -> pd.DataFrame:
    """Wczytuje alerty z plikow CSV (Spark zapisuje jako katalog part-*)."""
    csv_dir = ALERTS_PATH
    files = []

    if os.path.isdir(csv_dir):
        files = glob.glob(os.path.join(csv_dir, "**", "part-*.csv"), recursive=True)
    elif os.path.isfile(csv_dir):
        files = [csv_dir]

    if not files:
        return pd.DataFrame()

    chunks = []
    for f in files:
        try:
            chunk = pd.read_csv(f)
            if not chunk.empty:
                chunks.append(chunk)
        except Exception:
            continue

    if not chunks:
        return pd.DataFrame()

    df = pd.concat(chunks, ignore_index=True)

    if "amount" in df.columns:
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    if "risk_score" in df.columns:
        df["risk_score"] = pd.to_numeric(df["risk_score"], errors="coerce")
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    if "processed_at" in df.columns:
        df["processed_at"] = pd.to_datetime(df["processed_at"], errors="coerce")

    return df.sort_values("processed_at", ascending=False).reset_index(drop=True)


# -------------------------------------------------------------------
#  Naglowek
# -------------------------------------------------------------------
st.markdown(
    """
    <style>
        .main-header {
            font-size: 28px;
            font-weight: 700;
            margin-bottom: 2px;
            letter-spacing: -0.5px;
        }
        .sub-header {
            font-size: 14px;
            color: #888;
            margin-bottom: 24px;
        }
    </style>
    <div class="main-header">Fraud Monitor</div>
    <div class="sub-header">
        Pipeline: Producer → Kafka → Spark Streaming → alerty CSV
        &nbsp;·&nbsp; odswiezanie co 5s
    </div>
    """,
    unsafe_allow_html=True,
)

col_r, _ = st.columns([1, 5])
with col_r:
    if st.button("Odswiez"):
        st.cache_data.clear()

df = load_alerts()

if df.empty:
    st.info(
        "Brak alertow. Sprawdz czy pipeline dziala:\n\n"
        "`docker compose up --build`\n\n"
        "Spark musi przetworzyc kilka transakcji zanim pojawia sie alerty."
    )
    st.stop()

# -------------------------------------------------------------------
#  Metryki (gora strony)
# -------------------------------------------------------------------
m1, m2, m3, m4 = st.columns(4)
m1.metric("Alerty", len(df))
m2.metric("Srednia kwota", f"{df['amount'].mean():,.0f} PLN")

crit = len(df[df["risk_level"] == "CRITICAL"]) if "risk_level" in df.columns else 0
high = len(df[df["risk_level"] == "HIGH"]) if "risk_level" in df.columns else 0
m3.metric("CRITICAL", crit)
m4.metric("HIGH", high)

st.divider()

# -------------------------------------------------------------------
#  Wykresy — 2 kolumny
# -------------------------------------------------------------------
left, right = st.columns(2)

with left:
    if "timestamp" in df.columns and "amount" in df.columns:
        fig = px.scatter(
            df.head(300),
            x="timestamp", y="amount",
            color="risk_level",
            color_discrete_map={
                "CRITICAL": "#d32f2f",
                "HIGH": "#ef6c00",
                "MEDIUM": "#fbc02d",
            },
            hover_data=["tx_id", "category", "store"],
        )
        fig.update_layout(
            title="Kwoty alertow w czasie",
            height=380,
            margin=dict(l=20, r=20, t=40, b=20),
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis_title=None,
            yaxis_title="PLN",
        )
        st.plotly_chart(fig, use_container_width=True)

with right:
    if "store" in df.columns:
        counts = df["store"].value_counts().reset_index()
        counts.columns = ["store", "n"]
        fig2 = px.bar(
            counts, x="store", y="n",
            color="n",
            color_continuous_scale=["#e3f2fd", "#1565c0"],
        )
        fig2.update_layout(
            title="Alerty per sklep",
            height=380,
            margin=dict(l=20, r=20, t=40, b=20),
            plot_bgcolor="rgba(0,0,0,0)",
            showlegend=False,
            coloraxis_showscale=False,
            xaxis_title=None,
            yaxis_title="Liczba",
        )
        st.plotly_chart(fig2, use_container_width=True)

# -------------------------------------------------------------------
#  Dolny rzad — kategorie + poziomy ryzyka
# -------------------------------------------------------------------
bl, br = st.columns(2)

with bl:
    if "category" in df.columns:
        fig3 = px.pie(df, names="category", hole=0.45)
        fig3.update_layout(
            title="Kategorie",
            height=320,
            margin=dict(l=10, r=10, t=40, b=10),
        )
        st.plotly_chart(fig3, use_container_width=True)

with br:
    if "risk_level" in df.columns:
        rc = df["risk_level"].value_counts().reset_index()
        rc.columns = ["level", "n"]
        fig4 = px.bar(
            rc, x="level", y="n",
            color="level",
            color_discrete_map={
                "CRITICAL": "#d32f2f",
                "HIGH": "#ef6c00",
                "MEDIUM": "#fbc02d",
            },
        )
        fig4.update_layout(
            title="Poziomy ryzyka",
            height=320,
            margin=dict(l=10, r=10, t=40, b=10),
            plot_bgcolor="rgba(0,0,0,0)",
            showlegend=False,
            xaxis_title=None,
            yaxis_title="Liczba",
        )
        st.plotly_chart(fig4, use_container_width=True)

# -------------------------------------------------------------------
#  Tabela alertow
# -------------------------------------------------------------------
st.divider()
st.subheader("Ostatnie alerty")

show = [
    "tx_id", "user_id", "amount", "category", "store",
    "hour", "risk_score", "risk_level", "triggered_rules", "timestamp",
]
show = [c for c in show if c in df.columns]
st.dataframe(df[show].head(100), use_container_width=True, height=360)

st.caption(f"Ostatnia aktualizacja: {time.strftime('%H:%M:%S')}")
