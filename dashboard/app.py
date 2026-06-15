import time
import os
import pandas as pd
import streamlit as st

# --- Configuration ---
_HERE = os.path.dirname(os.path.abspath(__file__))
PARQUET_PATH = os.path.join(_HERE, "../spark_processing/data/gold/traffic_aggregated")
REFRESH_INTERVAL = 5  # secondes

CONGESTION_CONFIG = {
    "FLUIDE":        {"color": "#2ecc71", "label": "Fluide"},
    "EMBOUTEILLAGE": {"color": "#f39c12", "label": "Embouteillage"},
    "CRITIQUE":      {"color": "#e74c3c", "label": "Critique"},
}

st.set_page_config(
    page_title="Smart City — Traffic Monitor",
    layout="wide",
)

st.title("Smart City — Monitoring du Trafic Urbain")
st.caption(f"Données Gold issues du pipeline Spark · Rafraîchissement toutes les {REFRESH_INTERVAL}s")


def load_parquet(path: str) -> pd.DataFrame | None:
    if not os.path.exists(path):
        return None
    try:
        df = pd.read_parquet(path)
        if df.empty:
            return None
        df["window_start"] = pd.to_datetime(df["window_start"])
        df["window_end"] = pd.to_datetime(df["window_end"])
        df["camera_id"] = df["camera_id"].astype(str)

        # Spark réécrit chaque fenêtre à chaque micro-batch en mode update.
        # On garde la dernière valeur écrite par fenêtre (la plus affinée).
        df = (
            df.sort_values("window_start")
            .groupby(["window_start", "camera_id"], as_index=False)
            .last()
        )
        return df
    except Exception:
        return None


df = load_parquet(PARQUET_PATH)

if df is None:
    st.info("En attente des données Parquet... Lance le producer et le consumer Spark.")
    st.code(
        "# Terminal 1 — Infrastructure\n"
        "docker compose -f infrastructure/docker-compose.yml up -d\n\n"
        "# Terminal 2 — Consumer Spark\n"
        "cd spark_processing && python consumer_spark.py\n\n"
        "# Terminal 3 — Producer YOLOv8\n"
        "cd edge_ingestion && python producer_yolo.py",
        language="bash",
    )
else:
    # --- Filtrage par caméra ---
    cameras = sorted(df["camera_id"].unique().tolist())
    selected_cam = st.selectbox("Caméra", cameras)
    df_cam = df[df["camera_id"] == selected_cam].sort_values("window_start").copy()

    latest = df_cam.iloc[-1]
    cfg = CONGESTION_CONFIG.get(latest["congestion_status"], CONGESTION_CONFIG["FLUIDE"])

    # --- Ligne 1 : KPIs ---
    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("#### État de congestion")
        st.markdown(
            f"""
            <div style="
                background-color:{cfg['color']}22;
                border-left: 6px solid {cfg['color']};
                border-radius: 8px;
                padding: 16px 20px;
                font-size: 1.4rem;
                font-weight: bold;
                color: {cfg['color']};
            ">
                {cfg['label']}
            </div>
            """,
            unsafe_allow_html=True,
        )

    with col2:
        st.metric(
            label="Véhicules moyens (dernière fenêtre d'1 min)",
            value=f"{latest['avg_vehicles']:.1f}",
        )

    with col3:
        nb_fenetres = len(df_cam)
        st.metric(
            label="Fenêtres d'analyse enregistrées",
            value=nb_fenetres,
        )

    st.divider()

    # --- Tableau détail ---
    with st.expander("Détail des fenêtres temporelles"):
        display_df = df_cam[["window_start", "window_end", "avg_vehicles", "congestion_status"]].copy()
        display_df.columns = ["Début fenêtre", "Fin fenêtre", "Véhicules moy.", "Statut"]
        st.dataframe(display_df, use_container_width=True, hide_index=True)

    st.caption(f"Dernière mise à jour : {pd.Timestamp.now().strftime('%H:%M:%S')} · {len(df_cam)} fenêtres · {len(cameras)} caméra(s)")

time.sleep(REFRESH_INTERVAL)
st.rerun()
