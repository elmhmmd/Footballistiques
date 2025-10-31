import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px

DB_USER = "postgres"
DB_PASSWORD = "password"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "Footballistiques"

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Créer le moteur de connexion
engine = create_engine(DATABASE_URL)

# --- 2. FONCTIONS POUR RÉCUPÉRER LES DONNÉES (REQUÊTES SQL) ---

# Utiliser le cache de Streamlit pour éviter de relancer les requêtes inutilem
def run_query(query):
    """Exécute une requête SQL et retourne un DataFrame pandas."""
    with engine.connect() as connection:
        return pd.read_sql(query, connection)

def get_top_scorers(limit=10):
    query = f"""
        SELECT j.nomjoueur, e.nomequipe, s.buts
        FROM statistiquejoueur s
        JOIN joueur j ON s.idjoueur = j.idjoueur
        JOIN equipe e ON j.id_equipe = e.idequipe
        ORDER BY s.buts DESC
        LIMIT {limit};
    """
    return run_query(query)

def get_most_decisive_players(limit=10):
    query = f"""
        SELECT 
            j.nomjoueur, 
            e.nomequipe, 
            (s.buts + s.passesdecisives) AS "Buts + Passes D."
        FROM statistiquejoueur s
        JOIN joueur j ON s.idjoueur = j.idjoueur
        JOIN equipe e ON j.id_equipe = e.idequipe
        ORDER BY "Buts + Passes D." DESC
        LIMIT {limit};
    """
    return run_query(query)

def get_discipline_stats():
    query = """
        SELECT 
            j.nomjoueur, 
            e.nomequipe, 
            s.cartonsjaunes, 
            s.cartonsrouges
        FROM statistiquejoueur s
        JOIN joueur j ON s.idjoueur = j.idjoueur
        JOIN equipe e ON j.id_equipe = e.idequipe
        WHERE s.cartonsjaunes > 0 OR s.cartonsrouges > 0
        ORDER BY s.cartonsrouges DESC, s.cartonsjaunes DESC;
    """
    return run_query(query)

def get_team_nationality_distribution():
    query = """
        SELECT 
            e.nomequipe, 
            j.nationalite, 
            COUNT(j.idjoueur) AS nombre_joueurs
        FROM joueur j
        JOIN equipe e ON j.id_equipe = e.idequipe
        GROUP BY e.nomequipe, j.nationalite
        ORDER BY e.nomequipe, nombre_joueurs DESC;
    """
    return run_query(query)
    
def get_team_goal_stats():
    query = """
        SELECT 
            e.nomequipe,
            SUM(r.butsmarques) AS buts_marques,
            SUM(r.butsconcedes) AS buts_concedes,
            AVG(r.butsmarques) AS moyenne_marques,
            AVG(r.butsconcedes) AS moyenne_concedes,
            COUNT(r.idmatch) AS matchs_joues
        FROM resultatmatch r
        JOIN equipe e ON r.idequipe = e.idequipe
        GROUP BY e.nomequipe
        ORDER BY buts_marques DESC;
    """
    return run_query(query)

def get_league_table():
    query = """
        SELECT
            e.nomequipe,
            COUNT(r.idmatch) AS matchs_joues,
            SUM(CASE WHEN r.resultat = 'W' THEN 1 ELSE 0 END) AS victoires,
            SUM(CASE WHEN r.resultat = 'D' THEN 1 ELSE 0 END) AS nuls,
            SUM(CASE WHEN r.resultat = 'L' THEN 1 ELSE 0 END) AS defaites,
            SUM(r.butsmarques) AS buts_pour,
            SUM(r.butsconcedes) AS buts_contre,
            (SUM(r.butsmarques) - SUM(r.butsconcedes)) AS difference_buts,
            SUM(CASE 
                WHEN r.resultat = 'W' THEN 3 
                WHEN r.resultat = 'D' THEN 1 
                ELSE 0 
            END) AS points
        FROM resultatmatch r
        JOIN equipe e ON r.idequipe = e.idequipe
        GROUP BY e.nomequipe
        ORDER BY points DESC, difference_buts DESC, buts_pour DESC;
    """
    return run_query(query)

def get_top_scorers_per_team():
    query = """
        WITH RankedScorers AS (
            SELECT
                j.nomjoueur,
                e.nomequipe,
                s.buts,
                ROW_NUMBER() OVER(PARTITION BY e.nomequipe ORDER BY s.buts DESC) as rn
            FROM statistiquejoueur s
            JOIN joueur j ON s.idjoueur = j.idjoueur
            JOIN equipe e ON j.id_equipe = e.idequipe
            WHERE s.buts > 0
        )
        SELECT nomequipe, nomjoueur, buts
        FROM RankedScorers
        WHERE rn = 1
        ORDER BY nomequipe;
    """
    return run_query(query)

# --- 3. FONCTION UTILITAIRE POUR LE TÉLÉCHARGEMENT CSV ---
@st.cache_data
def convert_df_to_csv(df):
    return df.to_csv(index=False).encode('utf-8')

# --- 4. CONSTRUCTION DU DASHBOARD STREAMLIT ---

st.set_page_config(page_title="Dashboard Premier League 2024-2025", layout="wide")

st.title("⚽ Dashboard d'Analyse - Premier League 2024-2025")
st.markdown("Analyse des performances des équipes et des joueurs.")

# --- Section 1: Classement de la ligue ---
st.header("Classement de la Ligue")
league_table_df = get_league_table()
st.dataframe(league_table_df.style.format({"moyenne_marques": "{:.2f}", "moyenne_concedes": "{:.2f}"}))
st.download_button(
    label="📥 Télécharger le classement en CSV",
    data=convert_df_to_csv(league_table_df),
    file_name="classement_pl_2024_2025.csv",
    mime="text/csv",
)

# --- Section 2: Statistiques des Équipes ---
st.header("Statistiques Collectives")
team_stats_df = get_team_goal_stats()

col1, col2 = st.columns(2)

with col1:
    st.subheader("Puissance Offensive (Total Buts Marqués)")
    fig_goals_for = px.bar(team_stats_df, x='nomequipe', y='buts_marques', title="Total des Buts Marqués par Équipe", labels={'nomequipe': 'Équipe', 'buts_marques': 'Buts Marqués'})
    st.plotly_chart(fig_goals_for, use_container_width=True)

with col2:
    st.subheader("Solidité Défensive (Total Buts Concédés)")
    fig_goals_against = px.bar(team_stats_df.sort_values('buts_concedes'), x='nomequipe', y='buts_concedes', title="Total des Buts Concédés par Équipe", labels={'nomequipe': 'Équipe', 'buts_concedes': 'Buts Concédés'})
    st.plotly_chart(fig_goals_against, use_container_width=True)

st.subheader("Tableau détaillé des statistiques par équipe")
st.dataframe(team_stats_df.style.format({"moyenne_marques": "{:.2f}", "moyenne_concedes": "{:.2f}"}))
st.download_button(
    label="📥 Télécharger les stats des équipes en CSV",
    data=convert_df_to_csv(team_stats_df),
    file_name="stats_equipes.csv",
    mime="text/csv",
)

# --- Section 3: Statistiques des Joueurs ---
st.header("Statistiques Individuelles des Joueurs")

tab1, tab2, tab3 = st.tabs(["🏆 Meilleurs Buteurs", "🎯 Joueurs Décisifs", "🟨 Discipline"])

with tab1:
    st.subheader("Top 10 des Meilleurs Buteurs")
    top_scorers_df = get_top_scorers()
    fig_top_scorers = px.bar(top_scorers_df, x='nomjoueur', y='buts', color='nomequipe', title="Top 10 Buteurs", labels={'nomjoueur': 'Joueur', 'buts': 'Buts', 'nomequipe': 'Équipe'})
    st.plotly_chart(fig_top_scorers, use_container_width=True)
    st.dataframe(top_scorers_df)
    st.download_button("📥 Télécharger les meilleurs buteurs", convert_df_to_csv(top_scorers_df), "top_buteurs.csv", "text/csv")


with tab2:
    st.subheader("Top 10 des Joueurs les Plus Décisifs (Buts + Passes)")
    decisive_players_df = get_most_decisive_players()
    fig_decisive = px.bar(decisive_players_df, x='nomjoueur', y='Buts + Passes D.', color='nomequipe', title="Top 10 Joueurs Décisifs", labels={'nomjoueur': 'Joueur', 'Buts + Passes D.': 'Total Décisif', 'nomequipe': 'Équipe'})
    st.plotly_chart(fig_decisive, use_container_width=True)
    st.dataframe(decisive_players_df)
    st.download_button("📥 Télécharger les joueurs décisifs", convert_df_to_csv(decisive_players_df), "joueurs_decisifs.csv", "text/csv")


with tab3:
    st.subheader("Statistiques Disciplinaires")
    discipline_df = get_discipline_stats()
    st.dataframe(discipline_df)
    st.download_button("📥 Télécharger les stats disciplinaires", convert_df_to_csv(discipline_df), "discipline.csv", "text/csv")


# --- Section 4: Analyse par Équipe (avec filtres interactifs) ---
st.header("Analyse Détaillée par Équipe")
all_teams = team_stats_df['nomequipe'].unique()
selected_team = st.selectbox("Sélectionne une équipe pour voir les détails", all_teams)

if selected_team:
    # Meilleur buteur de l'équipe sélectionnée
    st.subheader(f"Meilleur Buteur pour {selected_team}")
    top_scorers_team_df = get_top_scorers_per_team()
    st.dataframe(top_scorers_team_df[top_scorers_team_df['nomequipe'] == selected_team])

    # Répartition des nationalités
    st.subheader(f"Répartition des Nationalités des Joueurs de {selected_team}")
    nationality_df = get_team_nationality_distribution()
    team_nationality_df = nationality_df[nationality_df['nomequipe'] == selected_team]
    
    fig_nat = px.pie(team_nationality_df, names='nationalite', values='nombre_joueurs', title=f"Origine des joueurs de {selected_team}")
    st.plotly_chart(fig_nat, use_container_width=True)
    
    st.dataframe(team_nationality_df)
    st.download_button(
        f"📥 Télécharger les nationalités pour {selected_team}",
        convert_df_to_csv(team_nationality_df),
        f"nationalites_{selected_team.replace(' ', '_')}.csv",
        "text/csv"
    )