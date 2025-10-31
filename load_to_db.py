import pandas as pd
from sqlalchemy import create_engine, text
import os
import glob

DB_USER = "postgres"
DB_PASSWORD = "password"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "Footballistiques" 

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)


SILVER_PATH = './Silver'

def get_mappings(connection):
   
    competitions_map = pd.read_sql("SELECT idcompetition, nomcompetition FROM competition", connection).set_index('nomcompetition').to_dict()['idcompetition']
    saisons_map = pd.read_sql("SELECT id_saison, annee FROM saison", connection).set_index('annee').to_dict()['id_saison']
    equipes_map = pd.read_sql("SELECT idequipe, nomequipe FROM equipe", connection).set_index('nomequipe').to_dict()['idequipe']
    return competitions_map, saisons_map, equipes_map

def populate_database():
    try:
        with engine.connect() as connection:
            print("Connexion à la base de données réussie.")
            
        
            print("Peuplement initial des tables de dimensions (saison)...")
            saison_annee = "2024-2025"
            connection.execute(text("INSERT INTO saison (annee) VALUES (:annee) ON CONFLICT (annee) DO NOTHING;"), {"annee": saison_annee})

           
            print("\n--- Traitement des fichiers _standard.csv ---")
            standard_files = glob.glob(os.path.join(SILVER_PATH, '*_standard.csv'))

            for filepath in standard_files:
                filename = os.path.basename(filepath)
                team_name = filename.replace('_standard.csv', '').replace('_', ' ')
                print(f"  -> Fichier : {filename} (Équipe : {team_name})")

               
                connection.execute(text("INSERT INTO equipe (nomequipe) VALUES (:team) ON CONFLICT (nomequipe) DO NOTHING;"), {"team": team_name})
                
               
                _, _, equipes_map = get_mappings(connection)
                id_equipe = equipes_map[team_name]

                player_df = pd.read_csv(filepath)
                for _, row in player_df.iterrows():
                    
                    joueur_insert_query = text("""
                        INSERT INTO joueur (nomjoueur, position, nationalite, id_equipe)
                        VALUES (:nom, :pos, :nat, :id_eq)
                        RETURNING idjoueur;
                    """)
                    result = connection.execute(joueur_insert_query, {
                        "nom": row['Player'], "pos": row['Pos'], "nat": row['Nation'], "id_eq": id_equipe
                    })
                    joueur_id = result.scalar()

                    
                    stats_insert_query = text("""
                        INSERT INTO statistiquejoueur (idjoueur, buts, passesdecisives, nbmatchesplayed, cartonsjaunes, cartonsrouges)
                        VALUES (:id_j, :buts, :passes, :nbmatch, :cj, :cr);
                    """)
                    connection.execute(stats_insert_query, {
                        "id_j": joueur_id, "buts": row['Gls'], "passes": row['Ast'], "nbmatch": row['MP'], "cj": row['CrdY'], "cr": row['CrdR']
                    })

            print("Peuplement des joueurs et de leurs statistiques terminé.")

           
            print("\n--- Traitement des fichiers _matchlogs.csv ---")
            matchlog_files = glob.glob(os.path.join(SILVER_PATH, '*_matchlogs.csv'))

            for filepath in matchlog_files:
                filename = os.path.basename(filepath)
                current_team_name = filename.replace('_matchlogs.csv', '').replace('_', ' ')
                print(f"  -> Fichier : {filename} (Perspective : {current_team_name})")

                match_df = pd.read_csv(filepath)
                
                
                for comp in match_df['Comp'].unique():
                    connection.execute(text("INSERT INTO competition (nomcompetition) VALUES (:comp) ON CONFLICT (nomcompetition) DO NOTHING;"), {"comp": comp})
                for team in match_df['Opponent'].unique():
                    connection.execute(text("INSERT INTO equipe (nomequipe) VALUES (:team) ON CONFLICT (nomequipe) DO NOTHING;"), {"team": team})

                competitions_map, saisons_map, equipes_map = get_mappings(connection)
                saison_id = saisons_map[saison_annee]

                for _, row in match_df.iterrows():
                    if pd.isna(row['Date']):
                        continue

                    equipe1_id = equipes_map.get(current_team_name)
                    equipe2_id = equipes_map.get(row['Opponent'])

                    id_domicile = min(equipe1_id, equipe2_id)
                    id_exterieur = max(equipe1_id, equipe2_id)
                    
                    if row['Venue'] == 'Home':
                        venue_reel = 'Home'
                    elif row['Venue'] == 'Away':
                        venue_reel = 'Away'
                    else: 
                        venue_reel = 'Neutral'
                    
                    if equipe1_id != id_domicile:
                        venue_reel = 'Away' if venue_reel == 'Home' else 'Home'

                    match_insert_query = text("""
                        INSERT INTO match (date_match, heure, round, venue, idteamhome, idteam__away, id_competition, id_saison)
                        VALUES (:date, :heure, :round, :venue, :id_home, :id_away, :id_comp, :id_saison)
                        ON CONFLICT (date_match, idteamhome, idteam__away) DO NOTHING;
                    """)
                    connection.execute(match_insert_query, {
                        "date": pd.to_datetime(row['Date']).date(),
                        "heure": pd.to_datetime(row['Time'], format='%H:%M').time(),
                        "round": row['Round'],
                        "venue": venue_reel, 
                        "id_home": id_domicile,
                        "id_away": id_exterieur,
                        "id_comp": competitions_map.get(row['Comp']),
                        "id_saison": saison_id
                    })

                    select_match_id_query = text("""
                        SELECT idmatch_ FROM match 
                        WHERE date_match = :date AND idteamhome = :id_home AND idteam__away = :id_away;
                    """)
                    match_id = connection.execute(select_match_id_query, {
                        "date": pd.to_datetime(row['Date']).date(), "id_home": id_domicile, "id_away": id_exterieur
                    }).scalar()

                    if match_id:
                        resultat_insert_query = text("""
                            INSERT INTO resultatmatch (idmatch, idequipe, butsmarques, butsconcedes, resultat)
                            VALUES (:id_m, :id_eq, :bm, :bc, :res)
                            ON CONFLICT DO NOTHING; -- Évite les doublons si le script est relancé
                        """)
                        connection.execute(resultat_insert_query, {
                            "id_m": match_id,
                            "id_eq": equipe1_id,
                            "bm": row['GF'],
                            "bc": row['GA'],
                            "res": row['Result']
                        })
            
            connection.commit()
            print("Peuplement des matchs et résultats terminé.")
        
        print("\n--- La base de données a été peuplée avec succès ! ---")

    except Exception as e:
        print(f"\nUNE ERREUR EST SURVENUE : {e}")
        print("Le processus a été interrompu. La transaction a été annulée (rollback).")

if __name__ == "__main__":
    populate_database()