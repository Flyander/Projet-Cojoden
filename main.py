import os
import pandas as pd # pip install pandas
import numpy as np # pip install numpy
from dotenv import load_dotenv # pip install python-dotenv
from functions.sql_wrapper import SQL_connector
    

def extract_data_from_csv(list_columns, csv_file):
    df = pd.read_csv(csv_file, sep=";", usecols=list_columns)
    return df

def add_data_to_db(df):
    sql = SQL_connector(os.getenv('DB_HOST'), os.getenv('DB_USERNAME'), os.getenv('DB_PASSWORD'), "joconde")
    sql.connect()
    df = df.replace({np.nan: None})
    for _, row in df.iterrows():
        if row["POP_COORDONNEES"] == "NULL":
            latitude, longitude = "NULL", "NULL"
        else:
            latitude, longitude = row["POP_COORDONNEES"].split(",")

        # Check if the row already exists in the database
        sql.execute('SELECT * FROM Ville WHERE nom = "{}"'.format(row["Ville_"]))
        sql_result = sql.fetchall()
        if len(sql_result) == 0:
            # sql.execute('INSERT INTO Ville(nom, longitude, latitude, pays, departement, region) VALUES ("{}", {}, {}, "{}", "{}", "{}")'.format(
            #     row["Ville_"], longitude, latitude, row["Ecole"], row["DPT"], row["REGION"]))

            # sql.execute('SELECT id_ville FROM Ville WHERE nom = "{}"'.format(row["Ville_"]))
            # id_ville_actuel = sql.fetchall()

            # sql.execute('''INSERT INTO Oeuvre(titre, domaine, description, dimensions, epoque, geographie_hist, lieu_creation, lieu_de_conservation, materiaux_techniques, id_ville)
            # VALUES ("{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", {})'''.format(
            #     row["Titre"], row["Domaine"], row["DESC"], row["Dimensions"], row["Epoque"], row["Géographie historique"], row["Lieu de création"],
            #     row["Lieu de conservation"], row["Matériaux-techniques"], id_ville_actuel)
            # )

            print("INSERT INTO Ville(nom, longitude, latitude, pays, departement, region) VALUES ({}, {}, {}, {}, {}, {})".format(
                row["Ville_"], latitude, longitude, row["Ecole"], row["DPT"], row["REGION"]))

    sql.close()

def test():
    list_columns = [
        'Auteur',
        'Titre',
        'Domaine',
        'DESC',
        'Dimensions', 
        'DPT',
        'Ecole',
        'Epoque',
        'Matériaux-techniques',
        'Géographie historique',
        'Lieu de création',
        'Lieu de conservation',
        "Précisions sur l'auteur",
        'POP_COORDONNEES',
        'Ville_',
        'REGION'
    ]
    csv_file = 'data/base-joconde.csv'
    df = extract_data_from_csv(list_columns, csv_file)
    add_data_to_db(df)
    # sql = SQL_connector("localhost", "root", "test123", "joconde")
    # sql.connect()
    # sql.execute("SELECT * FROM Ville")
    # print(sql.fetchall())
    # sql.close()



def main():
    print("Hello World")




if __name__ == "__main__":
    load_dotenv()
    test()