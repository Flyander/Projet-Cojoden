import mysql.connector as mysql # pip install mysql-connector-python
import pandas as pd # pip install pandas
import numpy as np # pip install numpy
from dotenv import load_dotenv # pip install python-dotenv

class SQL_connector:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        self.cursor = None

    def connect(self):
        self.connection = mysql.connect(host=self.host, user=self.user, password=self.password, database=self.database)
        self.cursor = self.connection.cursor()

    def close(self):
        self.cursor.close()
        self.connection.close()

    def execute(self, sql):
        self.cursor.execute(sql)

    def fetchall(self):
        return self.cursor.fetchall()
    

def extract_data_from_csv(list_columns, csv_file):
    df = pd.read_csv(csv_file, sep=";", usecols=list_columns)
    return df

def add_data_to_db(df):
    sql = SQL_connector("localhost", "root", "test123", "joconde")
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
    test()