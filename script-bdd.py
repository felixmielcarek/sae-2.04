# Début du script

# Pour connexion
from operator import index
from tkinter import W
import psycopg2 as psy
import getpass as gp
# Pour script
import pandas as pd
import matplotlib.pyplot as plt

co=None

# Code dans le try est executé
try:
    # Connecte à la base
    co=psy.connect(
        host='berlin',
        database='dbsaeafjv',
        user=gp.getuser(),
        password=gp.getpass('Password: ')
    )

    # Lis la BDD
    data=pd.read_csv(r'top10s.csv')
    df=pd.DataFrame(data)

    # Nettoie les données
    df=df.drop_duplicates()
    #a completer

    # Affiche les données
    print('\n')
    print(df)
    print('\n')

    # Création du curseur pour impacter la BDD
    curs=co.cursor()

    # Création de la table
    # curs.execute('''DROP TABLE IF EXISTS TopSpot;''')
    # curs.execute('''
    #     CREATE TABLE TopSpot(
            
    #     );'''
    # )
    #a completer

    # Insertion des valeurs
    # for row in df.itertuples():
    #     curs.execute('''
    #         INSERT INTO TopSpot 
    #         VALUES ();'''
    #             ,() 
    #     )
    #a completer

    # Récupération de la BDD màj
    df2=pd.read_sql('''SELECT * FROM topspot;''',con=co)
  
    # Commandes pour les questions de la SAE
    # test=pd.read_sql('''
    #     select
    #     from topspot
    #     where'
    #     ''',con=co
    # )
    #a completer

    # Fermeture du curseur
    co.commit()
    curs.close()


# Code dans le except est executé SI condition d'erreur respecté
except(Exception,psy.DatabaseError) as error:
    print(error)

# Code executé à la fin dans tous les cas
finally:
    if co is not None:
        co.close()