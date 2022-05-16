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

# Lis la BDD
data=pd.read_csv(r'top10s.csv', encoding='UTF-8')
df=pd.DataFrame(data)
# Nettoie les données
df=df.drop_duplicates()

# Code dans le try est executé
try:
    # Connecte à la base
    co=psy.connect(
        host='berlin',
        database='dbsaeafjv',
        user=gp.getuser(),
        password=gp.getpass('Password: ')
    )

    # Affiche les données
    print('\n')
    print(df)
    print('\n')

    # Création du curseur pour impacter la BDD
    curs=co.cursor()

    # Création de la table
    curs.execute('''DROP TABLE IF EXISTS TopSpot;''')


    curs.execute('''
        CREATE TABLE TopSpot(
            title varchar(400),
            artist varchar(100),
            genre varchar(100),
            year numeric,
            bpm numeric,
            nrgy numeric,
            dnce numeric,
            dB smallint,
            live numeric,
            val numeric,
            dur numeric,
            acous numeric,
            spch numeric,
            pop numeric,
            PRIMARY KEY (title, artist, genre, year, pop)
        );'''
    )
    #a completer

    # Insertion des valeurs
    for row in df.itertuples():
        curs.execute('''
            INSERT INTO TopSpot (title, artist, genre, year, bpm, nrgy, dnce, dB, live, val, dur, acous, spch, pop) 
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);''',
                (row.title, row.artist, 'row.top genre', row.year, row.bpm, row.nrgy, row.dnce, row.dB, row.live, row.val, row.dur, row.acous, row.spch, row.pop) 
        )
    #a completer

    curs.execute('''GRANT SELECT, INSERT, UPDATE, DELETE ON topspot TO PUBLIC''')

    # Commandes pour les questions de la SAE
    # test=pd.read_sql('''
    #     select
    #     from topspot
    #     where'
    #     ''',con=co
    # )
    #a completer
    curs.execute('''SELECT count(*) FROM TopSpot''')
    res=curs.fetchall()
    print(res)

    df2=pd.read_sql('''SELECT * FROM topspot;''',con=co)

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