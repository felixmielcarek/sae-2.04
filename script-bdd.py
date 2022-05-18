# Début du script

# Pour connexion
from operator import index
from tkinter import W
import psycopg2 as psy
import getpass as gp
# Pour script
import pandas as pd

### MODIFIER LE CSV --- TOP GENRE

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
    curs.execute('''DROP TABLE IF EXISTS Artiste CASCADE''')
    curs.execute('''DROP TABLE IF EXISTS Musique CASCADE''')
    curs.execute('''DROP TABLE IF EXISTS TopSpot CASCADE''')

    curs.execute('''
        CREATE TABLE Artiste(
            id numeric PRIMARY KEY,
            artist varchar(100)
        );'''
    )

    curs.execute('''
        CREATE TABLE Musique(
            id numeric PRIMARY KEY,
            title varchar(400),
            genre varchar(100),
            bpm numeric,
            nrgy numeric,
            dnce numeric,
            dB smallint,
            live numeric,
            val numeric,
            dur numeric,
            acous numeric,
            spch numeric
        );'''
    )


    curs.execute('''
        CREATE TABLE TopSpot(
            IdArtiste numeric REFERENCES artiste(id),
            idMusique numeric REFERENCES musique(id),
            year numeric(4),
            pop numeric,
            PRIMARY KEY (idArtiste, idMusique, year, pop)
        );'''
    )
    #a completer

    # Insertion des valeurs
    i = 1
    for row in df.itertuples():
        curs.execute('''
            INSERT INTO Artiste(id, artist) 
            VALUES (%s,%s);''',
                (i, row.artist,) 
        )
        i += 1

    j = 1
    for row in df.itertuples():
        curs.execute('''
            INSERT INTO Musique(id, title, genre, bpm, nrgy, dnce, dB, live, val, dur, acous, spch)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                (j, row.title, row.topgenre, row.bpm, row.nrgy, row.dnce, row.dB, row.live, row.val, row.dur, row.acous, row.spch)
        )
        j += 1

    k = 1
    for row in df.itertuples():
        curs.execute('''
            INSERT INTO topSpot (idArtiste, idMusique, year, pop)
            VALUES (%s,%s,%s,%s)''',
            (k, k, row.year, row.pop)
        )
        k += 1
    
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
    curs.execute('''SELECT count(*) FROM Artiste''')
    res=curs.fetchall()
    print(res)

    curs.execute('''SELECT count(*) FROM Musique''')
    res=curs.fetchall()
    print(res)

    curs.execute('''SELECT count(*) FROM topSpot''')
    res=curs.fetchall()
    print(res)

    curs.execute('''SELECT a.id AS Id_Artiste, a.artist, m.id AS Id_Musique, m.title, t.idArtiste AS id_Art_Top, t.idMusique AS id_Mus_Top
                    FROM Artiste a, Musique m, topSpot t
                    WHERE a.id=1 AND m.id=1 AND t.idArtiste=a.id AND t.idMusique=m.id;    
                ''')
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