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
            id char(4) PRIMARY KEY,
            nom varchar(100)
        );'''
    )

    curs.execute('''
        CREATE TABLE Musique(
            id char(4) PRIMARY KEY,
            titre varchar(400),
            genre varchar(100),
            bpm numeric,
            energie numeric,
            danse numeric,
            dB smallint,
            live numeric,
            ambiance numeric,
            duree numeric,
            acoustique numeric,
            texte numeric
        );'''
    )

    curs.execute('''
        CREATE TABLE TopSpot(
            IdArtiste char(4) REFERENCES artiste(id),
            idMusique char(4) REFERENCES musique(id),
            annee numeric(4),
            popularite numeric,
            PRIMARY KEY (idArtiste, idMusique, annee, popularite)
        );'''
    )
    #a completer

    # Insertion des valeurs
    A = 'A00'
    i = 1
    for row in df.itertuples():
        if i > 9:
            A = 'A0'
        if i > 99:
            A = 'A'
        idArt = A+str(i)
        curs.execute('''
            INSERT INTO Artiste(id, nom) 
            VALUES (%s,%s);''',
                (idArt, row.artist,) 
        )
        i += 1

    M = 'M00'
    j = 1
    for row in df.itertuples():
        if j > 9:
            M = 'M0'
        if j > 99:
            M = 'M'

        idMus = M+str(j)
        curs.execute('''
            INSERT INTO Musique(id, titre, genre, bpm, energie, danse, dB, live, ambiance, duree, acoustique, texte)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                (idMus, row.title, row.topgenre, row.bpm, row.nrgy, row.dnce, row.dB, row.live, row.val, row.dur, row.acous, row.spch)
        )
        j += 1

    M = 'M00'
    A = 'A00'
    k = 1
    for row in df.itertuples():
        if k > 9:
            M = 'M0'
            A = 'A0'
        if k > 99:
            M = 'M'
            A = 'A'
        idMus = M+str(k)
        idArt = A+str(k)
        curs.execute('''
            INSERT INTO topSpot (idArtiste, idMusique, annee, popularite)
            VALUES (%s,%s,%s,%s)''',
            (idArt, idMus, row.year, row.pop)
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