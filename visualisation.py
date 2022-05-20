from tkinter import W
import pandas as pd
import psycopg2 as psy
import matplotlib.pyplot as plt
import getpass as gp

co = None

try:
    co =  psy.connect(host='berlin',
                      database='dbsaeafjv',
                      user=gp.getuser(),
                      password=gp.getpass('Password: '))
    
    # requête numéro 1 : (répartition des genres dans le top)

    datafr = pd.read_sql('''SELECT  count(*)*100/(SELECT count(m2.id) FROM musique m2) AS pourcentage, genre
                            FROM Musique
                            GROUP BY genre;
                            ''', con=co)

    fig=datafr.plot(y='pourcentage', x='genre', legend=False)
    fig.set_ylabel('Pourcentage dans le top 50')
    fig.set_xticks(datafr.index)
    fig.set_xticklabels(datafr['genre'], rotation=90)
    fig.set_ylim(0,60)
    fig.set_xlim(0,50)
    plt.show()

    # requête numero 3 : (top des artistes qui font de la pop dance)

    datafr3 = pd.read_sql('''
                            SELECT a.nom, count(*) AS nbtop 
                            FROM Musique m, Artiste a, TopSpot t
                            WHERE a.Id=t.IdArtiste AND m.Id=t.IdMusique AND m.genre='dance pop'
                            GROUP BY a.nom
                            ORDER BY count(*) DESC
                            FETCH FIRST 5 ROWS ONLY;
                            ''', con=co)
    
    fig3=datafr3.plot(x='nom',y='nbtop', legend=False)
    fig3.set_xticks(datafr3.index)
    fig3.set_xticklabels(datafr3['nom'], fontsize=10)
    fig3.set_xlabel('Artiste : ')
    fig3.set_ylabel('Nombre de fois dans le top spotify (genre = pop dance):')
    plt.show()

    # requête numero 5 : (moyenne temps de musique par année)

    datafr5 = pd.read_sql('''
                            SELECT t.annee, ((sum(m.duree)/count(m.*))/60) AS tmpmoy
                            FROM Musique m, TopSpot t
                            WHERE t.IdMusique=m.Id
                            GROUP BY t.annee
                            ORDER BY annee ASC;
                        ''', con=co)

    fig5=datafr5.plot(x='annee',y='tmpmoy', kind='bar', legend=False)
    fig5.set_xticklabels(datafr5['annee'], rotation=0,fontsize=10) 
    fig5.set_xlabel('Années : ')
    fig5.set_ylabel('Temps moyen des musiques:(en min)')
    plt.show()

    # requête numero 6 : (classe les artistes par popularité meilleur)

    datafr6 = pd.read_sql('''
                            SELECT a.nom, t.popularite
                            FROM Artiste a, TopSpot t
                            WHERE a.Id=t.IdArtiste
                            GROUP BY a.nom, t.popularite
                            ORDER BY t.popularite DESC
                            FETCH FIRST 10 ROWS ONLY;
                        ''', con=co)

    fig6=datafr6.plot(x='nom',y='popularite', kind='bar'  ,legend=False)
    fig6.set_xticklabels(datafr6['nom'], rotation=70,fontsize=10) 
    fig6.set_xlabel('Nom de l artiste : ')
    fig6.set_ylabel('Popularité de l artiste : ')
    plt.show()

    # requête numero 7 : (classe les artistes par popularité somme)

    datafr7 = pd.read_sql('''
                            SELECT a.nom, sum(t.popularite) AS sumpop
                            FROM Artiste a, TopSpot t
                            WHERE a.Id=t.IdArtiste
                            GROUP BY a.nom
                            ORDER BY sum(t.popularite) DESC
                            FETCH FIRST 10 ROWS ONLY;
                        ''', con=co)

    fig7=datafr7.plot(x='nom',y='sumpop', kind='bar', legend=False)
    fig7.set_xticklabels(datafr7['nom'], rotation=70,fontsize=10) 
    fig7.set_xlabel('Nom de l artiste : ')
    fig7.set_ylabel('Somme popularité de l artiste : ')
    plt.show()

    # requete numero 7.5 : (regarde le nombre de fois qu'apparaissent le 2 plus populaire du top, pour voir si quantité est gage de qualité)

    datafr75 = pd.read_sql('''
                            SELECT a.nom, count(*) AS nbtotalapp
                            FROM Artiste a, TopSpot t
                            WHERE a.Id=t.IdArtiste
                            GROUP BY a.nom
                            ORDER BY sum(t.popularite) DESC
                            FETCH FIRST 2 ROWS ONLY;
                        ''', con=co)

    fig75=datafr75.plot(x='nom',y='nbtotalapp', kind='bar', legend=False)
    fig75.set_xticklabels(datafr75['nom'], rotation=70,fontsize=10) 
    fig75.set_xlabel('Nom de l artiste : ')
    fig75.set_ylabel('Nombre d apparition de l artiste dans le top : ')
    plt.show()

    # requête numero 8 : (genre en fonction de leur bpm)

    datafr8 = pd.read_sql('''
                            SELECT DISTINCT genre, sum(bpm)/count(bpm) AS moybpm
                            FROM Musique
                            GROUP BY genre
                            ORDER BY sum(bpm)/count(bpm) DESC
                            FETCH FIRST 5 ROWS ONLY;
                        ''', con=co)

    fig8=datafr8.plot(x='genre',y='moybpm', legend=False)
    fig8.set_xticks(datafr8.index)
    fig8.set_xticklabels(datafr8['genre'], fontsize=10)
    fig8.set_xlabel('Genre : ')
    fig8.set_ylabel('Moyenne bpm : ')
    plt.show()

    # requête numero 9 : (texte dans les musiques par années)

    datafr9 = pd.read_sql('''
                            SELECT t.annee, (sum(m.texte)/count(m.texte)) AS txtmoy
                            FROM Musique m, TopSpot t
                            WHERE t.IdMusique=m.Id
                            GROUP BY t.annee
                            ORDER BY annee ASC;
                        ''', con=co)

    fig9=datafr9.plot(x='annee',y='txtmoy', kind='bar', legend=False)
    fig9.set_xticklabels(datafr9['annee'], rotation=0,fontsize=10) 
    fig9.set_xlabel('Années : ')
    fig9.set_ylabel('Niveau de texte moyen des musiques:(sur 50)')
    plt.show()

    # requête numero 11 : (repartition des 4 styles les plus populaire en fonction de leur capacité a etre fait en live)

    datafr10 = pd.read_sql('''
                            SELECT sum(m1.live)/count(m1.live) as livedancepop, sum(m2.live)/count(m2.live) AS livepop, sum(m3.live)/count(m3.live) AS livecanadianpop, sum(m4.live)/count(m4.live) as liveboyband
                            FROM Musique m1, Musique m2, Musique m3, Musique m4
                            WHERE m1.genre='dance pop' AND m2.genre='pop' AND m3.genre='canadian pop' AND m4.genre='boy band';
                        ''', con=co)
    datafr10=datafr10.transpose()
    print(datafr10)
    fig10=datafr10.plot(y=0 ,kind='pie',autopct='%1.0f%%')
    fig10.legend(['livedancepop','livePop','liveCanadaPop','liveBoyBand'])
    fig10.set_ylabel('')
    plt.show()

    # requête numero 12 : le nombre d'apparition dans le top des genres ayant la moyenne de bpm le plus haut

    datafr12 = pd.read_sql('''
                            SELECT m1.genre AS genre,(
                                SELECT COUNT(m2.ID)
                                FROM musique m2
                                WHERE m1.genre = m2.genre
                                ) AS nbApparition
                            FROM musique m1
                            GROUP BY m1.genre
                            ORDER BY avg(m1.bpm) DESC
                            FETCH FIRST 5 ROWS ONLY;
                        ''', con=co)
    print(datafr12)
    fig12=datafr12.plot(x='genre',y='nbapparition' ,kind='bar')
    fig12.set_xticklabels(datafr12['genre'],rotation='10')
    fig12.set_ylabel('')
    fig12.set_ylim(0,603)
    plt.show()

    # requête numero 13 : moyenne de dansabilité des 5 genres les plus ecoutés

    datafr13 = pd.read_sql('''
                            SELECT m1.genre AS genre,(
                                SELECT avg(m2.danse)
                                FROM musique m2
                                WHERE m2.genre=m1.genre
                                ) AS dansant
                            FROM musique m1
                            GROUP BY m1.genre
                            ORDER BY dansant DESC;
                        ''', con=co)
    print(datafr13)
    fig13=datafr13.plot(x='genre',y='dansant',style='o--r')
    fig13.set_xticks(datafr13.index)
    fig13.set_xticklabels(datafr13['genre'], rotation=90)
    fig13.set_xlim(0,50)
    fig13.set_ylim(0,100)
    plt.show()

    # requête numero 14 : comparaison de l'energie avec les décibels

    datafr14 = pd.read_sql('''
                            SELECT db,energie
                            FROM musique
                            ORDER BY db;
                        ''', con=co)
    print(datafr14)
    fig14=datafr14.plot(y=['db','energie'],style=['.--r','.--b'])
    plt.show()


except (Exception, psy.DatabaseError) as error:
    print(error)

finally:
    if co is not None:
        co.close()