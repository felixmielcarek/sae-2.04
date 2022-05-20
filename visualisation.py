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

    datafr = pd.read_sql('''SELECT  count(*) AS pourcentage, genre
                            FROM Musique
                            GROUP BY (genre);
                            ''', con=co)

    fig=datafr.plot(x='genre',y='pourcentage', legend=False)
    fig.set_xticks(datafr.index)
    fig.set_xticklabels(datafr['genre'], rotation=90, fontsize=7)
    fig.set_xlabel('Genre')
    fig.set_ylabel('Pourcentage dans le top 50')
    fig.set_xlim(0,50)
    fig.set_ylim(0,350)
    plt.show()

# requête numéro 2 : (quels genre a le plus de dancabilité)

    datafr2 = pd.read_sql('''
                        SELECT DISTINCT genre, sum(danse) AS sumdnce
                        FROM Musique
                        GROUP BY genre
                        ORDER BY sum(danse) DESC
                        FETCH FIRST 5 ROWS ONLY;
                        ''', con=co)

    fig2=datafr2.plot(x='genre',y='sumdnce', legend=False)
    fig2.set_xticks(datafr2.index)
    fig2.set_xticklabels(datafr2['genre'], rotation=0, fontsize=10)
    fig2.set_xlabel('Genre')
    fig2.set_ylabel('Somme dansabilté')
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

# requetes numero 10 : prendre le son le plus populaire et le comparer avec la moeynne

    datafr11 = pd.read_sql('''
                           SELECT (sum(m1.danse)/count(m1.danse))  as danse, (sum(m2.danse)/count(m2.danse)) as moydanse
                           FROM Musique m1, Musique m2, TopSpot t
                           WHERE m1.id=t.idmusique AND t.popularite >= ALL (SELECT t.popularite FROM Musique m, TopSpot t WHERE m.id=t.idmusique);
                        ''', con=co)

    fig11=datafr11.plot(x='titre',y='moydanse', kind='bar', legend=False)
    fig11.set_xticklabels(datafr11['titre'], rotation=0,fontsize=10) 
    fig11.set_xlabel('Titre : ')
    fig11.set_ylabel('Statisitques :')
    plt.show()

# requête numero 11 : (repartition des 4 styles les plus populaire en fonction de leur capacité a etre fait en live)

    datafr10 = pd.read_sql('''
                            SELECT sum(m1.live)/count(m1.live) as livedancepop, sum(m2.live)/count(m2.live) AS livepop, sum(m3.live)/count(m3.live) AS livecanadianpop, sum(m4.live)/count(m4.live) as liveboyband
                            FROM Musique m1, Musique m2, Musique m3, Musique m4
                            WHERE m1.genre='dance pop' AND m2.genre='pop' AND m3.genre='canadian pop' AND m4.genre='boy band';
                        ''', con=co)
    datafr100=datafr10.transpose()
    print(datafr100)
    fig10=datafr100.plot(y=0 ,kind='pie',autopct='%1.0f%%')
    fig.legend(['livedancepop','livePop','liveCanadaPop','liveBoyBand'])
    fig.set_ylabel('')
    plt.show()

except (Exception, psy.DatabaseError) as error:
    print(error)

finally:
    if co is not None:
        co.close()