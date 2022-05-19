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
    
# requête numéro 1 : (pourcentage dans les top selon le genre)

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

# requête numéro 2 : (quels genre a le plus de dancabilité - pop dance ?)

    datafr2 = pd.read_sql('''
                        SELECT DISTINCT genre, sum(dnce) AS sumdnce
                        FROM Musique
                        GROUP BY genre
                        ORDER BY sum(dnce) DESC
                        FETCH FIRST 5 ROWS ONLY;
                        ''', con=co)

    fig2=datafr2.plot(x='genre',y='sumdnce', legend=False)
    fig2.set_xticks(datafr2.index)
    fig2.set_xticklabels(datafr2['genre'], rotation=65, fontsize=10)
    fig2.set_xlabel('Genre')
    fig2.set_ylabel('Somme dansabilté')
    plt.show()

# requête numero 3 : (top des artistes qui font de la pop dance)

    datafr3 = pd.read_sql('''
                            SELECT a.artist, count(*) AS nbtop 
                            FROM Musique m, Artiste a, TopSpot t
                            WHERE a.Id=t.IdArtiste AND m.Id=t.IdMusique AND m.genre='dance pop'
                            GROUP BY a.artist
                            ORDER BY count(*) DESC
                            FETCH FIRST 5 ROWS ONLY;
                            ''', con=co)
    
    fig3=datafr3.plot(x='artist',y='nbtop', legend=False)
    fig3.set_xticks(datafr3.index)
    fig3.set_xticklabels(datafr3['artist'], fontsize=10)
    fig3.set_xlabel('Artiste : ')
    fig3.set_ylabel('Nombre de fois dans le top spotify (genre = pop dance):')
    plt.show()

# requête numero 4 : (sert a rien mais on c jamais mgl)

    datafr4 = pd.read_sql('''
                        SELECT a.artist, count(*) AS nbart 
                        FROM Musique m, Artiste a, TopSpot t
                        WHERE a.Id=t.IdArtiste AND m.Id=t.IdMusique
                        GROUP BY a.artist
                        ORDER BY count(*) DESC
                        FETCH FIRST 5 ROWS ONLY;
                        ''', con=co)

    fig4=datafr4.plot(x='artist',y='nbart', legend=False)
    fig4.set_xticks(datafr4.index)
    fig4.set_xticklabels(datafr4['artist'], fontsize=10)
    fig4.set_xlabel('Artiste : ')
    fig4.set_ylabel('Nombre de fois dans le top spotify (tout les genres):')
    plt.show()

# requête numero 5 : (moyenne temps de musique par année)

    datafr5 = pd.read_sql('''
                            SELECT t.year, ((sum(m.dur)/count(m.*))/60) AS tmpmoy
                            FROM Musique m, TopSpot t
                            WHERE t.IdMusique=m.Id
                            GROUP BY t.year
                            ORDER BY year ASC;
                        ''', con=co)

    fig5=datafr5.plot(x='year',y='tmpmoy', kind='bar', legend=False)
    fig5.set_xticklabels(datafr5['year'], rotation=0,fontsize=10) 
    fig5.set_xlabel('Années : ')
    fig5.set_ylabel('Temps moyen des musiques:(en min)')
    plt.show()

# requête numero 6 : (classe les artistes par popularité meilleur)

    datafr6 = pd.read_sql('''
                            SELECT a.artist, t.pop
                            FROM Artiste a, TopSpot t
                            WHERE a.Id=t.IdArtiste
                            GROUP BY a.artist, t.pop
                            ORDER BY t.pop DESC
                            FETCH FIRST 10 ROWS ONLY;
                        ''', con=co)

    fig6=datafr6.plot(x='artist',y='pop', kind='bar'  ,legend=False)
    fig6.set_xticklabels(datafr6['artist'], rotation=70,fontsize=10) 
    fig6.set_xlabel('Nom de l artiste : ')
    fig6.set_ylabel('Popularité de l artiste : ')
    plt.show()

# requête numero 7 : (classe les artistes par popularité somme)

    datafr7 = pd.read_sql('''
                            SELECT a.artist, sum(t.pop) AS sumpop
                            FROM Artiste a, TopSpot t
                            WHERE a.Id=t.IdArtiste
                            GROUP BY a.artist
                            ORDER BY sum(t.pop) DESC
                            FETCH FIRST 10 ROWS ONLY;
                        ''', con=co)

    fig7=datafr7.plot(x='artist',y='sumpop', kind='bar', legend=False)
    fig7.set_xticklabels(datafr7['artist'], rotation=70,fontsize=10) 
    fig7.set_xlabel('Nom de l artiste : ')
    fig7.set_ylabel('Somme popularité de l artiste : ')
    plt.show()

# requête numero 8 : ()

    datafr8 = pd.read_sql('''
                            
                        ''', con=co)
    datafr88=datafr8.transpose()
    print(datafr88)
    fig8=datafr88.plot(y=0 ,kind='pie',autopct='%1.0f%%')
    plt.show()

# requête numero 9 : (speechless)





except (Exception, psy.DatabaseError) as error:
    print(error)

finally:
    if co is not None:
        co.close()