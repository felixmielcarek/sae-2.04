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
 
    datafr = pd.read_sql('''SELECT  sum(genre) AS pourcentage, genre
                            FROM Musique
                            GROUP BY genre;
                            ''', con=co)

    fir=datafr.plot(y='pourcentage', kind='pie', labels=datafr['genre'], legend=False)
    fir.set_ylabel("genre")
    plt.show()

except (Exception, psy.DatabaseError) as error:
    print(error)

finally:
    if co is not None:
        co.close()