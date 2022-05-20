import psycopg2 as psy
import pandas as pd
import getpass as gp

co = None

try:
    co =  psy.connect(host='berlin',
                      database='dbsaeafjv',
                      user=gp.getuser(),
                      password=gp.getpass('Password: '))
    
    # requête numéro 1 : (répartition des genres dans le top)

    datafr = pd.read_sql('''DROP TABLE IF EXISTS artiste,musique,topspot CASCADE;
                            ''', con=co)

except (Exception, psy.DatabaseError) as error:
    print(error)

finally:
    if co is not None:
        co.close()