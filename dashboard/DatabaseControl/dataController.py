import psycopg2
import pandas as pd
import re
from decimal import Decimal
from configparser import ConfigParser

def load_config(filename='database.ini', section='postgresql'):
    parser = ConfigParser()
    parser.read(filename)

    # get section, default to postgresql
    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return config

# Create a list of tupples from the dataframe values
def getFullDataframe(table):
    # SQL quert to execute
    columnquery  = "SELECT * FROM information_schema.columns WHERE table_schema = 'public' and table_name = '"+table +"'"
    dataquery = "SELECT * FROM "+table
    legendequery = "SELECT pg_catalog.obj_description('"+table+"'::regclass, 'pg_class');"
    try:
        config = load_config()
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                cur.execute(columnquery)
                tableschema = cur.fetchall()
                cur.execute(dataquery)
                data = cur.fetchall()
                cur.execute(legendequery)
                legende = cur.fetchall()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cur.close()
        return 1
    cur.close()
    
    columnnames = []
    for i in range(len(tableschema)):
        if tableschema[i][3][0] == "t":
            columnnames.append(tableschema[i][3][1:]) 
        else:
            columnnames.append(tableschema[i][3])
    dataframe = pd.DataFrame(data,columns=columnnames)

    dataframe = dataframe.replace(Decimal('-999.9'), None)
    dataframe.attrs["legende"] = legende[0][0].split(",")
    return dataframe
    
if __name__ == '__main__':
    dataframe = getFullDataframe("investment")
    print(dataframe.attrs["legende"])
    config = load_config()
    print(config)