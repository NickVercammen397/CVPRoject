import numpy as np
import pandas as pd
import re
import psycopg2
from dashboard.DatabaseControl.dataController import load_config

def setupEurostatDataframe(dataframe, splitName):
    splitHeaders = dataframe.columns.values[0].split(",")
    valueArrays = np.empty(shape=(len(splitHeaders),len(dataframe.index)), dtype =object)



    for index, row in dataframe.iterrows():
        rowValues = row[splitName].split(",")
        for i in range(len(valueArrays)):
            valueArrays[i][index]= rowValues[i] 

    for i in range(len(splitHeaders)):
        dataframe.insert(i,splitHeaders[i],valueArrays[i])

    dataframe = dataframe.rename(columns={'geo\\TIME_PERIOD':'geo'})
    dataframe = dataframe.drop(columns=[splitName])
    dataframe = dataframe.assign(errors=np.empty(len(dataframe.index), dtype =object))
    
    for column in dataframe:
        if re.match('^\d{4}$',column.strip()):
            dataframe = dataframe.rename(columns={column:"t"+column})

    #verwerken van foutcodes bij jaartallen naar numerieke data + errorkolom met de te behouden errorcodes
    #dit maakt mogelijk om numerieke data te behouden voor verdere berekeningen
    for index, row in dataframe.iterrows():
        for i in range(len(splitHeaders),len(dataframe.columns)-1):
            row[i] = row[i].replace(":","-999.9")

            #als data een letter bevat: verwijder letter en voeg kollomnaam + letter toe aan errors
            if re.match('.*[a-zA-Z].*',row[i].strip()):                 
                #ãls eerste error is verander lege array naar string van error
                if row["errors"] == None:
                    row["errors"] = "(" + row.index[i] + ": "+row[i].split(" ")[(len(row[i].split(" "))-1)]+"),"
                #toevoegen van errors aan string
                else:
                    row["errors"] = "(" + row.index[i] + ": "+row[i].split(" ")[(len(row[i].split(" "))-1)]+"),"
                
                #set the value of the year to only the number
                if re.match('.*[0-9].*',row[i].split(" ")[0]):
                    row[i] = row[i].split(" ")[0]
    return dataframe

def create_tables():
    commands = (
        """CREATE TABLE IF NOT EXISTS investment (
            id SERIAL PRIMARY KEY,
            freq VARCHAR(255) NOT NULL,
            unit CHAR(10) NOT NULL,
            expend VARCHAR(255) NOT NULL,
            geo VARCHAR(255) NOT NULL,
            t1992 NUMERIC(10,1),
            t1993 NUMERIC(10,1),
            t1994 NUMERIC(10,1),
            t1995 NUMERIC(10,1),
            t1996 NUMERIC(10,1),
            t1997 NUMERIC(10,1),
            t1998 NUMERIC(10,1),
            t1999 NUMERIC(10,1),
            t2000 NUMERIC(10,1),
            t2001 NUMERIC(10,1),
            t2002 NUMERIC(10,1),
            t2003 NUMERIC(10,1),
            t2004 NUMERIC(10,1),
            t2005 NUMERIC(10,1),
            t2006 NUMERIC(10,1),
            t2007 NUMERIC(10,1),
            t2008 NUMERIC(10,1),
            t2009 NUMERIC(10,1),
            t2010 NUMERIC(10,1),
            t2011 NUMERIC(10,1),
            t2012 NUMERIC(10,1),
            t2013 NUMERIC(10,1),
            t2014 NUMERIC(10,1),
            t2015 NUMERIC(10,1),
            t2016 NUMERIC(10,1),
            t2017 NUMERIC(10,1),
            t2018 NUMERIC(10,1),
            t2019 NUMERIC(10,1),
            t2020 NUMERIC(10,1),
            t2021 NUMERIC(10,1),
            t2022 NUMERIC(10,1),
            errors TEXT)
        """,
        """
        COMMENT ON TABLE investment IS 'frequency:A=Annual,unit:Million euro,expend:INF=Expenditure on infrastructure/INF_INV =Investment in infrastructure/INF_MNT=Maintenance in infrastructure/VEH=Expenditure on transport vehicles/VEH_INV=Investment in transport vehicles/VEH_MNT=Maintenance in transport vehicles, geo: European country code, 1992-2022: resulting value, errors:p=provisional/e=estimated' """,
        """
        CREATE TABLE IF NOT EXISTS  len_motorway (
            id SERIAL PRIMARY KEY,
            freq CHAR(1) NOT NULL,
            tra_infr CHAR(6) NOT NULL,
            unit CHAR(2) NOT NULL,
            geo VARCHAR(255) NOT NULL,
            t1970 NUMERIC(10,1),
            t1971 NUMERIC(10,1),
            t1972 NUMERIC(10,1),
            t1973 NUMERIC(10,1),
            t1974 NUMERIC(10,1),
            t1975 NUMERIC(10,1),
            t1976 NUMERIC(10,1),
            t1977 NUMERIC(10,1),
            t1978 NUMERIC(10,1),
            t1979 NUMERIC(10,1),
            t1980 NUMERIC(10,1),
            t1981 NUMERIC(10,1),
            t1982 NUMERIC(10,1),
            t1983 NUMERIC(10,1),
            t1984 NUMERIC(10,1),
            t1985 NUMERIC(10,1),
            t1986 NUMERIC(10,1),
            t1987 NUMERIC(10,1),
            t1988 NUMERIC(10,1),
            t1989 NUMERIC(10,1),
            t1990 NUMERIC(10,1),
            t1991 NUMERIC(10,1),
            t1992 NUMERIC(10,1),
            t1993 NUMERIC(10,1),
            t1994 NUMERIC(10,1),
            t1995 NUMERIC(10,1),
            t1996 NUMERIC(10,1),
            t1997 NUMERIC(10,1),
            t1998 NUMERIC(10,1),
            t1999 NUMERIC(10,1),
            t2000 NUMERIC(10,1),
            t2001 NUMERIC(10,1),
            t2002 NUMERIC(10,1),
            t2003 NUMERIC(10,1),
            t2004 NUMERIC(10,1),
            t2005 NUMERIC(10,1),
            t2006 NUMERIC(10,1),
            t2007 NUMERIC(10,1),
            t2008 NUMERIC(10,1),
            t2009 NUMERIC(10,1),
            t2010 NUMERIC(10,1),
            t2011 NUMERIC(10,1),
            t2012 NUMERIC(10,1),
            t2013 NUMERIC(10,1),
            t2014 NUMERIC(10,1),
            t2015 NUMERIC(10,1),
            t2016 NUMERIC(10,1),
            t2017 NUMERIC(10,1),
            t2018 NUMERIC(10,1),
            t2019 NUMERIC(10,1),
            t2020 NUMERIC(10,1),
            t2021 NUMERIC(10,1),
            t2022 NUMERIC(10,1),
            errors TEXT)
        """,
        """COMMENT ON TABLE len_motorway IS 'frequency:A=Annual,tra_infr:MWAY=Motorways/RD_EUR=E-roads,unit:Kilometre, geo: European country code, 1970-2022: resulting value, errors:b=break in time series/e=estimated/s=Eurostat estimate/z=not applicable' """,
        """ CREATE TABLE IF NOT EXISTS  len_road (
            id SERIAL PRIMARY KEY,
            freq CHAR(1) NOT NULL,
            unit CHAR(2) NOT NULL,
            tra_infr CHAR(6) NOT NULL,
            geo VARCHAR(255) NOT NULL,
            t1970 NUMERIC(10,1),
            t1971 NUMERIC(10,1),
            t1972 NUMERIC(10,1),
            t1973 NUMERIC(10,1),
            t1974 NUMERIC(10,1),
            t1975 NUMERIC(10,1),
            t1976 NUMERIC(10,1),
            t1977 NUMERIC(10,1),
            t1978 NUMERIC(10,1),
            t1979 NUMERIC(10,1),
            t1980 NUMERIC(10,1),
            t1981 NUMERIC(10,1),
            t1982 NUMERIC(10,1),
            t1983 NUMERIC(10,1),
            t1984 NUMERIC(10,1),
            t1985 NUMERIC(10,1),
            t1986 NUMERIC(10,1),
            t1987 NUMERIC(10,1),
            t1988 NUMERIC(10,1),
            t1989 NUMERIC(10,1),
            t1990 NUMERIC(10,1),
            t1991 NUMERIC(10,1),
            t1992 NUMERIC(10,1),
            t1993 NUMERIC(10,1),
            t1994 NUMERIC(10,1),
            t1995 NUMERIC(10,1),
            t1996 NUMERIC(10,1),
            t1997 NUMERIC(10,1),
            t1998 NUMERIC(10,1),
            t1999 NUMERIC(10,1),
            t2000 NUMERIC(10,1),
            t2001 NUMERIC(10,1),
            t2002 NUMERIC(10,1),
            t2003 NUMERIC(10,1),
            t2004 NUMERIC(10,1),
            t2005 NUMERIC(10,1),
            t2006 NUMERIC(10,1),
            t2007 NUMERIC(10,1),
            t2008 NUMERIC(10,1),
            t2009 NUMERIC(10,1),
            t2010 NUMERIC(10,1),
            t2011 NUMERIC(10,1),
            t2012 NUMERIC(10,1),
            t2013 NUMERIC(10,1),
            t2014 NUMERIC(10,1),
            t2015 NUMERIC(10,1),
            t2016 NUMERIC(10,1),
            t2017 NUMERIC(10,1),
            t2018 NUMERIC(10,1),
            t2019 NUMERIC(10,1),
            t2020 NUMERIC(10,1),
            t2021 NUMERIC(10,1),
            t2022 NUMERIC(10,1),
            errors TEXT)
        """,
        """COMMENT ON TABLE len_road IS 'frequency:A=Annual,unit:Kilometre,tra_infr:MWAY=Motorways/RD_EUR=E-roads,geo:European country code, 1970-2022:resulting value,errors:u=low reliability/b=break in time series/p=provisional/z=not applicablee=estimated/' """,
        """ CREATE TABLE IF NOT EXISTS  performance (
            id SERIAL PRIMARY KEY,
            freq VARCHAR(255) NOT NULL,
            mot_nrg CHAR(6) NOT NULL,
            regisveh VARCHAR(255) NOT NULL,
            vehicle VARCHAR(255) NOT NULL,
            unit VARCHAR(255) NOT NULL,
            geo VARCHAR(255) NOT NULL,
            t2013 NUMERIC(10,1),
            t2014 NUMERIC(10,1),
            t2015 NUMERIC(10,1),
            t2016 NUMERIC(10,1),
            t2017 NUMERIC(10,1),
            t2018 NUMERIC(10,1),
            t2019 NUMERIC(10,1),
            t2020 NUMERIC(10,1),
            t2021 NUMERIC(10,1),
            t2022 NUMERIC(10,1),
            errors TEXT)
        """,
        """COMMENT ON TABLE performance IS 'frequency:A=Annual,mot_nrg:TOTAL=total/PET=Petroleum products/DIE=Diesel/OTH=other,regisveh:TERNAT_REG=Traffic performed on the national territory by vehicles registered in the reporting country or in foreign countries/TERNAT_REGNAT=Traffic performed on the national territory by vehicles registered in the reporting country/TER_REGNAT=Traffic performed on the national or foreign territory by vehicles registered in the reporting country,vehicle:Total=total/LOR=lorries/LOR_LE3P5=Lorries <= 3.5 tonnes/LOR_GT3P5-6=Lorries from 3.6 to <= 6 tonnes/LOR_GT6=Lorries > 6 tonnes/TRC=Road Tractors/MOTO_MOP=Motorcycles and mopeds/MOP=Mopeds/MOTO=Motorcycles/CAR=Passenger Cars/BUS_MCO_TRO=Buses motorcoaches and trolleybuses/BUS_TOT=Motorcoaches buses trolleybuses/BUS_MCO_MIN=Minibuses and Minicoaches/BUS_TRO=Trolleybuses/MCO=Motorcoaches/BIKE=Bicycles/RMDVEH_OTH=other road motor vehicles,unit:Million vehicle kilometres,geo:European country code,2013-2022:resulting value,errors:e=estimated/z=not applicablee=estimated/p=provisional'"""
        )
    try:
        config = load_config()
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                # execute the CREATE TABLE statement
                for command in commands:
                    cur.execute(command)
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)

def inputData(dataframe, table):   
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in dataframe.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(dataframe.columns))
    valueStringCount =""
    for i in range(len(dataframe.columns)):
        valueStringCount = valueStringCount + "%s,"
    valueStringCount = valueStringCount[:-1]

    # SQL quert to execute
    query  = "INSERT INTO "+table+"("+cols+") VALUES("+valueStringCount+")" 
    try:
        config = load_config()
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                cur.executemany(query, tuples)
                conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cur.close()
        return 1
    print("execute_many() done")
    cur.close()

if __name__ == '__main__':
    #run setup per dataset
    df_investment = setupEurostatDataframe(pd.read_csv('data\investment.tsv', sep='\t'), 'freq,unit,expend,geo\TIME_PERIOD')
    df_len_motorway = setupEurostatDataframe(pd.read_csv("data\len_motorway.tsv", sep='\t'), 'freq,tra_infr,unit,geo\TIME_PERIOD')
    def_len_road= setupEurostatDataframe(pd.read_csv("data\len_road.tsv", sep='\t'), 'freq,unit,tra_infr,geo\TIME_PERIOD')
    df_performance = setupEurostatDataframe(pd.read_csv("data\performance_cat.tsv", sep='\t'), 'freq,mot_nrg,regisveh,vehicle,unit,geo\TIME_PERIOD')
    
    #maak tables in postgreSQL database
    create_tables()
    
    #input dataset in correcte table
    inputData(df_investment, "investment")
    inputData(df_len_motorway, "len_motorway")
    inputData(def_len_road, "len_road")
    inputData(df_performance, "performance")