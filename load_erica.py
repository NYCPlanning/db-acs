import pandas as pd
from utils import psycopg2_connect
from sqlalchemy import create_engine
from pathlib import Path
from dotenv import load_dotenv, find_dotenv
# from data import VERSION
from multiprocessing import Pool, cpu_count
import io
import os
import json

VERSION = 'Y2006-2010'

# df = pd.read_excel('erica/ACSDatabase_0610_inflatedfor1418.xlsx', index_col=False)
df = pd.read_csv('erica/ACS_0610.csv', low_memory=False)
df.columns = df.loc[0, :]
df = df.loc[1:, :]
df = df.rename(columns={"GeoType": "geotype", "GeogName": "geogname", "GeoID": "geoid"})
# df.to_csv('erica/erica_acs.csv', index=False)
# df = pd.read_csv('erica/erica_acs.csv', index_col=False, low_memory=False)
meta = pd.read_csv('data/factfinder_metadata.csv', index_col=False, dtype=str)
meta = meta.loc[meta.release_year.str.contains('2006-2010',na=False), :]
meta.profile = meta.profile.str.lower()

def pivot(category):
    meta_cat = meta.loc[meta.profile.str.contains(category), 'variablename']
    var = meta_cat.to_list()
    r = []

    for i in var:
        cols = ['geotype', 'geogname', 'geoid', i+'C', i+'E', i+'M', i+'P', i+'Z']
        dff = df.loc[:, cols]
        dff.columns=['geotype', 'geogname', 'geoid', 'C', 'E', 'M', 'P', 'Z']
        dff['dataset'] = VERSION
        dff['variable'] = i
        r.append(dff)

    result = pd.concat(r)

    result = result[['geotype', 'geogname', 'geoid', 'dataset', 'variable', 'C', 'E', 'M', 'P', 'Z']]
    result.to_csv(f'erica/{category}_final_pivoted.csv', index=False)

with Pool(processes=cpu_count()) as pool:
    pool.map(pivot, ['demo', 'hous', 'econ', 'soci'])

def export_pff(name, path, con):
    df = pd.read_csv(path, index_col=False, dtype=str)

    db_connection = psycopg2_connect(con.url)
    db_cursor = db_connection.cursor()
    str_buffer = io.StringIO()

    df.to_csv(str_buffer, sep='\t', header=True, index=False)
    str_buffer.seek(0)

    con.execute(f'CREATE SCHEMA IF NOT EXISTS pff_{name};')
    con.execute(f'''
        DROP TABLE IF EXISTS pff_{name}."{VERSION}";
        CREATE TABLE pff_{name}."{VERSION}" (
            geotype text,
            geogname text,
            geoid text,
            dataset text,
            variable text,
            c double precision,
            e double precision,
            m double precision,
            p double precision,
            z double precision
        );
    ''')

    db_cursor.copy_expert(f'''COPY pff_{name}."{VERSION}" FROM STDIN WITH NULL AS '' DELIMITER E'\t' CSV HEADER''', str_buffer)
    db_cursor.connection.commit()
    str_buffer.close()
    db_cursor.close()
    db_connection.close()

load_dotenv(Path(__file__).parent/'.env')
con = create_engine(os.getenv('EDM_DATA'))

export_pff('demographic', 'erica/demo_final_pivoted.csv', con)
export_pff('economic', 'erica/econ_final_pivoted.csv', con)
export_pff('social', 'erica/soci_final_pivoted.csv', con)
export_pff('housing', 'erica/hous_final_pivoted.csv', con)

# con.execute(f'''
# INSERT INTO pff_social."{VERSION}"(geotype,geogname,geoid,dataset,variable,c,e,m,p,z)
# select geotype, geogname, geoid, '{VERSION}' as dataset, variable, c,e,m,p,z from pff_social."Y2013-2017-old" 
# where variable not in (select distinct variable from pff_social."{VERSION}");''')

# con.execute(f'''
# INSERT INTO pff_housing."{VERSION}"(geotype,geogname,geoid,dataset,variable,c,e,m,p,z)
# select geotype, geogname, geoid, '{VERSION}' as dataset, variable, c,e,m,p,z from pff_housing."Y2013-2017-old" 
# where variable not in (select distinct variable from pff_housing."{VERSION}");''')