import pandas as pd
from utils import psycopg2_connect
from sqlalchemy import create_engine
from pathlib import Path
from dotenv import load_dotenv, find_dotenv
from data import VERSION
import io
import os

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

if __name__ == "__main__":
    load_dotenv(Path(__file__).parent/'.env')
    con = create_engine(os.getenv('EDM_DATA'))

    export_pff('demographic', 'data/demo_final_pivoted.csv', con)
    export_pff('economic', 'data/econ_final_pivoted.csv', con)
    export_pff('social', 'data/soci_final_pivoted.csv', con)
    export_pff('housing', 'data/hous_final_pivoted.csv', con)

    # Taking variables no longer produced by acs
    con.execute(f'''
    INSERT INTO pff_social."{VERSION}"(geotype,geogname,geoid,dataset,variable,c,e,m,p,z)
    select geotype, geogname, geoid, '{VERSION}' as dataset, variable, c,e,m,p,z from pff_social."Y2013-2017-old" 
    where variable not in (select distinct variable from pff_social."{VERSION}");
    delete from pff_social."{VERSION}" where variable ~* 'name.';''')

    # add deleted variables back into new table, but all values as nulls
    con.execute(f'''
    INSERT INTO pff_housing."{VERSION}"(geotype,geogname,geoid,dataset,variable,c,e,m,p,z)
    select geotype, geogname, geoid, '{VERSION}' as dataset, variable, NULL as c, NULL as e, NULL as m, NULL as p, NULL as z from pff_housing."Y2006-2010" 
    where variable not in (select distinct variable from pff_housing."{VERSION}");''')

    con.execute(f'''
        GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA pff_demographic to labs;
        GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA pff_housing to labs;
        GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA pff_economic to labs;
        GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA pff_social to labs;
        GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA pff_acs_metadata to labs;
        GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA pff_decennial to labs;
        GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA pff_decennial_metadata to labs;
        GRANT USAGE ON SCHEMA pff_decennial_metadata TO labs;
        GRANT USAGE ON SCHEMA pff_decennial TO labs;
        GRANT USAGE ON SCHEMA pff_acs_metadata TO labs;
        GRANT USAGE ON SCHEMA pff_social TO labs;
        GRANT USAGE ON SCHEMA pff_economic TO labs;
        GRANT USAGE ON SCHEMA pff_housing TO labs;
        GRANT USAGE ON SCHEMA pff_demographic TO labs;
        GRANT USAGE ON SCHEMA pff_demographic TO labs;
    ''')