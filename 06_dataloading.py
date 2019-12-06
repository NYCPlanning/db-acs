import pandas as pd
from utils import psycopg2_connect
from sqlalchemy import create_engine
from pathlib import Path
from dotenv import load_dotenv, find_dotenv
import io
import os

def export_pff(name, path, con):
    df = pd.read_csv(path, index_col=False, dtype=str)

    db_connection = psycopg2_connect(con.url)
    db_cursor = db_connection.cursor()
    str_buffer = io.StringIO()

    df.to_csv(str_buffer, sep='\t', header=True, index=False)
    str_buffer.seek(0)

    con.execute('CREATE SCHEMA IF NOT EXISTS staging;')
    con.execute(f'''
        DROP TABLE IF EXISTS staging.{name};
        CREATE TABLE staging.{name} (
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

    db_cursor.copy_expert(f"COPY staging.{name} FROM STDIN WITH NULL AS '' DELIMITER E'\t' CSV HEADER", str_buffer)
    # con.execute(f'''
    #     DROP INDEX IF EXISTS new_{name}_geoid;
    #     CREATE INDEX new_{name}_geoid ON staging.{name}(geoid text_ops);''')
    db_cursor.connection.commit()
    str_buffer.close()
    db_cursor.close()
    db_connection.close()

if __name__ == "__main__":
    load_dotenv(Path(__file__).parent/'.env')
    con = create_engine(os.getenv('BUILD_ENGINE'))

    export_pff('demographic', 'data/demo_final_pivoted.csv', con)
    export_pff('economic', 'data/econ_final_pivoted.csv', con)
    export_pff('social', 'data/soci_final_pivoted.csv', con)
    export_pff('housing', 'data/hous_final_pivoted.csv', con)