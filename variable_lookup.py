import pandas as pd
from utils import psycopg2_connect
from sqlalchemy import create_engine
from pathlib import Path
from dotenv import load_dotenv, find_dotenv
import json
import io
import os

def make_lookup(name):
    sql = '''
        with a as (select distinct variable from {0}),
        b as  (select distinct variable from staging.{0})
        select distinct a.variable, b.variable as lowercase
        from a, b
        where lower(a.variable) = b.variable;
    '''
    name_lookup = {
        'demo':'demographic',
        'econ':'economic', 
        'soci':'social', 
        'hous':'housing'
    }
    load_dotenv(Path(__file__).parent/'.env')
    con = create_engine(os.getenv('BUILD_ENGINE'))

    df = pd.read_sql(sql.format(name_lookup.get(name)), con=con)
    records = list(df.to_records())
    lookup = {}
    for i in records:
        lookup[i[2]] = i[1]

    with open(f'data/{name}_variable_lookup.json', 'w') as json_file:
        json.dump(lookup, json_file, indent = 4, sort_keys=True)

if __name__ == "__main__":
    make_lookup('demo')
    make_lookup('soci')
    make_lookup('hous')
    make_lookup('econ')