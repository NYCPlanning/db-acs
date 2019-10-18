import pandas as pd
import json
import numpy as np

def pivot_output(category):
    df = pd.read_csv(f'data/{category}_final.csv', index_col=False)
    with open(f'data/{category}_meta_lookup.json', 'r') as f:
        meta_lookup = json.load(f)
    var = list(meta_lookup.keys())

    r = []

    for i in var: 
        cols = ['GEO_ID', 'NAME', i+'C', i+'E', i+'M', i+'P', i+'Z']
        dff = df.loc[:, cols]
        dff.columns=['geoid', 'geoname', 'c', 'e', 'm', 'p', 'z']
        dff['variable'] = i
        r.append(dff)

    result = pd.concat(r)
    result.to_csv(f'data/{category}_final_pivoted.csv')

if __name__ == "__main__":
    pivot_output('soci')
    pivot_output('econ')
    pivot_output('demo')
    pivot_output('hous')