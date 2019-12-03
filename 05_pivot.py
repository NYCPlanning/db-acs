import pandas as pd
import json
import numpy as np

def pivot_output(category):
    df = pd.read_csv(f'data/{category}_final1.csv', index_col=False)
    var = set(list(map(lambda x: x[:-1], df.columns)))

    r = []

    for i in var: 
        cols = ['geoid', 'name', i+'c', i+'e', i+'m', i+'p', i+'z']
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