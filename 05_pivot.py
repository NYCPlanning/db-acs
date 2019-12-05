import pandas as pd
import json
import numpy as np

# decimal needed for both E and M
rounding = {
    'mdage':1,
    'avghhsz':2,
    'avgfmsz':2,
    'mntrvtm':1,
    'hovacrt':1,
    'rntvacrt':1,
    'mdrms':1,
    'avghhsooc':2,
    'avghhsroc':2
}

def pivot_output(category):
    df = pd.read_csv(f'data/{category}_final1.csv', index_col=False, low_memory=False)
    cols = [i for i in df.columns if i not in ['geoid', 'geo_id', 'geotype', 'name', 'geogname']]
    var = set(list(map(lambda x: x[:-1], cols)))

    r = []

    for i in var:
        cols = ['geotype', 'geogname', 'geoid', i+'c', i+'e', i+'m', i+'p', i+'z']
        dff = df.loc[:, cols]
        dff.columns=['geotype', 'geogname', 'geoid', 'c', 'e', 'm', 'p', 'z']
        dff['variable'] = i
        dff.loc[:,'c'] = dff['c'].round(1)
        dff.loc[:,'e'] = dff['e'].round(rounding.get(i, 0))
        dff.loc[:,'m'] = dff['m'].round(rounding.get(i, 0))
        dff.loc[:,'e'] = dff['e'].round(0)
        dff.loc[:,'m'] = dff['m'].round(0)
        dff.loc[:,'p'] = dff['p'].round(1)
        dff.loc[:,'z'] = dff['z'].round(1)
        dff['dataset'] = 'Y2013-2017'
        r.append(dff)

    result = pd.concat(r)
    result = result[['geotype', 'geogname', 'geoid', 'dataset', 'variable', 'c', 'e', 'm', 'p', 'z']]
    result.to_csv(f'data/{category}_final_pivoted.csv', index=False)

if __name__ == "__main__":
    pivot_output('soci')
    pivot_output('econ')
    pivot_output('demo')
    pivot_output('hous')