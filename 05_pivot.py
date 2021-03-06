import pandas as pd
import json
import numpy as np
from multiprocessing import Pool, cpu_count
from data import mdage, mdefftwrk, mdemftwrk, \
                mdewrk, mdfaminc, mdgr, mdhhinc, \
                mdnfinc, mdrms, mdvl
from data import VERSION

# Decimal needed for both E and M
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

median_cols = []
for i in [mdage, mdefftwrk, mdemftwrk, \
            mdewrk, mdfaminc, mdgr, mdhhinc, \
                mdnfinc, mdrms, mdvl]: 
    median_cols += list(i.keys())

def pivot_output(category):
    '''
    Pivots tables, removes invalid fields, and rounds
    to required number of digits.

    Exports final table as CSV with the suffix 'final_pivoted'.

    Parameters
    ----------
    category: str
        'demo', 'hous', 'econ', 'soci'
        Category of variables contained in the table
        
    '''
    df = pd.read_csv(f'data/{category}_final2.csv', index_col=False, low_memory=False)
    cols = [i for i in df.columns if i not in ['geoid', 'geo_id', 'geotype', 'name', 'geogname']]
    var = set(list(map(lambda x: x[:-1], cols)))

    r = []
    with open(f'data/{category}_variable_lookup.json', 'r') as f:
            variable_lookup = json.load(f)
    for i in var:
        cols = ['geotype', 'geogname', 'geoid', i+'c', i+'e', i+'m', i+'p', i+'z']
        dff = df.loc[:, cols]
        dff.columns=['geotype', 'geogname', 'geoid', 'c', 'e', 'm', 'p', 'z']
        dff['variable'] = variable_lookup.get(i, i)

        # Null out median input p and z
        if i in median_cols:
            dff.loc[df.geotype != 'NTA2010','c'] = np.nan
            dff.loc[df.geotype != 'NTA2010','m'] = np.nan
            dff.loc[df.geotype != 'NTA2010','p'] = np.nan
            dff.loc[df.geotype != 'NTA2010','z'] = np.nan

        # Round estimate and MOE columns
        dff.loc[:,'c'] = dff['c'].round(1)
        dff.loc[:,'e'] = dff['e'].round(rounding.get(i, 0))
        dff.loc[:,'m'] = dff['m'].round(rounding.get(i, 0))
        dff.loc[:,'p'] = dff['p'].round(1)
        dff.loc[:,'z'] = dff['z'].round(1)
        dff['dataset'] = VERSION
        r.append(dff)

    # Reombine variables into single DataFrame
    result = pd.concat(r)

    # Set meaningless negative values to None
    result.loc[result.c <= 0,'c'] = np.nan
    result.loc[result.e < 0,'e'] = np.nan
    result.loc[result.m <= 0,'m'] = np.nan
    result.loc[result.p < 0,'p'] = np.nan
    result.loc[result.z < 0,'z'] = np.nan

    # Set MOEs and proportion variables of zero estimates to None
    result.loc[result.e==0, 'c'] = np.nan
    result.loc[result.e==0, 'm'] = np.nan
    result.loc[result.e==0, 'p'] = np.nan
    result.loc[result.e==0, 'z'] = np.nan

    # Set proportion variables and coeff of var to None where MOE is none
    result.loc[result.m.isna(), 'c'] = np.nan
    result.loc[result.m.isna(), 'p'] = np.nan
    result.loc[result.m.isna(), 'z'] = np.nan

    # Export to CSV
    result = result[['geotype', 'geogname', 'geoid', 'dataset', 'variable', 'c', 'e', 'm', 'p', 'z']]
    result.to_csv(f'data/{category}_final_pivoted.csv', index=False)

if __name__ == "__main__":
    with Pool(processes=cpu_count()) as pool:
        pool.map(pivot_output, ['demo', 'hous', 'econ', 'soci'])