import pandas as pd
import math
import numpy as np
from multiprocessing import Pool, cpu_count

def calculate_nta(category):
    df = pd.read_csv(f'data/{category}.csv', index_col=False)
    df = df.replace([999999999, 333333333, 222222222, 666666666, 888888888,
                    -999999999, -333333333, -222222222, -666666666, -888888888], 0)
    df = df.replace([-555555555, 555555555], 0)
    variables = list(df.columns)
    variables.remove('GEO_ID')
    variables.remove('NAME')
    var = list(set([i[:-1] for i in variables if i[:-1][-1] != 'P']))
    for i in var:
        df.loc[df[f'{i}E']==0, f'{i}M'] = np.nan

    nta = pd.read_excel('data/nyc2010census_tabulation_equiv.xlsx',
                       skiprows=4, dtype=str,
                       names=['borough', 'fips', 'borough_code', 
                              'tract', 'puma', 'nta_code', 'nta_name'])
    nta['GEO_ID'] = '1400000US36' + nta['fips'] + nta['tract']
    dff = pd.merge(nta[['nta_code', 'GEO_ID']], df, how='left', left_on='GEO_ID', right_on='GEO_ID')

    results = []
    for i in dff.nta_code.unique():
        dfff = dff[dff.nta_code == i]
        record = {}
        record['nta'] = i
        for v in var:
            e = dfff[f'{v}E'].sum()
            if f'{v}M' not in dfff.columns:
                m = np.nan
            else:
                m =(dfff.loc[dfff[f'{v}M'].notnull(), f'{v}M']**2).sum()**0.5
            record[f'{v}E'] = e
            record[f'{v}M'] = m
        results.append(record)
    
    r = pd.DataFrame(results)
    r['GEO_ID'] = r.nta
    r = r.rename(columns={'nta':'NAME'})
    output = pd.concat([r, df], sort=True)
    output.to_csv(f'data/{category}_intermediate.csv', index=False)

if __name__ == "__main__":
    with Pool(processes=cpu_count()) as pool:
        pool.map(calculate_nta, ['demo', 'hous', 'econ', 'soci'])