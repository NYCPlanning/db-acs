import pandas as pd
import math
import numpy as np
from multiprocessing import Pool, cpu_count

def calculate_nta(category):
    '''
    Calculate NTA-level estimates and MOEs by merging
    with a look-up table relating census geography
    with NYC NTA geography
    
    Parameters
    ----------
    category: str
        'demo', 'hous', 'econ', 'soci'
        Describes subject category for which to create
        intermediate table

    '''
    df = pd.read_csv(f'data/{category}.csv', index_col=False)
    
    # Replace annotation codes with None -- these indicate data not appropriate for statistical tests
    df = df.replace([999999999, 555555555, 333333333, 222222222, 666666666, 888888888,
                    -999999999, -555555555, -333333333, -222222222, -666666666, -888888888], np.nan)
    
    # Get a list of variables, excluding geography ID and name
    variables = list(df.columns)
    variables.remove('GEO_ID')
    variables.remove('NAME')
    
    # Remove rate variables and their MOEs
    var = list(set([i[:-1] for i in variables if i[:-1][-1] != 'P']))
    
    # Set MOE to None where estimate is zero
    # TODO: check to see if this is appropriate.
    for i in var:
        df.loc[df[f'{i}E']==0, f'{i}M'] = np.nan

    # Load geography look-up table, and merge on census geoIDs
    nta = pd.read_excel('data/nyc2010census_tabulation_equiv.xlsx',
                       skiprows=4, dtype=str,
                       names=['borough', 'fips', 'borough_code', 
                              'tract', 'puma', 'nta_code', 'nta_name'])
    nta['GEO_ID'] = '1400000US36' + nta['fips'] + nta['tract']
    dff = pd.merge(nta[['nta_code', 'GEO_ID']], df, how='left', left_on='GEO_ID', right_on='GEO_ID')

    results = []
    # For each unique NTA, calculate aggregate estimates and MOEs.
    for i in dff.nta_code.unique():
        dfff = dff[dff.nta_code == i]
        record = {}
        record['nta'] = i
        for v in var:
            # Calculate estimate of combined geographies
            e = dfff[f'{v}E'].sum()
            if f'{v}M' not in dfff.columns:
                # If there is no MOE, set to None -- controlled var
                m = np.nan
            else:
                # Calculate MOE of combined geographies
                m =(dfff.loc[dfff[f'{v}M'].notnull(), f'{v}M']**2).sum()**0.5
            record[f'{v}E'] = e
            record[f'{v}M'] = m
        results.append(record)
    
    # Export as a csv of NTA-level estimates and MOEs
    r = pd.DataFrame(results)
    r['GEO_ID'] = r.nta
    r = r.rename(columns={'nta':'NAME'})
    output = pd.concat([r, df], sort=True)
    output.to_csv(f'data/{category}_intermediate.csv', index=False)

if __name__ == "__main__":
    with Pool(processes=cpu_count()) as pool:
        pool.map(calculate_nta, ['demo', 'hous', 'econ', 'soci'])