import pandas as pd
import math

def calculate_nta(df):
    nta = pd.read_excel('data/nyc2010census_tabulation_equiv.xlsx',
                       skiprows=4, dtype=str,
                       names=['borough', 'fips', 'borough_code', 
                              'tract', 'puma', 'nta_code', 'nta_name'])
    nta['GEO_ID'] = '1400000US36' + nta['fips'] + nta['tract']
    dff = pd.merge(nta[['nta_code', 'GEO_ID']], df, how='left', left_on='GEO_ID', right_on='GEO_ID')
    
    variables = list(df.columns)
    variables.remove('GEO_ID')
    variables.remove('NAME')
    var = list(set([i[:-1] for i in variables if i[:-1][-1] != 'P']))
    
    results = []
    for i in dff.nta_code.unique():
        dfff = dff[dff.nta_code == i]
        record = {}
        record['nta'] = i
        for v in var:
            e = dfff[f'{v}E'].sum()
            m = math.sqrt(dfff[f'{v}M'].apply(lambda x: x**2).sum())
            record[f'{v}E'] = e
            record[f'{v}M'] = m
        results.append(record)
    
    r = pd.DataFrame(results)
    r['GEO_ID'] = r.nta
    r = r.rename(columns={'nta':'NAME'})
    output = pd.concat([r, df], sort=True)
    return output

if __name__ == "__main__":
    demo = pd.read_csv('data/demo.csv', index_col=False)
    demo_intermediate = calculate_nta(demo)
    demo_intermediate.to_csv('data/demo_intermediate.csv', index=False)

    econ = pd.read_csv('data/econ.csv', index_col=False)
    econ_intermediate = calculate_nta(econ)
    econ_intermediate.to_csv('data/econ_intermediate.csv', index=False)

    hous = pd.read_csv('data/hous.csv', index_col=False)
    hous_intermediate = calculate_nta(hous)
    hous_intermediate.to_csv('data/hous_intermediate.csv', index=False)
    
    soci = pd.read_csv('data/soci.csv', index_col=False)
    soci_intermediate = calculate_nta(soci)
    soci_intermediate.to_csv('data/soci_intermediate.csv', index=False)