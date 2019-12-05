import pandas as pd 
from pathlib import Path

def get_c(e, m): 
    if e == 0:
        return np.nan
    else:
        return m/1.645/e*100
        
def format_geoid(geoid):
    fips_lookup = {
        '05': '2',
        '47': '3',
        '61': '1',
        '81': '4',
        '85': '5',}
    # NTA
    if geoid[:2] in ['MN', 'QN', 'BX', 'BK', 'SI']: 
        return geoid
    # Community District (PUMA)
    elif geoid[:2] == '79': 
        return geoid[-4:]
    # Census tract (CT2010)
    elif geoid[:2] == '14':
        boro = fips_lookup.get(geoid[-8:-6])
        return boro + geoid[-6:]
    # Boro
    elif geoid[:2] == '05': 
        return fips_lookup.get(geoid[-2:])
    # City 
    elif geoid[:2] == '16':
        return 0

def assign_geotype(geoid): 
    # NTA
    if geoid[:2] in ['MN', 'QN', 'BX', 'BK', 'SI']: 
        return 'NTA2010'
    # Community District (PUMA)
    elif geoid[:2] == '79': 
        return 'PUMA2010'
    # Census tract (CT2010)
    elif geoid[:2] == '14':
        return 'CT2010'
    # Boro
    elif geoid[:2] == '05': 
        return 'Boro2010'
    # City 
    elif geoid[:2] == '16':
        return 'City2010'

def assign_geogname(geotype, name, geoid):
    boro_lookup = {
        '1': 'Manhattan',
        '2': 'Bronx', 
        '3': 'Brooklyn',
        '4': 'Queens', 
        '5': 'Staten Island'}
    if geotype == 'Boro2010': 
        return boro_lookup.get(geoid)
    elif geotype == 'City2010': 
        return 'New York City'
    elif geotype == 'CT2010': 
        return NTA.nta_code[NTA.boroct == geoid].to_list()[0]
    elif geotype == 'PUMA2010': 
        return name
    elif geotype == 'NTA2010': 
        return NTA.nta_name[NTA.nta_code == geoid].to_list()[0]

NTA = pd.read_excel(Path(__file__).parent/'data/nyc2010census_tabulation_equiv.xlsx', 
                   skiprows=4, dtype=str,
                  names=['borough', 'fips', 'borough_code', 'tract', 'puma', 'nta_code', 'nta_name'])
NTA['boroct']=NTA['borough_code'] + NTA['tract']   