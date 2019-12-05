def format_geoid(geoid):
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

NTA = pd.read_excel('data/nyc2010census_tabulation_equiv.xlsx', 
                   skiprows=4, dtype=str,
                  names=['borough', 'fips', 'borough_code', 'tract', 'puma', 'nta_code', 'nta_name'])
NTA['boroct']=NTA['borough_code'] + NTA['tract']   