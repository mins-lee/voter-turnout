#################################################
# hw1.py
#
# Your name: Min Lee    
# Your andrew id: minseonl

#################################################

import pandas as pd
import censusdata
import yaml
import ohio.ext.pandas
from sqlalchemy import create_engine
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.precision', 2)

# with open('/data/users/minseonl/variables.yaml', 'r') as f:
#   # loads contents of variables.yaml into a python dictionary
#   config = yaml.safe_load(f.read())

with open('secrets.yaml', 'r') as f:
  # loads contents of variables.yaml into a python dictionary
  sec = yaml.safe_load(f.read())

with open('config.yaml', 'r') as f:
  # loads contents of variables.yaml into a python dictionary
  config = yaml.safe_load(f.read())  

state = config['acs']['state']
year = config['acs']['year']
key = sec['web_resource']['key']
tableIDs = config['acs']['tableIDs']
variables = config['acs']['variables']

def getState(state, year, key):
    """Generate state identifier for ACS 5-year data."""
    allState = censusdata.geographies(censusdata.censusgeo([('state', '*')]), 'acs5', year, key=key)
    return allState[state].geo

def getAllCounties(state, year, key):
    """Generate all county identifiers for ACS 5-year data."""
    state = getState(state, year, key)
    return censusdata.geographies(censusdata.censusgeo(list(state) + 
            [('county', '*')]), 'acs5', year, key=key)


def downloadBlockgroupData(state, year, key, tableIDs):
    """Download ACS 5-year data at the block group level."""
    newTable = pd.DataFrame()
    allCounties = getAllCounties(state, year, key)
    for county in allCounties:
        table = censusdata.download('acs5', year, censusdata.censusgeo(list(allCounties[county].geo) + 
        [('block group', '*')]), tableIDs, key=key)
        newTable = newTable.append(table)
    return newTable 

def computeVar(state, year, key, tableIDs):
    """Compute variables using ACS data tables.""" 
    t = downloadBlockgroupData(state, year, key, tableIDs)
    result = t.assign(total_population = t['B01003_001E'], 
        percent_white = t['B02001_002E']/t['B02001_001E'], 
        percent_married = (t['B12001_004E']+t['B12001_013E'])/t['B12001_001E'],
        percent_college_degree_higher = (t['B15003_022E']+t['B15003_023E']+
            t['B15003_024E']+t['B15003_025E'])/t['B15003_001E'],
        vietnam_war_veterans = t['B21002_007E'],
        percent_food_stamp = t['B22010_002E']/t['B22010_001E'],
        percent_unemployed = t['B23025_007E']/t['B23025_001E'],
        percent_no_internet = t['B28011_008E']/t['B28011_001E'],
        voter_65yr_and_over = t['B29001_005E'],
        voter_percent_college_degree_higher = (t['B29002_007E']+
            t['B29002_008E'])/t['B29002_001E'],
        voter_percent_below_poverty = t['B29003_002E']/t['B29003_001E'],
        voter_median_income = t['B29004_001E'])
    return result

def cleanTable(state, year, key, tableIDs, variables):
    """Drop columns with table ID.""" 
    table = computeVar(state, year, key, tableIDs)
    return table[variables]

# def loadTable(state, year, key, tableIDs, variables):
#     """Load pandas dataframe into postgres db table."""
#     username = config['db']['username']
#     password = config['db']['password']
#     host = config['db']['host']
#     port = config['db']['port']
#     database = config['db']['database']
#     engine = create_engine('postgresql://' + username + ':' + password + '@' + 
#             host + ':' + port + '/' + database)
#     df = cleanTable(state, year, key, tableIDs, variables)
#     return df.pg_copy_to('test', engine)

def loadTable(state, year, key, tableIDs, variables):
    """Load pandas dataframe into postgres db table."""
    username = sec['db']['username']
    password = sec['db']['password']
    host = sec['db']['host']
    port = sec['db']['port']
    database = sec['db']['database']
    engine = create_engine('postgresql://' + username + ':' + password + '@' + 
            host + ':' + port + '/' + database)
    df = cleanTable(state, year, key, tableIDs, variables)
    return df.pg_copy_to('test', engine)

if __name__ == '__main__':
    loadTable(state, year, key, tableIDs, variables)    