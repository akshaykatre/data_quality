def completeness(connectionmap):
    query = 'SELECT count(*) FROM {database}.{schema}.{tablename} where [{attribute}] is null'.format(**connectionmap)
    return query 

def descriptive(connectionmap):
    query = '''SELECT count({attribute}), max({attribute}), min({attribute}),      
                FROM {database}.{schema}.{tablename} where [{attribute}] is null'''.format(**connectionmap)
    return query 
