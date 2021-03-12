import pandas
import pyodbc 
import json 
import queries
from multiprocessing import Pool
import pdb 
import time 


with open('login.txt') as jload:
    data = json.load(jload)


attlist = pandas.read_csv('intcols.csv')

d = []

for atts in attlist.itertuples():

    maps = {'database':atts.TABLE_CATALOG,
            'schema':atts.TABLE_SCHEMA,
            'tablename':atts.TABLE_NAME,
            'attribute':atts.COLUMN_NAME}
    
    d.append(maps)

    q = queries.completeness(maps)
  #  print(q)
    #val = pandas.read_sql(q, conn)
    #print(val.values[0][0])

def results(connectionmap):
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=AdventureWorks2017;UID='+data['username']+';PWD='+data['password']) 
    q = queries.completeness(connectionmap)
    #print(q)
    val = pandas.read_sql(q,conn)
    #print(val)
    #pdb.set_trace()
    return val.values[0][0]

for agents in range(1,9):
    #agents = 4
    print("For core:", str(agents))
    times = []
    for iters in range(0,10):
        t0 = time.time()
        chunksize = 3 
        with Pool(processes=agents) as pool:
            result = pool.map(results, d, chunksize)

    #    print('Result: ' + str(result))

        t1 = time.time()

        times.append(t1-t0)
    print("Average time:", sum(times)/len(times))
