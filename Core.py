import sqlite3
import re

##find the source of table
def fromClause(script):
    fc = re.search('from (.*?)[\s\n]',script)
    if fc:
        return fc.group(1)

##find joins
def joinClause(script):
    fc = re.search('join (.*?)[\s\n]',script)
    if fc:
        return fc.group(1)

##find query columns
def selectClause(script):
    fc = re.search('select(.*?)into|from', script,re.DOTALL | re.IGNORECASE)
    retcols=[]
    if fc:
        allcol = []
        allcol=str(fc.group(1)).split(',')
        #print(allcol)
        for i in allcol:
            keypair=i.strip().split()
            print(keypair)
            if len(keypair)==2:
                retcols.append([keypair[0],keypair[1]])
            else:
                retcols.append([keypair[0], ''])
    return retcols

##find where clause
def whereClause(script):
    fc = re.search('select(.*?)into|from', script, re.DOTALL)
    if fc:
        return fc.group(1)

##find into clause
def intoClause(script):
    fc = re.search('into (.*?)[\s\n]', script)
    if fc:
        return fc.group(1)

# let's define a script we need to parse
examplescript="""
select orid, orcompletedate, opprsku
INTO #tmpTable
from csn_order..vwOrder a
inner join csn_order..vworderproduct b on a.orid=b.oporid
where orcompletedate>'1/1/2015';


select
opprsku, count(*) count
into #finaltable
from #tmptable
group by opprsku
having count(*)>100;
"""

parsed=examplescript.split(';')
print(parsed)
for i in parsed:
   # print (i)
    print(fromClause(i))
    print(joinClause(i))
    print(selectClause(i))
    print(intoClause(i))


#this will hold all our parsedprocedure information
#db = sqlite3.connect('data/coredb')

# let's keep it in memory for our purposes
db = sqlite3.connect(':memory:')


cursor = db.cursor()
'''
cursor.execute(
CREATE TABLE
)
'''

db.close()