import re

tables={}

aggregates = ['SUM(', 'COUNT(']

# this is a generic function to parse tables and aliases
def t_name_alias(dict,match):
    match=match.split()
    dict['name'] = match[0]
    dict['alias'] = '"'+match[0].replace('"',"")+'"'
    if len(match) == 2:
        dict['alias'] = match[1]
    tables[dict['alias']]=dict['name']
    return dict

# this is a generic function to parse measures and aliases
def parse_join(match):
    dict = {}
    match=match.split('.')
    dict['source'] = 'undefined'
    dict['name'] = match[0]

    if len(match) == 2:
        dict['name'] = match[1]
        dict['source'] = tables[match[0]]
    return dict


# this finds the base table in the from statement
def fromClause(script):
    fc = re.findall('from (.*?\s.*?)(?:INNER|GROUP|Where|\n)',script,re.DOTALL | re.IGNORECASE)
    #print(fc)
    if fc:
        ret={}
        ret = t_name_alias(ret, fc[0])
        fcalias=fc[0].split()
        return ret


# this finds joins
def joinClause(script):
    retclause=[]
    joinlist=[]
    joins = re.findall('(left|right|full|inner)(?:[\s\n]*outer?[\s\n]*join|[\s\n]*join)[\s\n]*(.*?)[\s\n]*(?: on)\s*(.*?)=((CASE.*?END)|.*?)(?:INNER|OUTER|FULL|WHERE|LEFT|GROUP)',script,re.DOTALL | re.IGNORECASE)
    if joins:
        for join in joins:
            joindict={}
            joindict['type']=join[0]
            print(join)
            joindict=t_name_alias(joindict,join[1])
            con1 = parse_join(join[2].strip())
            con2 = parse_join(join[3].strip())
            #print(join[3])
            joindict['jcl'] = con1
            joindict['jcr'] = con2

            joinlist.append(joindict)
        #print(joinlist)
        return joinlist

# This handles finding columns within the select clause
def selectClause(script):

    fc = re.search('select(.*?)(?:into|from)', script,re.DOTALL | re.IGNORECASE)
    retcols=[]
    if fc:
        allcol=str(fc.group(1)).split(',')
        #print(allcol)
        for i in allcol:
            colinfo={}
            keypair=i.strip().split()
            col=keypair[0].split('.')
            colinfo['table'] = 'unspec'
            colinfo['column'] = col[0]
            if len(col)==2:
                colinfo['table']=col[0]
                colinfo['column']=col[1]
            colinfo['alias'] = '"'+keypair[0].replace('"',"")+'"'
            if len(keypair)==2:
                colinfo['alias']='"'+keypair[1].replace('"',"")+'"'
            retcols.append(colinfo)
    return retcols



# This handles the into clause
# T-SQL only
def intoClause(script):
    fc = re.search('into (.*?)[\s\n]', script,re.DOTALL | re.IGNORECASE)
    if fc:
        return fc.group(1)

# This handles the group by clause
def groupClause(script):
    gc = re.search('group by (.*?)(?:having|order|$)', script, re.DOTALL | re.IGNORECASE)
    retcols=[]
    if gc:
        allcol = []
        allcol=str(gc.group(1)).split(',')
        for i in allcol:
            colinfo={}
            keypair=i.strip().split()
            col=keypair[0].split('.')
            colinfo['table'] = 'unspec'
            colinfo['column'] = col[0]
            if len(col)==2:
                colinfo['table']=col[0]
                colinfo['column']=col[1]
            retcols.append(colinfo)
    return retcols

# This handles the where clause
def whereClause(script):
    wc = re.search('WHERE(.*?)(?:GROUP|INTO|$)', script,re.DOTALL | re.IGNORECASE)
    retcols=[]
    if wc:
        #TODO: fix this split
        allcol=re.split('(AND | OR )',wc.group(1), 0, re.DOTALL | re.IGNORECASE)
        for crit in allcol:
            crit=crit.strip()
            where={}
            col = re.search('(.*?)(?:<=|>=|=|>|<| in)', crit, re.DOTALL | re.IGNORECASE)
            if col:
                where['column']=col.group(1)
            op = re.search('(<=|>=|=|>|<| in )', crit, re.DOTALL | re.IGNORECASE)
            if op:
                where['operator'] = op.group(1)
            vals=re.search('(?:<=|>=|=|>|<| in )(.*)', crit, re.DOTALL | re.IGNORECASE)
            if vals:
                where['value'] = vals.group(1)
            logic=re.search('(AND|OR)', crit, re.DOTALL | re.IGNORECASE)
            if logic:
                where['logical'] = logic.group(1)
            else:
                where['logical'] = ''
            keypair=crit.strip().split()
            col=keypair[0].split('.')
            #colinfo['table'] = 'unspec'
            #colinfo['column'] = col[0]
            if len(col)==2:
                thing=1
            #    colinfo['table']=col[0]
            #    colinfo['column']=col[1]
            #print(where)
            retcols.append(where)
    return retcols


# this handles a having clause
def havingClause(script):
    hc = re.search('having (.*?)(?:order|$)', script, re.DOTALL | re.IGNORECASE)
    retcols=[]
    if hc:
        allcol=str(hc.group(1)).split('AND |OR ')
        for crit in allcol:
            hav={}
            hav['function'] = re.search('(.*?)\(.*\)', crit, re.DOTALL | re.IGNORECASE).group(1).upper()
            hav['col'] = re.search('\((.*)\)', crit, re.DOTALL | re.IGNORECASE).group(1)
            hav['operator'] = re.search('(=|>|<|<=|>=)', crit, re.DOTALL | re.IGNORECASE).group(1)
            hav['value'] = re.search('(?:=|>|<|<=|>=)(.*)', crit, re.DOTALL | re.IGNORECASE).group(1)
            keypair=crit.strip().split()
            col=keypair[0].split('.')
            #colinfo['table'] = 'unspec'
            #colinfo['column'] = col[0]
            if len(col)==2:
                thing=1
            #    colinfo['table']=col[0]
            #    colinfo['column']=col[1]

            retcols.append(hav)
    return retcols

#return serialized format
def compile(statement):
    ret = {}
    select = selectClause(statement)
    cols=[]
    for idx, col in enumerate(select):
            cols.append(col)
    ret['type']=1
    ret['select']=cols
    ret['into'] = intoClause(statement)
    ret['from'] = fromClause(statement)
    ret['joins'] = joinClause(statement)
    ret['where'] = whereClause(statement)
    ret['group by'] = groupClause(statement)
    ret['having'] = havingClause(statement)
    return ret

#return raw text format
def text(select_dict):
    ##select statement
    statement='SELECT'
    select = select_dict['select']
    for idx, col in enumerate(select):
        if idx == 0:
            statement = statement + '\n\t'
        else:
            statement = statement + '\n\t,'
        statement = statement + col['column'] + ' AS ' + col['alias']
    ##into Statement
    into = select_dict['into']
    if into:
        statement = statement + '\ninto ' + into
    fc = select_dict['from']
    statement = statement + '\nFROM ' + fc['name'] + ' AS ' + fc['alias']
    jc = select_dict['joins']
    if jc:
        for idx, table in enumerate(jc):
            statement = statement + '\n\t' + table['type'] + ' JOIN ' + table['name'] + ' AS ' + table[
                'alias'] + ' ON ' + table['jcl']['name'] + '=' + table['jcr']['name']

    wc = select_dict['where']
    if wc:
        for idx, col in enumerate(wc):
            if idx == 0:
                statement = statement + '\nWHERE\n\t' + col['column'] + '' +  col['operator'] + ''+ \
                            col['value']
            else:
                statement = statement + '\n\t' + col['logical']+' ' + col.get('column', '') + ' ' + col.get('operator', '') + \
                            col.get('value', '')
    ##  print(groupClause(i))
    gc = select_dict['group by']
    if gc:
        for idx, col in enumerate(gc):
            if idx == 0:
                statement = statement + '\nGROUP BY\n\t' + col['column']
            else:
                statement = statement + '\n\t,' + col['column']
    hc = select_dict['having']
    # print(hc)
    if hc:
        for idx, col in enumerate(hc):
            if idx == 0:
                statement = statement + '\nHAVING\n\t' + col['function'] + '(' + col['col'] + ')' + col['operator'] + \
                            col['value']
            else:
                statement = statement + '\n\t' + col['function'] + '(' + col['col'] + ')' + col['operator'] + col[
                    'value']
    return(statement)