import re
import Execution

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
    fc = re.findall('from (.*?\s.*?)(?:[\s\n]|GROUP|Where)',script,re.DOTALL | re.IGNORECASE)
    if fc:
        ret={}
        ret = t_name_alias(ret, fc[0])
        fcalias=fc[0].split()
        return ret


# this finds joins
def joinClause(script):
    retclause=[]
    joinlist=[]
    joins = re.findall('(left|right|full|inner)(?:[\s\n]*outer?[\s\n]*join|[\s\n]*join)[\s\n]*(.*?)[\s\n]*on\s*(.*?)=(.*?)[\s\n]',script,re.DOTALL | re.IGNORECASE)
    if joins:
        for join in joins:
            joindict={}
            joindict['type']=join[0]
            joindict=t_name_alias(joindict,join[1])
            con1 = parse_join(join[2])
            con2 = parse_join(join[3])
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
        allcol=re.split('(?:AND|OR)',wc.group(1), re.DOTALL | re.IGNORECASE)
        for crit in allcol:
            hav={}
            hav['column'] = re.search('(.*?)(?:=|>|<|<=|>=|\sin)', crit, re.DOTALL | re.IGNORECASE).group(1)
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
def compile_select(statement):
    ret = {}
    select = selectClause(statement)
    cols=[]
    for idx, col in enumerate(select):
            cols.append(col)
    ret['select']=cols
    ret['into'] = intoClause(statement)
    ret['from'] = fromClause(statement)
    ret['joins'] = joinClause(statement)
    ret['where'] = whereClause(statement)
    ret['group by'] = groupClause(statement)
    ret['having'] = havingClause(statement)
    return ret

# this is used to parse select statements
def text_select(select_dict):
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
            statement = statement + '\n\t' + table['type'] + ' join ' + table['name'] + ' AS ' + table[
                'alias'] + ' ON ' + table['jcl']['name'] + '=' + table['jcr']['name']

    wc = select_dict['where']
    if wc:
        for idx, col in enumerate(wc):
            if idx == 0:
                statement = statement + '\nWHERE\n\t' + col['column'] + col['operator'] + \
                            col['value']
            else:
                statement = statement + '\n\t,' + col['operator'] + \
                            col['value']
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

# dictionary of possible statement types
# This will be expanded to include pythonic syntactic sugar
statement_type={1:'SELECT', 2:'INSERT', 3:'DROP', 4:'CTAS', 5:'UPDATE', 6:'ALTER', 7:'CREATE', 8:'CREATE LIKE', 99:'UNSURE'}

# this is used to identify what kind of statement it is
def query_type(stat):
    stat=stat.upper()
    if stat.find('DROP') > -1:
        return 3
    elif stat.find('CREATE ') > -1:
        if re.search('[\s\n]SELECT[\s\n]', stat, re.DOTALL | re.IGNORECASE):
            return 4
        elif stat.find(' LIKE ')> -1:
            return 8
        else:
            return 7
    elif stat.find('UPDATE') > -1:
        return 5
    elif stat.find('ALTER') > -1:
        return 6
    elif stat.find('INSERT') > -1:
        return 2
    elif stat.find('SELECT') > -1:
        return 1
    else:
        return 99


#this deals with drop table statements
def compile_drop(statement):
    drop={}
    drop['table'] = re.search('DROP TABLE(?: IF EXISTS)?(.*)', statement, re.DOTALL | re.IGNORECASE).group(1).strip()
    return drop

def text_drop(dict):
    return 'DROP TABLE IF EXISTS '+dict['table']+';'

# handle create table as statements
def compile_ctas(statement):
    create={}
    # remove hints
    # TODO: store hints
    statement = re.sub('\/\*(?:[\s\n]*)?\+(.*?)\*\/', '', statement)
    if re.match('CREATE.*(?:TEMP[\s\n]|TEMPORARY[\s\n])', statement, re.DOTALL | re.IGNORECASE):
        create['tempflag'] = 1
    else:
        create['tempflag'] = 0
    create['table'] = re.search('TABLE[\s\n]*(.*?)[\s\n]*(ON(?:\s|\n)|AS(?:\s|\n))', statement, re.DOTALL | re.IGNORECASE).group(1).strip()
    query=re.search('AS(.*?)(?:$|ENCODED|SEGMENTED)', statement, re.DOTALL | re.IGNORECASE).group(1)
    create['query']=compile_select(query)
    return create

def text_ctas(ctasdict):
    if ctasdict['tempflag']==1:
        retstring = 'CREATE LOCAL TEMPORARY TABLE '+ctasdict['table'] + ' ON COMMIT PRESERVE ROWS AS\n' + text_select(ctasdict['query'])
    else:
        retstring = 'CREATE TABLE ' + ctasdict['table'] + ' AS\n' + text_select(ctasdict['query'])
    return retstring

def text_drop(dict):
    return 'DROP TABLE IF EXISTS '+dict['table']+';'

# this deals with comments and prepares the script to be split
def script_prep(script):
    script = re.search('BEGIN;(.*)COMMIT;', script, re.DOTALL | re.IGNORECASE).group(1)
    mlcomments = re.findall('\/\*\*(.*?)\*\*\/', script, re.DOTALL | re.IGNORECASE)
    slcomments = re.findall('--(.*?)\n', script, re.DOTALL | re.IGNORECASE)
    """
    if mlcomments:
        for comment in mlcomments:
            print(comment)
    if slcomments:
        for comment in slcomments:
            print(comment)
    """
    script = re.sub('\/\*\*(.*?)\*\*\/', '', script)
    script = re.sub('--(.*?)\n', '', script)
    return script



# this is the master compilation script
def compile_script(script, text=0, run=0):
    #split into statements
    script=script_prep(script)
    parsed=script.strip().split(';')
    #clean out trailing whitespace
    parsed=filter(None, parsed)
    print('Parsing ' + str(len(parsed)) + ' statements')
    #iterate on statements
    for idx, statement in enumerate(parsed):
        if idx<=3:
            statement=statement.strip()
            #print('Parsing statement: '+str(idx+1))
            type=query_type(statement.upper())
            #print('Type:'+statement_type[type])
            if type == 3:
                raw = compile_drop(statement)
                compiled=text_drop(raw)
                if text==1:
                    print(compiled+'\n')
                else:
                    print(raw)
                if run == 1:
                    print(Execution.execQuery(compiled))
                #print(text)
            elif type == 1:
                print(statement+';\n')
                #compile_select(statement)
            elif type == 4:
                #print(statement)
                raw = compile_ctas(statement)
                compiled = text_ctas(raw)
                if text == 1:
                    print( compiled+';\n')
                else:
                    print(raw)
                if run == 1:
                    print(Execution.execQuery(compiled))
            else:
                print(statement+'\n')




