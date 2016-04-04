import re
import Execution
import select
import ctas
import update
import create
import create_like
import insert
import drop
import uuid
import sys

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

# this deals with comments and prepares the script to be split
def script_prep(script):
    script = re.search('BEGIN;(.*)COMMIT;', script, re.DOTALL | re.IGNORECASE).group(1)
    mlcomments = re.findall('\/\*\*(.*?)\*\*\/', script, re.DOTALL | re.IGNORECASE)
    slcomments = re.findall('--(.*?)\n', script, re.DOTALL | re.IGNORECASE)
    astats = re.findall('(select.*?analyze.*?;)', script, re.IGNORECASE)
    if astats:
        for stat in astats:
            print('stripping analyze statistics: '+stat)
    if mlcomments:
        for comment in mlcomments:
            print('stripping multline comment: '+comment)
    if slcomments:
        for comment in slcomments:
            print('stripping single line comment: '+comment)

    script = re.sub('\/\*\*(.*?)\*\*\/', '', script, 0, re.DOTALL | re.IGNORECASE)
    script = re.sub('--.*?\n', '', script, 0, re.DOTALL | re.IGNORECASE)
    script = re.sub('(select.*?analyze.*?;)\n', '', script, 0, re.IGNORECASE)
    return script

#statement_type={1:'SELECT', 2:'INSERT', 3:'DROP', 4:'CTAS', 5:'UPDATE', 6:'ALTER', 7:'CREATE', 8:'CREATE LIKE', 99:'UNSURE'}
#this statement determines how to build the text of the statement
def compile_statement(statement, type):
    #Select
    if type == 1:
        raw = select.compile(statement)
        compiled = select.text(raw)
    #insert
    elif type == 2:
        raw = insert.compile(statement)
        compiled = insert.text(raw)
    #Drop
    elif type == 3:
        raw = drop.compile(statement)
        compiled = drop.text(raw)
    #CTAS
    elif type == 4:
        raw = ctas.compile(statement)
        compiled = ctas.text(raw)
    #Update
    elif type == 5:
        raw = update.compile(statement)
        compiled = update.text(raw)
        #Alter
        # TODO: Alter entity statements
        '''
        elif type == 6:
            raw = compile_alter(statement)
            compiled = text_alter(raw)
        '''
    #Create
    elif type == 7:
        raw = create.compile(statement)
        compiled = create.text(raw)
    # Create like
    elif type == 8:
        raw = create_like.compile(statement)
        compiled = create_like.text(raw)
    else:
        raw=statement
        compiled= 'underfined'
    return {'JSON': raw,'Compiled': compiled+ ';\n'}

def varsubstitution(script,variables):
    for variable in variables:
        script = re.sub(variable['ident'], variable['val'], script, re.DOTALL | re.IGNORECASE)

    return script

# this is the master compilation script
def compile_script(procedure, text=0, run=0, variables=() ):
    #Remove comments
    guid = procedure['guid']
    name = procedure['name']
    script = procedure['body']

    #ToDO: store comments for later
    script=script_prep(script)

    if variables:
        script=varsubstitution(script,variables)


    #split between statements

    parsed=script.strip().split(';')

    #clean out trailing whitespace
    parsed=filter(None, parsed)

    #indicate how many statements were found
    print('Parsing ' + str(len(parsed)) + ' statements')
    scriptJSON=[]
    scriptfull=[]
    #iterate on statements
    for idx, statement in enumerate(parsed):

        #strip off trailing whitespace
        statement=statement.strip()

        #identify query
        type=query_type(statement.upper())

        #report what type of query has been identified
        print('Type:'+statement_type[type])
        print(statement)
        #compile the statement into parsed and raw input
        comp_stat = compile_statement(statement, type)

        try:
            comp_stat = compile_statement(statement, type)
        except:
            print('exception')
            print(idx)
            print(type)
            print(statement)

        scriptfull.append(comp_stat)
        scriptJSON.append(comp_stat['JSON'])

    #now that we've compiled, let's evaluate
    conn = Execution.connect(name,guid,scriptJSON)
    for statement in scriptfull:
        #determine what to output controller
        if text == 1:
            print(statement['Compiled'])
        else:
            print(statement['JSON'])

        #do we want to run
        if run == 1:
            # build a guid
            queryguid = uuid.uuid4().int
            print(Execution.execQuery(conn, statement['Compiled'], queryguid, statement['JSON'], guid))
            # print(text)




