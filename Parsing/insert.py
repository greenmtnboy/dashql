import re
import select
# handle create table as statements
def compile(statement):
    # statement_type={1:'SELECT', 2:'INSERT', 3:'DROP', 4:'CTAS', 5:'UPDATE', 6:'ALTER', 7:'CREATE', 8:'CREATE LIKE', 99:'UNSURE'}
    create = {'type': 2}
    #TODO: Direct insert
    #if re.match('CREATE.*(?:TEMP[\s\n]|TEMPORARY[\s\n])', statement, re.DOTALL | re.IGNORECASE):
    #    create['tempflag'] = 1
    #else:
    #    create['tempflag'] = 0
    create['directflag'] = 0
    #TODO: Capture flags
    statement = re.sub('\/\*(?:[\s\n]*)?\+(.*?)\*\/', '', statement)
    #print(statement)
    create['table'] = re.search('(?:INTO[\s\n]*)(.*?)(?:[\s\n]SELECT)', statement, re.DOTALL | re.IGNORECASE).group(1).strip()


    query=re.search('(SELECT.*?)$', statement, re.DOTALL | re.IGNORECASE).group(1)
    #query='select this'
    #create rest
    create['query'] = select.compile(query)
    return create

def text(ctasdict):
    if ctasdict['directflag'] == 1:
        retstring = 'INSERT /* +direct */ INTO '+ctasdict['table'] + '\n' + select.text(ctasdict['query'])
    else:
        retstring = 'INSERT INTO ' + ctasdict['table'] + '\n' + select.text(ctasdict['query'])
    return retstring