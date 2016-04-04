import re
import select
# handle create table as statements
def compile(statement):
    # statement_type={1:'SELECT', 2:'INSERT', 3:'DROP', 4:'CTAS', 5:'UPDATE', 6:'ALTER', 7:'CREATE', 8:'CREATE LIKE', 99:'UNSURE'}
    create={'type': 8}
    # remove hints
    # TODO: store QUERY hints
    statement = re.sub('\/\*(?:[\s\n]*)?\+(.*?)\*\/', '', statement)

    if re.match('INCLUDING PROJECTIONS', statement, re.DOTALL | re.IGNORECASE):
        create['projflag'] = 1
    else:
        create['projflag'] = 0
    create['table'] = re.search('TABLE[\s\n]*(.*?)[\s\n]*LIKE', statement, re.DOTALL | re.IGNORECASE).group(1).strip()
    create['sourcetable'] = re.search('LIKE[\s\n]*(.*?)(?:[\s\n]*INCLUDING|$)', statement, re.DOTALL | re.IGNORECASE).group(1).strip()

    return create

def text(ctasdict):
    if ctasdict['projflag']==1:
        projstring = ' INCLUDING PROJECTIONS'
    else:
        projstring=''

    retstring = 'CREATE TABLE ' + ctasdict['table'] + ' LIKE ' + ctasdict['sourcetable'] + projstring

    return retstring