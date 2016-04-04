import re
import select
# handle create table as statements
# statement_type={1:'SELECT', 2:'INSERT', 3:'DROP', 4:'CTAS', 5:'UPDATE', 6:'ALTER', 7:'CREATE', 8:'CREATE LIKE', 99:'UNSURE'}
def compile(statement):
    create={'type': 4}
    # remove hints
    # TODO: store QUERY hints
    statement = re.sub('\/\*(?:[\s\n]*)?\+(.*?)\*\/', '', statement)
    if re.match('CREATE.*(?:TEMP[\s\n]|TEMPORARY[\s\n])', statement, re.DOTALL | re.IGNORECASE):
        create['tempflag'] = 1
    else:
        create['tempflag'] = 0
    create['table'] = re.search('TABLE[\s\n]*(.*?)[\s\n]*(ON(?:\s|\n)|AS(?:\s|\n))', statement, re.DOTALL | re.IGNORECASE).group(1).strip()
    query=re.search('AS(.*?)(?:$|ENCODED|UNSEGMENTED|SEGMENTED)', statement, re.DOTALL | re.IGNORECASE).group(1).strip()

    create['query']=select.compile(query)

    return create

def text(ctasdict):
    if ctasdict['tempflag']==1:
        retstring = 'CREATE LOCAL TEMPORARY TABLE '+ctasdict['table'] + ' ON COMMIT PRESERVE ROWS AS\n' + select.text(ctasdict['query'])
    else:
        retstring = 'CREATE TABLE ' + ctasdict['table'] + ' AS\n' + select.text(ctasdict['query'])
    return retstring