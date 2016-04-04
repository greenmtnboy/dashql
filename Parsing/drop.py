import re

#this deals with drop table statements
def compile(statement):
    # statement_type={1:'SELECT', 2:'INSERT', 3:'DROP', 4:'CTAS', 5:'UPDATE', 6:'ALTER', 7:'CREATE', 8:'CREATE LIKE', 99:'UNSURE'}
    drop={'type': 3}
    drop['table'] = re.search('DROP TABLE(?: IF EXISTS)?(.*)', statement, re.DOTALL | re.IGNORECASE).group(1).strip()
    return drop

def text(dict):
    return 'DROP TABLE IF EXISTS '+dict['table']
