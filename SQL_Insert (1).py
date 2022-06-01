import json
import pandas as pd
import pymssql
import os
from icecream import ic
def splitFileIntoListOfDict(filename):
    contents = ic(open(filename, "r").read())
    data = [ic(json.loads(str(item))) for item in contents.strip().split('\n')]
    return data
def connectToDb():
    server = '185.136.157.12'
    user = 'ukws_user'
    paswd = 'NWzDRtnMDTxf5DqT'
    database = 'ukwebscrapper'
    conn = pymssql.connect(server, user, paswd, database)
    return [conn.cursor(),conn]
def loadJsonData(filename):
    # Opening JSON file
    f = open(filename)
    # returns JSON object as
    # a dictionary
    data = json.load(f)
    # Closing file	
    f.close()
    return data
def splitDataNewLine(filename):
    with open(filename, 'r') as f:
        url_list = f.read().split("\n")
    return url_list
file_list = [i for i in splitDataNewLine('/home/dev04/webScrapingProj/data_insertion_schema.txt') if len(i)>0]
for i in file_list:
    ic(i)
    try:
        lod = splitFileIntoListOfDict(ic(i))
    except Exception as e:
        print("Exception - ",e," - \nFile doesent exist or have any data . Checking the next available file","\n",80*'-',)
        continue
    df = pd.DataFrame(lod)
    cursor,conn = connectToDb()
    mappingSchema = loadJsonData('/home/dev04/webScrapingProj/mapping_schema.json')
    col_join = ','.join(['"'+i['sql_col']+'"' for i in mappingSchema['attributes']])
    type_join = ','.join([i['sql_type'] for i in mappingSchema['attributes']])
    # tuple_col = tuple([eval(i['pyt_field']) for i in config['attributes'] ])
    insertionQuerry = 'insert into {}({}) values({}) '.format(mappingSchema['table'],col_join,type_join)
    print(insertionQuerry )
    df = df.fillna('nan')
    #Converting all non string type to string type
    for mpField in mappingSchema['attributes']:
        if(mpField['p_type']=='str'):
            df[mpField['pyt_field']]=df[mpField['pyt_field']].astype(str)
        elif(mpField['p_type']=='int'):
            df[mpField['pyt_field']]=df[mpField['pyt_field']].astype(int)
        elif(mpField['p_type']=='datetime'):
            df[mpField['pyt_field']]=pd.DatetimeIndex(df[mpField['pyt_field']])        
    python_col_list = [i['pyt_field'] for i in mappingSchema['attributes']]
    print('Python filed list : ',python_col_list)
    insertionQuerryArr = [tuple(x) for x in df[python_col_list].to_numpy()]
    # print('sample insertion array : ',insertionQuerryArr[:5])
    print("Querry : ",insertionQuerry)
    print("Querry Array : ",insertionQuerryArr)
    cursor.executemany(insertionQuerry,insertionQuerryArr)
    conn.commit()
    os.remove(str(i))
