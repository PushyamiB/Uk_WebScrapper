import json
import pandas as pd
import pymssql
import os
def splitFileIntoListOfDict(filename):
    contents = open(filename, "r").read()
    contents=json.loads(contents)
    data = [(item) for item in contents]
    #print(data)
    return data
def connectToDb():
    server = '198.27.98.137'
    user = 'blockchain'
    paswd = 'Qazxsw123!@#'
    database = 'blockchain'
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
file_list = splitDataNewLine('/home/kumar/Amos/blockchain/data_insertion_schema.txt')
for i in file_list:
    try:
        lod = splitFileIntoListOfDict(i)
        df = pd.DataFrame(lod)
        cursor,conn = connectToDb()
        
        mappingSchema = loadJsonData('/home/kumar/Amos/blockchain/mapping_schema_blockchain.json')
       
        col_join = ','.join([i['sql_col'] for i in mappingSchema['attributes']])
        type_join = ','.join([i['sql_type'] for i in mappingSchema['attributes']])
        # tuple_col = tuple([eval(i['pyt_field']) for i in config['attributes'] ])
        insertionQuerry = 'insert into {}({}) values({}) '.format(mappingSchema['table'],col_join,type_join)
        print(insertionQuerry )
        try:
            df = df.fillna('nan')
        except:
            df=df.fillna(0)
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
        #print('sample insertion array : ',insertionQuerryArr[:5])
        print("Querry : ",insertionQuerry)
        print("Querry Array : ",insertionQuerryArr)
        cursor.executemany(insertionQuerry,insertionQuerryArr)
        #print("Executed")
        conn.commit()
        os.remove(str(i))
    except Exception as e:
        print("Exception encountered : ",e)
