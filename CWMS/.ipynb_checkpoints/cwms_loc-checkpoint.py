from utils import queryCDA
import pandas as pd
import json

def retreive_loc_group(apiRoot,p_loc_group_id,p_category_id,p_office_id,output = 'dataframe'):
    
    endPoint = f'/location/group/{p_loc_group_id}'
    
    params = {
        "office": p_office_id,
        "category-id": p_category_id
    }
    
    headerList={
        "Accept": "application/json"
    }
    
    responce = queryCDA(apiRoot+endPoint,params,headerList,output,dict_key = 'assigned-locations')
    
    #if dataframe:
    #   responce = pd.DataFrame(responce['assigned-locations'])
    
    return responce


def ExpandAliases(df):
    
    df_alias = pd.DataFrame()
    temp = df.aliases.apply(pd.Series)
    for i in temp.columns:
        temp2 = temp[i].apply(pd.Series).dropna(how='all')
        temp2 = temp2.dropna(how='all', axis = 'columns')
        temp2 = temp2.reset_index()
        df_alias = pd.concat([df_alias,temp2], ignore_index=True)
    df_alias = df_alias.drop_duplicates(subset=['locID','name'], keep='last')
    df_alias = df_alias.pivot(index='locID', columns='name',values = 'value')
    df_alias = pd.concat([df, df_alias], axis=1)
    return df_alias