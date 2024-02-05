from .utils import queryCDA
from ._constants import *
import pandas as pd
import json


class CwmsLocMixin:
    def retreive_loc_group(self,p_loc_group_id,p_category_id,p_office_id, return_type='df'):

        endPoint = f'location/group/{p_loc_group_id}'

        params = {
            OFFICE_PARAM: p_office_id,
            "category-id": p_category_id
        }

        headerList={
            "Accept": HEADER_JSON_V1
        }

        responce = queryCDA(self,endPoint,params,headerList,return_type,dict_key = ['assigned-locations'])

        #if dataframe:
        #   responce = pd.DataFrame(responce['assigned-locations'])
        
        return responce

    def retreive_locs(self, p_office_id=None, p_loc_ids = None, p_units = None, p_datum = None,  return_type='df'):
        
        endPoint = 'locations'
        
        params = {
            OFFICE_PARAM: p_office_id,
            'names' : p_loc_ids,
            'units' : p_units,
            'datum' : p_datum,
        }
        headerList={
            "Accept": HEADER_JSON_V2
        }
                      
        responce = queryCDA(self,endPoint,params,headerList,return_type,dict_key = ['locations','locations'])  
        #if output = 'dataframe':
            #responce = 
        return responce            
                      
    def ExpandLocations(df):

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