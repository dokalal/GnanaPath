#!/usr/bin/env python
# coding: utf-8

# In[1]:


"""
 - A General purpose Utility class to read the yml file 
 - The yml file could contain db strings, file paths etc
 - Currently this file can be used to read the json file locally/remotely

"""
import yaml

class LoadConfigUtil:
    
    @staticmethod
    def get_config_data():
        try:
            with open('config.yml') as f:
                data = yaml.safe_load(f)
            return data
        except Exception as e:
            print("***** An Exception occured in the LoadConfigUtil class ***** \n {}".format(str(e)))


import pandas as pd
import json

class ParseJson:
    
    def __init__(self, **kwargs):
        if kwargs is not None:
            if 'type' in kwargs:
                self.file_type=kwargs['type']
            else:
                self.file_type='local'
            
            if 'path' in kwargs:
                self.file_path=kwargs['path']
            else:
                raise ValueException('Missing file path')
    
    #return header dict
    def ret_header_dict(self,drop_col=False):
        # read the json file and return a flattened dataframe 
        
        if self.file_type=='remote':
            df=self.read_json_url()
        else:
            with open(self.file_path) as data_file:    
                data = json.load(data_file)
                df = pd.json_normalize(data)
        df.columns = df.columns.str.replace(r"[.#$,\/]", "_")
        if drop_col==True:
            df_new = self.drop_columns(df)
        else:
            df_new =df
        # create a dict containing the column header
        new_col_len = len(df_new.columns)
        col_dict = {x:df_new.columns[x] for x in range(new_col_len)}
        
        # create data dict
        row_dict =[dict(df_new.iloc[tmp,:]) for tmp in range(df_new.shape[0])]
       
           
        return col_dict, row_dict[0]
    
    #Read Json from a url
    def read_json_url(self):
        import urllib.request
        try:
            operUrl = urllib.request.urlopen(self.file_path)
            data = operUrl.read()
            jsonData = json.loads(data)
            df=pd.json_normalize(jsonData)
            return df
        except Exception as e:
            print("Error reading url data", operUrl.getcode())
    
    #Drop columns if the json data has list type, process later for further flatenning
    def drop_columns(self, df):
        df_len=len(df.columns)
        print(df_len)
        header =df.iloc[0]
        col_exclude = [c for c in range(df_len) if type(header[c])==list]
        print(col_exclude)
        df2=df.drop(df.columns[col_exclude],axis=1)
        return df2


data=LoadConfigUtil.get_config_data()
#file_path =data['FilePath']['local_path_list'][1]
#file_path=data['FilePath']['local_path']
file_path=data['FilePath']['remote_path']['url']
print(file_path)



p=ParseJson(type='remote',path=file_path)
col,row=p.ret_header_dict()
print(col)
print(row)




