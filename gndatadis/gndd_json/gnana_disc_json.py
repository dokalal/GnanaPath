
#LaodConfig utility class present in util package which parses the yml file to load the file path
from gnutils.read_props import LoadConfigUtil

#filter_chars is a decorated function present in util package to filter special characters
from gnutils.replace_spl_chars import filter_chars

import pandas as pd
import json
import os

# Class to read and parse Json file
class ParseJson:
    # The constructor sets the file path to be local or remote. If no path is set the default is local.
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
        #df.columns = df.columns.str.replace(r"[.#$,\/]", "_")
        # Call the decorated function filter_chars present in the util package 
        # Usage filter_chars('chartobereplaced', 'withreplacechar', df.col, replace=True)
        df.columns = filter_chars("\.","_",df.columns,replace=True)
        
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
file_path=data['FilePath']['local_path']
#file_path=data['FilePath']['remote_path']['url']
file_path=os.path.join(LoadConfigUtil.SAMPLE_DATA,file_path)



#p=ParseJson(type='remote',path=file_path)
p=ParseJson(path=file_path)
col,row=p.ret_header_dict()
print(col)
print(row)




