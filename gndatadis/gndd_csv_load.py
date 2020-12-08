import os,sys
curentDir = os.getcwd()
listDir = curentDir.rsplit('/', 1)[0]
gndwdbDir = listDir + '/gndwdb'
if listDir not in sys.path:
    sys.path.append(listDir)
if gndwdbDir not in sys.path:
    sys.path.append(gndwdbDir)


from gndwdb_neo4j_dbops import gndw_metarepo_metanode_add_api, gndw_datarepo_datanode_add_api
from gnutils.replace_spl_chars import filter_chars
from gnutils.read_props import LoadConfigUtil
import csv
import json
import pandas as pd
import neo4j

# if '/home/jovyan/gnanapath' not in sys.path:
#    sys.path.append('/home/jovyan/gnanapath')

# if '/home/jovyan/gnanapath/gndwdb' not in sys.path:
#    sys.path.append('/home/jovyan/gnanapath/gndwdb')

# LaodConfig utility class present in util package which parses the yml
# file to load the file path

# filter_chars is a decorated function present in util package to filter
# special characters


# Class to read CSV file

class ParseCSV:
    # The constructor sets the file path to be local or remote. If no path is
    # set the default is local.
    def __init__(self, **kwargs):
        if kwargs is not None:
            if 'type' in kwargs:
                self.file_type = kwargs['type']
            else:
                self.file_type = 'local'

            if 'path' in kwargs:
                self.file_path = kwargs['path']
            else:
                raise ValueException('Missing file path')

            if self.file_type == 'json':
                self.df = self.read_json_url()
            elif self.file_type == 'csv':
                self.df = pd.read_csv(self.file_path, skiprows=0)
            else:
                raise ValueException('Not Valid file Extension')

    # return header dict

    def ret_header_dict(self):
        # Call the decorated function filter_chars present in the util package
        # Usage filter_chars('chartobereplaced', 'withreplacechar', df.col,
        # replace=True)
        df_new = self.df
        df_new.columns = filter_chars(r"\.", "_", df_new.columns, replace=True)
        # create a dict containing the column header
        new_col_len = len(df_new.columns)
        #col_dict = {x:df_new.columns[x] for x in range(new_col_len)}
        self.col_dict = {
            'attr' + str(x + 1): df_new.columns[x] for x in range(new_col_len)}

        # create data dict
        #row_dict =[dict(df_new.iloc[tmp,:]) for tmp in range(df_new.shape[0])]
        # return col_dict, row_dict[0]

    # Read Json from a url
    def read_json_url(self):
        import urllib.request
        try:
            operUrl = urllib.request.urlopen(self.file_path)
            data = operUrl.read()
            jsonData = json.loads(data)
            df = pd.json_normalize(jsonData)
            return df
        except Exception as e:
            print("Error reading url data", operUrl.getcode())

    # Drop columns if the json data has list type, process later for further flatenning
#     def drop_columns(self, df):
#         df_len=len(df.columns)
#         print(df_len)
#         header =df.iloc[0]
#         col_exclude = [c for c in range(df_len) if type(header[c])==list]
#         print(col_exclude)
#         df2=df.drop(df.columns[col_exclude],axis=1)
#         return df2

# Class to read CSV file


class gndwdbCall:
    def __init__(self, **kwargs):
        self.node_name = kwargs['fil_name']
        self.node_attr_list = kwargs['colum_list']
        self.node_df = kwargs['df_data']
        self.verbose = 4
        self.datanode_name = ''

    def gndwdbMetaNode(self):
        self.meta_ret = gndw_metarepo_metanode_add_api(
            self.node_name, self.node_attr_list, self.verbose)
        if (self.verbose > 1):
            print("gndw_neo4j_db: metaadd node returned " + str(self.meta_ret))

    def gndwdbDataNode(self):
        self.data_ret = gndw_datarepo_datanode_add_api(
            self.node_name,
            self.node_attr_list,
            self.datanode_name,
            self.dic,
            self.verbose)
        if (self.verbose > 1):
            print("gndw_neo4j_db: Data node returned " + str(self.data_ret))

    def gndwdbDataNodeCall(self):
        df = self.node_df
        for x in range(df.shape[0]):
            self.dic = dict(df.iloc[x, :])
            print("***********************************")
            print(self.dic)
            datanode_name = self.node_name + str(x + 1)
            self.datanode_name = filter_chars(
                r"\.", "_", datanode_name, replace=True)
            print(self.datanode_name)
            self.gndwdbDataNode()


def gndwdbDataUpload(filePath, fileName):
    file_name = fileName
    file_path = os.path.join(filePath, file_name)
    print(file_name)
    print(file_path)
    file_name, file_ext = file_name.split(".")
    p = ParseCSV(path=file_path, type=file_ext)
    p.ret_header_dict()
    gndb = gndwdbCall(fil_name=file_name, colum_list=p.col_dict, df_data=p.df)
    gndb.gndwdbMetaNode()
    gndb.gndwdbDataNodeCall()


def gndwdbData_check():
    data = LoadConfigUtil.get_config_data()
    file_name = data['FilePathCSV']['local_path'].split('.')[0]
    print(file_name)
    file_path = data['FilePathCSV']['local_path']
    file_path = os.path.join(LoadConfigUtil.SAMPLE_DATA, file_path)
    print(file_path)
    p = ParseCSV(path=file_path)
    p.ret_header_dict()
    print(p.col_dict)
    # gndb=gndwdbCall(fil_name=file_name,colum_list=p.col_dict,df_data=p.df)
    # gndb.gndwdbMetaNode()
    # gndb.gndwdbDataNodeCall()

    for file_path in data['FilePathCSV']['local_path_list']:
        file_name = file_path.split('.')[0]
        print(file_name)
        file_path = os.path.join(LoadConfigUtil.SAMPLE_DATA, file_path)
        print(file_path)
        p = ParseCSV(path=file_path)
    #     print(p.df)
        p.ret_header_dict()
        print(p.col_dict)
        gndb = gndwdbCall(
            fil_name=file_name,
            colum_list=p.col_dict,
            df_data=p.df)
        gndb.gndwdbMetaNode()
        gndb.gndwdbDataNodeCall()


if __name__ == "__main__":
    filePath = "/home/jovyan/GnanaPath/gnappsrv/uploads"
    fileName = "product_sample.csv"
    gndwdbDataUpload(filePath, fileName)
