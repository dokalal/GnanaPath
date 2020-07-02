import csv
import json
import pandas as pd
#import gndw_neo4j_db as nj

def main():
    try:
        file='../gnsampledata/customer.csv'
        file_name=file.split('.')[0]
        inputds=pd.read_csv(file,skiprows=0)
        #print(inputds[1:2]) 
        inputds_columns=(list(inputds.columns))
        inputds_count= len(inputds_columns)
        inputds_dic = {'attr'+str(x+1): inputds_columns[x] for x in range(inputds_count)}
        print(inputds_dic)    
        type(inputds.columns)
        print(file_name)

        for x in range(inputds.shape[0]) :
            dic=dict(inputds.iloc[x,:])
            print("***********************************")
            print(dic)
            print("***********************************")

        #print(type(inputds_dic))
        #inputds_dic
    except Exception as e:
        print('\nMain.Error: Some error occured!\n see the details : ' + str(e))

main()
