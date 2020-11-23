############################################################
# gntests/testConnConfig.py
#
#   Setup neo4j connection parameters
#   Create connection config file
#   Verify connection
#
############################################################
import os
import csv
import json
import sys
import numpy as np
import warnings
import logging
import pandas

curentDir=os.getcwd()
listDir=curentDir.rsplit('/',1)[0]
if listDir not in sys.path:
    sys.path.append(listDir)


from  gnutils.replace_spl_chars import gnutils_filter_json_escval;
from  gnutils.get_config_file import get_config_neo4j_conninfo_file;
from  gndwdb.gndwdb_neo4j_conn import gndwdb_neo4j_conn_check_api;


class       gntestConnConfig:

     def    __init__(self, verbose):
        ###self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.cfg_file = get_config_neo4j_conninfo_file();
        self.verbose = verbose;
        if (self.verbose > 2):
            print('testConnConfig: init testConnConfig test cfg_file:'+self.cfg_file);

            
     def     test_conn_check(self):
         
         if (self.verbose > 3):
             print('testConnConfig: testConnConfig test conn check' );
            # Don't forget to close the driver connection when you are finished withit
         ret = gndwdb_neo4j_conn_check_api(self.cfg_file, self.verbose);

         if (ret == 0):
             print('testConnConfig: test connection check SUCCESS');
         else:
             print('testConnConfig: ERROR test connection check failed');



if   __name__ == "__main__":

    verbose = 5;
    print('Start the script');
    testp = gntestConnConfig(verbose);
    testp.test_conn_check();
    
   

    
