####################################################################
# GnanaWarehouse_DB module does following tasks
#    - Add nodes to neo4j
#    - Add attributes to node to neo4j
###################################################################
import os
import csv
import json
import sys
import numpy as np
import neo4j
import warnings

import re

warnings.simplefilter('ignore')

from neo4j import GraphDatabase, basic_auth


#### Append system path
curentDir=os.getcwd();
listDir=curentDir.rsplit('/',1)[0];
sys.path.append(listDir);

from gnutils.get_config_file import get_config_neo4j_conninfo_file;


def           gndwdb_neo4j_conn_connect(uri, userName, passw, verbose):

    # Database Credentials
    #uri = "bolt://192.168.0.59"
    #userName = "neo4j"
    #password = os.getenv("NEO4J_PASSWORD")
    #password='ca$hc0w'

    try:
        # Connect to the neo4j database server
        graphDB_Driver = GraphDatabase.driver(uri, auth=(userName, passw))

        if (verbose >= 1):
            print("gndw_neo4j_connect: Successfully connected to "+uri)
    except:
        print("gndw_neo4j_connect: ERROR in connecting Graph")
        graphDB_Driver = '';
    finally:
        print("gndw_neo4j_connect: op is completed")
        return graphDB_Driver;



def             gndwdb_neo4j_conn_metarepo(verbose):

    # Database Credentials
    uri = "bolt://172.17.0.10"
    userName = "neo4j"
    #password = os.getenv("NEO4J_PASSWORD")
    passw = 'ca$hc0w'
    ret = 0

    ### Check db connection
    graph_conn  = gndwdb_neo4j_conn_connect(uri, userName, passw,verbose)

    return graph_conn



def            gndwdb_neo4j_conn_datarepo(verbose):

    # Database Credentials
    uri = "bolt://172.17.0.6"
    userName = "neo4j"
    #password = os.getenv("NEO4J_PASSWORD")
    passw = 'ca$hc0w'
    ret = 0

    ### Check db connection
    graph_conn  = gndwdb_neo4j_conn_connect(uri, userName, passw, verbose)

    return graph_conn


def           gndwdb_neo4j_conn_check_api(cfgfile, verbose):

      if (verbose > 3):
          print('gndwdb_neo4j_conn: parsing cfg file'+cfgfile);
          
      ###with open(os.path.join(JSON_PATH, jfile)) as json_file:
      with open(cfgfile) as cfg_jsonf: 
          cfg_json = json.load(cfg_jsonf);
          print(cfg_json);

          def_config = cfg_json['_default'];
          nconfig = def_config['1'];
          print(nconfig);
          # read config list
          uri = "bolt://"+nconfig['serverIP']+":7689";
          userName = nconfig['username'];
          passw = nconfig['password'];
          graph_conn = gndwdb_neo4j_conn_connect(uri, userName, passw, verbose);
          if graph_conn is None:
             ### Unable to connect
             print('Error! Unable to connect to graph server'); 
             return -1;
          print('Graph Server connected! '+uri);
          print(graph_conn);
          #else:

          # return -1;          


        
if   __name__ == "__main__":

    verbose = 5;
    ###cfgfile = './server_config.json';
    cfg_file = get_config_neo4j_conninfo_file();
    if (verbose > 3):
       print('gndwdb_neo4j_conn: path for neo4j conn cfg: '+cfg_file);
    
    gndwdb_neo4j_conn_check_api(cfg_file, verbose);
