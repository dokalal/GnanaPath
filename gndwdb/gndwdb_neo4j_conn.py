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


def       gndwdb_neo4j_conn_connect(uri, userName, passw, verbose):
        
        # Database Credentials        
        try:
            # Connect to the neo4j database server
            graphDB_Driver = GraphDatabase.driver(uri, auth=(userName, passw))
            
            if (verbose >= 1):
                print("gndw_neo4j_connect: Successfully connected to "+uri)
        except Exception as e:
            print("gndw_neo4j_connect: ERROR in connecting Graph:",e)
            graphDB_Driver = '';
        finally:
            print("gndw_neo4j_connect: op is completed");

            
        return graphDB_Driver;



def             gndwdb_neo4j_conn_metarepo(verbose):


    ret = 0
 
    conn_params = gndwdb_neo4j_conn_getconfig(verbose);
    if (verbose > 3):
       print('gndwdb_neo4j_conn: path for neo4j conn cfg: '+conn_params['uri']);

    uri = conn_params['uri'];
    userName = conn_params['userName'];
    passw = conn_params['passw'];
    
    
    ### Check db connection
    graph_conn  = gndwdb_neo4j_conn_connect(uri, userName, passw,verbose)

    if (verbose > 3):
       if (graph_conn == ''):
            print('gndwdb_neo4j_conn: connection failed '+graph_conn);
    
    return graph_conn;

def           gndwdb_neo4j_conn_metarepo_close(graph_conn, verbose):

        if (verbose > 3):
            print("gndwdb_neo4j_conn_metarepo_close: closing the connection ");

        graph_conn.close();


def            gndwdb_neo4j_conn_datarepo(verbose):

 
    ret = 0
    conn_params = gndwdb_neo4j_conn_getconfig(verbose);
    if (verbose > 3):
       print('gndwdb_neo4j_conn: path for neo4j conn cfg: '+conn_params['uri']);

    uri = conn_params['uri'];
    userName = conn_params['userName'];
    passw = conn_params['passw'];
    
    ### Check db connection
    graph_conn  = gndwdb_neo4j_conn_connect(uri, userName, passw, verbose)

    return graph_conn


def           gndwdb_neo4j_conn_datarepo_close(graph_conn, verbose):

        if (verbose > 3):
            print("gndwdb_neo4j_conn_datarepo_close: closing the connection ");

        graph_conn.close();

        
def             gndwdb_neo4j_conn_check_api(cfgfile, verbose):

      if (verbose > 3):
           print('gndwdb_neo4j_conn: parsing cfg file:'+cfgfile);
          
      ###with open(os.path.join(JSON_PATH, jfile)) as json_file:
      with open(cfgfile) as cfg_jsonf: 
          cfg_json = json.load(cfg_jsonf);
          
          if (verbose > 5):
             print(cfg_json);

          def_config = cfg_json['_default'];
          nconfig = def_config['1'];
          print(nconfig);
          # read config list
          uri = "bolt://"+nconfig['serverIP'];
          ###uri = nconfig['uri'];
          userName = nconfig['username'];
          passw = nconfig['password'];
          if (verbose > 3):
             print('gndwdb_neo4j_conn: check conn for  uri:'+uri+"   user:"+userName);
          
          graph_conn = gndwdb_neo4j_conn_connect(uri, userName, passw, verbose);
          if graph_conn=='':
             print("Error..")
             return "Error"
          if graph_conn is None:
             ### Unable to connect
             print('Error! Unable to connect to graph server');
             print("None...") 
             return -1;
          #print('Graph Server connected! '+uri);
          #print(graph_conn);
          #else:
          if (verbose > 3):
             #print(graph_conn);
             print('gndwdb_neo4j_conn_check_api: '+uri+' Connection established successfully');
          # return -1;
          graph_conn.close(); 
          
          return 0;

def         gndwdb_neo4j_conn_getconfig(verbose):

      conn_params = dict();
      ###cfgfile = './server_config.json';
      cfg_file = get_config_neo4j_conninfo_file();
      if (verbose > 3):
         print('gndwdb_neo4j_conn: path for neo4j conn cfg: '+cfg_file);

      ###with open(os.path.join(JSON_PATH, jfile)) as json_file:
      with open(cfg_file) as cfg_jsonf:
          cfg_json = json.load(cfg_jsonf);
          print(cfg_json);

          def_config = cfg_json['_default'];
          nconfig = def_config['1'];
          print(nconfig);
          # read config list
          conn_params['uri'] = "bolt://"+nconfig['serverIP'];
          conn_params['userName'] = nconfig['username'];
          conn_params['passw'] = nconfig['password'];

          return (conn_params);
 
             
        
if   __name__ == "__main__":

    verbose = 5;
    cfg_file = get_config_neo4j_conninfo_file();
    if (verbose > 3):
       print('gndwdb_neo4j_conn: Checking connection path for neo4j conn cfg: '+cfg_file);
    
    gndwdb_neo4j_conn_check_api(cfg_file, verbose);
