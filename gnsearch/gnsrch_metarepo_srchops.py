###################################################################################
#  GnanaSearch on metarepo  module
#
#   through this module, 
#   - Overall meta view 
#   - Busines rules view 
#
####################################################################################


from moz_sql_parser import parse
import json

import numpy as np
import neo4j
import warnings

import re
warnings.simplefilter('ignore')

from neo4j import GraphDatabase, basic_auth
import sys
import os

curentDir=os.getcwd();
listDir=curentDir.rsplit('/',1)[0];
#print(' Test listdir: '+listDir)
sys.path.append(listDir);
#print(sys.path);

###### Imports required
from gndwdb.gndwdb_neo4j_conn import gndwdb_neo4j_conn_metarepo, gndwdb_neo4j_conn_datarepo

    
    
def          gnsrch_metarepo_getnodes(meta_graph_conn, verbose): 
      
    #Convert sql query to CYPHER
    rjson = '';
    cqlstr = 'MATCH (s)-[r:HAS_ATTR]->(t) RETURN s,r,t';
    ##cqlstr = 'MATCH p
    
    if (verbose > 3):
        print('gnsrch_metarepo_getall: Get all node and relationships:'+cqlstr);
        
    #### Execute it on Data Repo
    with meta_graph_conn.session() as metaGrpDbSes:
        ## Execute csql 
         nodes = metaGrpDbSes.run(cqlstr);
         nlen = 0;
         rjson = '{'+"\n";
         rjson += ' nodes: ['+"\n";
         ###rjson += ' {'+"\n";

         for record in nodes:
             print(" Record  ".format(record=record));
             
         
         for record in nodes:
            ####
            print(record['s'].__dict__);
            print(record['r'].__dict__);
            rec_nodes = record['s'];
            print("Record Nodes  \n\n");
            ##print(" Record Node type: "+rec_nodes.type);
            print(rec_nodes);
            rec_rel = record['r'];
            print("Record Node Relationships ");
            print(rec_rel);
            print("Record Relationship id "+str(rec_rel.id));
            print("Record Relationship type "+rec_rel.type);

            for n in rec_nodes.items():
                print(" Iterative node \n");
                print(n);

            
            
            print(" Parsing  Relationship \n\n");
            for r  in rec_props:
                print (" Relationships :"+r);
                #for rkey in r.keys():
                #    print ("Relationship Key "+rkey);
                #    rkeyval = r.get(rkey);
                #    print(" Relationship key val :"+str(rkeyval));
                
                ####print (v);
            
            #for x in n:
            #    print('noderec item '+x);
            ##s = json.dumps(n);

            ##print (s);
            if (nlen > 0):
                rjson += ','+"\n";
            ###print(' node rec '+n['p']);    
            for k in n.keys():
                rjson += '{ '+"\n";
                a = 0;
                print(' Node key '+k+' node val:'+n[k]); 
                nprops = n.get(k);
                for p in nprops.keys():
                   pv = nprops.get(p);
                   if (a > 0):
                        rjson += ', '+"\n";
                   if (p == "name"):
                      rjson += '"id": "'+str(pv)+'"';
                   else:
                      rjson += '"'+str(p)+'": "'+str(pv)+'"'; 
                   
                   ##print('nodeid-'+str(len)+' key:'+str(p)+' val:'+str(pv));
                   a = a + 1;
                if (a > 0):
                    rjson += "\n";
                rjson += '} ';
            nlen += 1;
         if (nlen > 0):
            rjson += "\n";
         ####rjson += ' }'+"\n";
         rjson += ']'+"\n";
         rjson += '}'+"\n";
         return rjson;
            
############API to send srch string  and return json output 
def                  gnsrch_metarepo_srch_api(verbose):

      if (verbose > 3):
          print('gnrch_metarepo_srch_api: Starting search api ');

      meta_graph_conn = gndwdb_neo4j_conn_metarepo(verbose);
      if (meta_graph_conn):
           print('gnsrch_metarepo_srch_api: meta graph connect is not null');
  ###    data_graph_conn = gndwdb_neo4j_conn_datarepo(verbose);

      ret = gnsrch_metarepo_getnodes(meta_graph_conn, verbose);

      return ret;    
    
    
    
    
if __name__ == '__main__':
    
    ### Setting up metarepo and db repo conns
    verbose = 6
    #       print('gnsrch: cql qry returned:'+cql);
    curentDir=os.getcwd();
    listDir=curentDir.rsplit('/',1)[0];
    print(' Test listdir: '+listDir)
    print(sys.path);
    
    rjson = gnsrch_metarepo_srch_api(verbose);

    if (verbose > 3):
        print('gnsrch_metarepo_srch_api: rjson:');
        print(rjson);
