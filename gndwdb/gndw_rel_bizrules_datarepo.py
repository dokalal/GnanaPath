####################################################################
# GnanaWarehouse_DB module does following tasks
#    - Add data relationships 
#    - Add relationship attributes to node to neo4j
###################################################################
import os
import csv
import json
import psycopg2
import sys
import numpy as np
import neo4j
import warnings

import re
warnings.simplefilter('ignore')

from neo4j import GraphDatabase, basic_auth
from gndwdb_neo4j_dbops import gndw_metarepo_metanode_check
from gndwdb_neo4j_conn import gndwdb_neo4j_conn_metarepo, gndwdb_neo4j_conn_datarepo


###### Add datanode relationship

def              gndw_rel_bizrule_datarepo_datanode_rule_exists(data_graph_conn, 
                                                                snodename,  tnodename, 
                                                                mnodename, rulename, 
                                                                relname, verbose):
    
      with data_graph_conn.session() as dataGrpDB_Ses:
            
            cqlqry = "MATCH (s {name:'"+str(snodename)+"'})";
            cqlqry += "-[r :"+str(relname)+" {name:'"+str(relname)+"' ";
            cqlqry += " , matchnode:'"+str(mnodename)+"' ";
            cqlqry += "}]-";
            cqlqry += "(t {name:'"+str(tnodename)+"'}) ";
            cqlqry += " return COUNT(r);";
            
            if (verbose > 4):
                print('gndw_rel_bizrule_datanode_rule_exists:cqlqry: '+cqlqry);
            
            nodes = dataGrpDB_Ses.run(cqlqry);   
            nr = 0;
            for n in nodes:
                nr = n["COUNT(r)"];
                
            if (verbose > 4):
                print('gndw_rel_bizrule_datarepo_datanode_rule_exists: number of rels '+str(nr));
                
            return nr;
        

def              gndw_rel_bizrule_datarepo_datanode_rule_add(data_graph_conn, snodename, 
                                                             tnodename, rulename, 
                                                             relname, matchnode, verbose):
    
      with data_graph_conn.session() as dataGrpDB_Ses:
            
            cqlqry = "MATCH (s {name:'"+str(snodename)+"'}), ";
            cqlqry += "(t {name:'"+str(tnodename)+"'}) ";
            cqlqry += " MERGE (s)-[:"+str(relname)+" {name:'"+str(relname)+"', ";
            cqlqry += " rulename:'"+str(rulename)+"', ";
            cqlqry += " type:'RuleRelation', ";
            cqlqry += " matchnode:'"+str(matchnode)+"'  }]->(t) ";
            ###cqlqry += "(t {name:'"+str(tnodename)+"'}) ";
            ##cqlqry += " return COUNT(r);";
            
            if (verbose > 4):
                print('gndw_rel_bizrule_datarepo_datanode_rule_add: cqlqry :'+cqlqry);
                
            dataGrpDB_Ses.run(cqlqry);  
                
            if (verbose > 4):
                print('gndw_rel_bizrule_datarepo_datanode_rule_add: New relation'+str(relname)+'  ');

            

def             gndw_rel_bizrule_datarepo_add(meta_graph_conn, data_graph_conn, rjson, verbose):
     
      #### Apply Biz Rule on Data repo side
      rulename = rjson["rulename"];
      srcnode = rjson["srcnode"];
      tgtnode = rjson["targetnode"];
      matchnode = rjson["matchnode"];
      matcond = rjson['matchcondition'];
      relname = rjson["relation"];
    
      ########MATCH (so {metanode:'salesorder'}), (c {metanode:'customer'}), 
      ####(p {metanode:'product'}) WHERE so.customerid = c.customerid  AND
      ####so.productid = p.productid return COUNT(so), COUNT(c), COUNT(p)
      retval = 0;
      
      with data_graph_conn.session()  as dataGrpDB_Ses:
            
            cqlqry = "MATCH ("+str(srcnode)+" {metanode:'"+str(srcnode)+"'}),";
            cqlqry += " ("+str(tgtnode)+" {metanode:'"+str(tgtnode)+"'}), ";
            cqlqry += " ("+str(matchnode)+" {metanode:'"+str(matchnode)+"'}) ";
            cqlqry += " WHERE "+str(matcond)+" ";
            #cqlqry += " return COUNT("+str(srcnode)+"),"+str(srcnode)+", "+str(tgtnode)+" ;";
            cqlqry += "return "+str(srcnode)+", "+str(tgtnode)+", "+str(matchnode)+" " ; 
            cqlqry += "  LIMIT 15;";
            
            if (verbose > 2):
                print("gndw_rel_bizrule_datarepo_add: cqlqry search:"+cqlqry);
                
            nr = 0;
            nodes = dataGrpDB_Ses.run(cqlqry)
            for n in nodes:
               ###nr = n["COUNT("+str(srcnode)+")"];
               sn = n[str(srcnode)];
               tn = n[str(tgtnode)];
               mn = n[str(matchnode)];
                
               if (sn):
                    snodename = sn["name"];
                    print('sn node: '+snodename+' ');
               if (tn):
                    tnodename = tn["name"];
                    print('tn node: '+tnodename+' ');
                    
               if (mn):
                    mnodename = mn["name"];
                    print('mn node: '+mnodename+' ');
               n = 0;     
               n = gndw_rel_bizrule_datarepo_datanode_rule_exists(data_graph_conn, 
                                                                   snodename,  
                                                                   tnodename,mnodename, 
                                                                   rulename, relname, verbose);
               
               if (n == 0):
                   #gndw_rel_bizrule_datarepo_datanode_rule_add(data_graph_conn, snodename,);
                  gndw_rel_bizrule_datarepo_datanode_rule_add(data_graph_conn, snodename,  
                                                              tnodename, rulename, relname, 
                                                              mnodename, verbose);
    
                
               if (verbose > 3):
                 print('gndw_rel_bizrule_datarepo_add: num of rels:'+str(n));
            
            print("gndw_rel_bizrule_datarepo_add: number of rules to add:");
            
            return retval;
      
            
if __name__ == '__main__':
    
    
    ### Setting up metarepo and db repo conns
    verbose = 6;
    meta_graph_conn = gndwdb_neo4j_conn_metarepo(verbose)
    data_graph_conn = gndwdb_neo4j_conn_datarepo(verbose)
    rfilepath = "./r.json"

    #with open(os.path.join(JSON_PATH, jfile)) as json_file:
    with open(rfilepath) as rfile:
        rjson = json.load(rfile)
        ####matchcondition: "(salesorder.customerid \= customer.customerid) AND (salesorder.productid = product.productid)" }'
    
        gndw_rel_bizrule_datarepo_add(meta_graph_conn, data_graph_conn, rjson, verbose)
    




    
