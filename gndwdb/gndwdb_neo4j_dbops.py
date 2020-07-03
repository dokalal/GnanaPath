####################################################################
# GnanaWarehouse_DB module does following tasks
#    - Add nodes to neo4j
#    - Add attributes to node to neo4j
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


def        gn_neo4j_connect(uri, userName, passw, verbose):
    
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
        graphDB_Driver = ''
    finally:
        print("gndw_neo4j_connect: op is completed")
        return graphDB_Driver;
        
        
def               gndw_neo4j_metanode_add(graph_conn, nodename, node_attr_list, verbose):
    
    if (verbose > 0):      
       print("gndw_neo4j_addnode: Adding new node name %s" % nodename)
    
    ####node attrlist format: attr1: attrname, attr2:
    with graph_conn.session() as grpDB_Ses:
       #### Verify that the drug does not exist first
       cqlqry = "MATCH(x:"+nodename+" {name:'"+nodename+"', type:'TableNode'}) return x"
       nodes = grpDB_Ses.run(cqlqry)
       len = 0
       for n in nodes:
            len = len + 1
       
       if (verbose > 1): 
           print("gndw_neo4j_metanode_add: Verifying if node existig nnodes found:"+ str(len))
   
       if (len == 0):
            ##### Create new tablenode  entity does not exist in db
            cqlqry = "CREATE (d:"+nodename+" {name:'"+nodename+"' , type:'TableNode'})"
            if (verbose > 1):
                print("gndw_neo4j_meta_addnode: new meta CQLqry", cqlqry)
            grpDB_Ses.run(cqlqry)
            
       else:
            if (verbose > 1):
                print("gndw_ndeo4j_meta_tablenode_add: TableNode "+nodename+ " already exist")
       
       ##### Check TableAttrNodes exist 
       #attr_list {attr1: attrname, attr2: attrname, }
       attr_len = 0
       for k, v in node_attr_list.items():
            #### K is attr
            if (verbose > 1):
                print("gndw_neo4j_meta_tablenode_add: key "+k+"  attr:"+v)
            
            attrname = v
            
            ### Add new node for attribute
            cqlqry = "MATCH(x:"+attrname+" {name:'"+attrname+"', type:'TableAttrNode'}) return x"
            nodes = grpDB_Ses.run(cqlqry)
            
            len = 0
            for n in nodes:
                len = len + 1
            
            if (len == 0):
                #### TableAttrNode does not exist and add new node
                cqlqry = "CREATE (d:"+attrname+" {name:'"+attrname+"' , type:'TableAttrNode'})"
                if (verbose > 1):
                    print("gndw_neo4j_meta_addnode: new meta CQLqry", cqlqry)
                    
                grpDB_Ses.run(cqlqry)
                
            else:
                if (verbose > 1):
                    print("gndw_neo4j_meta_addnode: existing attrnode "+attrname)
            
            #### Now add relationship between tablenode and tablenodeattr
            cqledgeqry = "MATCH (d:"+nodename+" {name:'"+nodename+"'})-[r]-(a:"+attrname+" {name:'"+attrname+"'}) return TYPE(r), PROPERTIES(r)"        
            nodes = grpDB_Ses.run(cqledgeqry)
            
            len = 0
            rcount = 0
            for n in nodes:
                len = len + 1
                r = n["PROPERTIES(r)"]
                for k in r.keys():
                    kv = r.get(k)
                    rcount = kv
                    if (verbose > 1):
                        print("gndw_neo4j_meta_addnode: relationship count key "+k+" val:"+str(kv))

                    
            if (verbose > 1):    
               print("gndw_neo4j_meta_addnode: Verifying if node has HAS_ATTR relationship length count is : "+str(rcount))
            
            if (len == 0):
                
               if (verbose > 1):
                   print("gndw_neo4j_meta_addnode: Node relationship  is not present ")
    
               cqlrelins =  "MATCH (d:"+nodename+"), (p:"+attrname+") WHERE d.name ='"+nodename+"' AND p.name = '"+attrname+"' CREATE (d)-[:HAS_ATTR {count: 1}]->(p)"

               if (verbose > 1):
                   print("gndw_neo4j_addnode: associate HAS_ATTR relationship:")
                   print(cqlrelins)
                
               grpDB_Ses.run(cqlrelins)
            
            else:
                print("Node relationship with count:"+str(rcount))
              
                cqlrelins = "MATCH (x:"+nodename+" {name:'"+nodename+"'})-[r]->(y:"+attrname+" {name:'"+attrname+"'}) SET r.count="+str(rcount+1)+" RETURN r.count" 
                if (verbose > 1):
                    print("gndw_neo4j_addnode: update coun qry :"+cqlrelins)
                grpDB_Ses.run(cqlrelins)  
            

###########################################
# gndw_metarepo_module
#
########################
            
def                gndw_metanode_add_api(node_name, node_attr_list, verbose):
    # Database Credentials
    uri = "bolt://172.17.0.4"
    userName = "neo4j"
    #password = os.getenv("NEO4J_PASSWORD")
    passw = 'ca$hc0w'
    ret = 0
    
    ### Check db connection
    graph_conn  = gn_neo4j_connect(uri, userName, passw,verbose)
    

    
    gndw_neo4j_metanode_add(graph_conn, node_name, node_attr_list, verbose)
    
    if (verbose > 1):
        print("gndw_neo4j_db: Meta nodes are added")
    
    return ret;
  
    
#####################Datanode Repo

def                 gndw_datarepo_datanode_add(graph_conn, nodename, node_attr_list, datanode_name_inp, datanode_attr_list, verbose):
    
    if (verbose > 1):          
        print("gndw_datarepo_datanode_add: Adding new node name %s" % nodename)
    
    ####node attrlist format: attr1: attrname, attr2:
    with graph_conn.session() as grpDB_Ses:
        
       #### Verify that the drug does not exist first
       cqlqry = "MATCH(x:"+nodename+" {name:'"+nodename+"', type:'TableNode'}) return x"
       nodes = grpDB_Ses.run(cqlqry)
       len = 0
        
       for n in nodes:
           len = len + 1
            
       if (len == 0):
            ##### Create new tablenode on datarepo  entity does not exist in db
            cqlqry = "CREATE (d:"+nodename+" {name:'"+nodename+"' , type:'TableNode'})"
            if (verbose > 1):
                print("gndw_datarepo_datanode_add: new meta CQLqry", cqlqry)
            grpDB_Ses.run(cqlqry)
       else:
            if (verbose > 1):
                print("gndw_datarepo_datanode_add: TableNode "+nodename+ " already exist")

       datanode_name = datanode_name_inp.replace(" ", '')
       #### Verify that the datnode  exist first
       cqlqry = "MATCH(x:"+datanode_name+" {name:'"+datanode_name+"', type:'TableDataNode'}) return x"
    
       nodes = grpDB_Ses.run(cqlqry)
       len = 0
    
       for n in nodes:
            len = len + 1
            
       if (len == 0):
            i = 0
            #### Prepate data attr list
            ######### Check TableDataNode Attr list exist 
            ####attr_list {attr1: attrname, attr2: attrname, }
            alist = ''
            for k, v in datanode_attr_list.items():
                if (i > 0):
                    alist += ", "
                attrname = k
                alist += "'"+k+"':'"+str(v)+"'"
                i = i+1            
        
        
            if (verbose > 1):
                print('gndw_datarepo_datanode_add: Adding data attribute list '+alist)
            ##### Create new tablenode  entity does not exist in db
            cqlqry = "CREATE (d:"+datanode_name+" {name:'"+datanode_name+"' , type:'TableDataNode', "+alist+"})"
                
                
            if (verbose > 1):
                print("gndw_datarepo_datanode_add: new meta CQLqry", cqlqry)
                
            grpDB_Ses.run(cqlqry)
       else:
            if (verbose > 1):
                print("gndw_datarepo_datanode_add: TableNode "+nodename+ " already exist")
       
    
       #### Now add relationship between tablenode and tablenodeattr
       cqledgeqry = "MATCH (d:"+nodename+" {name:'"+nodename+"'})-[r]-(a:"+datanode_name+" {name:'"+datanode_name+"'}) return TYPE(r), PROPERTIES(r)"        
       nodes = grpDB_Ses.run(cqledgeqry)
        
       len = 0
       rcount = 0
       for n in nodes:
         r = n["PROPERTIES(r)"]
         for k in r.keys():
            kv = r.get(k)
            if (verbose > 1):
               print("gndw_datarepo_datanode_add: relationship count key "+k+" val:"+str(kv))
            rcount = kv
         len = len + 1
    
    
       if (verbose > 1):
           print("gndw_datarepo_datanode_add: IS relationship exist count : "+str(len)+ "  rcount: "+str(rcount))
        
        
       if (len == 0):
                
          if (verbose > 1):
               print("gndw_datarepo_datanode_add: Node relationship  is not present ")
    
          cqlrelins =  "MATCH (d:"+nodename+"), (p:"+datanode_name+") WHERE d.name ='"+nodename+"' AND p.name = '"+datanode_name+"' CREATE (d)-[:IS {count: 1}]->(p)"
          
          if (verbose > 1):
             print("gndw_datarepo_datanode_add: associate HAS_ATTR relationship:")
             print(cqlrelins)
            
          grpDB_Ses.run(cqlrelins)
        
       else:
          print("gndw_datarepo_datanode_add: Node relationship with count:"+str(rcount))
              
          cqlrelins = "MATCH (x:"+nodename+" {name:'"+nodename+"'})-[r]->(y:"+datanode_name+" {name:'"+datanode_name+"'}) SET r.count="+str(rcount+1)+" RETURN r.count" 
          if (verbose > 1):
              print("gndw_datarepo_datanode_add: update qry :"+cqlrelins)
          grpDB_Ses.run(cqlrelins)  
    
    
#####################################################################    
def              gndw_datanode_add_api(node_name, node_attr_list, datanode_name, datanode_attr_list, verbose):
    
    # Database Credentials
    uri = "bolt://172.17.0.6"
    userName = "neo4j"
    #password = os.getenv("NEO4J_PASSWORD")
    passw = 'ca$hc0w'
    ret = 0
    
    ### Check db connection
    graph_conn  = gn_neo4j_connect(uri, userName, passw,verbose)
    
    
    gndw_datarepo_datanode_add(graph_conn, node_name, node_attr_list, datanode_name, datanode_attr_list, verbose)
    
    if (verbose > 1):
        print("gndw_datanode_add_api: Data Node is added")
    
    return ret;
    
    
if __name__ == '__main__':
   
     verbose = 2
    
     if( verbose > 0):
            print("gndw_module_start: Start the module")
  
     node_name = 'customer'
     node_attr_list =  {'attr1': 'customerid ', 'attr2': 'custemail', 'attr3': 'addrline', 'attr4': 'city', 'attr5': 'state', 'attr6': 'country', 'attr7': 'zip', 'attr8': 'custphone'}

     ### Add new node       
     ret = gndw_metanode_add_api(node_name, node_attr_list, verbose)
        
     if (verbose > 1):
        print("gndw_neo4j_db: metaadd node returned "+str(ret))
        
     datanode_name = 'John Smith'
     #datanode_attr_list = {'customerid':'1', 'custname':'John Smith'}
     datanode_attr_list = {'customerid ': 1, 'custemail': 'a@a.com', 'addrline': '45 sdsds 34343', 'city': 'Boston', 'state': 'MA', 'country': 'US', 'zip': 2110, 'custphone': '123-232-2323'}
         
     gndw_datanode_add_api(node_name, node_attr_list, datanode_name, datanode_attr_list, verbose)
    
  
    
