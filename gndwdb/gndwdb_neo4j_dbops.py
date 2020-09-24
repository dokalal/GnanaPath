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
        
        

def            gndw_dtval_filter(dataval):
    
        # if (verbose > 3):
        #    print('gndw_dataval_filter: Check and add escape char filter to data value');
         datavalstr = str(dataval);
         dataval_s1 = datavalstr.replace("'", "\\'")
         dataval_s2 = dataval_s1.replace('"', '\\"')
         ###dataval_s3 = dataval_s2.replace('\\', '\\\')
           
         
         dataval_filtered = str(dataval_s2)
         return dataval_filtered;
      
         

def              gndw_nodelabel_filter(nodename, prefix, verbose):

          if (verbose > 2):
              print('gndw_nodelabel_filter: Check and apply filters to nodename '+str(nodename))

          nodename_filtered = nodename

          s = nodename[0]
          
          
          ##try:
              ####int(nodename)
          if s.isnumeric() is True:    
              nodename_filtered = prefix+str(nodename);
              
          if (verbose > 2):
              print('gndw_nodelabel_filter: nodename is integer prefix with DN:'+nodename_filtered)
              
                    
          ###except ValueError:
          ## nodename_filterd = nodename;

                    
                    
          return nodename_filtered



def                gndw_metarepo_metanode_check(graph_conn,  nodename, verbose):

    if (verbose > 2):
        print("gndw_metarepo_metanode_check:  Verify if the node exist in metarepo"+nodename)

    #### Add nodename filters first
    nodename_filtered = gndw_nodelabel_filter(nodename, 'GNNode', verbose);

        
    with graph_conn.session() as grpDB_Ses:
        #### Verify that the node does not exist first

        cqlqry = "MATCH(x:"+str(nodename_filtered)+" {name:'"+gndw_dtval_filter(nodename)+"', type:'TableNode'}) return x"
        nodes = grpDB_Ses.run(cqlqry)
        len = 0
        for n in nodes:
            len = len + 1

        if (verbose > 2):
           print("gndw_metarepo_metanode_check: Verifying if node existig nnodes found:"+ str(len))

        if (len == 0):
           if (verbose > 2):
               print("gndw_metarepo_metanode_check: Node does not exist in metarepo "+nodename);
           return len;    
        else:
           if (verbose > 2):
               print("gndw_metarepo_metanode_check: Node does exist in metarepo "+nodename);
           return len;
       
           
def                gndw_metarepo_metanode_add(graph_conn, nodename, node_attr_list, verbose):
    
    if (verbose > 0):      
       print("gndw_metarepo_metanode_add: Adding new node name %s" % nodename)

    #### Add nodename filters first
    nodename_filtered = gndw_nodelabel_filter(nodename, 'GNNode', verbose); 

                    
    ####node attrlist format: attr1: attrname, attr2:
    with graph_conn.session() as grpDB_Ses:
       #### Verify that the node does not exist first
                    
       cqlqry = "MATCH(x:"+str(nodename_filtered)+" {name:'"+gndw_dtval_filter(nodename)+"', type:'TableNode'}) return x"
       nodes = grpDB_Ses.run(cqlqry)
       len = 0
       for n in nodes:
            len = len + 1
       
       if (verbose > 1): 
           print("gndw_metarepo_metanode_add: Verifying if node existig nnodes found:"+ str(len))
   
       if (len == 0):
            ##### Create new tablenode  entity does not exist in db
            cqlqry = "CREATE (d:"+nodename_filtered+" {name:'"+gndw_dtval_filter(nodename)+"' , type:'TableNode'})"
            if (verbose > 1):
                print("gndw_metarepo_metanode_add: new meta CQLqry", cqlqry)
            grpDB_Ses.run(cqlqry)
            
       else:
            if (verbose > 1):
                print("gndw_metarepo_metanode_add: TableNode "+gndw_dtval_filter(nodename)+ " already exist")
       
       ##### Check TableAttrNodes exist 
       #attr_list {attr1: attrname, attr2: attrname, }
       attr_len = 0
       for k, v in node_attr_list.items():
            #### K is attr
            if (verbose > 1):
                print("gndw_metarepo_metanode_add: key "+k+"  attr:"+str(v))
            
            attrname = str(v)

            attrname_filtered = gndw_nodelabel_filter(attrname, 'GNNodeAttr', verbose)        
            ### Add new node for attribute
            cqlqry = "MATCH(x:"+str(attrname_filtered)+" {name:'"+gndw_dtval_filter(attrname)+"', type:'TableAttrNode'}) return x"
            nodes = grpDB_Ses.run(cqlqry)
            
            len = 0
            for n in nodes:
                len = len + 1
            
            if (len == 0):
                #### TableAttrNode does not exist and add new node
                cqlqry = "CREATE (d:"+attrname_filtered+" {name:'"+gndw_dtval_filter(attrname)+"' , type:'TableAttrNode'})"
                if (verbose > 1):
                    print("gndw_metarepo_metadata_add: new qry to add TableAttrNode CQLqry:", cqlqry)
                    
                grpDB_Ses.run(cqlqry)
                
            else:
                if (verbose > 1):
                    print("gndw_metarepo_metadata_add: TableAttrNode:"+attrname_filtered+" exists")
            
            #### Now add relationship between tablenode and tablenodeattr
            cqledgeqry = "MATCH (d:"+str(nodename_filtered)+" {name:'"+gndw_dtval_filter(nodename)+"'})-[r]-(a:"+attrname_filtered+" {name:'"+gndw_dtval_filter(attrname)+"'}) return TYPE(r), PROPERTIES(r)"        
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
                        print("gndw_metarepo_metanode_add: HAS_ATTR relship count key "+k+" val:"+str(kv))

                    
            if (verbose > 1):    
               print("gndw_metarepo_metanode_add: TableAttrNode HAS_ATTR relship rcount: "+str(rcount))
            
            if (len == 0):
                
               if (verbose > 1):
                   print("gndw_metarepo_metanode_add: TableAttrNode HAS_ATTR rel does not exist ")
    
               cqlrelins =  "MATCH (d:"+str(nodename_filtered)+"), (p:"+str(attrname_filtered)+") WHERE d.name ='"+gndw_dtval_filter(nodename)+"' AND p.name = '"+gndw_dtval_filter(attrname)+"' CREATE (d)-[:HAS_ATTR {count: 1}]->(p)"

               if (verbose > 2):
                   print("gndw_metarepo_metanode_add: TableAttrNode:"+gndw_dtval_filter(attrname)+" HAS_ATTR relship qry :")
                   print(cqlrelins)
                
               grpDB_Ses.run(cqlrelins)
            
            else:
                print("gndw_metarepo_metanode_add: TableAttrNode HAS_ATTR relship already exists with rcount:"+str(rcount))
              
                cqlrelins = "MATCH (x:"+str(nodename_filtered)+" {name:'"+gndw_dtval_filter(nodename)+"'})-[r]->(y:"+attrname_filtered+" {name:'"+gndw_dtval_filter(attrname)+"'}) SET r.count="+str(rcount+1)+" RETURN r.count" 
                if (verbose > 2):
                    print("gndw_metarepo_metanode_add: TableAttrNode HAS_ATTR relship updating rcount qry:"+cqlrelins)
                grpDB_Ses.run(cqlrelins)  
            

###########################################
# gndw_metarepo_module
#
########################
            
def                gndw_metarepo_metanode_add_api(node_name, node_attr_list, verbose):
    # Database Credentials
    uri = "bolt://172.17.0.4"
    userName = "neo4j"
    #password = os.getenv("NEO4J_PASSWORD")
    passw = 'ca$hc0w'
    ret = 0
    
    ### Check db connection
    graph_conn  = gn_neo4j_connect(uri, userName, passw,verbose)
    

    
    gndw_metarepo_metanode_add(graph_conn, node_name, node_attr_list, verbose)
    
    if (verbose > 1):
        print("gndw_metarepo_metanode_add: Meta node "+node_name+" and attributes are added to metarepo")
    
    return ret;
  
    
#####################Datanode Repo

def                 gndw_datarepo_datanode_add(graph_conn, nodename, node_attr_list, datanode_name_inp, datanode_attr_list, verbose):
    
    if (verbose > 0):          
        print("gndw_datarepo_datanode_add: Adding new DataNode name %s" % nodename)
    
    ####node attrlist format: attr1: attrname, attr2:
    with graph_conn.session() as grpDB_Ses:

       nodename_filtered = gndw_nodelabel_filter(nodename, 'GNNode', verbose);             
       #### Verify that the drug does not exist first
       cqlqry = "MATCH(x:"+nodename_filtered+" {name:'"+gndw_dtval_filter(nodename)+"', type:'TableMetaNode'}) return x"
       nodes = grpDB_Ses.run(cqlqry)
       len = 0
        
       for n in nodes:
           len = len + 1
            
       if (len == 0):
            ##### Create new tablenode on datarepo  entity does not exist in db
            cqlqry = "CREATE (d:"+nodename_filtered+" {name:'"+gndw_dtval_filter(nodename)+"' , type:'TableMetaNode'})"
            if (verbose > 2):
                print("gndw_datarepo_datanode_add: Add new metanode to Data Repo CQLqry:", cqlqry)
            grpDB_Ses.run(cqlqry)
       else:
            if (verbose > 1):
                print("gndw_datarepo_datanode_add: TableMetaNode "+nodename_filtered+ " already exist in datarepo ")

       #### Let us not ask user to set datanode label yet. We generate         
                
       datanode_name = datanode_name_inp.replace(" ", '');
       datanode_name_filtered = gndw_nodelabel_filter(datanode_name, 'GNNode', verbose);  

                    
       if (verbose > 2):
           print("gndw_datarepo_datanode_add: Removed extra spaces in datanode label :"+datanode_name);
           
       #### Verify that the datnode  exist first
       cqlqry = "MATCH(x:"+str(datanode_name_filtered)+" {name:'"+gndw_dtval_filter(datanode_name)+"', type:'TableDataNode'}) return x"

       if (verbose > 2):
           print("gndw_datarepo_datanode_add: Checking if datanode "+gndw_dtval_filter(datanode_name)+" exists CQLqry:", cqlqry)

       
       nodes = grpDB_Ses.run(cqlqry)
       len = 0
    
       for n in nodes:
            len = len + 1
            
       if (len == 0):
            i = 0
            #### Prepate data attr list
            ######### Check TableDataNode Attr list exist 
            ####attr_list {attr1: attrname, attr2: attrname, }
            if (verbose > 2):
                print("gndw_datarepo_datanode_add: Preparing list of datanode attr list")
            alist = ''
            for k, v in datanode_attr_list.items():
                if (i > 0):
                    alist += ", "
                attrname = gndw_nodelabel_filter(k, 'GNAttr', verbose);
                alist += ""+attrname+":'"+gndw_dtval_filter(v)+"'"
                i = i+1            
        
        
            if (verbose > 1):
                print('gndw_datarepo_datanode_add: Prepared DataNode attribute list '+alist)
            ##### Create new tablenode  entity does not exist in db
            cqlqry = "CREATE (d:"+str(datanode_name_filtered)+" {name:'"+gndw_dtval_filter(datanode_name)+"' , metanode:'"+gndw_dtval_filter(nodename)+"', type:'TableDataNode', "+alist+"})"
                
                
            if (verbose > 2):
                print("gndw_datarepo_datanode_add: Creating new DataNode"+datanode_name_filtered+" CQLqry:", cqlqry)
                
            grpDB_Ses.run(cqlqry)
       else:
            if (verbose > 1):
                print("gndw_datarepo_datanode_add: DataNode "+datanode_name_filtered+ " already exist and attribute list not updated")
       
    
       #### Now add relationship between tablenode and tablenodeattr
       cqledgeqry = "MATCH (d:"+str(nodename_filtered)+" {name:'"+gndw_dtval_filter(nodename)+"'})-[r]-(a:"+str(datanode_name_filtered)+" {name:'"+gndw_dtval_filter(datanode_name)+"'}) return TYPE(r), PROPERTIES(r)"        
       nodes = grpDB_Ses.run(cqledgeqry)
        
       len = 0
       rcount = 0
       for n in nodes:
         r = n["PROPERTIES(r)"]
         for k in r.keys():
            kv = r.get(k)
            if (verbose > 1):
               print("gndw_datarepo_datanode_add: DataNode IS relship count key "+k+" val:"+str(kv))
            rcount = kv
         len = len + 1
    
    
       if (verbose > 1):
           print("gndw_datarepo_datanode_add: DataNode IS relationship exist count : "+str(len)+ "  rcount: "+str(rcount))
        
        
       if (len == 0):
                
          if (verbose > 1):
               print("gndw_datarepo_datanode_add: DataNode:"+gndw_dtval_filter(datanode_name)+" IS relship  is not present ")
    
          cqlrelins =  "MATCH (d:"+str(nodename_filtered)+"), (p:"+str(datanode_name_filtered)+") WHERE d.name ='"+gndw_dtval_filter(nodename)+"' AND p.name = '"+gndw_dtval_filter(datanode_name)+"' CREATE (d)-[:IS {count: 1}]->(p)"
          
          if (verbose > 2):
             print("gndw_datarepo_datanode_add: DataNode:"+datanode_name_filtered+" IS relship CypQry:")
             print(cqlrelins)
            
          grpDB_Ses.run(cqlrelins)
        
       else:
          print("gndw_datarepo_datanode_add: DataNode IS relship exists  with rcount:"+str(rcount))
              
          cqlrelins = "MATCH (x:"+str(nodename_filtered)+" {name:'"+gndw_dtval_filter(nodename)+"'})-[r]->(y:"+datanode_name_filtered+" {name:'"+gndw_dtval_filter(datanode_name)+"'}) SET r.count="+str(rcount+1)+" RETURN r.count" 
          if (verbose > 2):
              print("gndw_datarepo_datanode_add: DataNode IS relship update qry :"+cqlrelins)
          grpDB_Ses.run(cqlrelins)  
    
    
#####################################################################    
def              gndw_datarepo_datanode_add_api(node_name, node_attr_list, datanode_name, datanode_attr_list, verbose):
    
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
    
    

def     maintest_api():   
     verbose = 3
    
     if( verbose > 0):
            print("gndw_module_start: Start the module")
  
     node_name = 'test1'
     #node_attr_list =  {'attr1': '1', 'attr2': 'custemail', 'attr3': 'addrline', 'attr4': 'city', 'attr5': 'state', 'attr6': 'country', 'attr7': 'zip', 'attr8': 'custphone'}

     node_attr_list = {'attr1':'1at', 'attr2':'2at'};
     ### Add new node       
     ret = gndw_metarepo_metanode_add_api(node_name, node_attr_list, verbose)
        
     if (verbose > 1):
        print("gndw_neo4j_db: metaadd node returned "+str(ret))
        
     datanode_name = '2'
     #datanode_attr_list = {'customerid':'1', 'custname':'John Smith'}
     #datanode_attr_list = {'customerid ': 1, 'custemail': 'a@a.com', 'addrline': '45 sdsds 34343', 'city': 'Boston', 'state': 'MA', 'country': 'US', 'zip': 2110, 'custphone': '123-232-2323'}

     datanode_attr_list  = {'1at': 'Spl"C', '2at':"Spl' s"}
     gndw_datarepo_datanode_add_api(node_name, node_attr_list, datanode_name, datanode_attr_list, verbose)
    
  
    
