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
        graphDB_Driver = ''
    finally:
        print("gndw_neo4j_connect: op is completed")
        return graphDB_Driver;



def             gndwdb_neo4j_conn_metarepo(verbose):

    # Database Credentials
    uri = "bolt://172.17.0.4"
    userName = "neo4j"
    #password = os.getenv("NEO4J_PASSWORD")
    passw = 'ca$hc0w'
    ret = 0

    ### Check db connection
    graph_conn  = gndwdb_neo4j_conn_connect(uri, userName, passw,verbose)

    return graph_conn



def           gndwdb_neo4j_conn_datarepo(verbose):

    # Database Credentials
    uri = "bolt://172.17.0.6"
    userName = "neo4j"
    #password = os.getenv("NEO4J_PASSWORD")
    passw = 'ca$hc0w'
    ret = 0

    ### Check db connection
    graph_conn  = gndwdb_neo4j_conn_connect(uri, userName, passw, verbose)

    return graph_conn

