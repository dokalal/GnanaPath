####################################################################
# GnanaWarehouse_DB module does following tasks
#    - Add meta relationships
#    - Add meta attributes to node to neo4j
###################################################################
from .gndw_rel_bizrules_datarepo import gndw_rel_bizrule_datarepo_add
from .gndwdb_neo4j_conn import gndwdb_neo4j_conn_metarepo, gndwdb_neo4j_conn_datarepo
from .gndwdb_neo4j_dbops import gndw_metarepo_metanode_check
from neo4j import GraphDatabase, basic_auth
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


def gndw_rel_verify_rules(rjson, meta_graph_conn, data_graph_conn, verbose):

    fail = 0
    # Verify if srcnode exists in metarepo
    print("gndw_rel_verify: verify rules")
    if (rjson["srcnode"]):
        if (verbose > 2):
            print(
                'gndw_rel_verify_rules: verify if srcnode exists:' +
                rjson['srcnode'])
        l = gndw_metarepo_metanode_check(
            meta_graph_conn, rjson['srcnode'], verbose)
        if (l == 0):
            fail = fail + 1
    else:
        fail = fail + 1

    if (rjson["targetnode"]):
        if (verbose > 2):
            print(
                'gndw_rwl_verify_rules: verify if tgtnode exists:' +
                rjson['targetnode'])
        l = gndw_metarepo_metanode_check(
            meta_graph_conn, rjson['targetnode'], verbose)
        if (l == 0):
            fail = fail + 1
    else:
        fail = fail + 1

    if (rjson["matchnode"]):
        if (verbose > 2):
            print(
                'gndw_rwl_verify_rules: verify if matchnode exists:' +
                rjson['matchnode'])
        l = gndw_metarepo_metanode_check(
            meta_graph_conn, rjson['matchnode'], verbose)
        if (l == 0):
            fail = fail + 1
    else:
        fail = fail + 1

    return fail


def gndw_rel_rules_bizrulesmaster_nrules_get(meta_graph_conn, verbose):

    with meta_graph_conn.session() as metaGrpDB_Ses:

        cqlqry = "MATCH (x {name:'BizRulesMaster'})-[r :RULE]->() return COUNT(r)"
        nodes = metaGrpDB_Ses.run(cqlqry)
        for n in nodes:
            nr = n["COUNT(r)"]
            print(
                "gndw_rel_rules_bizrulesmaster_nrules_get: number of rules " +
                str(nr))

        if (nr == 0):
            # verify that BizRulesMaster exist
            cqlqry = "MATCH(x:BizRulesMaster {name:'BizRulesMaster', type:'RuleMasterNode'}) return PROPERTIES(x)"
            nodes = metaGrpDB_Ses.run(cqlqry)
            len = 0
            for n in nodes:
                len = len + 1

            if (len == 0):
                # BizRulesMaster does not exist
                # Create new tablenode  entity does not exist in db
                cqlqry = "CREATE (m:BizRulesMaster {name:'BizRulesMaster' , type:'RuleMasterNode', nrules: '0'})"

                metaGrpDB_Ses.run(cqlqry)
                if (verbose > 3):
                    print(
                        "gndw_rel_rules_bizrulesmaster_nrules_get: CREATED BizRulesMaster Node CQLqry:",
                        cqlqry)

        return nr


def gndw_rel_rules_bizrule_metaconn(meta_graph_conn, rjson, verbose):

    with meta_graph_conn.session() as metaGrpDB_Ses:

        cqlqry = ""


def gndw_rel_rules_bizrule_exists(meta_graph_conn, rjson, verbose):

    with meta_graph_conn.session() as metaGrpDB_Ses:

        cqlqry = "MATCH (m {name:'BR" + str(rjson["rulename"]) + "'}) return m"
        nodes = metaGrpDB_Ses.run(cqlqry)
        len = 0

        for n in nodes:
            len = len + 1

        if (verbose > 2):
            print("gndw_rel_rules_bizrule_exists: BizRule " +
                  rjson["rulename"] + " len:" + str(len))

        return len

# Add new Match rule


def gndw_rel_rules_bizrule_relation_exists(meta_graph_conn, rulename, verbose):

    rulenode = "BR_" + str(rulename)

    with meta_graph_conn.session() as metaGrpDB_Ses:
        cqlqry = "MATCH (n)-[r:RULE]-(p {name:'" + \
            str(rulenode) + "'}) return p"
        nodes = metaGrpDB_Ses.run(cqlqry)
        len = 0
        for n in nodes:
            len = len + 1
        if (verbose > 2):
            print(
                'gndw_rel_rules_bizrule_relation_exists: Bix rule for node ' +
                str(rulenode) +
                ' len:' +
                str(len))

        return len


def gndw_rel_rules_bizrule_node_exists(meta_graph_conn, rulename, verbose):

    rulenode = "BR_" + str(rulename)

    with meta_graph_conn.session() as metaGrpDB_Ses:

        cqlqry = "MATCH (n {name:'" + rulenode + "'})  return n"
        nodes = metaGrpDB_Ses.run(cqlqry)

        len = 0
        for n in nodes:
            len = len + 1

        if (verbose > 4):
            print('gndw_rel_rules_bizrule_node_exists: rulenode' +
                  str(rulenode) + ' ' + str(len) + '  nodes')

        return len


def gndw_rel_rules_bizrule_add(meta_graph_conn, rjson, nrules, verbose):

    rid = nrules + 1

    with meta_graph_conn.session() as metaGrpDB_Ses:

        rulename = rjson['rulename']
        # Check if BizRule Node exists
        found = gndw_rel_rules_bizrule_node_exists(
            meta_graph_conn, rulename, verbose)

        if (found == 0):
            cqlqry = "MERGE (m:BR_" + str(rulename) + " "
            cqlqry += "{ id:'" + str(rid) + "', name:'BR_" + \
                str(rjson["rulename"]) + "',"
            cqlqry += "srcnode:'" + str(rjson["srcnode"]) + "',"
            cqlqry += "targetnode:'" + str(rjson["targetnode"]) + "',"
            cqlqry += "relation:'" + rjson["relation"] + "',"
            cqlqry += "matchnode:'" + rjson["matchnode"] + "',"
            cqlqry += "type:'BizRuleNode', "
            cqlqry += "matchcondition:'" + rjson["matchcondition"] + "' })"

            metaGrpDB_Ses.run(cqlqry)

            if (verbose > 2):
                print(
                    "gndw_rel_rules_bizrule_add: Adding BizRule Node CQLqry:",
                    cqlqry)

        rulename = rjson['rulename']

        rfound = gndw_rel_rules_bizrule_relation_exists(
            meta_graph_conn, rulename, verbose)

        if (rfound == 0):
            cqlqry = "MATCH (d {name:'BizRulesMaster'}),"
            cqlqry += "(p {name:'BR_" + str(rjson["rulename"]) + "'}) "
            cqlqry += "MERGE (d)-[:RULE {rid:'" + str(rid) + "'}]->(p)"
            ##cqlqry += "(p {name:'BR"+str(rid)+"'}) ";

            metaGrpDB_Ses.run(cqlqry)

        if (verbose > 2):
            print("gndw_rel_rules_bizrule_add: Adding BizRule RULE relation")

        #cqlqry = "MATCH (n {name:'BizRulesMaster'})-[r :RULE]->() return COUNT(r)";

        #n = metaGrpDB_Ses.run(cqlqry);
        # if (verbose > 2):
        #   print('gndw_rel_rules_bizrule_add: BizRulesMaster num RULE rels'+str(n))

        # Update nrules count
        #cqlqry =  "MATCH (d {name:'BizRulesMaster'}) ";
        #cqlqry += " SET d.nrules="+str(rid)+" return d.nrules ";

        # metaGrpDB_Ses.run(cqlqry);
        # if (verbose > 2):
        #    print("gndw_rel_rules_bizrule_add: BizRulesMaster updated nrules "+str(rid)+" ");


def gndw_rel_rules_metanode_rule_exists(meta_graph_conn, rjson, verbose):

    with meta_graph_conn.session() as metaGrpDBSes:

        # Check if Biz Rule Rulename exists

        srcnode = rjson["srcnode"]
        tgtnode = rjson["targetnode"]
        rulename = rjson["rulename"]

        cqlqry = "MATCH ()-[r:BRule_" + str(rulename) + \
            " {type:'BizRule'}]-()  return r"

        if (verbose > 4):
            print(
                'gndw_rel_rules_metanode_rule_exists: metanode bizrule exists cqlqry:' +
                cqlqry)

        rels = metaGrpDBSes.run(cqlqry)

        if (verbose > 2):
            print(
                'gndw_rel_rules_metanodes_add: BR Added ' +
                str(srcnode) +
                ' BR-' +
                str(rulename) +
                ' ' +
                str(tgtnode))
        len = 0
        for r in rels:
            len = len + 1

        return len


def gndw_rel_rules_metanodes_add(meta_graph_conn, rjson, verbose):

    with meta_graph_conn.session() as metaGrpDBSes:

        # Check if Biz Rule Rulename exists

        exists = gndw_rel_rules_metanode_rule_exists(
            meta_graph_conn, rjson, verbose)

        if (exists == 0):

            srcnode = rjson["srcnode"]
            tgtnode = rjson["targetnode"]
            rulename = rjson["rulename"]

            cqlqry = ""
            cqlqry = "MATCH (d {name:'" + str(srcnode) + \
                "' , type:'TableNode' }),"
            cqlqry += "(p {name:'" + str(tgtnode) + "'}) "
            cqlqry += "MERGE (d)-[:BRule_" + str(rulename) + \
                " {name:'" + str(rulename) + "', type:'BizRule'}]->(p)"

            if (verbose > 4):
                print(
                    'gndw_rel_rules_metanodes_add: Adding bizrule to meta nodes:' +
                    cqlqry)

            metaGrpDBSes.run(cqlqry)

            if (verbose > 2):
                print(
                    'gndw_rel_rules_metanodes_add: BR Added ' +
                    str(srcnode) +
                    ' BR-' +
                    str(rulename) +
                    ' ' +
                    str(tgtnode))


def gndw_rel_rules_metarepo_add(rjson, meta_graph_conn, verbose):

    # Metarepo Add Match Rule between source and target node on metarepo with
    # match condition
    with meta_graph_conn.session() as metaGrpDBSes:

        # check if rulenode exists
        nrules = gndw_rel_rules_bizrulesmaster_nrules_get(
            meta_graph_conn, verbose)
        rid = nrules + 1
        print(
            'gndw_rel_rules_metarepo_add: Add match condition to rulenode rid:' +
            str(rid))

        found = gndw_rel_rules_bizrule_exists(meta_graph_conn, rjson, verbose)
        found = 0
        retval = 0

        if (found == 0):
            gndw_rel_rules_bizrule_add(meta_graph_conn, rjson, nrules, verbose)

            # Add Rule between src metatnode to target
            gndw_rel_rules_metanodes_add(meta_graph_conn, rjson, verbose)

        else:
            if (verbose > 2):
                print(
                    'gndw_rel_rules_metarepo_add: Rule Name already exists ' + str(rjson["rulename"]))

        return retval


def gndw_rel_rules_add(rjson, meta_graph_conn, data_graph_conn, verbose):

    ###
    # argetnode": "Product", "relation": "BUYS",  "matchnode": "salesorder",
    # "matchcondition": "(salesorder.customerid = customer.customerid)
    # AND (salesorder.productid = product.productid)"}

    if (verbose > 2):
        print("gndw_relAPI:   snode:" + rjson["srcnode"])
        print("gndw_relAPI:   tnode:" + rjson['targetnode'])
        print("gndw_relAPI:   matchnode:" + rjson["matchcondition"])

    # first Add Meta repo MATCH_RULE
    retval = gndw_rel_verify_rules(
        rjson, meta_graph_conn, data_graph_conn, verbose)

    if (retval == 0):

        print(
            "gndw_rel_rules_add: rules are verified. Add relationships" +
            str(retval))

        retval = gndw_rel_rules_metarepo_add(rjson, meta_graph_conn, verbose)

        if (verbose > 4):
            print(
                'gndw_rel_rules_add: bizules nodes added to metarepo retval : ' +
                str(retval))

        retval = gndw_rel_bizrule_datarepo_add(
            meta_graph_conn, data_graph_conn, rjson, verbose)

        if (verbose > 4):
            print('gndw_rel_rules_add: bizrules datanodes added ' + str(retval))

    else:

        if (verbose > 1):
            print(
                "gndw_rel_rules_add: ERROR rules are not verified  " +
                str(retval))

    return retval

# API to add business rules to graphdb


def gndw_bizrules_add_api(rjson, verbose):

    if (verbose > 3):
        print('gndw_bizrules_add: adding bizrules to meta and data repo')

    meta_graph_conn = gndwdb_neo4j_conn_metarepo(verbose)
    data_graph_conn = gndwdb_neo4j_conn_datarepo(verbose)

    ret = gndw_rel_rules_add(rjson, meta_graph_conn, data_graph_conn, verbose)

    return ret


# if __name__ == '__main__':
def bizrules_maintest_api():
    # Setting up metarepo and db repo conns
    verbose = 4
    meta_graph_conn = gndwdb_neo4j_conn_metarepo(verbose)
    data_graph_conn = gndwdb_neo4j_conn_datarepo(verbose)
    rfilepath = "./r.json"

    # with open(os.path.join(JSON_PATH, jfile)) as json_file:
    with open(rfilepath) as rfile:
        rjson = json.load(rfile)
        # matchcondition: "(salesorder.customerid \= customer.customerid) AND
        # (salesorder.productid = product.productid)" }'

        gndw_rel_rules_add(rjson, meta_graph_conn, data_graph_conn, verbose)
