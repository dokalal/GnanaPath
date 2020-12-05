import pytest
import os
from pytest_mock import mocker
from GnanaPath.gnappsrv.gnp_db_ops import ConnectModel

#----------------------------------------------------------------------------
# Define setup and teardown methods and  define their scope
# TODO move the fixtures in conf.py
@pytest.fixture(autouse=True)
def load_test_data(tmpdir):
    _dict={'serverIP':'172.0.0.1:7787','username':'neo4j','password':'test$123'}
    con=ConnectModel(tmpdir)
    
    return _dict,con

@pytest.fixture(scope='module')
def init_db(load_test_data):
    
    yield
    load_test_data[1].stop_db()
#-----------------------------------------------------------------------------

# Test - Search for  existence and non existence of a Connection based on server ip
@pytest.mark.parametrize("test_input, expected",[(True,True),(False,False)],\
                         ids=["serverIPExists", "NoServerIPExists"])
def test_search_op(mocker,load_test_data,test_input,expected):
    _dict,con = load_test_data   
    mocker.patch('GnanaPath.gnappsrv.gnp_db_ops.ConnectModel.search_res',return_value=test_input)
    assert con.search_op(_dict) == expected

# Test insert a connection record with and without server ip value
@pytest.mark.parametrize("test_input, expected",[(True,"None_Insert"), \
                        (False, [{'serverIP':'172.0.0.1:7787','username':'neo4j','password':'test$123'}])],\
                        ids=["InsertNone:value exists", "Insert:No Value exists"])
def test_insert_op(mocker,load_test_data,test_input,expected):
    _dict,con = load_test_data
    mocker.patch('GnanaPath.gnappsrv.gnp_db_ops.ConnectModel.search_op',return_value=test_input)
    assert con.insert_op(_dict) == expected
    
_newdict={'serverIP':'192.10.10.1:7777','username':'testneo4j','password':'pass$123'}
_oldvalue='172.0.0.1:7787'  
@pytest.mark.parametrize("test_input, expected",[(_oldvalue, [_newdict]), ([],False)],\
                         ids=["Update: value found", "Update:None"])

#Test update connection record , test the cases with and without server ip values 
def test_update_op(mocker,load_test_data,test_input,expected):
    _dict,con=load_test_data
    mocker.patch('GnanaPath.gnappsrv.gnp_db_ops.ConnectModel.search_res',return_value=test_input)
    if test_input:
        mocker.patch('GnanaPath.gnappsrv.gnp_db_ops.ConnectModel.search_op',return_value=False)
        con.insert_op(_dict)
    assert con.update_op(_dict['serverIP'],_newdict)==expected

test_list=[({},{}),({"header":"abc",'serverIP':'172.0.0.1:7787','username':'neo4j','password':'test$123',"junk":"#@#"},\
               {'serverIP':'172.0.0.1:7787','username':'neo4j','password':'test$123'})]


#Test filtering the  request header to return only the values related to connection params
@pytest.mark.parametrize("test_input, expected",test_list,ids=['Emptydict','Extrakey'])
def test_req_fileds_json(load_test_data,test_input,expected):
    _temp,conn=load_test_data
    assert conn.req_fields_json(test_input)== expected

