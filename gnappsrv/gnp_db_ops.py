import sys,os
import base64
from tinydb import TinyDB,Query,where
#### Append system path

class ConnectModel:
    def __init__(self):
        self.db=TinyDB('server_config.json')
        self.Connect_Query =Query()  
    
    def __req_fields_json(self,dict_result):
        req_items=['serverIP', 'username', 'password']
        self.req_dict = {key:value for key, value in dict_result.items() if key in req_items}  
    
    def search_op(self, dict_result):
        self.__req_fields_json(dict_result)
        if not self.db.search(self.Connect_Query.serverIP==self.req_dict['serverIP']):
            return False
        return True
    
    def search_res(self,servIP):
        serv_details=self.db.search(self.Connect_Query.serverIP==servIP)
        return serv_details  
   
    def insert_op(self, dict_result):
        if not self.search_op(dict_result):
            self.db.insert(self.req_dict)
            return True
        return False

    def delete_op(self,dict_result):
        if self.search_op(dict_result):
            self.db.remove(where('serverIP') == self.req_dict['serverIP'])
            return True  
        return False
    
    def update_op(self, old_srv_IP, dict_result):
        if not self.search_res(old_srv_IP):
           return False
        self.__req_fields_json(dict_result)
        self.db.update({'serverIP':self.req_dict['serverIP'],\
                        'username':self.req_dict['username'],\
                        'password':self.req_dict['password']},\
                         self.Connect_Query.serverIP==old_srv_IP)
       
        return True
    

   


