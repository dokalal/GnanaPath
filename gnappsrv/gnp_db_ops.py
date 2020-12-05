
from tinydb import TinyDB,Query,where
import os
class ConnectModel:

    query = Query()
    
    def __init__(self,db_path):
        dbpath=os.path.join(db_path,'server_config.json') 
        self._db = TinyDB(dbpath)
 
    def req_fields_json(self,dict_result):
        req_items=['serverIP', 'username', 'password']
        return {key:value for key, value in dict_result.items() if key in req_items}  
    
    def search_op(self,req_dict):
        return True if self.search_res(req_dict['serverIP']) else False
    
    def search_res(self,servIP):
        return self._db.search(ConnectModel.query.serverIP == servIP)
   
    def insert_op(self,req_dict):
        if not self.search_op(req_dict):
            self._db.insert(req_dict)
            return self._db.all()
        return "None_Insert"

    def delete_op(self,req_dict):
        if self.search_op(req_dict):
            self._db.remove(where('serverIP') == req_dict['serverIP'])
            return self._db.all()
        return "None_Delete"
    
    def update_op(self, old_srv_IP,req_dict):
        if not self.search_res(old_srv_IP):
           return False
        self._db.update({'serverIP':req_dict['serverIP'],\
                        'username':req_dict['username'],\
                        'password':req_dict['password']},\
                         ConnectModel.query.serverIP==old_srv_IP)

        return self._db.all()
   
    def stop_db(self):
        self._db.close()
   


