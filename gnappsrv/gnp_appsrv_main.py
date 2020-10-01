###################################################################
#  GnanaPah Main App Server
#
#
#
#################################################################

import flask
from flask import request, jsonify
import sys
import os
from moz_sql_parser import parse
import json

#### Append system path

curentDir=os.getcwd();
listDir=curentDir.rsplit('/',1)[0];
#print(' Test listdir: '+listDir)
sys.path.append(listDir);

from gnsearch.gnsrch_sql_srchops  import gnsrch_sqlqry_api;







def dequote(s):
    """
    If a string has single or double quotes around it, remove them.
    Make sure the pair of quotes match.
    If a matching pair of quotes is not found, return the string unchanged.
    """
    if (s[0] == s[-1]) and s.startswith(("'", '"')):
        return s[1:-1]
    return s




app = flask.Flask(__name__);
app.config["DEBUG"] = True;


@app.route('/', methods=['GET'])
def  gn_home():
     htmlStr = '<h2>Welcome Gnanapath</h2>';
     htmlStr += '<p> Gnanapath provide business data platform </p>';
     return htmlStr;



@app.route('/api/v1/search',methods=['GET'])
def   gnsrch_api():

     verbose = 5;
     print('GNPAppserver: search api ');     
     #### Get srchstring and then pass to search func
     if 'srchqry' in request.args:
          srchqry = request.args['srchqry'];

          # Remove "' begining and end
          srchqry_filtered = dequote(srchqry);
              
          #### Let us invoke gnsrch api
          if (verbose > 3):
             print('GNPAppServer: search qry : '+srchqry_filtered);

          
          #### call gnsearch api
          res = gnsrch_sqlqry_api(srchqry_filtered, verbose);
          
          if (verbose > 4):
             print('GNPAppSrch:   res : '+res);
             
          rjson = {
                    'status': "SUCCESS",
                    'data': res
          }


          
          return jsonify(rjson);
          
     else:
          errstr = { 
                     'status':'ERROR', 
                     'errmsg':"No Search query provided "
                   }
          ###return jsonify('{status:"ERROR", errmsg:"No Search query provided "}');
          return jsonify(errstr);



if __name__ == '__main__':
     app.run(host='0.0.0.0', port=5050);

