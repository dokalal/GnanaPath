###################################################################
#  GnanaPah Main App Server
#
#
#
#################################################################

import flask
from flask import request, jsonify


app = flask.Flask(__name__);
app.config["DEBUG"] = True;


@app.route('/', methods=['GET'])
def  gn_home():
     htmlStr = '<h2>Welcome Gnanapath</h2>';
     htmlStr += '<p> Gnanapath provide business data platform </p>';
     return htmlStr;



@app.route('/api/v1/search',methods=['GET'])
def   gnsrch_api():

     
     #### Get srchstring and then pass to search func
     if 'srchqry' in request.args:
          srchqry = request.args['srchqry'];
          #### Let us invoke gnsrch api

     else:
          return jsonify('{"status":"ERROR", "errmsg":"No Search query provided "}');




if __name__ = '__main__':
     app.run(host='0.0.0.0', port=5050);

