###################################################################
#  GnanaPah Main App Server
#
#
#
#################################################################

import flask
from flask import request, jsonify, request, redirect, render_template,flash
from werkzeug.utils import secure_filename

import sys
import os
from moz_sql_parser import parse
import json,re
from connect_form  import ConnectServerForm
from collections import OrderedDict
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

app.secret_key = "f1eaff5ddef68e5025adef1db4cf7807"
app.config['MAX_CONTENT_LENGTH'] = 256 * 1024 * 1024
#app.config["JSONIFY_PRETTYPRINT_REGULAR"]=True
#Get current path
path = os.getcwd()
# file Upload
UPLOAD_FOLDER = os.path.join(path, 'uploads')

# Make directory if uploads is not exists
if not os.path.isdir(UPLOAD_FOLDER):
    os.mkdir(UPLOAD_FOLDER)

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Allowed extension you can set your own
ALLOWED_EXTENSIONS = set(['csv', 'json',])


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route('/', methods=['GET'])
def  gn_home():
	
     '''htmlStr = '<h2>Welcome Gnanapath</h2>';
     htmlStr += '<p> Gnanapath provide business data platform </p>';
     return htmlStr;'''

     return render_template('upload.html')

@app.route('/', methods=['POST'])
def upload_file():
    if request.method == 'POST':

        if 'fd' not in request.files:
            flash('No file part')
            return redirect(request.url)

        files = request.files["fd"]
        print(files)

        if not allowed_file(files.filename):
            flash('Please upload CSV or JSON file')
            return redirect(request.url)
        elif files and allowed_file(files.filename):
            filename = secure_filename(files.filename)
            files.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))

        flash('File(s) successfully uploaded')
        return redirect('/')

@app.route("/connect", methods=['GET', 'POST'])
def connect_server():
    form = ConnectServerForm()
    if form.validate_on_submit():
        flash(f'Connected to server {form.serverIP.data}!', 'success')
        return redirect('/')
    return render_template('connect.html', title='Connect Graph Server', form=form)

##### GnView
#@app.route('/gnview', methods=['GET'])
#def  gnview_api():
#    print('GnApp: gnview is initiated');
#    return render_template('gnview.html');



@app.route('/gnsrchview', methods=['GET'])
def  gnview_cola_api():
    print('GnApp: gnview cola is initiated');
    return render_template('gnview/gnsrchview.html');


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
          res = gnsrch_sqlqry_api(srchqry_filtered, verbose)
          res_data = re.sub("(\w+):", r'"\1":', res)         
         
          if (verbose > 4):
             print('GNPAppSrch:   res : '+res);
             
          rjson = {
                    "status": "SUCCESS",
                    "data": res_data
          }
          
          #return json.JSONDecoder(object_pairs_hook=OrderedDict).decode()
          return res_data 
         
          #return  json.dumps(rjson, indent=4, separators=(',', ': '))
          
     else:
          errstr = { 
                     'status':'ERROR', 
                     'errmsg':"No Search query provided "
                   }
          ###return jsonify('{status:"ERROR", errmsg:"No Search query provided "}');
          return jsonify(errstr);



if __name__ == '__main__':
     

     app.run(host='0.0.0.0', port=5050, debug=True);

