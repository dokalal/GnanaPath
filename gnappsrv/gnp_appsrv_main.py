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
app.secret_key = "s3cr3tk3y"
app.config['MAX_CONTENT_LENGTH'] = 256 * 1024 * 1024

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
          res_data = re.sub("(\w+):", r'"\1":', res)
          
          if (verbose > 4):
             print('GNPAppSrch:   res : '+res);
             
          rjson = {
                    'status': "SUCCESS",
                    'data': res_data
          }


          
          return res_data
          
     else:
          errstr = { 
                     'status':'ERROR', 
                     'errmsg':"No Search query provided "
                   }
          ###return jsonify('{status:"ERROR", errmsg:"No Search query provided "}');
          return jsonify(errstr);



if __name__ == '__main__':
     app.run(host='0.0.0.0', port=5050);

