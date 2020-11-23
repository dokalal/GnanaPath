###################################################################
#  GnanaPath Main App Server
#
#
#
#################################################################

import flask
from flask import request, jsonify, request, redirect, render_template,flash,url_for,session,Markup,abort
from werkzeug.utils import secure_filename
from flask_login import login_required, current_user, login_user, logout_user,LoginManager
import sys,os
import base64
import reg_users
from moz_sql_parser import parse
import json,re
from connect_form  import ConnectServerForm,LoginForm
from collections import OrderedDict
from gnp_db_ops import ConnectModel
#### Append system path

curentDir=os.getcwd();
listDir=curentDir.rsplit('/',1)[0];
#print(' Test listdir: '+listDir)
sys.path.append(listDir);


from gnsearch.gnsrch_sql_srchops  import gnsrch_sqlqry_api;
from gndwdb.gndwdb_neo4j_fetchops import gndwdb_metarepo_nodes_fetch_api, gndwdb_metarepo_edges_fetch_api;
from gndwdb.gndwdb_neo4j_conn import  gndwdb_neo4j_conn_check_api;
from gnutils.get_config_file import get_config_neo4j_conninfo_file;


def    dequote(s):
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

login_manager = LoginManager()
login_manager.init_app(app)

usr = reg_users.User()
all_users={"gnadmin":usr}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@login_manager.user_loader
def load_user(user_id):
    return all_users.get(user_id)

@app.route('/', methods=['GET'])
def gn_home():
    return render_template('base_layout.html') 

@app.route('/upload', methods=['GET','POST'])
@login_required
def upload_file():
    if request.method == 'GET':
       return render_template('upload.html') 
    if request.method == 'POST':

        if 'fd' not in request.files:
            flash('No file part','danger')
            return redirect(request.url)

        files = request.files["fd"]
        print(files)

        if not allowed_file(files.filename):
            flash('Please upload CSV or JSON file','danger')
            return redirect(request.url)
        elif files and allowed_file(files.filename):
            filename = secure_filename(files.filename)
            files.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))

        flash(f'File {filename} successfully uploaded','success')
        return redirect('/')

def neo4j_conn_check_api():
    verbose = 0;
    cfg_file = get_config_neo4j_conninfo_file();
    res=gndwdb_neo4j_conn_check_api(cfg_file, verbose);    
    return res

@app.route("/connect", methods=['GET', 'POST'])
@login_required
def connect_server():
  
    form = ConnectServerForm()
    connect = ConnectModel()
    if 'serverIP' in session:
       srv_ip_encode = base64.urlsafe_b64encode(session['serverIP'].encode("utf-8"))
       srv_encode_str = str(srv_ip_encode, "utf-8")
       flash(Markup('Already connected to neo4j server,Click <a href=/modify/{}\
             class="alert-link"> here</a> to modify'.format(srv_encode_str)),'warning')
       return redirect("/")
    if form.validate_on_submit():
        result = request.form.to_dict()
        connect.insert_op(result)
        res=neo4j_conn_check_api();
        if res=="Error":
           flash(f'Error connecting to neo4j server {form.serverIP.data}', 'danger')
           connect.delete_op(result)
           return render_template('connect.html', title='Connect Graph Server', form=form)
        else:    
           session['serverIP']=form.serverIP.data
           flash(f'Connected to server {form.serverIP.data}!', 'success')
           return redirect('/')
    return render_template('connect.html', title='Connect Graph Server', form=form)

@app.route("/modify/<serverIP>",methods=['GET','POST'])
@login_required
def modify_conn_details(serverIP):
  decoded_bytes = base64.b64decode(serverIP)
  servIP = str(decoded_bytes, "utf-8")
  form = ConnectServerForm()
  connect=ConnectModel()
  serv_details=connect.search_res(servIP)
  if request.method=='GET':
     form.serverIP.data = serv_details[0]['serverIP']
     form.username.data = serv_details[0]['username']
     form.password.data = serv_details[0]['password']
     form.connect.label.text='Update'
     return render_template("connect.html", title='Modify connection details',form=form)
  if request.method=='POST':
     if form.validate_on_submit():
        result = request.form.to_dict()
        connect.update_op(servIP,result)
        res=neo4j_conn_check_api()
        if res=="Error":
           flash(f'Error connecting to neo4j server {form.serverIP.data}', 'danger')
           form.connect.label.text='Update'
           connect.delete_op(result)
           session.pop('serverIP', None)
           return redirect('/')  
        flash('Connection details modified successfully','success')
        session['serverIP']=form.serverIP.data
        return redirect('/')
     else:
        flash('Error in  Server Connection parameters','danger')
        form.connect.label.text='Update'
        return render_template('connect.html', title='Modify connection details', form=form)

 

@app.route("/logout/")
@login_required
def logout():
    logout_user()
    session.pop('serverIP', None)
    if os.path.exists('server_config.json'):
        os.remove('server_config.json')
    return redirect(url_for('user_login'))

@app.route("/login", methods=['GET','POST'])
def user_login():
    form = LoginForm()
    if request.method == 'GET':
       return render_template('login.html', title='Login ', form=form)
    
    username = request.form["username"]
    if username not in all_users:
        flash(f'Invalid  user','danger')
        return render_template("login.html", form=form)
    user = all_users[username]
    if not user.check_password(request.form["password"]):
        flash(f'Incorrect password','danger')
        return render_template("login.html", form=form)

    login_user(user)
    flash("Please input the server config details",'success')
    return redirect(url_for('connect_server'))


@app.route('/gnsrchview', methods=['GET'])
@login_required
def  gnview_cola_api():
    print('GnApp: gnview cola is initiated');
    return render_template('gnview/gnsrchview.html');


@app.route('/api/v1/search',methods=['GET'])
@login_required
def   gnsrch_api():

     verbose = 5;
     srchqry = '';
     print('GNPAppserver: search api ');     
     #### Get srchstring and then pass to search func
     if 'srchqry' in request.args:
          srchqry = request.args['srchqry'];

          if (verbose > 3):
             print('GNPAppServer: search qry string:'+srchqry);

          # Remove "' begining and end
          srchqry_filtered = dequote(srchqry);

          slen = len(srchqry_filtered);
          if (slen == 0):
              res_data = '';

              if (verbose > 3):
                  print('GNPAppServer: Input search qry string is empty');
              return res_data;

              
          
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


@app.route('/gnmetaview', methods=['GET'])
@login_required
def  gnmetaview_cola_api():
    print('GnApp: gnview cola is initiated');
    return render_template('gnview/gnmetaview.html');



@app.route('/api/v1/metanodes',methods=['GET'])
@login_required
def   gnmetanodes_fetch_api():

     verbose = 5;
     print('GNPAppserver: Meta nodes search api ');
     #### Get srchstring and then pass to search func
     if 'srchqry' in request.args:
          srchqry_raw = request.args['srchqry'];

          # Remove "' begining and end
          srchqry = dequote(srchqry_raw);

          #### Let us invoke gnsrch api
          if (verbose > 3):
             print('GNPAppServer: search qry for metanodes : '+srchqry);
     else:
         srchqry = '';


     #### call gnmeta node search api Right now ignore searchqry arg

     res = gndwdb_metarepo_nodes_fetch_api(verbose);
     res_data = re.sub("(\w+):", r'"\1":', res)

     if (verbose > 4):
        print('GNPAppServer:   res : '+res);

     rjson = {
             "status": "SUCCESS",
             "data": res_data
     }

     #return json.JSONDecoder(object_pairs_hook=OrderedDict).decode()
     return res_data;

     #return  json.dumps(rjson, indent=4, separators=(',', ': '))

@app.route('/api/v1/metaedges',methods=['GET'])
@login_required
def    gnmetaedges_fetch_api():

     verbose = 5;
     print('GNPAppserver: Meta edges search api ');
     #### Get srchstring and then pass to search func
     if 'srchqry' in request.args:
          srchqry_raw = request.args['srchqry'];

          # Remove "' begining and end
          srchqry = dequote(srchqry_raw);

          #### Let us invoke gnsrch api
          if (verbose > 3):
             print('GNPAppServer: search qry for metanodes : '+srchqry);

     else:
         srchqry = '';


     #### call gnmeta node search api Right now ignore searchqry arg

     res = gndwdb_metarepo_edges_fetch_api(verbose);
     res_data = re.sub("(\w+):", r'"\1":', res)

     if (verbose > 4):
        print('GNPAppServer:   res : '+res);

     rjson = {
             "status": "SUCCESS",
             "data": res_data
     }

     #return json.JSONDecoder(object_pairs_hook=OrderedDict).decode()
     return res_data;


      
if __name__ == '__main__':
     app.run(host='0.0.0.0', port=5050, debug=True);

