import os
#Get the base directory path, base_dir returns Gnanapath
BASE_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def get_json_path():
    # Get the appsrv dir path, construct the appsrv path from the basedir
    json_file_path=os.path.join(BASE_DIR,'gnappsrv')
    # Change the current working directory to appsrvpath
    os.chdir(json_file_path)
    # Check if the server_config json file exists 
    if os.path.isfile("server_config.json"):
       # Return the path if the json file exists
       config_path = os.path.join(json_file_path, "server_config.json")
       return config_path
    # User has not entered the connection details so return "Error"
    return "Error"
