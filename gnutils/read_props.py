import yaml
import os
class LoadConfigUtil:
    BASE_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    SAMPLE_DATA=os.path.join(BASE_DIR,"gnsampledata")
    
    @staticmethod
    def get_config_data():
        
        FILE_PATH=os.path.join(LoadConfigUtil.SAMPLE_DATA,"config.yml")
        try:
            with open(FILE_PATH) as f:
                data = yaml.safe_load(f)
            return data
        except Exception as e:
            print("***** An Exception occured in the LoadConfigUtil class ***** \n {}".format(str(e)))
