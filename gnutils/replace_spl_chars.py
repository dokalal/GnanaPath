import re
import pandas
def replace_special_chars(func):
    def sub_chars(*args,**kwargs):
      res=func(*args, **kwargs)
      if len(args)==3 and kwargs.get('replace')==True:
        if type(args[2])==pandas.core.indexes.base.Index:
            res=args[2].str.replace(args[0],args[1])
        else:
            res =re.sub(args[0],args[1],args[2])
      return res
    return sub_chars


@replace_special_chars
def filter_chars(*args, **kwargs):
    arg_len=len(args)
    if not arg_len==0:
        return args[len(args)-1]
    return "Function called with no args"




def       gnutils_filter_neo4j_val(dtval):

         dtvalstr = str(dtval);
         dtval_s1 = dtvalstr.replace("'", "\\'");
         dtval_s2 = dtval_s1.replace('"', '\\"');
         ###dataval_s3 = dataval_s2.replace('\\', '\\\')
         dtval_filtered = str(dtval_s2);
         return dtval_filtered;


def       gnutils_filter_json_escval(dtval):

         dtvalstr = str(dtval);
         dtval_s2 = dtvalstr.replace('"', '\\"');
         ###dataval_s3 = dataval_s2.replace('\\', '\\\')
         dtval_filtered = str(dtval_s2);
         return dtval_filtered;

     
