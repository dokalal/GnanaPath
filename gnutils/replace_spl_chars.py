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