from typing import Union,Dict
from pathlib import Path,Path
from os import listdir
from pandas import DataFrame,merge,concat
from functools import reduce
from sys import stdout

def files_to_dict(dirpath: Union[Path,str]) -> Dict:
    num = 0
    files_dict = {}
    for filename in listdir(dirpath):
        if '.' in filename and\
             '20191021_170314.txt' not in filename\
                 and '20191022_003746.txt' not in filename: 
            num += 1
            fullpath = Path(dirpath+filename)
            files_dict[str(num)] = fullpath

    return files_dict

def merge_object_data(dictionary: Dict) -> DataFrame:
    data_list =[]
    for key,value in dictionary.items():
        data_list.append(dictionary[key])
    print(data_list)
    return concat(data_list)
    # return reduce(lambda left, right: merge(left, right, how='outer'), data_list)

def progress_bar(Index:int,EndNum:int):
    bar_len = 60
    filled_len = int(round(bar_len * Index / float(EndNum)))
    status = 'Running Loop of length: {}'.format(f'{EndNum:,d}')
    percents = round(100.0*Index/float(EndNum),1)
    bar = chr(9608) * filled_len + '-' * (bar_len - filled_len)
    stdout.write(' [%s] %s%s ...%s\r' % (bar, percents, '%', status))
    stdout.flush()