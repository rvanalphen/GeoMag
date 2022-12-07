from datetime import datetime
from typing import Dict,Union
from pathlib import Path, PosixPath
import os
from pandas.core.frame import DataFrame
from abc import ABC, abstractmethod

#TODO finshing making export classes 

class DataExporter(ABC):

    @abstractmethod
    def _make_directory(path: Union[Path, str]) -> str:
        pass

    @abstractmethod
    def _make_outfile(path: Union[Path, str],override_name: str = None) -> str:
        pass 

    @abstractmethod
    def exporter(path: Union[Path, str],data : DataFrame, lines: Dict,override_name: str = None) -> None:
        pass


class ExportPatch(DataExporter):

    def _make_directory(self,path: Union[Path, str]) -> str:
        if type(path) == PosixPath:
            parent_dir = path.parent

            outpath = str(parent_dir) + '/' + 'cleaned_data'
                
            if not os.path.isdir(outpath):
                os.mkdir(outpath)

            return outpath

        else:
            #TODO fill in for string
            pass
        

    def _make_outfile(self,path: Union[Path, str],override_name: str = None) -> str:

        original_file = os.path.basename(path)
        name,_= original_file.split('.')
        
        date_created = datetime.now().strftime("%Y %m, %d").replace(",","").replace(" ", "_")

        if override_name != None:
            new_file = override_name+'_processedOn_'+date_created+'.csv'
        else:
            new_file = name+'_processedOn_'+date_created+'.csv'
        
        return new_file

    def exporter(self,path: Union[Path, str],data : DataFrame, lines: Dict,override_name: str = None) -> None:
        outpath = self._make_directory(path)
        outfile = self._make_outfile(path,override_name)
        outpath = outpath+'/'+outfile

        if os.path.isfile(outpath):
            Q = input('Do you want to overwrite %s ? (y/n)' % outfile)
        else:
            Q = 'y'

        if Q =='y':
            data.to_csv(outpath, header=True, index=False)

            print('###')
            print('data output to %s' % outpath)
            print("###")

        else:
            print('###')
            print('data not saved')
            print('###')
            pass

class ExportLines(DataExporter):

    def _make_directory(self,path: Union[Path, str]) -> str:
        if type(path) == PosixPath:
            parent_dir = path.parent
        else:
            #TODO fill in for string
            pass
        
        outpath = str(parent_dir) + '/' + 'cleaned_lines'

        
        if not os.path.isdir(outpath):
            os.mkdir(outpath)

        return outpath

    def _make_outfile(self,path: Union[Path, str],key: str) -> str:

        original_file = os.path.basename(path)
        name,_= original_file.split('.')
        
        date_created = datetime.now().strftime("%Y %m, %d").replace(",","").replace(" ", "_")

        new_file = name+'_'+str(key)+'_processedOn_'+date_created+'.csv'
        
        return new_file

    def exporter(self,path: Union[Path, str],data : DataFrame, lines: Dict,override_name: str = None) -> None:
        for index, (key,value) in enumerate(lines.items()):
            outpath = self._make_directory(path)
            outfile = self._make_outfile(path,key)

            outpath = outpath+'/'+outfile

            if index ==0 and os.path.isfile(outpath):
                Q = input('Do you want to overwrite all line files ? (y/n)')
            else:
                Q = 'y'

            if Q =='y':
                lines[key].to_csv(outpath, header=True, index=False)

                print('###')
                print('data output to %s' % outpath)
                print("###")

            else:
                print('###')
                print('data not saved')
                print('###')
                break

class ExportAll(DataExporter):
    
    def _make_directory(self,path: Union[Path, str]) -> str:
        pass

    def _make_outfile(self,path: Union[Path, str]) -> str:
        pass

    def exporter(self,path: Union[Path, str], data: DataFrame, lines: Dict,override_name: str = None) -> None:
        ExportPatch().exporter(path,data,lines,override_name)
        ExportLines().exporter(path,data,lines,override_name)
