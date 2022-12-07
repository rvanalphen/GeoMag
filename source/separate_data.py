from abc import ABC, abstractmethod
from math import dist
from typing import Dict, Tuple, Union,List
from pandas.core.frame import DataFrame
from shapely.geometry import Point,Polygon,LineString
from geopandas import GeoSeries,GeoDataFrame

def rename_dict(dictionary):
    i = 0
    new_dict={}
    for key,_ in dictionary.items():
        i+=1
        if i > 9:
            newkey = 'line_{}'.format(i)
        else:
            newkey = 'line {}'.format(i)
           
        if newkey!=key:  
            new_dict[newkey] = dictionary[key]
        else:
            new_dict[key] = dictionary[key]
    return new_dict

class DataSeparator(ABC):
    
    @abstractmethod
    def _parameter_calculator(self) -> None:
        pass
    
    @abstractmethod
    def split(self, data: DataFrame):
        pass

class DistanceSperator(DataSeparator):

    #! this version calculates distance between points
    def _parameter_calculator(self, data: DataFrame) -> None:

            distances = []
            length = len(data.index)
            cols = data.columns
            data.sort_values(by=['Northing'], ascending=True).reset_index(drop=True)

            if 'Easting' in cols:
                for i in range(length-1):
                    pointa = (
                        data.Easting.values[i], data.Northing.values[i])
                    pointb = (
                        data.Easting.values[i+1], data.Northing.values[i+1])

                    distances.append(dist(pointa, pointb))
            else:
                print('no easting')
                exit()

            distances.insert(0, 0)

            data["Dist"] = distances

    def split(self, data: DataFrame, cutoff_length=99, cutoff_dist=None) -> Dict:
        cols = data.columns
        if 'Dist' not in cols:
            self._parameter_calculator(data)

        if cutoff_dist == None:
            cutoff_dist = data.Dist.mean()+5

        line_dict = {}
        t = 0
        k = 1
        length = len(data.index)
        i = 0
        while i <= length-1:
            if data.Dist.values[i] > cutoff_dist:
                if k > 9:
                    key = 'line_'+str(k)
                else:
                    key = 'line '+str(k)
                line_dict[key] = data.iloc[t:i]
                t = i
                k += 1
            if i == length-1:
                if k > 9:
                    key = 'line_'+str(k)
                else:
                    key = 'line '+str(k)
                line_dict[key] = data.iloc[t:length]
            i += 1

        # returns the key-value pairs that are greater than the cut off length, so no short lines
        line_dict = {key: val for key, val in line_dict.items() if len(
            line_dict[key]) > cutoff_length}
        
        return rename_dict(line_dict)


class SingleSeparator(DataSeparator):
    
    def _parameter_calculator(self,line_params: Dict,buffer: int) -> Polygon:
            lines=[]
            for i, (key,value) in enumerate(line_params.items()):
                start = line_params[key][0]
                end = line_params[key][1]
                    
                line = LineString([start,end])
                line = (GeoSeries([line]).buffer(buffer)
    )
                lines.append(line)

            return lines

    def split(self, data: DataFrame, line_params: Dict, buffer: int) -> DataFrame:
        
        ideal_lines = self._parameter_calculator(line_params,buffer)
        
        dfp = GeoDataFrame(data,geometry=data.loc[:,["Easting","Northing"]].apply(Point, axis=1))
        
        line_dict = {}
        for i,line in enumerate(ideal_lines):
            name = 'line '+str(i+1)

            actual_line = GeoDataFrame(geometry=line).sjoin(dfp)
            
            line_dict[name] =  actual_line.iloc[:,2:].reset_index(drop=True)
            
        return line_dict         