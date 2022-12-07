from abc import ABC, abstractmethod
from ast import Str
from lib2to3.pgen2.token import STRING
from pathlib import Path
from pandas import DataFrame, Series
import pandas as pd
import numpy as np
from typing import List, Tuple, Union
import datetime
import requests

class MagCorrector:

    def _parse_dates(self, dates: List[str]) -> Tuple:
        
        if len(dates) > 1:
            dates = sorted(dates, key=lambda x: datetime.datetime.strptime(x, '%Y-%m-%d'))
        
            startyear,startmonth,startday = dates[0].split('-')
            endyear,endmonth,endday = dates[-1].split('-')
       
        else:
            startyear,startmonth,startday = dates[0].split('-')
            endyear,endmonth,endday = dates[0].split('-')

        return (startyear,startmonth,startday),(endyear,endmonth,endday)


    def _get_value(self, data: DataFrame, dates: List[str], elevation: str) -> Union[float,int]:

        start,end = self._parse_dates(dates)

        lat = np.mean(data.Lat.values)
        lon = np.mean(data.Long.values)

        url = ('https://www.ngdc.noaa.gov/geomag-web/calculators/calculateIgrfwmm?model=IGRF&elevationUnistation_formatted=M&'
        'coordinateSystem=D&lat1=%s&lon1=%s&elevation=%s&startYear=%s&startMonth=%s&startDay=%s&endYear=%s&endMonth=%s'
        '&endDay=%s&resultFormat=json') % (
            lat, lon, elevation, start[0],start[1],start[2],end[0],end[1],end[2])

        print(url)
        r = requests.get(url)
        if r.status_code != 200:
            raise requests.exceptions.RequestException
        
        print(r.json()['result'][0]['totalintensity'])
        return r.json()['result'][0]['totalintensity']

    def global_detrend(self, data: DataFrame, value: Union[float,int] = None, dates: List[str] = None, elevation: str = None) -> None:
        if not value:
            value = self._get_value(data, dates, elevation)

        data.Mag_nT -= value

    def _get_time_from_raw(self, filepath,data):
        #to get real time 
        starttime = filepath.name.split(".")[0]
        T0 = pd.to_datetime(starttime,format="%Y%m%d_%H%M%S")
        idx = pd.date_range(T0,periods=len(data[" GPSTime"]),freq='5L')
        data.index=idx

    def _get_and_format_station(self, station_file: str):
        station=pd.read_csv(station_file,skiprows=18,header=0,delimiter=r"\s+")
        c=station.columns
        del station[c[-1]]
        station['datetime']=pd.to_datetime(station[c[0]]+'T'+station[c[1]])
        
        return pd.Series(data = station[c[6]].values, index = station['datetime'])
            
    def _four_difference(self, station_formatted: Series):
        #four difference 
        station_formatted[(station_formatted.shift(periods=-2)+station_formatted.shift(periods=2)-4*(station_formatted.shift(periods=-1)+station_formatted.shift(periods=1))+6*station_formatted)/16>0.1]=99999
        station_formatted[(station_formatted.shift(periods=-2)+station_formatted.shift(periods=2)-4*(station_formatted.shift(periods=-1)+station_formatted.shift(periods=1))+6*station_formatted)/16<-0.1]=99999
        station_formatted[station_formatted==99999]=np.nan
        return station_formatted.interpolate()
          
    def _shift_station(self, station_four_diff: Series,station_long:float,data_long:float):
        #shift by station long difference 240 sec per deg
        difTime = int((data_long-station_long)*86400/360+0.5)
        return station_four_diff.shift(periods=difTime)
        
    def _trim_station(self, station_shifted: Series,data:DataFrame):
        #cuttting it to match data
        ts1 = station_shifted[(station_shifted.index >= data.index[0])&(station_shifted.index<=data.index[-1])]
        return ts1.resample('5L').interpolate()
    
    def dirunal_correction(self,data_filepath:Path,data:DataFrame,station_file:str,station_long:float):
        idx = self._get_time_from_raw(data_filepath,data)
        station = self._get_and_format_station(station_file)
        station = self._four_difference(station)

        data_long = data.Long.mean()+360
        station = self._shift_station(station,station_long,data_long)
        station = self._trim_station(station,data)

        data.rename(columns={"Mag_nT":"Mag_nT_uncorr"},inplace=True)
        data["Mag_nT"] = data["Mag_nT_uncorr"]-station
        data.dropna(inplace=True)
        
    def minus_mean(self,data: DataFrame):
        print(data["Mag_nT"].mean())
        data.Mag_nT -=  data['Mag_nT'].mean()

class MagDetrender(ABC):

    @abstractmethod
    def _make_detrend_line(self):
        pass

    @abstractmethod
    def detrend(self):
        pass

class NorthSouthDetrend(MagDetrender):

    def _make_detrend_line(self,data: DataFrame):
        X1, Y1 = data.Northing.min(), data.Mag_nT.iloc[0]
        X2, Y2 = data.Northing.max(), data.Mag_nT.iloc[-1]
        dy = (Y2-Y1)
        dx = (X2-X1)
        m = (dy/dx)
        b = Y1 - (m*X1)

        correction_line = []
        for i,_ in enumerate(data.index):
            mx = data.Northing.values[i] *m
            y = mx +b 
            correction_line.append(y)

        return correction_line

    def detrend(self, data: DataFrame):

        data = data.sort_values(by=['Northing'], ascending=True).reset_index(drop=True)
        
        correction_line = self._make_detrend_line(data)
        
        detrend_mag =[]
        for i,_ in enumerate(data.index):
            detrend_mag.append(
                data.Mag_nT.values[i]-correction_line[i]
            )

        data['Mag_nT'] = detrend_mag

        return data

class EastWestDetrend(MagDetrender):

    def _make_detrend_line(self,data: DataFrame):
        X1, Y1 = data.Easting.min(),data.Mag_nT.iloc[0]
        X2, Y2 = data.Easting.max(),data.Mag_nT.iloc[-1]
        dy = (Y2-Y1)
        dx = (X2-X1)
        m = (dy/dx)
        b = Y1 - (m*X1)

        correction_line = []
        for i,_ in enumerate(data.index):
            mx = data.Easting.values[i] *m
            y = mx +b 
            correction_line.append(y)

        return correction_line

    def detrend(self, data: DataFrame):

        data = data.sort_values(by=['Easting'], ascending=True).reset_index(drop=True)
        
        correction_line = self._make_detrend_line(data)
        
        detrend_mag =[]
        for i,_ in enumerate(data.index):
            detrend_mag.append(
                data.Mag_nT.values[i]-correction_line[i]
            )

        data['Mag_nT'] = detrend_mag

        return data
