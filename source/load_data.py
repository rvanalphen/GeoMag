import string
import numpy as np
from typing import Tuple
from xmlrpc.client import Boolean
from pandas import DataFrame,read_csv


def _parse_file(filepath: string) -> Tuple:
    import csv
    try:
        with open(filepath, 'r') as tmp:
            sniffer = csv.Sniffer()
            dialect = sniffer.sniff(tmp.read(4024))  #### detect delimiters
            head = sniffer.has_header(tmp.read(4024))  #### detect header 
            tmp.seek(0)
            if not head:
                head = 'None'
            head = 0
        return (dialect.delimiter,head)
    except csv.Error:
        with open(filepath, 'r') as tmp:
            sniffer = csv.Sniffer()
            head = sniffer.has_header(tmp.read(4024))  #### detect header 
            tmp.seek(0)
            if not head:
                head = 'None'
            head = 0
        return (',',head)

def path_to_df(filepath: string,raw:Boolean) -> DataFrame:

    if raw == False:
        params = _parse_file(filepath)
        return read_csv(filepath,sep=params[0],header=params[1])
    else:
        df = read_csv(filepath,skiprows=20,header=1,sep=';')
        del df['Unnamed: 17']
        del df[' Satellites']
        del df[' Quality']
        del df[' AccX [g]']
        del df[' AccY [g]'] 
        del df[' AccZ [g]']
        del df[' Temp [Deg]']
        df[[' Latitude [Decimal Degrees]', ' Longitude [Decimal Degrees]', ' Altitude [m]',' GPSTime']]=df[[' Latitude [Decimal Degrees]', ' Longitude [Decimal Degrees]',' Altitude [m]',' GPSTime']].replace(0,np.nan).interpolate()
        df.rename(columns={' Latitude [Decimal Degrees]':"Lat",' Longitude [Decimal Degrees]':"Long",},inplace=True)
        
        return df 
