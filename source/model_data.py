from pandas.core.frame import DataFrame
from typing import Dict,Callable
from pprint import pprint
from math import cos, sin,pi,sqrt,log,atan2
from sklearn.model_selection import ParameterGrid
from source.helper_functions import progress_bar
from source.load_data import path_to_df
from pathlib import Path

def grid_search(observed: DataFrame,parameter_dict: Dict,error_func: Callable,shape: DataFrame=None, filt: bool = False):
    grid_search = list(ParameterGrid(parameter_dict))
    grid_len = len(list(grid_search))
    errors = {}

    current = 1000

    for i,grid in enumerate(grid_search):
            # progress_bar(i,grid_len)
            # if i <= 3:
        if isinstance(shape, DataFrame):
            model = PloufModel(
            line = observed,
            shape= shape,
            top_bound= grid_search[i]['top'],
            bottom_bound= grid_search[i]['bottom'],
            inclination= -67,
            declination= 177,
            intensity= grid_search[i]['intensity']
            )

            model.run_plouf(filt)

            error = error_func(model)

            if error < current:
                current = error
                good_grid = grid

        else:
            model = None
            err = None
            
            model = PloufModel(
                line = observed,
                shape= path_to_df(grid_search[i]['shape'],raw=False),
                top_bound= grid_search[i]['top'],
                bottom_bound= grid_search[i]['bottom'],
                inclination= -67,
                declination= 177,
                intensity= grid_search[i]['intensity']
            )
            
            model.run_plouf(filt)

            error = error_func(model)

            if error < current:
                current = error
                good_grid = grid

    return current,good_grid

class PloufModel():

    def __init__(self,line: DataFrame,shape: DataFrame,
                    top_bound: float,bottom_bound: float,inclination: float,
                        declination: float,intensity: float) -> None:
        
        self.line = line 
        self.shape = shape
        self.top_bound = top_bound
        self.bottom_bound = bottom_bound
        self.inclination = inclination
        self.declination = declination
        self.intensity = intensity
        self.results: DataFrame = {}
        self.residuals: DataFrame  = {}

    @property
    def Parameters(self):
        pprint(self.__dict__)

    def _center_line(self,x0: int, y0: int):

        #Centering data around the middle of Survey Line  
        self.line['Northing'] -= x0
        self.line['Easting'] -= y0

        self.line = self.line


    def _center_shapes(self,x0: int, y0: int):

        self.shape['Northing'] -= x0
        self.shape['Easting'] -= y0

        self.shape = self.shape

    def plouf(self,X,Y,z1):
 
        #Setting Parameters
        # print('Running Plouf...')
        x = X
        y = Y
        z2 = self.bottom_bound
        sides=len(X)-1
        df_model = DataFrame()

        #x must be the northing (north coordinate), 
        #y must be the easting (east coordinate),
        #Map coordinates, must be clockwise 

        minc = self.inclination*(pi/180) #down in rad 
        mdec = self.declination*(pi/180) #east in rad

        #calculate direction cosines of magnetiation
        ml = cos(minc) * cos(mdec)
        mm = cos(minc) * sin(mdec)
        mn = sin(minc)

        #components of magnetization in x,y,z, directions

        mx = self.intensity*ml
        my = self.intensity*mm
        mz = self.intensity*mn

        # set earths field
        einc = 62*(pi/180)
        edec = 12*(pi/180)

        el = cos(einc) * cos(edec)
        em = cos(einc) * sin(edec)
        en = sin(einc)

        #proportionality constant 

        prop = 400*pi

        northing_list =[]
        easting_list =[]
        b_total_list =[]
    ########################################################
        #insert for J loop
        for j,k in zip(self.line.Northing,self.line.Easting):
            #px is the northing of the observation point
            #py is the easting of the observation point
            #northing and easting observation points 
            px = j
            py = k
            
            #setting volume integral to zero 
            v1 = 0
            v2 = 0
            v3 = 0 
            v4 = 0 
            v5 = 0 
            v6 = 0 
            
            for i in range(sides):
                if i == (sides-1):
                    x1 = x[i]-px
                    y1 = y[i]-py
                    x2 = x[0]-px
                    y2 = y[0]-py
                else:
                    x1 = x[i]-px
                    y1 = y[i]-py
                    x2 = x[i+1]-px
                    y2 = y[i+1]-py
                    
                #calculate geometry 
                delta_x = x2-x1
                delta_y = y2-y1
                delta_s = sqrt(delta_x**2 + delta_y**2)
            
                #avoid division by zero if delta s = 0
                if delta_s == 0:
                    delta_s = 0.1
            
                c = delta_y/delta_s
                s = delta_x/delta_s
                p = ((x1*y2) - (x2*y1))/delta_s
                
                d1 = x1*s + y1*c
                d2 = x2*s + y2*c

                r1sq = x1**2 + y1**2
                r2sq = x2**2 + y2**2

                r11 = sqrt(r1sq + z1**2)
                r12 = sqrt(r1sq + z2**2)
                r21 = sqrt(r2sq + z1**2)
                r22 = sqrt(r2sq + z2**2)
                
                f = log((r22+z2)/(r12+z2)*(r11+z1)/(r21+z1))
                q = log((r22+d2)/(r12+d1)*(r11+d1)/(r21+d2))
                w = atan2((z2*d2),(p*r22)) - \
                    atan2((z2*d1),(p*r12)) - \
                    atan2((z1*d2),(p*r21)) + \
                    atan2((z1*d1),(p*r11))
                
                v1 += s*c*f - c*c*w
                v2 += s*c*w + c*c*f
                v3 += c*q
                v4 += -(s*c*f + s*s*w)
                v5 += -(s*q)
                v6 += w

            #calculate the components of the magnetic field 
            bx = prop*(mx*v1 + my*v2 + mz* v3);
            by = prop*(mx*v2 + my*v4 + mz* v5);
            bz = prop*(mx*v3 + my*v5 + mz* v6);

            #calculate total anomaly
            #this calculation works for anomaly << total field strength
            b_total = el*bx + em*by + en*bz
            
            northing_list.append(px)       
            easting_list.append(py)         
            b_total_list.append(b_total)

        df_model['mag'] = b_total_list
        df_model['ydist'] = northing_list
        df_model['xdist'] = easting_list

        return df_model


    def run_plouf(self,filt: bool =False):

        x0 = self.line.Northing.median() 
        y0 = self.line.Easting.median()

        # self._shapes_path_to_list()
        self._center_line(x0,y0)
        self._center_shapes(x0,y0)

        shapex = self.shape.Northing.tolist()

        shapey = self.shape.Easting.tolist()

        self.results= self.plouf(shapex,shapey,self.top_bound)

        self.residuals = self.results.copy()

        if not filt:
            self.residuals["mag"] = (self.line.Mag_nT - self.results.mag)
        else:
            self.residuals["mag"] = (self.line.filtered - self.results.mag)

        # print('Model Made')
