#importing library functions
from source.load_data import path_to_df
from pathlib import Path
from source.plot_data import plot_model,plot_residuals
from source.stats import*

def get_rmse(model: PloufModel):

    return sqrt(((model.residuals.mag) ** 2).mean())


def get_abs_max_error(model: PloufModel):
    
    return abs(model.residuals.mag).max()

# setting shape variable as before
SHAPE_DIR = './shapes'
LINE_DIR = './patches'

# SHAPE = path_to_df(Path(f'{SHAPE_DIR}/line_56a_shape2.utm'),raw=False) 
LINE = path_to_df(Path(f'{LINE_DIR}/example_line.csv'),raw=False)

bottoms = [50]
intensitys = [0.5]
shapes = ['./shapes/line_56a_shape2.utm']
tops = [41,42,43,44]

parameters = []
for i in bottoms:
    for j in intensitys:
        for k in shapes:
            for h in tops:
                parameters.append( ( i, j, k, h ) )

print("Available combinations : ",  parameters )

all_models = {}

for k in range( len( parameters ) ):
    print(k)

    shape = path_to_df(parameters[k][2],raw=False)
    
    name = 'model_'+str(k)

    all_models[name]  = PloufModel(
        line = LINE,
        shape= shape,
        top_bound= parameters[k][3],
        bottom_bound= parameters[k][0],
        inclination= -67,
        declination= 177,
        intensity= parameters[k][1]
        )
        
for model in all_models.values():

    model.run_plouf()
    print(get_abs_max_error(model))

