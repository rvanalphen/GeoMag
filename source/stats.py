import matplotlib.pyplot as plt 
from pandas.core.frame import DataFrame
from source.model_data import PloufModel
from source.app import MagApp
from numpy import linspace
from math import sqrt
from numpy import mean, std, var
from scipy.stats import kurtosis,skew
from statsmodels.api import distributions
from statsmodels.stats.stattools import durbin_watson

def _ks_stats(dfree,y,y2):

    max_diff = abs(max(y-y2))
    confidence_level = 1.36/sqrt(dfree)

    df = DataFrame.from_dict(
            {'max difference':[max_diff],
            'degrees of freedom':[dfree],
            'at 95% confidence':[confidence_level]})
    
    print("Confidence Table:")
    print(df)
    return df 

def _plot_ks(x,y,x2,y2,df):
    # Initialize the vertical-offset for the stacked bar chart.
    
    fig, axs = plt.subplots(figsize=(10, 10))

    fig.suptitle('KS Test')
    axs.step(x, y)
    axs.step(x2, y2)
    axs.table(cellText=df.values, colLabels=df.columns,loc='top')

    plt.ylabel("Cumulative Probability")
    plt.xlabel("bins (nT)")
    plt.show()

def ks_test(observed: MagApp, model: PloufModel,bins: int = 10, key_name='line 1'):
    
    model = model.results
    observed = observed.lines[key_name]
    
    dfree = len(observed)-1

    ecdf = distributions.ECDF(observed.Mag_nT)
    ecdf_model = distributions.ECDF(model.mag)
    
    x = linspace(min(observed.Mag_nT.values), max(observed.Mag_nT.values),bins)
    x2 = linspace(min(model.mag.values), max(model.mag.values),bins)

    print("\n Splitting data into {} bins".format(bins))

    y = ecdf(x)
    y2= ecdf_model(x2)

    ks_stats = _ks_stats(dfree,y,y2)
    
    _plot_ks(x,y,x2,y2,ks_stats)

def get_rmse(model: PloufModel):

    return sqrt(((model.residuals.mag) ** 2).mean())


def get_abs_max_error(model: PloufModel):
    
    return abs(model.residuals.mag).max()

def get_durban_watson(model: PloufModel):
    
    return durbin_watson(model.residuals.mag)

def get_stats(app: MagApp,bins=50):
    data = app.data.copy()

    df = DataFrame.from_dict(
        {   "mean" : [mean(data.Mag_nT.values)],
            "var"  : [var(data.Mag_nT.values)],
            "std"  : [std(data.Mag_nT.values)],
            "skew" : [skew(data.Mag_nT.values)],
            "kurt" : [kurtosis(data.Mag_nT.values)]
        }
    )

    length_max = max(data.Mag_nT)
    length_min = min(data.Mag_nT)

    bin_width = (length_max - length_min)/bins # notice the use of parentheses
    print("bin width = ", bin_width, "nT")

    fig, ax = plt.subplots(figsize=(15,15))
    fig.suptitle('Mag Signal Histogram')
    
    ax.hist(data.Mag_nT, bins=bins)
    ax.table(cellText=df.values, colLabels=df.columns,loc='top')
    
    plt.xlabel('Mag Siganl (nT)')
    plt.ylabel('Counts')
    plt.grid(True)

    data.Mag_nT.values.sort()
    y=[]
    for i in range (0,len(data.Mag_nT.values)):
        y_value = 1-(i/len(data.Mag_nT.values)) 
        y.append(y_value)

    fig, ax = plt.subplots(figsize=(10,10))
    plt.plot(data.Mag_nT,y)

    plt.title('Survivor Plot')
    plt.xlabel('Mag Siganl (nT)') 
    plt.ylabel('fraction')
    
    plt.show()
