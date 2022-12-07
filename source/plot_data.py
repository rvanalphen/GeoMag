import matplotlib.pyplot as plt
import subprocess
import pygmt as gmt
from os import path
from pandas import DataFrame
from source.app import MagApp
from source.model_data import PloufModel
from cartopy import crs as ccrs
from math import floor
from matplotlib import patheffects
from matplotlib import patches as mpatches
from cartopy.io.img_tiles import GoogleTiles

def _choose_plot( data: DataFrame) -> str:

    mode_heading = data.Heading.round().mode()[0]
    if mode_heading < 44 or mode_heading > 316\
            or (mode_heading > 136 and mode_heading < 224):
        return 'NS'

    return 'EW'

def simple_plot(application: MagApp,lines: bool = False) -> None:

    if not application.parameters:
        path = 'Merged Files'
    else:
        path = application.parameters.filepath
        path = str(path).split('/')[-1]
    
    fig, ax = plt.subplots(figsize=(10, 10))

    if not lines:
        data = application.data 
        cols = data.columns
        if 'Easting' in cols:
            ax.plot(data.Easting, data.Northing,
                    linestyle='None', marker="o", ms=2)
            ax.set_title(path+': all data')
        else:
            ax.plot(data.Long, data.Lat,
                    linestyle='None', marker="o", ms=2)
            ax.set_title(path+': all data')
        plt.show()

    else:
        data = application.lines
        for key in data:
            ax.plot(data[key].Easting,data[key].Northing,
                    linestyle='None', marker="o", ms=2,label=key)
            
            ax.set_title(path+': all lines')
            plt.legend(title = "Key/Line Names:",loc='lower right')
        plt.show()


def plot_mag_profile(application: MagApp,key_name: str = None,direction: str = None) -> None:

    if not application.parameters:
        path = 'Merged Files'
    else:
        path = application.parameters.filepath
        path = str(path).split('/')[-1]

    data = application.lines[key_name]

    fig, ax = plt.subplots(figsize=(15, 5))

    if key_name:
        ax.set_title(path+': '+key_name)
    else:
        ax.set_title(path+' lines')
    
    if not direction:
        direction = _choose_plot(data)

    if "NS" not in direction:
        ax.plot(data.Easting, data.Mag_nT, marker="o",
                linestyle='None', markersize=3)

        ax.set_xlabel("Easting")
        ax.set_ylabel("Magnetic Signal (nT)")

        plt.show()

    else:
        ax.plot(data.Northing, data.Mag_nT, marker="o",
                linestyle='None', markersize=3)

        ax.set_xlabel("Northing")
        ax.set_ylabel("Magnetic Signal (nT)")

        ax.set_title(key_name)

        plt.show()


def plot_offset_profile(application: MagApp, offset=150) -> None:

    data = application.lines

    if not application.parameters:
        path = 'Merged Files'
    else:
        path = application.parameters.filepath
        path = str(path).split('/')[-1]

    start_offset = 0

    fig, ax = plt.subplots(figsize=(10, 10))
    
    for index, (key, value) in enumerate(data.items()):
        if index == 0:
            direction = _choose_plot(data[key])
        else:
            break

    if "NS" not in direction:
        for key in data:
            ax.plot(data[key].Easting, data[key].Mag_nT+start_offset, marker="o", linestyle='None',
                    markersize=3, label=str(key))

            ax.set_xlabel("Northing")
            ax.set_ylabel("Magnetic Signal (nT)")

            ax.set_title(path+' - Lines offset by: '+str(offset)+'nT')

            plt.legend(title = "Key/Line Names:",loc='lower right')
            start_offset += offset
    else:
        for key in data:
            ax.plot(data[key].Northing, data[key].Mag_nT+start_offset, marker="o", linestyle='None',
                    markersize=3, label=str(key))

            ax.set_xlabel("Northing")
            ax.set_ylabel("Magnetic Signal (nT)")

            ax.set_title(path+' - Lines offset by: '+str(offset)+'nT')

            plt.legend(title = "Key/Line Names:",loc='lower right')
            start_offset += offset
    plt.show()

def plot_model(model: PloufModel) -> None:
    
    fig, ax = plt.subplots(figsize=(10, 10))

    ax.plot(model.line.Northing,model.line.Mag_nT,'go',ms=2,label='Observed')

    ax.plot(model.results.ydist,model.results.mag,'ro',ms=2,label='Calculated')

    ax.set_xlabel('Horizontal distance north from line center')
    ax.set_ylabel('Magnetic Anomaly (nT)')
    ax.legend(loc='lower left')
    plt.show()

def plot_line_and_polygon(model: PloufModel) -> None:
    fig, ax = plt.subplots()
    ax.plot(model.line.Easting,model.line.Northing,color='red',markersize=3,label='Cleaned Observed Data')
    ax.plot(model.shape.Easting,model.shape.Northing,linestyle='dashed',color='black',label='Magnetic Body')
    ax.legend()
    plt.show()

def plot_residuals(model: PloufModel) -> None:

    fig, ax = plt.subplots(figsize=(10, 10))


    ax.plot(model.residuals.ydist,model.residuals.mag,'k-',ms=2,label='residuals')

    ax.set_xlabel('Horizontal distance north from line center')
    ax.set_ylabel('Residuals (nT)')
    ax.legend(loc='lower left')
    plt.show()

def _Createcpt(color,scale=None):

    netcdf = "./grid.nc"

    if scale:
        cptSeries=scale+'/1'
    else:
        cptSeries = subprocess.run(
            ['gmt', 'grdinfo', '-T-', netcdf], capture_output=True, text=True)


        
        cptSeries = cptSeries.stdout[2:]
        cptSeries = cptSeries+'/1'

    gmt.makecpt(cmap=color, series=cptSeries,
                continuous=True, verbose='w', output='geomag.cpt')

def _GMTcreatNetCDF( df, spacing,skip=False):
    print('###')
    if skip:
        Q = 'n'
    else:
        Q = input(
            'do you want to input your own bounds? Otherwise it will be calcuLated for you.(y/n) ')
        bounds = ''

    cols = df.columns
    # giving prioraty to UTM
    if 'Easting' in cols:
        table = df[['Easting', 'Northing', "Mag_nT"]]

        if Q != 'y':
            bounds = str(df.Easting.min())+'/'+str(df.Easting.max()) + '/' + \
                str(df.Northing.min())+'/' + \
                str(df.Northing.max())

        else:
            bnds = input('input bounds without -R ')
            bounds = str(bnds)

    else:
        print('Data needs to be transformed into a utm coordinate system')
        exit()

    gmt.blockmedian(table, spacing=spacing, region=bounds,
                    verbose='w', outfile='./blkmdn.gmt')

    gmt.surface(data='./blkmdn.gmt', spacing=spacing, region=bounds,
                verbose='w', outfile='./grid.nc')

def GMTHeatmap(df, outfile, spacing,colorscale='haxby',scale=None,skip=False):
        netcdf = "./grid.nc"
        cmap = "./geomag.cpt"
        bkmdn = './blkmdn.gmt'

        f1 = path.isfile(outfile)
        f2 = path.isfile(bkmdn)
        f3 = path.isfile(netcdf)
        f4 = path.isfile(cmap)
        if f1:
            subprocess.run(["rm",outfile])
        if f2:
            subprocess.run(["rm",bkmdn])
        if f3:
            subprocess.run(["rm",netcdf])
        if f4:
            subprocess.run(["rm",cmap])


        _GMTcreatNetCDF(df,spacing,skip)
        _Createcpt(colorscale,scale)


        grdBounds = subprocess.run(
            ['gmt', 'grdinfo', '-I-', netcdf], capture_output=True, text=True)
        grdBounds = grdBounds.stdout[2:]

        fig = gmt.Figure()
        gmt.config(MAP_FRAME_TYPE="plain")
        gmt.config(FORMAT_GEO_MAP="ddd.xx")

        fig.basemap(projection='x1:10000', region=grdBounds,
                    frame=['af', 'WeSn'],  verbose='w')

        fig.grdimage(grid=netcdf, cmap=cmap,
                    frame='+t"Survey Heatmap"', verbose='w')

        fig.colorbar(cmap=cmap,
                    frame='af', position='JMR', verbose='w')

        if skip:
            pass
        else:
            print('###')
            Q = input('do you want to contour your map?.(y/n) ')
            if Q == 'y':
                interval = input(
                    'What contour interval do you want? annotations will be 2 times your inteval ')
                contour = str(interval)
                anno = int(contour)*2
                anno = str(anno)
                fig.grdcontour(grid=netcdf, interval=contour,
                            annotation=anno, pen='0.5p', verbose='w')
            else:
                pass
        
        subprocess.run(['rm', './grid.nc', './blkmdn.gmt', './geomag.cpt'])
        print('saving figure')
        fig.savefig(outfile)



def _utm_from_lon(lon):
    """
    _utm_from_lon - UTM zone for a longitude

    Not right for some polar regions (Norway, Svalbard, Antartica)

    :param float lon: longitude
    :return: UTM zone number
    :rtype: int
    """
    return floor((lon + 180) / 6) + 1


def _make_segments(xy,segments,length):
    # written by me (Robert Van Alphen)
    all_segments=[]
    colors=[]
    for index in range(segments):
        if index %2== 0:
            colors.append('black')
        else:
            colors.append('white')

        if index==0:
            s1 = [xy[0],xy[1]-((length/segments)*(segments-1))]

        elif index > 0 and index < segments-1:  
            smid = [xy[1]-((length/segments)*(index+1)),xy[1]-((length/segments)*index)]
            all_segments.append(smid)
        else:
            snth = [xy[1]-((length/segments)),xy[1]]

    all_segments.reverse()
    all_segments.insert(0, s1)
    all_segments.insert(len(all_segments), snth)

    for index,seg in enumerate(all_segments):
        if index %2== 0:
            colors.append('black')
        else:
            colors.append('white')

    return all_segments,colors
    
def _get_segment_nums(length,segments,units):
    # written by me (Robert Van Alphen)
    seg_nums=[]
    for i in range(segments):
        if i !=0:
            seg_nums.append(
                str(round((length/segments)*i)))

    if units == 'km':
        for i in range(len(seg_nums)):
            seg_nums[i] = str(round(int(seg_nums[i])/1000))

    if units == 'km':
        seg_nums.insert(len(seg_nums),str(int(length)/1000) + ' ' + units)
    else:
        seg_nums.insert(len(seg_nums),str(length) + ' ' + units)

    seg_nums.insert(0,'0')
    return seg_nums

def _scale_bar(ax, proj, length,segments=4, location=(0.5, 0.05), linewidth=5,
            m_per_unit=1, units='m'):
    """

    http://stackoverflow.com/a/35705477/1072212
    ax is the axes to draw the scalebar on.
    proj is the projection the axes are in
    location is center of the scalebar in axis coordinates ie. 0.5 is the middle of the plot
    length is the length of the scalebar in km.
    linewidth is the thickness of the scalebar.
    units is the name of the unit
    m_per_unit is the number of meters in a unit
    """

    # find lat/lon center to find best UTM zone
    x0, x1, y0, y1 = ax.get_extent(proj.as_geodetic())
    # Projection in metres
    utm = ccrs.UTM(_utm_from_lon((x0+x1)/2))
    # Get the extent of the plotted area in coordinates in metres
    x0, x1, y0, y1 = ax.get_extent(utm)
    # Turn the specified scalebar location into coordinates in metres
    sbcx, sbcy = x0 + (x1 - x0) * location[0], y0 + (y1 - y0) * location[1]
    # Generate the x coordinate for the ends of the scalebar
    bar_xs = [(sbcx - length * m_per_unit/2), sbcx + length * m_per_unit/2]
    # buffer for scalebar
    buffer = [patheffects.withStroke(linewidth=7, foreground="w")]
    buffer2 = [patheffects.withStroke(linewidth=3, foreground="w")]

    # Plot the scalebar
    # written by me (Robert Van Alphen)
    all_segments,colors = _make_segments(bar_xs,segments,length)    
    seg_nums = _get_segment_nums(length,segments,units)
    for i in range(len(all_segments)):
        ax.plot(all_segments[i], [sbcy, sbcy], transform=utm, color=colors[i],
            linewidth=linewidth, path_effects=buffer)

        ax.text(all_segments[i][0], sbcy+(linewidth+(linewidth/2)),seg_nums[i],
            transform=utm,horizontalalignment='center',
            verticalalignment='bottom',path_effects=buffer2)


    # buffer for text
    buffer = [patheffects.withStroke(linewidth=3, foreground="w")]

    # Plot the scalebar label
    # written by me (Robert Van Alphen)
    end_coords=all_segments[-1][0]+(all_segments[-1][1]-all_segments[-1][0])#+500000

    if units =='km':
        end_num  = str(round(int(length)/1000))
    else:
        end_num = str(length)

    ax.text(end_coords-3, sbcy+(linewidth+(linewidth/2)),
        end_num + ' ' + units, transform=utm,
        horizontalalignment='center', verticalalignment='bottom',
        path_effects=buffer, zorder=2)

    # Plot the N arrow
    ax.text(end_coords-(length+(length/4)), (sbcy-linewidth)+2, u'\u25B2\nN', transform=utm,
        horizontalalignment='center', verticalalignment='bottom',
        path_effects=buffer, zorder=5)



def _get_zoom(ax, crs):
    x0, x1, _, _ = ax.get_extent(crs.as_geodetic())
    # Projection in metres
    utm = ccrs.UTM(_utm_from_lon((x0+x1)/2))
    # Get the extent of the plotted area in coordinates in metres
    x0, x1, _, _ = ax.get_extent(utm)

    diff = x1 - x0
    if diff > 300 and diff < 500:
        zoom = 10
    elif diff > 500 and diff < 2000:
        zoom = 15
    else:
        zoom = 20

    return zoom

def _plot_legend(ax, lcolors, label, location=(0.25, 0.05)):
    labels=[]
    labels.append(label)

    legend_list = []
    for color in lcolors:
        legend_list.append(
            mpatches.Rectangle((0, 0), 1, 1, facecolor=color))

    ax.legend(legend_list, labels,
            loc='lower left', bbox_to_anchor=location, fancybox=True)


def _cartopy_main(app: MagApp,transform, length=100, segments=5,unit='m',zoom=None):
        tiler = GoogleTiles(style='satellite')

        ext = (app.data.Long.min(),
                app.data.Long.max(),
                app.data.Lat.min(),
                app.data.Lat.max())
        
        fig = plt.figure(figsize=(10, 10))
        ax1 = fig.add_subplot(1, 1, 1, projection=tiler.crs)
        ax1.set_extent(ext)

        if zoom != None:
            ax1.add_image(tiler, _get_zoom(ax1,tiler.crs))
        else:
            ax1.add_image(tiler, zoom)


        gl = ax1.gridlines(draw_labels=True)
        gl.top_labels = False
        gl.right_labels = False
        
        plt.tight_layout()

        return fig,ax1

def cartoplot(app: MagApp,data_name: str,plot_lines=True,length=100, segments=5,unit='m',zoom=None):
    
    transform = ccrs.PlateCarree()
    color = ['blue']

    carto_fig,carto_ax = _cartopy_main(app,transform,length,segments,unit,zoom)

    _scale_bar(carto_ax, transform,length=length, segments=segments,location=(0.65, 0.025),units=unit)
    
    _plot_legend(carto_ax,color,data_name,location=(0.1, 0.025))

    if plot_lines:
        carto_ax.scatter(app.data["Long"],app.data["Lat"],color=color,s=1,transform=transform)

    plt.show()
    
    # print("\n")
    # q = input("save map? (y/n)")

    # if q == 'y':
    #     name = input("save map as...:")
    #     carto_fig.savefig(name)
