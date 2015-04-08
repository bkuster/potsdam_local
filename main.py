'''
Created on 12 Mar 2015

Get 3D info from psql server, and make a nice little kml

@author: Ben
'''
####
# ENVIRONMENT
####
# other files
import kml
import simplekml

# sys os
import sys
import re

# sql & numpy
import psycopg2
import numpy as np

# color and plot
import colorsys
import matplotlib as mpl
import matplotlib.pyplot as plt

# parallel processing
import pp

####
# FUCNTION
####
# make_actual_hex
# makes actual html hex for matplotlib
def make_descrete_hex(the_array):
    max_x = len(the_array[:,0])-1
    min_x = 0
    the_hex = []
    kml_hex = []
    
    for i in range(0, len(the_array[:,0])):
        h = (float(max_x-i) / (max_x-min_x)) * 120
        
        # convert hsv color (h,1,1) to its rgb equivalent
        r, g, b = colorsys.hsv_to_rgb(h/360, .6, 1.)
        
        # make hex
        the_hex.append("#%02x%02x%02x"  % (int(r*255), int(g*255), int(b*255)))
        temp = simplekml.Color.rgb(int(r*255), int(g*255), int(b*255))
        temp = re.sub('^.{2}','9f', temp)
        kml_hex.append(temp)
    
    the_array = np.append(the_array, np.array(the_hex, ndmin=2).T, axis=1)
    the_array = np.append(the_array, np.array(kml_hex, ndmin=2).T, axis=1)
    return(the_array)

# add_bar
# plots the color ramp
def add_legend(the_array):
    # get number
    n = len(the_array[:,0])
    dpi = 80
    
    # make fig with n*2 width
    fig = plt.figure(figsize=(4*n, 3),dpi=dpi)
    
    # add axes
    ax = fig.add_axes([0.05, 0.2, 0.9, 0.9], 
                      yticklabels=str(), yticks = [0.],
                      xlim=[0, n+0.2], 
                      xticks = [0.],
                      xticklabels = str())
    
    i = 0.1
    for row in the_array:
        col = row[2]
        rect = mpl.patches.Rectangle([i+0.1, 0.2], 
                                     0.8, 
                                     0.5, 
                                     color = col)
        ax.add_patch(rect)
        ax.annotate('%.2f' % float(row[0]), 
                    [i+0.5, 0.55], 
                    color = 'black', fontsize = 32, 
                    ha = 'center', va = 'center',
                    fontweight = 'bold')
        
        # add class
        the_class = row[1].split()[0]
        the_class = re.sub(',', '', the_class)
        ax.annotate(the_class,
                    [i+0.5, 0.3],
                    color = 'black', fontsize = 22,
                    ha = 'center', va = 'center',
                    fontweight = 'bold')
        i += 1

    # show figure to get ticks
    fig.text(0.5, 0.18, r'\textbf{CO2} [$\mathbf{kg/a\;m^3}$]', 
             ha = 'center', va = 'center',
             color = 'white', fontsize = 48, fontweight = 'bold')
    fig.show()
        
####
# SETUP
####
# SQL
configs = kml.get_config()
for d in configs:
    globals().update(d)

db_con = psycopg2.connect("dbname=%s user=%s host=%s password=%s" % (db, user_name, host_name, passwd))
cur = db_con.cursor()

# matplot
mpl.rc('font',**{'family':'sans-serif','sans-serif':['Helvetica']})
mpl.rcParams['text.usetex'] = True
mpl.rcParams['xtick.labelsize'] = 30
mpl.rcParams['xtick.color'] = "white"
mpl.rcParams['ytick.labelsize'] = 12
mpl.rcParams['axes.edgecolor'] = 'white'
mpl.rcParams['axes.linewidth'] = 0.
mpl.rcParams['axes.labelcolor'] = 'white'

# parallel
job_server = pp.Server(ncpus=8)

# get protocol
prot = sys.argv[1]

####
# EXECUTIONS
####
statement = kml.get_sql('sql/get_vals.sql')
statement = statement.format(prot)
cur.execute(statement)
vals = np.array(cur.fetchall())

# color array
colors = make_descrete_hex(vals)
add_legend(colors)
plt.savefig('../data/files/color_ramp.png', format='png', transparent=True)

# get the tiles
cur.execute("SELECT id FROM fishnet")
tiles = cur.fetchall()

# close connection
db_con.close()

jobs = []
for tile_id in tiles:
    the_out = job_server.submit(func=kml.make_tile, 
                                  depfuncs=(kml.make_building, 
                                            kml.get_sql, 
                                            kml.make_descrip,
                                            kml.make_coords,
                                            kml.get_config), 
                                  args=(tile_id, prot, colors),
                                  modules=("kml","simplekml", "psycopg2","re", "os", "ConfigParser", )
                                  )
    jobs.append(the_out)
    
job_server.wait()

# get errors
for job in jobs:
    what = job()
    if what not in (0,1):
        print what
           
job_server.print_stats()

# get ground
db_con = psycopg2.connect("dbname=%s user=%s host=%s password=%s" % (db, user_name, host_name, passwd))
cur = db_con.cursor()
cur.execute(kml.get_sql('sql/get_overlay_fishnet.sql'))
boxes = cur.fetchall()

db_con.close()

# set it up
geom_id = 1
jobs = []
ids = []
for box in boxes:
    geom = box[0]
    ids.append(geom_id)
    
    the_out = job_server.submit(func=kml.make_ground, 
                                  depfuncs=(kml.get_sql,
                                            kml.make_coords,
                                            kml.get_config), 
                                  args=(geom, geom_id, prot, colors),
                                  modules=("kml","simplekml", "psycopg2","re", "os", "ConfigParser", )
                                  )
    jobs.append(the_out)
    
    geom_id += 1

job_server.wait()

for job in jobs:
    what = job()
    if what not in (0,1):
        print what

job_server.print_stats()

geom_array = np.array(boxes)
geom_array = np.append(geom_array, np.array(ids, ndmin=2).T, axis = 1)

# open up again
db_con = psycopg2.connect("dbname=%s user=%s host=%s password=%s" % (db, user_name, host_name, passwd))
kml.make_parent(db_con, geom_array)
db_con.close()
