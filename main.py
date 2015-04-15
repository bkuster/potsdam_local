'''
Created on 12 Mar 2015

Get 3D info from psql server, and make a nice little kml

@author: Ben
'''
####
# ENVIRONMENT
####
# other files
import legend

# sys os
import sys, os
import argparse

# sql & numpy
import psycopg2
import numpy as np

# parallel processing
import pp
import kml

####
# FUCNTION
####
parser = argparse.ArgumentParser(description="""
Create a KMZ file for the Postdam LOCAL CityGML DB. 
Modes can be specified via command line.
Default will result in per building CO2 value representation
""")

# available protocols
protocols = ['ccr', 'gpc', 'ecoregion']

# modes are exclusive, group
modes = parser.add_mutually_exclusive_group()
modes.add_argument('-p', '--protocol', help='discrete mode, specify protocol', choices=protocols)
modes.add_argument('-d', '--difference', 
                    help='difference mode, specify protocol', choices=protocols)

# output dir
# parser.add_argument('-o', '--output', help)

####
# SETUP
####
# options
options = {# discrete mode protocol
           'protocol': None,
           # diff mode protocol
           'difference': None,
           'output': None
           }

# add parser args
args = parser.parse_args()

# update options
options.update(vars(args))

# SQL
configs = kml.get_config()
for d in configs:
    globals().update(d)

db_con = psycopg2.connect("dbname=%s user=%s host=%s password=%s" % (db, user_name, host_name, passwd))
cur = db_con.cursor()

# create temp

# parallel
job_server = pp.Server(ncpus=8)

####
# EXECUTIONS
####
# the discrete part
if options['protocol']:
    import kml_protocol
    prot = options['protocol']
    # get values
    statement = kml.get_sql('sql/get_vals.sql')
    statement = statement.format(prot)
    cur.execute(statement)
    vals = np.array(cur.fetchall())
    
    # make color
    plot = legend.the_legend(True, vals)
    plot.add_legend()
    plot.save_plot()
    
    # get the tiles
    cur.execute("SELECT id FROM fishnet")
    tiles = cur.fetchall()
    
    # close connection
    db_con.close()
    
    jobs = []
    for tile_id in tiles:
        the_out = job_server.submit(func=kml_protocol.make_tile, 
                                      depfuncs=(kml_protocol.make_building, 
                                                kml_protocol.get_sql, 
                                                kml_protocol.make_descrip,
                                                kml_protocol.make_coords,
                                                kml_protocol.get_config), 
                                      args=(tile_id, prot, plot.data),
                                      modules=("kml_protocol","simplekml", "psycopg2","re", "os", "ConfigParser", )
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
        
        the_out = job_server.submit(func=kml_protocol.make_ground, 
                                      depfuncs=(kml_protocol.get_sql,
                                                kml_protocol.make_coords,
                                                kml_protocol.get_config), 
                                      args=(geom, geom_id, prot, plot.data),
                                      modules=("kml_protocol","simplekml", "psycopg2","re", "os", "ConfigParser", )
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
    kml_protocol.make_parent(db_con, geom_array)
    db_con.close()
    
# the continous part
else:
    statement = kml.get_sql('sql/get_quantiles.sql')
    cur.execute(statement)
    quantiles = np.array(cur.fetchall()).astype(float)
    
    co2_min = 0.0
    co2_max = float(quantiles[3]+(3*(quantiles[3]-quantiles[1])))
    
    # make color ramp
    plot = legend.the_legend(False, [co2_min, co2_max])
    plot.add_bars(250)
    plot.save_plot()
    
    # get the tiles
    cur.execute("SELECT id FROM fishnet")
    tiles = cur.fetchall()
    
    jobs = []
    for tile_id in tiles:
        the_out = job_server.submit(func=kml.make_tile, 
                                      depfuncs=(kml.make_building, 
                                                kml.get_sql, 
                                                kml.make_hex, 
                                                kml.make_descrip,
                                                kml.make_coords), 
                                      args=(tile_id, co2_min, co2_max),
                                      modules=("kml","simplekml", "psycopg2","re", "os", "colorsys", )
                                      )
        jobs.append(the_out)
    
    job_server.wait()   
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
                                                kml.make_hex,
                                                kml.make_coords), 
                                      args=(geom, geom_id, co2_min, co2_max),
                                      modules=("kml","simplekml", "psycopg2","re", "os", "colorsys", )
                                      )
        jobs.append(the_out)
        geom_id += 1
    
    job_server.wait()   
    job_server.print_stats()
    
    geom_array = np.array(boxes)
    geom_array = np.append(geom_array, np.array(ids, ndmin=2).T, axis = 1)
    
    kml.make_parent(db_con, co2_min, co2_max, geom_array)
    db_con.close()
