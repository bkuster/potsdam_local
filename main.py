'''
Created on 12 Mar 2015

Get 3D info from psql server, and make a nice little kml

@author: Ben
'''
####
# ENVIRONMENT
####
# sys os
import os
import argparse
import time

# sql & numpy
import psycopg2
import numpy as np

# parallel processing
from mpi4py import MPI
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

parser.add_argument('-o', '--output', help='output directory of your KMZ file (absolute path)', default = '.')
parser.add_argument('-k', '--keep', help='keep temporary data. will be overwritten on next run', action='store_true')

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
           'output': None,
           'keep': False
           }

# add parser args
args = parser.parse_args()

# update options
options.update(vars(args))

# parallel
def enum(*sequential, **named):
    """Handy way to fake an enumerated type in Python
    http://stackoverflow.com/questions/36932/how-can-i-represent-an-enum-in-python
    """
    enums = dict(zip(sequential, range(len(sequential))), **named)
    return type('Enum', (), enums)

# Define MPI message tags
tags = enum('READY', 'DONE', 'EXIT', 'START_3D', 'START_GR')

# commander
comm = MPI.COMM_WORLD

####
# EXECUTIONS
####
# get MPI working
size = comm.Get_size()
rank = comm.Get_rank()
status = MPI.Status()
done_workers = []

# master
if rank == 0:
    # import only master modules
    import legend
    import zipfile
    import shutil
    
    ####
    # file directory stuff
    # check passed output
    if not os.path.exists(options['output']):
        print('specified output directory does not exist!')
        comm.Abort()
        
    # create temp directories
    try:
        for root, dirs, files in os.walk('./temp', topdown=False):
            for f in files:
                os.remove(os.path.join(root, f))
            for d in dirs:
                os.rmdir(os.path.join(root, d))
                
            os.rmdir('./temp')
    except:
        print('HINT: nothing to purge, moving on')
          
   
    
    # create some paths, depending on where we are
    dirs = ['./temp', './temp/kml', './temp/files', './temp/kml/ground']
    
    for path in dirs:
        os.makedirs(path, exist_ok = True)
        
    # move the logo, if it exists
    try:
        shutil.move('./data/logo.png', './temp/files/logo.png')
    except:
        print('WARNING: no logo found. can be added later')
    
    ####
    # Preliminary stuff
    # SQL
    configs = kml.get_config()
    for d in configs:
        globals().update(d)
    
    db_con = psycopg2.connect("dbname=%s user=%s host=%s password=%s" % (db, user_name, host_name, passwd))
    cur = db_con.cursor()
    
    # empty place holder for data (either colors or co2 min max)
    tile_data = []
    # check for mode
    if options['protocol']:
        prot = options['protocol']
        # get values
        statement = kml.get_sql('sql/get_vals.sql')
        statement = statement.format(prot)
        cur.execute(statement)
        vals = np.array(cur.fetchall())
        
        # make color
        plot = legend.the_legend(True, vals)
        plot.make_discrete_hex()
        plot.add_legend()
        plot.save_plot()
        
        # add tile_data
        tile_data = plot.data
    elif options['difference']:
        statement = kml.get_sql('sql/get_quantiles_diff.sql')
        cur.execute(statement.format(options['difference']))
        quantiles = np.array(cur.fetchall()).astype(float)

        co2_min = 0.0
        co2_max = float(quantiles[3]+(3*(quantiles[3]-quantiles[1])))
        
        tile_data = [co2_min, co2_max]
        # make plot
        plot = legend.the_legend(False, tile_data)
        plot.add_bars(250)
        plot.save_plot()   
    else:
        statement = kml.get_sql('sql/get_quantiles.sql')
        cur.execute(statement)
        quantiles = np.array(cur.fetchall()).astype(float)

        co2_min = 0.0
        co2_max = float(quantiles[3]+(3*(quantiles[3]-quantiles[1])))
        
        tile_data = [co2_min, co2_max]
        # make plot
        plot = legend.the_legend(False, tile_data)
        plot.add_bars(250)
        plot.save_plot()
        
    # get the tiles
    cur.execute("SELECT id FROM fishnet")
    tiles = cur.fetchall()
    tiles = tiles[0:10]
    
    # get the ground tiles
    cur.execute(kml.get_sql('sql/get_overlay_fishnet.sql'))
    boxes = cur.fetchall()
    boxes = boxes[0:4]
    
    geom_array = np.array(boxes)
    geom_array = np.append(geom_array, np.array(range(0, len(geom_array)), ndmin =2).T, axis = 1)

    # close connection
    db_con.close()
    
    ####
    # Multi stuff
    task_index = 0
    num_workers = size - 1
    closed_workers = 0
    
    # start dishing out
    while closed_workers < num_workers:
        # save some CPU
        while not comm.Iprobe(source = MPI.ANY_SOURCE, tag = MPI.ANY_TAG):
            time.sleep(0.1)
        
        # go!    
        data = comm.recv(source = MPI.ANY_SOURCE, tag = MPI.ANY_TAG, status = status)
        source = status.Get_source()
        tag = status.Get_tag()

        # more tasks to give!
        if tag == tags.READY:
            # the 3D first
            if task_index < len(tiles):
                comm.send([tiles[task_index], tile_data], dest=source, tag=tags.START_3D)
                print("Sending 3D task %d to worker %d" % (task_index, source))
                task_index += 1
            # the ground second
            elif task_index >= len(tiles) and task_index < len(tiles)+len(boxes):
                comm.send([geom_array[int(task_index - len(tiles)), 0],
                               geom_array[int(task_index - len(tiles)), 1],
                               tile_data], dest=source, tag=tags.START_GR)
                print("Sending GR task %d to worker %d" % (task_index, source))
                task_index += 1
            # no more tasks to give!
            else:
                comm.send(None, dest=source, tag=tags.EXIT)
        elif tag == tags.DONE:
            results = data
            print("Got data from worker %d" % source)
        elif tag == tags.EXIT:
            print("Worker %d exited." % source)
            closed_workers += 1
            print(closed_workers)
            
    print("Master finishing")
    db_con = psycopg2.connect("dbname=%s user=%s host=%s password=%s" % (db, user_name, host_name, passwd))
    kml.make_parent(db_con, geom_array)
    
    ####
    # the zipping part
    print("Making KMZ")
    
    # change path to ./temp
    os.chdir('./temp')
    if options['protocol']:
        kmz_name = 'potsdam_local_' + options['protocol'] + '.kmz'
    elif options['difference']:
        kmz_name = 'potsdam_local_difference_' + options['difference'] + '.kmz'
    else:
        kmz_name = 'potsdam_local.kmz' 
        
    kmz = zipfile.ZipFile(kmz_name, mode = 'w')
    
    for root, dirs, files in os.walk('.', topdown=False):
        for f in files:
            if not f == kmz_name: # dont include the kmz itself!
                kmz.write(os.path.join(root, f), compress_type = zipfile.ZIP_DEFLATED)
    
    kmz.close()
    
    # move the file to root and back to bin, kill data
    shutil.move(kmz_name, os.path.join(options['output'], kmz_name))
    os.chdir('..')
    
    # delete if not kept
    if not options['keep']:
        for root, dirs, files in os.walk('./temp', topdown=False):
            for f in files:
                os.remove(os.path.join(root, f))
            for d in dirs:
                os.rmdir(os.path.join(root, d))
          
            os.rmdir('./temp')
    print('done')
    
else:
    # Worker processes execute code below
    name = MPI.Get_processor_name()
    print("I am a worker with rank %d on %s." % (rank, name))
    while True:
        comm.send(None, dest=0, tag=tags.READY)                  
        task = comm.recv(source=0, tag=MPI.ANY_TAG, status=status)
        tag = status.Get_tag()
        if tag == tags.START_3D:
            # break it down
            tile_id = task[0]
            tile_data = task[1]
            
            # Do the work here
            the_tile = kml.tile(tile_id, options, tile_data)
            the_tile.make_tile()
            comm.send(the_tile.tile_stat, dest=0, tag=tags.DONE)
        elif tag == tags.START_GR:
            # break it down again
            geom = task[0]
            geom_id = task[1]
            tile_data = task[2]
            # do it
            the_ground = kml.ground(geom, geom_id, options, tile_data)
            the_ground.run()
            comm.send(None, dest=0, tag=tags.DONE)
        elif tag == tags.EXIT:
            done_workers.append(rank)
            break
        
    comm.send(None, dest=0, tag=tags.EXIT)
    
#if rank == 0:
        
"""
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
"""