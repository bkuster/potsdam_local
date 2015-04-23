'''
Created on 20 Mar 2015

Takes care of the kml side of things

@author: Ben
'''
####
# ENVIRONMENT
####
import simplekml
import re
import psycopg2
import os
import numpy as np
import ConfigParser

####
# FUNCTIONS
####
def get_config():
    config = ConfigParser.ConfigParser()
    config.read('.config.ini')
    
    dict_list = []
    for section in config.sections():
        sec = dict(config.items(section))
        dict_list.append(sec)
        
    return dict_list
####
# make_desc
# makes a nice html description
def make_descrip(co2_str, b_class, const, san):
    description = """<![CDATA[
        <table>
            <tr>
                <td><b> Klasse </b></td>
                <td> %s <td>
            </tr>
            <tr>
                <td><b> Baujahr <b></td>
                <td> %s </td>
            </tr>
            <tr>
                <td><b> Sanierungsgrad </b></td>
                <td> %s </td>
            </tr>
            <tr>
                <td> <b> CO<sup>2</sup> [kg/a m<sup>3</sup>] <b></td>
                <td> %.4f </td
            </tr>
        </table>
        <hr>
        <footer>
            <p> <i>GFZ</i> </p>
        </footer>
        ]]>
    """ % (b_class,  const, san, co2_str)
    return description.decode('utf-8')

####
# find_kml
# returns all kmls in the kml directory as a string for network linking
def find_kml(the_dir):
    kmls = []
    for files in os.listdir(the_dir):
        if files.endswith('.kml'):
            files = 'kml/%s' % files
            kmls.append(files)
    
    return kmls

####
# get_sql
# gets and returns the entire sql statement from file
def get_sql(fname):
    with open(fname, 'r') as the_file:
        statement = " ".join(line.rstrip() for line in the_file)
    
    # and return
    return statement

###
# make_coords
# makes a WKT KML confirm
def make_coords(wkt):
    wkt = re.findall("[0-9].*", wkt)
    wkt = re.sub('\)', '', wkt[0])
    wkt = re.sub('\(', '', wkt)
    
    # make the coords kml conformable
    coords = wkt.split(',')
    for i in range(0,len(coords)):
        mini_coords = str(coords[i]).split()
        the_tupl = []
        for j in range(0,len(mini_coords)):
            the_tupl.append(float(mini_coords[j]))
        coords[i] = tuple(the_tupl)
    return(coords)

####
# make_building
# adds the kml for a building
def make_building(b_id, con, the_kml, prot, colors):
    
    # make the statement
    statement = get_sql('sql/get_building_disc.sql')
    statement = statement.format(prot, b_id)
    
    # get the info
    cur = con.cursor()
    cur.execute(statement)
    the_set = cur.fetchone()
    
    # assign values
    building_id = the_set[0]
    co2 = the_set[1]
    addr = the_set[2]
    b_class = the_set[3]
    b_class_type = the_set[4]
    e_class = the_set[5]
    const = the_set[6]
    san = the_set[7]
    
    # get color
    if co2 == None:
        color = '9fD2D2D2'
        co2 = 0.0
    else:
        color = colors[colors[:,0].astype('int')==int(co2),3][0]
    
    if addr != None:
        addr = addr.decode('utf-8')

    # create the multi geometry
    multi = the_kml.newmultigeometry(name="%s" %(addr), 
                                     description = make_descrip(co2, b_class, const, san))
    
    statement = get_sql('sql/get_geoms.sql')
    statement = statement % building_id
    # get polygons
    cur.execute(statement)

    geoms = cur.fetchall()
    
    # loop over geoms
    for tupls in geoms:
        # get coords
        coords = make_coords(tupls[0])
            
        # append to multi
        multi.newpolygon(outerboundaryis=coords, extrude=1, altitudemode='absolute')
        
        # make it stylish
        multi.style.polystyle.color = color
        multi.style.linestyle.color = color

####
# make_tile
# makes the kml for a tile
def make_tile(tile_id, prot, colors):
    # get config -> globals not passed to pp
    configs = get_config()
    for d in configs:
        globals().update(d)
        
    db_con = psycopg2.connect("dbname=%s user=%s host=%s password=%s" % (db, user_name, host_name, passwd))
    cur = db_con.cursor()
    
    # get all the buildings
    statement = get_sql('sql/get_tile.sql')
    statement = statement % tile_id
    
    cur.execute(statement)
    buildings = cur.fetchall()
    
    if len(buildings) == 0:
        return 1
    
    # make kml
    the_kml = simplekml.Kml()
    
    # iterate over the buildings
    for b_id in buildings:
        make_building(b_id[0], db_con, the_kml, prot, colors)
        
    db_con.close()
    the_kml.save('../data/kml/%s_tile.kml' % tile_id, format=False)
    return 1

####
# make_screen
# makes the screens onto the final kml
def make_screen(the_kml):
    # add the screen
    # make screen
    screen = the_kml.newscreenoverlay(name='Legende')
    
    
    screen.icon.href = 'files/color_ramp.png'
    screen.icon.scale = 0.01
    
    screen.overlayxy = simplekml.OverlayXY(x=0,y=1,xunits=simplekml.Units.fraction,
                                           yunits=simplekml.Units.fraction)
    screen.screenxy = simplekml.ScreenXY(x=150,y=15,xunits=simplekml.Units.pixel,
                                         yunits=simplekml.Units.insetpixels,
                                         )
    screen.size.x = 0.4
    screen.size.y = 0.1
    screen.size.xunits = simplekml.Units.fraction
    screen.size.yunits = simplekml.Units.fraction
    
    # make second screen
    screen_2 = the_kml.newscreenoverlay(name='Logo')
    
    
    screen_2.icon.href = 'files/logo.png'
    screen_2.icon.scale = 0.01
    
    screen_2.overlayxy = simplekml.OverlayXY(x=0,y=1,xunits=simplekml.Units.fraction,
                                           yunits=simplekml.Units.fraction)
    screen_2.screenxy = simplekml.ScreenXY(x=15,y=15,xunits=simplekml.Units.pixel,
                                         yunits=simplekml.Units.insetpixels,
                                         )
    screen_2.size.x = 0.1
    screen_2.size.y = 0.1
    screen_2.size.xunits = simplekml.Units.fraction
    screen_2.size.yunits = simplekml.Units.fraction

####
# make_ground
# makes the larger overlay for the zoomed image
def make_ground(geom, geom_id, prot, colors):
    # get config -> globals not passed to pp
    configs = get_config()
    for d in configs:
        globals().update(d)
        
    db_con = psycopg2.connect("dbname=%s user=%s host=%s password=%s" % (db, user_name, host_name, passwd))
    cur = db_con.cursor()
    the_kml = simplekml.Kml()

    # get the values
    statement = get_sql('sql/get_overlay_single_disc.sql')
    statement = statement.format(prot,geom)
    cur.execute(statement)
    buildings = cur.fetchall()
    
    # add each building
    for building in buildings:
        # get the color
        co2 = float(building[0])
        color = colors[colors[:,0].astype('int')==int(co2),3][0]
                
        # get coords
        coords = make_coords(building[1])
            
        # append to multi
        the_poly = the_kml.newpolygon(outerboundaryis=coords)
        the_poly.style.polystyle.color = color
        the_poly.style.linestyle.color = color

    
    # get the double buildings
    statement = get_sql('sql/get_overlay_double_disc.sql')
    statement = statement.format(prot, geom)
    cur.execute(statement)
    buildings_2 = cur.fetchall()
    cur.close()
    
    for building in buildings_2:
        # get the color
        co2 = float(building[0])
        color = colors[colors[:,0].astype('int')==int(co2),3][0]
                    
        # make multi
        multi = the_kml.newmultigeometry()
        for geom in building[1:2]:
            # get coords
            coords = make_coords(geom)
                
            # append to multi
            multi.newpolygon(outerboundaryis=coords)
            multi.style.polystyle.color = color
            multi.style.linestyle.color = color
    
    the_kml.save('../data/kml/ground/%s.kml' % geom_id)
    return 1

####
# make_parent
# makes the network linked parent
def make_parent(con, ground_boxes):
    # add tiles
    kmls = find_kml('../data/kml')
    cur = con.cursor()
    
    # make the parent 
    parent = simplekml.Kml()
    camera = simplekml.Camera(latitude = 52.39296111111111, 
                              longitude = 13.012166666666667,
                              altitude = 10000,
                              altitudemode=simplekml.AltitudeMode.relativetoground,
                              tilt=0)
    parent.camera = camera
    
    for tile in kmls:
        # get bb
        tile_id = re.findall('([0-91].*)_', tile)
        statement = get_sql('sql/get_tile_bb.sql')
        statement = statement % tile_id[0]
        cur.execute(statement)
        the_str = cur.fetchone()
        
        # get the coords only
        coords = make_coords(the_str[0])
        
        # add region and link
        coords = np.array(coords).astype(float)
        east = max(coords[:,0])
        west = min(coords[:,0])
        north = max(coords[:,1])
        south = min(coords[:,1])
        
        box = simplekml.LatLonAltBox(minaltitude = 0,
                                     maxaltitude = 20,
                                     altitudemode = 'clampToGround',
                                     east = east,
                                     west = west,
                                     north = north,
                                     south = south)
        the_link = parent.newnetworklink(name=tile, 
                                         region=
                                         simplekml.Region(box,
                                                          lod = simplekml.Lod(minlodpixels = 2400)))
        the_link.link.href = tile
    
    # add ground kmls   
    for the_set in ground_boxes:
        the_id = the_set[1]
        coords = make_coords(the_set[0])
        
        # add region and link
        coords = np.array(coords).astype(float)
        east = max(coords[:,0])
        west = min(coords[:,0])
        north = max(coords[:,1])
        south = min(coords[:,1])
        
        box = simplekml.LatLonAltBox(minaltitude = 0,
                                     maxaltitude = 20,
                                     altitudemode = 'clampToGround',
                                     east = east,
                                     west = west,
                                     north = north,
                                     south = south)
        the_link = parent.newnetworklink(name='ground_%s' % the_id, 
                                         region=
                                         simplekml.Region(box,
                                                          lod = simplekml.Lod(minlodpixels = 360)))
        the_link.link.href = 'kml/ground/%s.kml' % the_id
        
    cur.close()
    make_screen(parent)
    parent.save('../data/parent.kml')
        
