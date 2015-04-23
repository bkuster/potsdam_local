'''
Created on 20 Mar 2015

Takes care of the kml side of things

@author: Ben
'''
####
# ENVIRONMENT
####
import configparser
import simplekml
import colorsys
import re
import psycopg2
import os
import numpy as np

####
# FUNCTIONS
####
def get_config():
    config = configparser.ConfigParser()
    config.read('.config.ini')
    
    dict_list = []
    for section in config.sections():
        sec = dict(config.items(section))
        dict_list.append(sec)
        
    return dict_list

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
# make_hex
# makes kml hex
def make_hex(x, max_x, min_x):
    # make hsv h from  0..120 red .. green
    if x < max_x:
        h = 120
    elif x > min_x:
        h = 0
    else:
        h = (float(max_x-x) / (max_x-min_x)) * 120
    
    # convert hsv color (h,1,1) to its rgb equivalent
    r, g, b = colorsys.hsv_to_rgb(h/360, .6, 1.)
    
    # make hex
    hexa = simplekml.Color.rgb(int(r*255), int(g*255), int(b*255))
    hexa = re.sub('^.{2}','9f', hexa)
    return(hexa)  
    
# class definition of tiles
class tile():
    # get configs
    configs = get_config()
    for d in configs:
        globals().update(d)

    db_con = psycopg2.connect("dbname=%s user=%s host=%s password=%s" % (db, user_name, host_name, passwd))
    cur = db_con.cursor()
    tile_stat = -1
    
    def __init__(self, tile_id, options, data):
        # assign passed arguments
        self.tile_id = tile_id
        self.options = options
        self.data = data
        self.id = tile_id
        self.the_kml = simplekml.Kml()
    
    ####
    # make_tile
    # makes the kml for a tile
    def make_tile(self):
        # get all the buildings
        statement = get_sql('sql/get_tile.sql')
        statement = statement % self.tile_id
        
        self.cur.execute(statement)
        self.buildings = self.cur.fetchall()
        
        if len(self.buildings) == 0: # no buildings!
            self.tile_stat = 0
        else:
            # iterate over the buildings
            if self.options['protocol']:
                for b_id in self.buildings:
                    self.make_building_prot(b_id)
            elif self.options['difference']:
                for b_id in self.buildings:
                    self.make_building_diff(b_id)
            else:
                for b_id in self.buildings:
                    self.make_building(b_id)
                
            self.db_con.close()
            self.the_kml.save('../data/kml/%s_tile.kml' % self.id, format=False)
            self.tile_stat = 1
    
    ####
    # make_building
    # adds the kml for a building
    def make_building(self, b_id):
        # make the statement
        statement = get_sql('sql/get_building.sql')
        statement = statement % b_id
        
        # get the info
        cur = self.db_con.cursor()
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
            
        # make color ramp
        if float(co2) > 0.00001:
            color = make_hex(float(co2), self.data[0], self.data[1])
            co2_str = "%.4f" % co2
        else:
            color = '9fD2D2D2'
            co2_str = 'NA'
            
        # create the multi geometry
        multi = self.the_kml.newmultigeometry(name="%s" %(addr), 
                                         description= self.make_descrip(co2_str, b_class, b_class_type, e_class, const, san))
        
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
    # make_building_diff
    # adds the kml for a building
    def make_building_diff(self, b_id):
        # make the statement
        statement = get_sql('sql/get_building_diff.sql')
        statement = statement.format(self.options['difference'], b_id[0])
        
        # get the info
        cur = self.db_con.cursor()
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
            
        # make color ramp
        if float(co2) > 0.00001:
            color = make_hex(float(co2), self.data[0], self.data[1])
            co2_str = "%.4f" % co2
        else:
            color = '9fD2D2D2'
            co2_str = 'NA'
            
        # create the multi geometry
        multi = self.the_kml.newmultigeometry(name="%s" %(addr), 
                                         description= self.make_descrip_diff(self.options['difference'],
                                                                             co2_str, b_class, const, san))
        
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
    # make_building
    # adds the kml for a building
    def make_building_prot(self, b_id):
        # make the statement
        statement = get_sql('sql/get_building_prot.sql')
        prot = self.options['protocol']
        statement = statement.format(prot, b_id[0])
        
        # get the info
        cur = self.db_con.cursor()
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
            color = self.data[self.data[:,0].astype('int')==int(co2),3][0]
        
        # create the multi geometry
        multi = self.the_kml.newmultigeometry(name="%s" %(addr), 
                                         description = self.make_descrip_prot(self.options['protocol'], 
                                                                              co2, b_class, const, san))
        
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
    # make_desc
    # makes a nice html description
    def make_descrip(self, co2_str, b_class, b_class_type, e_class, const, san):
        description = """<![CDATA[
            <table>
                <tr>
                    <td><b> Klasse </b></td>
                    <td> %s <td>
                </tr>
                <tr>
                    <td> <b> Funktion </b></td>
                    <td> % s </td>
                </tr>
                <tr>
                    <td><b> Energieklasse </b></td>
                    <td> %s </td>
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
                    <td> %s </td
                </tr>
            </table>
            <hr>
            <footer>
                <p> <i>GFZ</i> </p>
            </footer>
            ]]>
        """ % (b_class, b_class_type, e_class, const, san, co2_str)
        return description
    
    ####
    # make_desc_prot
    # makes a nice html description for protocol
    def make_descrip_prot(self, prot, co2_str, b_class, const, san):
        description = """<![CDATA[
            <table>
                <tr>
                    <td><b> Protokol </b></td>
                    <td> %s <td>
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
        """ % (prot, b_class,  const, san, co2_str)
        return description
    
    ####
    # make_desc_prot
    # makes a nice html description for protocol
    def make_descrip_diff(self, prot, co2_str, b_class, const, san):
        description = """<![CDATA[
            <table>
                <tr>
                    <td><b> Protokol </b></td>
                    <td> %s <td>
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
                    <td> %s </td
                </tr>
            </table>
            <hr>
            <footer>
                <p> <i>GFZ</i> </p>
            </footer>
            ]]>
        """ % (prot, b_class,  const, san, co2_str)
        return description
    
####
# class definiction of a ground tile
class ground():
    configs = get_config()
    for d in configs:
        globals().update(d)

    db_con = psycopg2.connect("dbname=%s user=%s host=%s password=%s" % (db, user_name, host_name, passwd))
    cur = db_con.cursor()
    
    def __init__(self, geom, geom_id, options, data):
        self.geom = geom
        self.id = geom_id
        self.options = options
        self.data = data
        self.the_kml = simplekml.Kml()
        
    def run(self):
        if self.options['protocol']:
            self.make_ground_prot()
        elif self.options['difference']:
            self.make_ground_diff()
        else:
            self.make_ground()
        
    ####
    # make_ground
    # makes the larger overlay for the zoomed image
    def make_ground(self):
        # get the values
        self.cur.execute(get_sql('sql/get_overlay_single.sql')% self.geom)
        buildings = self.cur.fetchall()
        
        # add each building
        for building in buildings:
            # get the color
            co2 = float(building[0])
            
            if float(co2) > 0.00001:
                color = make_hex(co2, self.data[0], self.data[1])
            else:
                color = '9fD2D2D2'
            
            # get coords
            coords = make_coords(building[1])
                
            # append to multi
            the_poly = self.the_kml.newpolygon(outerboundaryis=coords)
            the_poly.style.polystyle.color = color
            the_poly.style.linestyle.color = color
    
        
        # get the double buildings
        self.cur.execute(get_sql('sql/get_overlay_double.sql') % self.geom)
        buildings_2 = self.cur.fetchall()
        self.cur.close()
        
        for building in buildings_2:
            # get the color
            co2 = float(building[0])
            
            if float(co2) > 0.00001:
                color = make_hex(co2, self.data[0], self.data[1])
            else:
                color = '9fD2D2D2'
                
            # make multi
            multi = self.the_kml.newmultigeometry()
            for geom in building[1:2]:
                # get coords
                coords = make_coords(geom)
                    
                # append to multi
                multi.newpolygon(outerboundaryis=coords)
                multi.style.polystyle.color = color
                multi.style.linestyle.color = color
        
        self.the_kml.save('../data/kml/ground/%s.kml' % self.id)
    
    ###
    # make_ground_diff 
    # for the difference map
    def make_ground_diff(self):
        # get the values
        statement = get_sql('sql/get_overlay_single_diff.sql')
        prot = self.options['difference']
        statement = statement.format(prot, self.geom)
        self.cur.execute(statement)
        buildings = self.cur.fetchall()
        
        # add each building
        for building in buildings:
            # get the color
            co2 = float(building[0])
            
            if float(co2) > 0.00001:
                color = make_hex(co2, self.data[0], self.data[1])
            else:
                color = '9fD2D2D2'
            
            # get coords
            coords = make_coords(building[1])
                
            # append to multi
            the_poly = self.the_kml.newpolygon(outerboundaryis=coords)
            the_poly.style.polystyle.color = color
            the_poly.style.linestyle.color = color
    
        
        # get the double buildings
        statement = get_sql('sql/get_overlay_double_diff.sql')
        prot = self.options['difference']
        statement = statement.format(prot, self.geom)
        self.cur.execute(statement)
        buildings_2 = self.cur.fetchall()
        
        for building in buildings_2:
            # get the color
            co2 = float(building[0])
            
            if float(co2) > 0.00001:
                color = make_hex(co2, self.data[0], self.data[1])
            else:
                color = '9fD2D2D2'
                
            # make multi
            multi = self.the_kml.newmultigeometry()
            for geom in building[1:2]:
                # get coords
                coords = make_coords(geom)
                    
                # append to multi
                multi.newpolygon(outerboundaryis=coords)
                multi.style.polystyle.color = color
                multi.style.linestyle.color = color
        
        self.the_kml.save('../data/kml/ground/%s.kml' % self.id)
        
    # protocol ground
    def make_ground_prot(self):    
        # get the values
        statement = get_sql('sql/get_overlay_single_prot.sql')
        prot = self.options['protocol']
        statement = statement.format(prot, self.geom)
        self.cur.execute(statement)
        buildings = self.cur.fetchall()
        
        # add each building
        for building in buildings:
            # get the color
            co2 = float(building[0])
            color = self.data[self.data[:,0].astype('int')==int(co2),3][0]
                    
            # get coords
            coords = make_coords(building[1])
                
            # append to multi
            the_poly = self.the_kml.newpolygon(outerboundaryis=coords)
            the_poly.style.polystyle.color = color
            the_poly.style.linestyle.color = color
    
        
        # get the double buildings
        statement = get_sql('sql/get_overlay_double_prot.sql')
        statement = statement.format(self.options['protocol'], self.geom)
        self.cur.execute(statement)
        buildings_2 = self.cur.fetchall()
        self.cur.close()
        
        for building in buildings_2:
            # get the color
            co2 = float(building[0])
            color = self.data[self.data[:,0].astype('int')==int(co2),3][0]
                        
            # make multi
            multi = self.the_kml.newmultigeometry()
            for geom in building[1:2]:
                # get coords
                coords = make_coords(geom)
                    
                # append to multi
                multi.newpolygon(outerboundaryis=coords)
                multi.style.polystyle.color = color
                multi.style.linestyle.color = color
        
        self.the_kml.save('../data/kml/ground/%s.kml' % self.id)
    
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
    screen.size.x = 0.36
    screen.size.y = 0.08
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
        