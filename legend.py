'''
Created on 10 Apr 2015

Create the legend, either discrete or continous

@author: Ben
'''
import re
import numpy as np
import simplekml
import colorsys
import matplotlib as mpl
import matplotlib.pyplot as plt

import time

####
# the_legend
# a class object that creates the legend
class the_legend():
    ### 
    # INITIALLIZE
    ###
    def __init__(self, discrete_bool, co2_data):
        # matplot steup
        mpl.rc('font',**{'family':'sans-serif','sans-serif':['Helvetica']})
        mpl.rcParams['text.usetex'] = True
        mpl.rcParams['xtick.labelsize'] = 30
        mpl.rcParams['xtick.color'] = "white"
        mpl.rcParams['ytick.labelsize'] = 12
        mpl.rcParams['axes.edgecolor'] = 'white'
        mpl.rcParams['axes.linewidth'] = 0.
        mpl.rcParams['axes.labelcolor'] = 'white'
        
        # is the figure discrete or not?
        if discrete_bool:
            # create figure
            self.n = len(co2_data[:,0])
            self.fig = plt.figure(figsiz = (4*self.n, 3),dpi = 80)
            
            # assign the data, is array
            self.data = co2_data
        # for continuous
        else:
            self.fig = plt.figure(figsize=(10, 1.8), dpi = 80)
            self.min_x = co2_data[0]
            self.max_x = co2_data[1]
        
        
    ###
    # CLASS FUNCTIONS
    ###
    # color
    ###
    # make_descrete_hex
    # makes actual html hex for matplotlib
    def make_discrete_hex(self):
        max_x = self.n-1
        min_x = 0
        the_hex = []
        kml_hex = []
        
        for i in range(0, self.n):
            h = (float(max_x-i) / (max_x-min_x)) * 120
            
            # convert hsv color (h,1,1) to its rgb equivalent
            r, g, b = colorsys.hsv_to_rgb(h/360, .6, 1.)
            
            # make hex
            the_hex.append("#%02x%02x%02x"  % (int(r*255), int(g*255), int(b*255)))
            temp = simplekml.Color.rgb(int(r*255), int(g*255), int(b*255))
            temp = re.sub('^.{2}','9f', temp)
            kml_hex.append(temp)
        
        self.data = np.append(self.data, np.array(the_hex, ndmin=2).T, axis=1)
        self.data = np.append(self.data, np.array(kml_hex, ndmin=2).T, axis=1)
    
    # make_cont_hex
    # makes actual html hex for matplotlib
    def make_cont_hex(self, x):
        # make hsv h from  0..120 red .. green
        if x < self.min_x:
            h = 120
        elif x > self.max_x:
            h = 0
        else:
            h = (float(self.max_x-x) / (self.max_x-self.min_x)) * 120
        
        # convert hsv color (h,1,1) to its rgb equivalent
        r, g, b = colorsys.hsv_to_rgb(h/360, .6, 1.)
        
        # make hex
        hexa = "#%02x%02x%02x"  % (int(r*255), int(g*255), int(b*255))
        return(hexa)
    
    ###
    # drawing
    ###
    # add_legend
    # plots the color boxes
    def add_legend(self):
        # add axes
        ax = self.fig.add_axes([0.05, 0.2, 0.9, 0.9], 
                          yticklabels=str(), yticks = [0.],
                          xlim=[0, self.n+0.2], 
                          xticks = [0.],
                          xticklabels = str())
        
        i = 0.1
        for row in self.data:
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
        self.fig.text(0.5, 0.18, r'\textbf{CO2} [$\mathbf{kg/a\;m^3}$]', 
                 ha = 'center', va = 'center',
                 color = 'white', fontsize = 48, fontweight = 'bold')
        self.fig.show()
        

    
    # add_bar
    # plots the color ramp
    def add_bars(self, steps):
        ax = self.fig.add_axes([0.05, 0.2, 0.9, 0.9], 
                          yticklabels=str(), yticks = [0.],
                          xlim=[self.min_x, self.max_x], xlabel = "CO$^2$ [$kg/a\;m^3$]",
                          axisbelow = False)
    
        ax.xaxis.set_label_coords(0.5, 0.5)
        ax.xaxis.label.set_fontsize(25)
    
        inc = (self.max_x - self.min_x) / float(steps)
        bars = np.arange(self.min_x, self.max_x, inc)
        bars = np.append(bars, float(self.max_x))
        
        for i in  bars[0:-1]:
            col = self.make_cont_hex(i)
            ax.bar(i, 1, width=inc, color=col, linewidth=0, alpha=0.7)
        
        # show figure to get ticks    
        self.fig.show()
        
        # get and reset labels
        labels = [item.get_text() for item in ax.xaxis.get_ticklabels()]
        labels[-2] = '$>$'+labels[-2]
        ax.xaxis.set_ticklabels(labels)
        
        
    # save
    # save the plot to disc
    def save_plot(self):
        plt.savefig('../data/files/color_ramp.png', format='png', transparent=True)
        plt.close()
        