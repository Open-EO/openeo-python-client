'''
Created on Aug 25, 2020

@author: banyait
'''
import unittest
import numpy
from openeo_udf.api.datacube import DataCube #TODO
import xarray
from tempfile import TemporaryDirectory
import os
import matplotlib.pyplot as plt
from openeo.rest.conversions import datacube_plot


class TestDataCubePlotter(unittest.TestCase):

    def buildData(self):
        a=numpy.zeros((3,2,100,100),numpy.int32)
        for t in range(a.shape[0]):
            for b in range(a.shape[1]):
                for x in range(a.shape[2]):
                    for y in range(a.shape[3]):
                        a[t,b,x,y]=(t*b+1)/(t+b+1)*(x*y+1)/(x+y+1)
        return DataCube(
            xarray.DataArray(
                a, 
                dims=['t','bands','x','y'],
                coords={
                    't':[numpy.datetime64('2020-08-01'),numpy.datetime64('2020-08-11'),numpy.datetime64('2020-08-21')],
                    'bands':['bandzero','bandone'],
                    'x':[10.+float(i) for i in range(a.shape[2])],
                    'y':[20.+float(i) for i in range(a.shape[3])]
                }
            )
        )

    def testPlot(self):
        with TemporaryDirectory() as td:
            tmpfile = os.path.join(td, 'test.png')
            d = self.buildData()
            datacube_plot(d, "title", oversample=1.2, cbartext="some\nvalue", to_file=tmpfile, to_show=False)
            respng = plt.imread(tmpfile)
            # Just check basic file properties to make sure the file isn't empty.
            assert len(respng.shape) == 3
            assert respng.shape[0] > 100
            assert respng.shape[1] > 100
            assert respng.shape[2] == 4
