# -*- coding: utf-8 -*-
import os
import unittest
import numpy
from openeo_udf.api.datacube import DataCube
import xarray
from tempfile import TemporaryDirectory
from openeo.rest.conversions import datacube_from_file, datacube_to_file

formats=['netcdf','json']

class TestLoadSaveDatacube(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._tmpdir=TemporaryDirectory()
        cls.tmpdir=str(cls._tmpdir.name)

    @classmethod
    def tearDownClass(cls):
        cls._tmpdir.cleanup()

    def buildData(self):
        a=numpy.zeros((3,2,5,6),numpy.int32)
        for t in range(a.shape[0]):
            for b in range(a.shape[1]):
                for x in range(a.shape[2]):
                    for y in range(a.shape[3]):
                        a[t,b,x,y]=t*1000+b*100+x*10+y
        return DataCube(
            xarray.DataArray(
                a, 
                dims=['t','bands','x','y'],
                coords={
                    't':[numpy.datetime64('2020-08-01'),numpy.datetime64('2020-08-11'),numpy.datetime64('2020-08-21')],
                    'bands':['bandzero','bandone'],
                    'x':[10.,11.,12.,13.,14.],
                    'y':[20.,21.,22.,23.,24.,25.]
                }
            )
        )


    def test_full(self):
        ref=self.buildData()
        for ifmt in formats:
            fn=os.path.join(self.tmpdir,'test_full.'+ifmt)
            print("Testing "+fn)
            datacube_to_file(ref,fn, fmt=ifmt)
            res=datacube_from_file(fn, fmt=ifmt)
        xarray.testing.assert_allclose(res.get_array(),ref.get_array())
        
        
    def test_time_nolabels(self):
        ref=self.buildData()
        ref=DataCube(ref.get_array().drop('t'))
        for ifmt in formats:
            fn=os.path.join(self.tmpdir,'test_time_nolabels.'+ifmt)
            print("Testing "+fn)
            datacube_to_file(ref,fn, fmt=ifmt)
            res=datacube_from_file(fn, fmt=ifmt)
        xarray.testing.assert_allclose(res.get_array(),ref.get_array())


    def test_time_nodim(self):
        ref=self.buildData()
        ref=DataCube(ref.get_array()[0].drop('t'))
        for ifmt in formats:
            fn=os.path.join(self.tmpdir,'test_time_nodim.'+ifmt)
            print("Testing "+fn)
            datacube_to_file(ref,fn, fmt=ifmt)
            res=datacube_from_file(fn, fmt=ifmt)
        xarray.testing.assert_allclose(res.get_array(),ref.get_array())

        
    def test_1band_nolabels(self):
        ref=self.buildData()
        ref=DataCube(ref.get_array().isel(bands=[0]).drop('bands'))
        for ifmt in formats:
            fn=os.path.join(self.tmpdir,'test_1band_nolabels.'+ifmt)
            print("Testing "+fn)
            datacube_to_file(ref,fn, fmt=ifmt)
            res=datacube_from_file(fn, fmt=ifmt)
        xarray.testing.assert_allclose(res.get_array(),ref.get_array())
 
     
    def test_2bands_nolabels(self):
        ref=self.buildData()
        ref=DataCube(ref.get_array().drop('bands'))
        for ifmt in formats:
            fn=os.path.join(self.tmpdir,'test_2bands_nolabels.'+ifmt)
            print("Testing "+fn)
            datacube_to_file(ref,fn, fmt=ifmt)
            res=datacube_from_file(fn, fmt=ifmt)
        xarray.testing.assert_allclose(res.get_array(),ref.get_array())


    def test_band_nodim(self):
        ref=self.buildData()
        ref=DataCube(ref.get_array()[:,0].drop('bands'))
        for ifmt in formats:
            fn=os.path.join(self.tmpdir,'test_band_nodim.'+ifmt)
            print("Testing "+fn)
            datacube_to_file(ref,fn, fmt=ifmt)
            res=datacube_from_file(fn, fmt=ifmt)
        xarray.testing.assert_allclose(res.get_array(),ref.get_array())


    def test_xy_nolabels(self):
        ref=self.buildData()
        ref=DataCube(ref.get_array().drop('x').drop('y'))
        for ifmt in formats:
            fn=os.path.join(self.tmpdir,'test_xy_nolabels.'+ifmt)
            print("Testing "+fn)
            datacube_to_file(ref,fn, fmt=ifmt)
            res=datacube_from_file(fn, fmt=ifmt)
        xarray.testing.assert_allclose(res.get_array(),ref.get_array())


    def test_xy_nodim(self):
        ref=self.buildData()
        ref=DataCube(ref.get_array()[:,:,0,0].drop('x').drop('y'))
        for ifmt in formats:
            fn=os.path.join(self.tmpdir,'test_xy_nodim.'+ifmt)
            print("Testing "+fn)
            datacube_to_file(ref,fn, fmt=ifmt)
            res=datacube_from_file(fn, fmt=ifmt)
        xarray.testing.assert_allclose(res.get_array(),ref.get_array())


    def test_typing_int(self):
        ref=self.buildData()
        ref=DataCube(ref.get_array().astype(numpy.int64))
        for ifmt in formats:
            fn=os.path.join(self.tmpdir,'test_typing_int.'+ifmt)
            print("Testing "+fn)
            datacube_to_file(ref,fn, fmt=ifmt)
            res=datacube_from_file(fn, fmt=ifmt)
        xarray.testing.assert_allclose(res.get_array(),ref.get_array())
        self.assertEqual(res.get_array().dtype, ref.get_array().dtype)


    def test_typing_float(self):
        ref=self.buildData()
        ref=DataCube(ref.get_array().astype(numpy.float64))
        for ifmt in formats:
            fn=os.path.join(self.tmpdir,'test_typing_float.'+ifmt)
            print("Testing "+fn)
            datacube_to_file(ref,fn, fmt=ifmt)
            res=datacube_from_file(fn, fmt=ifmt)
        xarray.testing.assert_allclose(res.get_array(),ref.get_array())
        self.assertEqual(res.get_array().dtype, ref.get_array().dtype)
