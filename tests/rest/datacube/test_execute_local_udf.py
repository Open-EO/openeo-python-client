'''
Created on Aug 17, 2020

@author: banyait
'''
import unittest
import numpy
from xarray.core.dataarray import DataArray
from openeo_udf.api.datacube import DataCube
import xarray
from openeo.rest.datacube import DataCube as rest_DataCube
from tempfile import TemporaryDirectory
import os


udfcode=(
"# -*- coding: utf-8 -*-\n"
"# Uncomment the import only for coding support\n"
"from openeo_udf.api.datacube import DataCube\n"
"from typing import Dict\n"
"def apply_datacube(cube: DataCube, context: Dict) -> DataCube:\n"
"    inarr=cube.get_array()\n"
"    B4=inarr.loc[:,'bandzero']\n"
"    B8=inarr.loc[:,'bandone']\n"
"    ndvi=(B8-B4)/(B8+B4)\n"
"    ndvi=ndvi.expand_dims(dim='bands', axis=-3).assign_coords(bands=['ndvi'])\n"
"    return DataCube(ndvi)\n"
)

class TestLocalUDF(unittest.TestCase):

    def buildData(self):
        a=numpy.zeros((3,2,5,6),numpy.int32)
        for t in range(a.shape[0]):
            for b in range(a.shape[1]):
                for x in range(a.shape[2]):
                    for y in range(a.shape[3]):
                        a[t,b,x,y]=t*1000+b*100+x*10+y
        return DataCube(
            DataArray(
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


    def test_run_local_udf_fromfile(self):
        with TemporaryDirectory() as td:
            dc=self.buildData()
            tmpfile=os.path.join(td,'test_data')
            dc.to_file(tmpfile)
            r=rest_DataCube.execute_local_udf(udfcode, tmpfile)
            result=r.get_datacube_list()[0].get_array()
            exec(udfcode)
            ref=locals()["apply_datacube"](DataCube(dc.get_array().astype(numpy.float64).drop(labels='x').drop(labels='y')), {}).get_array()
            xarray.testing.assert_allclose(result,ref)


    def test_run_local_udf_frommemory(self):
        dc=self.buildData()
        r=rest_DataCube.execute_local_udf(udfcode, dc)
        result=r.get_datacube_list()[0].get_array()
        exec(udfcode)
        ref=locals()["apply_datacube"](DataCube(dc.get_array().astype(numpy.float64).drop(labels='x').drop(labels='y')), {}).get_array()
        xarray.testing.assert_allclose(result,ref)


    def test_run_local_udf_none(self):
        r=rest_DataCube.execute_local_udf(udfcode)
        self.assertEqual(r,None)
