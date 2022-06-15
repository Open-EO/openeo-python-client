# -*- coding: utf-8 -*-
from pathlib import Path
import openeo
import tarfile
import tempfile
import pstats

# reading NDVI compute UDF from file
# TODO #309 #312 Update UDF usage example
class UDFString():
    def __init__(self, filename):
        with open(str(Path(filename)), 'r+') as f:
            self.value=f.read()

if __name__ == '__main__':

    eoconn=openeo.connect('http://openeo-dev.vgt.vito.be/openeo/1.0.0/')
    eoconn.authenticate_basic()

    # creating a simple process that computes NDVI using python UDF 
    data=eoconn.load_collection("TERRASCOPE_S2_TOC_V2")\
        .filter_temporal('2020-09-09','2020-09-10')\
        .filter_bbox(**dict(zip(
            ["west", "south", "east", "north"],
            [5.047760653506093, 51.21384099522948, 5.062440732032759, 51.22255852538937]
        )))\
        .filter_bands(["B04", "B08"])
    data=data.apply_dimension(UDFString('udf/udf_profiling_ndvi.py').value, dimension='t',runtime="Python")
 
    # enable profiling and run the process
    # IMPORTANT: profiling can only be enabled in batch mode
    job_options={ 'profile':'true' }
    data.execute_batch('profiling_example_result.json',out_format='json',job_options=job_options)

    # read and display profiling results of the rdd's
    with tarfile.open('profile_dumps.tar.gz', 'r') as tf:
        with tempfile.TemporaryDirectory() as td:
            tf.extractall(td)
            for iprof in list(filter(lambda i: i.endswith('.pstats'), tf.getnames())):
                pstats.Stats(str(Path(td,iprof))).print_stats()
        

    
    
