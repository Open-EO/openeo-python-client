import shapely

from openeo.processes import ProcessBuilder, mean, array_create
from shapely.geometry import Polygon
import openeo
from openeo.udf import FeatureCollection
from geopandas import GeoDataFrame

polygon1 = Polygon([(16.138916, 48.1386), (16.138916, 48.320647), (16.2, 48.320647), (16.2, 48.1386)])
polygon2 = Polygon([(16.3, 48.1386), (16.3, 48.320647), (16.5, 48.320647), (16.5, 48.1386)])
polygons = shapely.geometry.GeometryCollection([polygon1, polygon2])

con = openeo.connect("https://openeocloud-dev.vito.be/openeo/1.0")

## Features ##
temp_ext_s2 = ["2020-04-02", "2020-04-20"]
spat_ext_s2 = {
    'west': 16.138916, 'east': 16.524124,
    'south': 48.1386, 'north': 48.320647
    }
cube = con.load_collection("boa_landsat_8",
                           spatial_extent=spat_ext_s2,
                           temporal_extent=temp_ext_s2,
                           bands=["B03", "B04"])
S2_month = cube.aggregate_temporal_period(period="month", reducer="mean")
s2_without_nodata = S2_month.apply_dimension(dimension="t", process="array_interpolate_linear")


def compute_stats(input_timeseries: ProcessBuilder):
    # Only take the first 5 months.
    tsteps = list([input_timeseries.array_element(index) for index in range(0, 5)])
    return array_create(tsteps)


s2_time_flattened = s2_without_nodata.apply_dimension(dimension='t', target_dimension='bands', process=compute_stats)
tstep_labels = ["t" + str(index) for index in range(0, 5)]
all_bands = [band + "_" + stat for band in cube.metadata.band_names for stat in tstep_labels]
s2_time_flattened = s2_time_flattened.rename_labels('bands', all_bands)

# 1 pixel per polygon with #bands features.
# Later features will be points that are randomly sampled from the original polygons.
features = s2_time_flattened.aggregate_spatial(geometries=polygons, reducer=mean)

## Target ##
labels = FeatureCollection(
    'labels', GeoDataFrame({"label": [1, 4]}, geometry=[polygon1.centroid, polygon2.centroid])
    )

## Training ##
ml_model = features.fit_class_random_forest(target=labels, training=100, num_trees=3, mtry=None)
ml_model.save_ml_model().send_job().start_and_wait()

example_req = {
    'process': {
        'process_graph': {
            'loadcollection1': {
                'process_id': 'load_collection','arguments': {
                    'bands': ['B03','B04'],'id': 'boa_landsat_8',
                    'spatial_extent': {'west': 16.138916,'east': 16.524124,'south': 48.1386,'north': 48.320647},
                    'temporal_extent': ['2020-04-02','2020-04-20']
                    }
                },'aggregatetemporalperiod1': {
                'process_id': 'aggregate_temporal_period','arguments': {
                    'data': {'from_node': 'loadcollection1'},'period': 'month','reducer': {
                        'process_graph': {
                            'mean1': {
                                'process_id': 'mean','arguments': {'data': {'from_parameter': 'data'}},'result': True
                                }
                            }
                        }
                    }
                },'applydimension1': {
                'process_id': 'apply_dimension','arguments': {
                    'data': {'from_node': 'aggregatetemporalperiod1'},'dimension': 't','process': {
                        'process_graph': {
                            'arrayinterpolatelinear1': {
                                'process_id': 'array_interpolate_linear',
                                'arguments': {'data': {'from_parameter': 'data'}},'result': True
                                }
                            }
                        }
                    }
                },'applydimension2': {
                'process_id': 'apply_dimension','arguments': {
                    'data': {'from_node': 'applydimension1'},'dimension': 't','process': {
                        'process_graph': {
                            'arrayelement1': {
                                'process_id': 'array_element',
                                'arguments': {'data': {'from_parameter': 'data'},'index': 0}
                                },'arrayelement2': {
                                'process_id': 'array_element',
                                'arguments': {'data': {'from_parameter': 'data'},'index': 1}
                                },'arrayelement3': {
                                'process_id': 'array_element',
                                'arguments': {'data': {'from_parameter': 'data'},'index': 2}
                                },'arrayelement4': {
                                'process_id': 'array_element',
                                'arguments': {'data': {'from_parameter': 'data'},'index': 3}
                                },'arrayelement5': {
                                'process_id': 'array_element',
                                'arguments': {'data': {'from_parameter': 'data'},'index': 4}
                                },'arraycreate1': {
                                'process_id': 'array_create','arguments': {
                                    'data': [{'from_node': 'arrayelement1'},{'from_node': 'arrayelement2'},
                                             {'from_node': 'arrayelement3'},{'from_node': 'arrayelement4'},
                                             {'from_node': 'arrayelement5'}]
                                    },'result': True
                                }
                            }
                        },'target_dimension': 'bands'
                    }
                },'renamelabels1': {
                'process_id': 'rename_labels','arguments': {
                    'data': {'from_node': 'applydimension2'},'dimension': 'bands',
                    'target': ['B03_t0','B03_t1','B03_t2','B03_t3','B03_t4','B04_t0','B04_t1','B04_t2','B04_t3',
                               'B04_t4']
                    }
                },'aggregatespatial1': {
                'process_id': 'aggregate_spatial','arguments': {
                    'data': {'from_node': 'renamelabels1'},'geometries': {
                        'type': 'GeometryCollection','geometries': [{
                                                                        'type': 'Polygon','coordinates': (((16.138916,
                                                                                                            48.1386),(
                                                                                                           16.138916,
                                                                                                           48.320647),(
                                                                                                           16.2,
                                                                                                           48.320647),(
                                                                                                           16.2,
                                                                                                           48.1386),(
                                                                                                           16.138916,
                                                                                                           48.1386)),)
                                                                        },{
                                                                        'type': 'Polygon','coordinates': (
                            ((16.3,48.1386),(16.3,48.320647),(16.5,48.320647),(16.5,48.1386),(16.3,48.1386)),)
                                                                        }]
                        },'reducer': {
                        'process_graph': {
                            'mean2': {
                                'process_id': 'mean','arguments': {'data': {'from_parameter': 'data'}},'result': True
                                }
                            }
                        }
                    }
                },'fitclassrandomforest1': {
                'process_id': 'fit_class_random_forest','arguments': {
                    'num_trees': 3,'predictors': {'from_node': 'aggregatespatial1'},'target': {
                        'id': 'labels','data': {
                            'type': 'FeatureCollection','features': [{
                                                                         'id': '0','type': 'Feature',
                                                                         'properties': {'label': 1},'geometry': {
                                    'type': 'Point','coordinates': (16.169458000000002,48.2296235)
                                    },'bbox': (16.169458000000002,48.2296235,16.169458000000002,48.2296235)
                                                                         },{
                                                                         'id': '1','type': 'Feature',
                                                                         'properties': {'label': 4},'geometry': {
                                    'type': 'Point','coordinates': (16.4,48.2296235)
                                    },'bbox': (16.4,48.2296235,16.4,48.2296235)
                                                                         }],
                            'bbox': (16.169458000000002,48.2296235,16.4,48.2296235)
                            }
                        },'training': 100
                    }
                },'savemlmodel1': {
                'process_id': 'save_ml_model',
                'arguments': {'data': {'from_node': 'fitclassrandomforest1'},'options': {}},'result': True
                }
            }
        }
    }
