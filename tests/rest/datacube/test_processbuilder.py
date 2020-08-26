from openeo.rest.processbuilder import ProcessBuilder


def test_apply_neighborhood_udf(con100):
    collection = con100.load_collection("S2")
    callback = collection.apply_neighborhood( size=[
        {'dimension': 'x', 'value': 128, 'unit': 'px'},
        {'dimension': 'y', 'value': 128, 'unit': 'px'}
    ], overlap=[
        {'dimension': 't', 'value': 'P10d'},
    ])
    neighbors = callback.run_udf(code='myfancycode',runtime='Python').done()
    check_apply_neighbors(neighbors)


def check_apply_neighbors(neighbors):
    actual_graph = neighbors.graph['applyneighborhood1']
    assert actual_graph == {'arguments': {'data': {'from_node': 'loadcollection1'},
                                          'overlap': [{'dimension': 't', 'value': 'P10d'}],
                                          'process': {'process_graph': {'runudf1': {'arguments': {'udf': 'myfancycode',
                                                                                                  'data': {
                                                                                                      'from_parameter': 'data'},
                                                                                                  'runtime': 'Python'},
                                                                                    'process_id': 'run_udf',
                                                                                    'result': True}}},
                                          'size': [{'dimension': 'x', 'unit': 'px', 'value': 128},
                                                   {'dimension': 'y', 'unit': 'px', 'value': 128}]},
                            'process_id': 'apply_neighborhood',
                            'result': True}


def test_apply_neighborhood_udf_callback(con100):
    collection = con100.load_collection("S2")

    def callback(data:ProcessBuilder):
        return data.run_udf(code='myfancycode', runtime='Python')

    neighbors = collection.apply_neighborhood(process=callback, size=[
        {'dimension': 'x', 'value': 128, 'unit': 'px'},
        {'dimension': 'y', 'value': 128, 'unit': 'px'}
    ], overlap=[
        {'dimension': 't', 'value': 'P10d'},
    ])
    check_apply_neighbors(neighbors)


def test_apply_neighborhood_complex_callback(con100):
    collection = con100.load_collection("S2")

    from openeo.rest.processbuilder import max
    neighbors = collection.apply_neighborhood(process=lambda data:max(data).absolute(), size=[
        {'dimension': 'x', 'value': 128, 'unit': 'px'},
        {'dimension': 'y', 'value': 128, 'unit': 'px'}
    ], overlap=[
        {'dimension': 't', 'value': 'P10d'},
    ])
    actual_graph = neighbors.graph['applyneighborhood1']
    assert actual_graph == {'arguments': {'data': {'from_node': 'loadcollection1'},
                                          'overlap': [{'dimension': 't', 'value': 'P10d'}],
                                          'process': {'process_graph': {
                                              'absolute1': {'arguments': {'x': {'from_node': 'max1'}},
                                                           'process_id': 'absolute',
                                                           'result': True},
                                             'max1': {'arguments': {'data': {'from_parameter': 'data'},
                                                                    'ignore_nodata': True},
                                                      'process_id': 'max'}}
                                                      },
                                          'size': [{'dimension': 'x', 'unit': 'px', 'value': 128},
                                                   {'dimension': 'y', 'unit': 'px', 'value': 128}]},
                            'process_id': 'apply_neighborhood',
                            'result': True}


def test_apply_bandmath(con100):
    collection = con100.load_collection("S2")

    from openeo.rest.processbuilder import array_element

    bandsum = collection.apply(process=lambda data:array_element(data,index=1) + array_element(data,index=2))

    actual_graph = bandsum.graph['apply1']
    assert actual_graph == {'arguments': {'data': {'from_node': 'loadcollection1'},

                                          'process': {'process_graph': {'add1': {'arguments': {'x': {'from_node': 'arrayelement1'},
                                                                    'y': {'from_node': 'arrayelement2'}},
                                                      'process_id': 'add',
                                                      'result': True},
                                             'arrayelement1': {'arguments': {'data': {'from_parameter': 'data'},
                                                                             'index': 1},
                                                               'process_id': 'array_element'},
                                             'arrayelement2': {'arguments': {'data': {'from_parameter': 'data'},
                                                                             'index': 2},
                                                               'process_id': 'array_element'}}}},
                            'process_id': 'apply',
                            'result': True}

