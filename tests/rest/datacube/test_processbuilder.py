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

