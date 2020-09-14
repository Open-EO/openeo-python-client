from openeo.internal.graph_building import PGNode
from openeo.processes.processes import ProcessBuilder

from ... import load_json_resource


def test_apply_callback_absolute_str(con100):
    im = con100.load_collection("S2")
    result = im.apply("absolute")
    assert result.graph == load_json_resource('data/1.0.0/apply_absolute.json')


def test_apply_callback_absolute_pgnode(con100):
    im = con100.load_collection("S2")
    result = im.apply(PGNode("absolute", x={"from_parameter": "x"}))
    assert result.graph == load_json_resource('data/1.0.0/apply_absolute.json')


def test_apply_callback_absolute_lambda_method(con100):
    im = con100.load_collection("S2")
    result = im.apply(lambda data: data.absolute())
    assert result.graph == load_json_resource('data/1.0.0/apply_absolute.json')


def test_apply_callback_absolute_function(con100):
    im = con100.load_collection("S2")
    from openeo.processes.processes import absolute
    result = im.apply(absolute)
    assert result.graph == load_json_resource('data/1.0.0/apply_absolute.json')


def test_apply_callback_absolute_custom_function(con100):
    def abs(x: ProcessBuilder) -> ProcessBuilder:
        return x.absolute()

    im = con100.load_collection("S2")
    result = im.apply(abs)
    assert result.graph == load_json_resource('data/1.0.0/apply_absolute.json')


def test_apply_callback_chain_lambda_method(con100):
    im = con100.load_collection("S2")
    result = im.apply(lambda data: data.absolute().cos().add(y=1.23))
    assert result.graph == load_json_resource('data/1.0.0/apply_chain.json')


def test_apply_callback_chain_lambda_functions(con100):
    im = con100.load_collection("S2")
    from openeo.processes.processes import absolute, cos, add
    result = im.apply(lambda data: add(cos(absolute(data)), 1.23))
    assert result.graph == load_json_resource('data/1.0.0/apply_chain.json')


def test_apply_callback_chain_lambda_mixed_and_operator(con100):
    im = con100.load_collection("S2")
    from openeo.processes.processes import cos
    result = im.apply(lambda data: cos(data.absolute()) + 1.23)
    assert result.graph == load_json_resource('data/1.0.0/apply_chain.json')


def test_apply_callback_chain_custom_function_methods(con100):
    def transform(x: ProcessBuilder) -> ProcessBuilder:
        return x.absolute().cos().add(y=1.23)

    im = con100.load_collection("S2")
    result = im.apply(transform)
    assert result.graph == load_json_resource('data/1.0.0/apply_chain.json')


def test_apply_callback_chain_custom_function_functions(con100):
    from openeo.processes.processes import absolute, cos, add

    def transform(x: ProcessBuilder) -> ProcessBuilder:
        return add(cos(absolute(x)), y=1.23)

    im = con100.load_collection("S2")
    result = im.apply(transform)
    assert result.graph == load_json_resource('data/1.0.0/apply_chain.json')


def test_apply_callback_chain_custom_function_mixed_and_operator(con100):
    from openeo.processes.processes import cos

    def transform(x: ProcessBuilder) -> ProcessBuilder:
        return cos(x.absolute()) + 1.23

    im = con100.load_collection("S2")
    result = im.apply(transform)
    assert result.graph == load_json_resource('data/1.0.0/apply_chain.json')


def test_apply_callback_chain_pgnode(con100):
    im = con100.load_collection("S2")
    result = im.apply(PGNode(
        "add",
        x=PGNode("cos", x=PGNode("absolute", x={"from_parameter": "x"})),
        y=1.23
    ))
    assert result.graph == load_json_resource('data/1.0.0/apply_chain.json')


def test_apply_callback_math_lambda(con100):
    im = con100.load_collection("S2")
    result = im.apply(lambda data: (((data + 1) - 2) * 3) / 4)
    assert result.graph == load_json_resource('data/1.0.0/apply_math.json')


def test_apply_callback_math_custom_function(con100):
    def do_math(data: ProcessBuilder) -> ProcessBuilder:
        return (((data + 1) - 2) * 3) / 4

    im = con100.load_collection("S2")
    result = im.apply(do_math)
    assert result.graph == load_json_resource('data/1.0.0/apply_math.json')


def test_apply_neighborhood_trim_str(con100):
    im = con100.load_collection("S2")
    result = im.apply_neighborhood(
        process="trim_cube",
        size=[{'dimension': 'x', 'value': 128, 'unit': 'px'}, {'dimension': 'y', 'value': 128, 'unit': 'px'}]
    )
    assert result.graph == load_json_resource('data/1.0.0/apply_neighborhood_trim.json')


def test_apply_neighborhood_trim_pgnode(con100):
    im = con100.load_collection("S2")
    result = im.apply_neighborhood(
        process=PGNode("trim_cube", data={"from_parameter": "data"}),
        size=[{'dimension': 'x', 'value': 128, 'unit': 'px'}, {'dimension': 'y', 'value': 128, 'unit': 'px'}]
    )
    assert result.graph == load_json_resource('data/1.0.0/apply_neighborhood_trim.json')


def test_apply_neighborhood_trim_callable(con100):
    from openeo.processes.processes import trim_cube
    im = con100.load_collection("S2")
    result = im.apply_neighborhood(
        process=trim_cube,
        size=[{'dimension': 'x', 'value': 128, 'unit': 'px'}, {'dimension': 'y', 'value': 128, 'unit': 'px'}]
    )
    assert result.graph == load_json_resource('data/1.0.0/apply_neighborhood_trim.json')


def test_apply_neighborhood_trim_lambda(con100):
    im = con100.load_collection("S2")
    result = im.apply_neighborhood(
        process=lambda data: data.trim_cube(),
        size=[{'dimension': 'x', 'value': 128, 'unit': 'px'}, {'dimension': 'y', 'value': 128, 'unit': 'px'}]
    )
    assert result.graph == load_json_resource('data/1.0.0/apply_neighborhood_trim.json')


def test_apply_neighborhood_udf_callback(con100):
    def callback(data: ProcessBuilder):
        return data.run_udf(udf='myfancycode', runtime='Python')

    collection = con100.load_collection("S2")
    neighbors = collection.apply_neighborhood(process=callback, size=[
        {'dimension': 'x', 'value': 128, 'unit': 'px'},
        {'dimension': 'y', 'value': 128, 'unit': 'px'}
    ], overlap=[
        {'dimension': 't', 'value': 'P10d'},
    ])
    actual_graph = neighbors.graph['applyneighborhood1']
    assert actual_graph == {
        'process_id': 'apply_neighborhood',
        'arguments': {
            'data': {'from_node': 'loadcollection1'},
            'overlap': [{'dimension': 't', 'value': 'P10d'}],
            'process': {'process_graph': {
                'runudf1': {
                    'process_id': 'run_udf',
                    'arguments': {
                        'udf': 'myfancycode',
                        'data': {'from_parameter': 'data'},
                        'runtime': 'Python',
                    },
                    'result': True
                }
            }},
            'size': [{'dimension': 'x', 'unit': 'px', 'value': 128}, {'dimension': 'y', 'unit': 'px', 'value': 128}]},
        'result': True
    }


def test_apply_neighborhood_complex_callback(con100):
    collection = con100.load_collection("S2")

    from openeo.processes.processes import max
    neighbors = collection.apply_neighborhood(process=lambda data: max(data).absolute(), size=[
        {'dimension': 'x', 'value': 128, 'unit': 'px'},
        {'dimension': 'y', 'value': 128, 'unit': 'px'}
    ], overlap=[
        {'dimension': 't', 'value': 'P10d'},
    ])
    actual_graph = neighbors.graph['applyneighborhood1']
    assert actual_graph == {
        'process_id': 'apply_neighborhood',
        'arguments': {
            'data': {'from_node': 'loadcollection1'},
            'overlap': [{'dimension': 't', 'value': 'P10d'}],
            'process': {'process_graph': {
                'max1': {
                    'process_id': 'max',
                    'arguments': {'data': {'from_parameter': 'data'}},
                },
                'absolute1': {
                    'process_id': 'absolute',
                    'arguments': {'x': {'from_node': 'max1'}},
                    'result': True
                },
            }},
            'size': [{'dimension': 'x', 'unit': 'px', 'value': 128}, {'dimension': 'y', 'unit': 'px', 'value': 128}]},
        'result': True
    }


def test_apply_dimension_max_str(con100):
    im = con100.load_collection("S2")
    res = im.apply_dimension(process="max", dimension="bands")
    assert res.graph == load_json_resource('data/1.0.0/apply_dimension_max.json')


def test_apply_dimension_max_pgnode(con100):
    im = con100.load_collection("S2")
    res = im.apply_dimension(process=PGNode("max", data={"from_parameter": "data"}), dimension="bands")
    assert res.graph == load_json_resource('data/1.0.0/apply_dimension_max.json')


def test_apply_dimension_max_callable(con100):
    im = con100.load_collection("S2")
    from openeo.processes.processes import max
    res = im.apply_dimension(process=max, dimension="bands")
    assert res.graph == load_json_resource('data/1.0.0/apply_dimension_max.json')


def test_apply_dimension_max_lambda(con100):
    im = con100.load_collection("S2")
    res = im.apply_dimension(process=lambda data: data.max(), dimension="bands")
    assert res.graph == load_json_resource('data/1.0.0/apply_dimension_max.json')


def test_apply_dimension_bandmath_lambda(con100):
    from openeo.processes.processes import array_element
    im = con100.load_collection("S2")
    res = im.apply_dimension(
        process=lambda d: array_element(d, index=1) + array_element(d, index=2),
        dimension="bands"
    )
    assert res.graph == load_json_resource('data/1.0.0/apply_dimension_bandmath.json')


def test_reduce_dimension_max_str(con100):
    im = con100.load_collection("S2")
    res = im.reduce_dimension(reducer="max", dimension="bands")
    assert res.graph == load_json_resource('data/1.0.0/reduce_dimension_max.json')


def test_reduce_dimension_max_pgnode(con100):
    im = con100.load_collection("S2")
    res = im.reduce_dimension(reducer=PGNode("max", data={"from_parameter": "data"}), dimension="bands")
    assert res.graph == load_json_resource('data/1.0.0/reduce_dimension_max.json')


def test_reduce_dimension_max_callable(con100):
    im = con100.load_collection("S2")
    from openeo.processes.processes import max
    res = im.reduce_dimension(reducer=max, dimension="bands")
    assert res.graph == load_json_resource('data/1.0.0/reduce_dimension_max.json')


def test_reduce_dimension_max_lambda(con100):
    im = con100.load_collection("S2")
    res = im.reduce_dimension(reducer=lambda data: data.max(), dimension="bands")
    assert res.graph == load_json_resource('data/1.0.0/reduce_dimension_max.json')


def test_reduce_dimension_bandmath_lambda(con100):
    from openeo.processes.processes import array_element
    im = con100.load_collection("S2")
    res = im.reduce_dimension(
        reducer=lambda data: array_element(data, index=1) + array_element(data, index=2),
        dimension='bands'
    )
    assert res.graph == load_json_resource('data/1.0.0/reduce_dimension_bandmath.json')


def test_merge_cubes_add_str(con100):
    im1 = con100.load_collection("S2")
    im2 = con100.load_collection("MASK")
    res = im1.merge_cubes(other=im2, overlap_resolver="add")
    assert res.graph == load_json_resource('data/1.0.0/merge_cubes_add.json')


def test_merge_cubes_add_pgnode(con100):
    im1 = con100.load_collection("S2")
    im2 = con100.load_collection("MASK")
    res = im1.merge_cubes(
        other=im2,
        overlap_resolver=PGNode("add", x={"from_parameter": "x"}, y={"from_parameter": "y"})
    )
    assert res.graph == load_json_resource('data/1.0.0/merge_cubes_add.json')


def test_merge_cubes_add_callable(con100):
    im1 = con100.load_collection("S2")
    im2 = con100.load_collection("MASK")
    from openeo.processes.processes import add
    res = im1.merge_cubes(other=im2, overlap_resolver=add)
    assert res.graph == load_json_resource('data/1.0.0/merge_cubes_add.json')


def test_merge_cubes_add_lambda(con100):
    im1 = con100.load_collection("S2")
    im2 = con100.load_collection("MASK")
    res = im1.merge_cubes(other=im2, overlap_resolver=lambda x, y: x + y)
    assert res.graph == load_json_resource('data/1.0.0/merge_cubes_add.json')


def test_merge_cubes_max_str(con100):
    im1 = con100.load_collection("S2")
    im2 = con100.load_collection("MASK")
    res = im1.merge_cubes(other=im2, overlap_resolver="max")
    assert res.graph == load_json_resource('data/1.0.0/merge_cubes_max.json')


def test_merge_cubes_max_pgnode(con100):
    im1 = con100.load_collection("S2")
    im2 = con100.load_collection("MASK")
    res = im1.merge_cubes(
        other=im2,
        overlap_resolver=PGNode("max", data=[{"from_parameter": "x"}, {"from_parameter": "y"}])
    )
    assert res.graph == load_json_resource('data/1.0.0/merge_cubes_max.json')


def test_merge_cubes_max_callable(con100):
    im1 = con100.load_collection("S2")
    im2 = con100.load_collection("MASK")
    from openeo.processes.processes import max
    res = im1.merge_cubes(other=im2, overlap_resolver=max)
    assert res.graph == load_json_resource('data/1.0.0/merge_cubes_max.json')


def test_merge_cubes_max_lambda(con100):
    im1 = con100.load_collection("S2")
    im2 = con100.load_collection("MASK")
    res = im1.merge_cubes(other=im2, overlap_resolver=lambda data: data.max())
    assert res.graph == load_json_resource('data/1.0.0/merge_cubes_max.json')
