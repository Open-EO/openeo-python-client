import builtins
import functools

import pytest

import openeo.processes
from openeo.internal.graph_building import PGNode
from openeo.processes import ProcessBuilder
from ... import load_json_resource


def test_apply_callback_absolute_str(con100):
    im = con100.load_collection("S2")
    result = im.apply("absolute")
    assert result.flat_graph() == load_json_resource('data/1.0.0/apply_absolute.json')


def test_apply_callback_absolute_pgnode(con100):
    im = con100.load_collection("S2")
    result = im.apply(PGNode("absolute", x={"from_parameter": "x"}))
    assert result.flat_graph() == load_json_resource('data/1.0.0/apply_absolute.json')


def test_apply_callback_absolute_lambda_method(con100):
    im = con100.load_collection("S2")
    result = im.apply(lambda data: data.absolute())
    assert result.flat_graph() == load_json_resource('data/1.0.0/apply_absolute.json')


def test_apply_callback_absolute_function(con100):
    im = con100.load_collection("S2")
    from openeo.processes import absolute
    result = im.apply(absolute)
    assert result.flat_graph() == load_json_resource('data/1.0.0/apply_absolute.json')


def test_apply_callback_absolute_custom_function(con100):
    def abs(x: ProcessBuilder) -> ProcessBuilder:
        return x.absolute()

    im = con100.load_collection("S2")
    result = im.apply(abs)
    assert result.flat_graph() == load_json_resource('data/1.0.0/apply_absolute.json')


def test_apply_callback_chain_lambda_method(con100):
    im = con100.load_collection("S2")
    result = im.apply(lambda data: data.absolute().cos().add(y=1.23))
    assert result.flat_graph() == load_json_resource('data/1.0.0/apply_chain.json')


def test_apply_callback_chain_lambda_functions(con100):
    im = con100.load_collection("S2")
    from openeo.processes import absolute, cos, add
    result = im.apply(lambda data: add(cos(absolute(data)), 1.23))
    assert result.flat_graph() == load_json_resource('data/1.0.0/apply_chain.json')


def test_apply_callback_chain_lambda_mixed_and_operator(con100):
    im = con100.load_collection("S2")
    from openeo.processes import cos
    result = im.apply(lambda data: cos(data.absolute()) + 1.23)
    assert result.flat_graph() == load_json_resource('data/1.0.0/apply_chain.json')


def test_apply_callback_chain_custom_function_methods(con100):
    def transform(x: ProcessBuilder) -> ProcessBuilder:
        return x.absolute().cos().add(y=1.23)

    im = con100.load_collection("S2")
    result = im.apply(transform)
    assert result.flat_graph() == load_json_resource('data/1.0.0/apply_chain.json')


def test_apply_callback_chain_custom_function_functions(con100):
    from openeo.processes import absolute, cos, add

    def transform(x: ProcessBuilder) -> ProcessBuilder:
        return add(cos(absolute(x)), y=1.23)

    im = con100.load_collection("S2")
    result = im.apply(transform)
    assert result.flat_graph() == load_json_resource('data/1.0.0/apply_chain.json')


def test_apply_callback_chain_custom_function_mixed_and_operator(con100):
    from openeo.processes import cos

    def transform(x: ProcessBuilder) -> ProcessBuilder:
        return cos(x.absolute()) + 1.23

    im = con100.load_collection("S2")
    result = im.apply(transform)
    assert result.flat_graph() == load_json_resource('data/1.0.0/apply_chain.json')


def test_apply_callback_chain_pgnode(con100):
    im = con100.load_collection("S2")
    result = im.apply(PGNode(
        "add",
        x=PGNode("cos", x=PGNode("absolute", x={"from_parameter": "x"})),
        y=1.23
    ))
    assert result.flat_graph() == load_json_resource('data/1.0.0/apply_chain.json')


def test_apply_callback_math_lambda(con100):
    im = con100.load_collection("S2")
    result = im.apply(lambda data: (((data + 1) - 2) * 3) / 4)
    assert result.flat_graph() == load_json_resource('data/1.0.0/apply_math.json')


def test_apply_callback_math_lambda_reflected(con100):
    im = con100.load_collection("S2")
    # Reflected operators __radd__, __rsub__, ...
    result = im.apply(lambda data: 1 + (2 - (3 * (4 / data))))
    assert result.flat_graph() == load_json_resource('data/1.0.0/apply_math_reflected.json')


def test_apply_callback_math_custom_function(con100):
    def do_math(data: ProcessBuilder) -> ProcessBuilder:
        return (((data + 1) - 2) * 3) / 4

    im = con100.load_collection("S2")
    result = im.apply(do_math)
    assert result.flat_graph() == load_json_resource('data/1.0.0/apply_math.json')


def test_apply_callback_math_custom_function_reflected(con100):
    # Reflected operators __radd__, __rsub__, ...
    def do_math(data: ProcessBuilder) -> ProcessBuilder:
        return 1 + (2 - (3 * (4 / data)))

    im = con100.load_collection("S2")
    result = im.apply(do_math)
    assert result.flat_graph() == load_json_resource('data/1.0.0/apply_math_reflected.json')


@pytest.mark.parametrize(["function", "expected"], [
    ((lambda data: data < 5), "lt"),
    ((lambda data: data > 5), "gt"),
    ((lambda data: data <= 5), "lte"),
    ((lambda data: data >= 5), "gte"),
])
def test_apply_callback_comparison_lambda(con100, function, expected):
    im = con100.load_collection("S2")
    result = im.apply(function)
    assert result.flat_graph()["apply1"]["arguments"] == {
        "data": {"from_node": "loadcollection1"},
        "process": {
            "process_graph": {
                expected + "1": {
                    "process_id": expected,
                    "arguments": {"x": {"from_parameter": "x"}, "y": 5},
                    "result": True,
                }
            }
        }
    }


def test_apply_neighborhood_trim_str(con100):
    im = con100.load_collection("S2")
    result = im.apply_neighborhood(
        process="trim_cube",
        size=[{'dimension': 'x', 'value': 128, 'unit': 'px'}, {'dimension': 'y', 'value': 128, 'unit': 'px'}]
    )
    assert result.flat_graph() == load_json_resource('data/1.0.0/apply_neighborhood_trim.json')


def test_apply_neighborhood_trim_pgnode(con100):
    im = con100.load_collection("S2")
    result = im.apply_neighborhood(
        process=PGNode("trim_cube", data={"from_parameter": "data"}),
        size=[{'dimension': 'x', 'value': 128, 'unit': 'px'}, {'dimension': 'y', 'value': 128, 'unit': 'px'}]
    )
    assert result.flat_graph() == load_json_resource('data/1.0.0/apply_neighborhood_trim.json')


def test_apply_neighborhood_trim_callable(con100):
    from openeo.processes import trim_cube
    im = con100.load_collection("S2")
    result = im.apply_neighborhood(
        process=trim_cube,
        size=[{'dimension': 'x', 'value': 128, 'unit': 'px'}, {'dimension': 'y', 'value': 128, 'unit': 'px'}]
    )
    assert result.flat_graph() == load_json_resource('data/1.0.0/apply_neighborhood_trim.json')


def test_apply_neighborhood_trim_lambda(con100):
    im = con100.load_collection("S2")
    result = im.apply_neighborhood(
        process=lambda data: data.trim_cube(),
        size=[{'dimension': 'x', 'value': 128, 'unit': 'px'}, {'dimension': 'y', 'value': 128, 'unit': 'px'}]
    )
    assert result.flat_graph() == load_json_resource('data/1.0.0/apply_neighborhood_trim.json')


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
    actual_graph = neighbors.flat_graph()['applyneighborhood1']
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

    from openeo.processes import max
    neighbors = collection.apply_neighborhood(process=lambda data: max(data).absolute(), size=[
        {'dimension': 'x', 'value': 128, 'unit': 'px'},
        {'dimension': 'y', 'value': 128, 'unit': 'px'}
    ], overlap=[
        {'dimension': 't', 'value': 'P10d'},
    ])
    actual_graph = neighbors.flat_graph()['applyneighborhood1']
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
    assert res.flat_graph() == load_json_resource('data/1.0.0/apply_dimension_max.json')


def test_apply_dimension_max_pgnode(con100):
    im = con100.load_collection("S2")
    res = im.apply_dimension(process=PGNode("max", data={"from_parameter": "data"}), dimension="bands")
    assert res.flat_graph() == load_json_resource('data/1.0.0/apply_dimension_max.json')


def test_apply_dimension_max_callable(con100):
    im = con100.load_collection("S2")
    from openeo.processes import max
    res = im.apply_dimension(process=max, dimension="bands")
    assert res.flat_graph() == load_json_resource('data/1.0.0/apply_dimension_max.json')


def test_apply_dimension_max_lambda(con100):
    im = con100.load_collection("S2")
    res = im.apply_dimension(process=lambda data: data.max(), dimension="bands")
    assert res.flat_graph() == load_json_resource('data/1.0.0/apply_dimension_max.json')


def test_apply_dimension_interpolate_lambda(con100):
    im = con100.load_collection("S2")
    res = im.apply_dimension(process=lambda data: data.array_interpolate_linear(), dimension="bands")
    assert res.flat_graph() == load_json_resource('data/1.0.0/apply_dimension_interpolate.json')


def test_apply_dimension_bandmath_lambda(con100):
    from openeo.processes import array_element
    im = con100.load_collection("S2")
    res = im.apply_dimension(
        process=lambda d: array_element(d, index=1) + array_element(d, index=2),
        dimension="bands"
    )
    assert res.flat_graph() == load_json_resource('data/1.0.0/apply_dimension_bandmath.json')


def test_apply_dimension_time_to_bands(con100):
    from openeo.processes import array_concat, quantiles, sd, mean
    im = con100.load_collection("S2")
    res = im.apply_dimension(
        process=lambda d: array_concat(quantiles(d, [0.25, 0.5, 0.75]), [sd(d), mean(d)]),
        dimension="t",
        target_dimension="bands"
    )
    assert res.flat_graph() == load_json_resource('data/1.0.0/apply_dimension_time_to_bands.json')


def test_reduce_dimension_max_str(con100):
    im = con100.load_collection("S2")
    res = im.reduce_dimension(reducer="max", dimension="bands")
    assert res.flat_graph() == load_json_resource('data/1.0.0/reduce_dimension_max.json')


def test_reduce_dimension_max_pgnode(con100):
    im = con100.load_collection("S2")
    res = im.reduce_dimension(reducer=PGNode("max", data={"from_parameter": "data"}), dimension="bands")
    assert res.flat_graph() == load_json_resource('data/1.0.0/reduce_dimension_max.json')


def test_reduce_dimension_max_callable(con100):
    im = con100.load_collection("S2")
    from openeo.processes import max
    res = im.reduce_dimension(reducer=max, dimension="bands")
    assert res.flat_graph() == load_json_resource('data/1.0.0/reduce_dimension_max.json')


def test_reduce_dimension_max_lambda(con100):
    im = con100.load_collection("S2")
    res = im.reduce_dimension(reducer=lambda data: data.max(), dimension="bands")
    assert res.flat_graph() == load_json_resource('data/1.0.0/reduce_dimension_max.json')


def test_reduce_dimension_bandmath_lambda(con100):
    from openeo.processes import array_element
    im = con100.load_collection("S2")
    res = im.reduce_dimension(
        reducer=lambda data: array_element(data, index=1) + array_element(data, index=2),
        dimension='bands'
    )
    assert res.flat_graph() == load_json_resource('data/1.0.0/reduce_dimension_bandmath.json')


@pytest.mark.parametrize(["reducer", "expected"], [
    (
            (lambda data: data.mean()),
            {"mean1": {"process_id": "mean", "arguments": {"data": {"from_parameter": "data"}}, "result": True}},
    ),
    (
            (lambda x: x.mean()),
            {"mean1": {"process_id": "mean", "arguments": {"data": {"from_parameter": "data"}}, "result": True}},
    ),
    (
            (lambda valyuez: valyuez.mean()),
            {"mean1": {"process_id": "mean", "arguments": {"data": {"from_parameter": "data"}}, "result": True}},
    ),
    (
            (lambda data, context: data.mean() + context),
            {
                "mean1": {"process_id": "mean", "arguments": {"data": {"from_parameter": "data"}}, },
                "add1": {
                    "process_id": "add",
                    "arguments": {"x": {"from_node": "mean1"}, "y": {"from_parameter": "context"}},
                    "result": True,
                },
            },
    ),
    (
            (lambda data, ignore_nan=False: data.mean()),
            {"mean1": {"process_id": "mean", "arguments": {"data": {"from_parameter": "data"}}, "result": True}},
    ),
])
def test_reduce_dimension_lambda_and_context(con100, reducer, expected):
    im = con100.load_collection("S2")
    res = im.reduce_dimension(reducer=reducer, dimension="bands")
    assert res.flat_graph()["reducedimension1"]["arguments"]["reducer"]["process_graph"] == expected


@pytest.mark.parametrize(["reducer", "expected_arguments"], [
    (
            "count",
            {"data": {"from_parameter": "data"}, "context": {"from_parameter": "context"}}
    ),
    (
            openeo.processes.count,
            {"data": {"from_parameter": "data"}}
    ),
    (
            lambda data: data.count(),
            {"data": {"from_parameter": "data"}}
    ),
    (
            lambda data: data.count(context={"foo": "bar"}),
            {"data": {"from_parameter": "data"}, "context": {"foo": "bar"}}
    ),
    (
            lambda data: openeo.processes.count(data),
            {"data": {"from_parameter": "data"}}
    ),
    (
            lambda data: openeo.processes.count(data, context={"foo": "bar"}),
            {"data": {"from_parameter": "data"}, "context": {"foo": "bar"}}
    ),
])
def test_reduce_dimension_count(con100, reducer, expected_arguments):
    """https://github.com/Open-EO/openeo-python-client/issues/317"""
    im = con100.load_collection("S2")
    res = im.reduce_dimension(reducer=reducer, dimension="t")
    assert res.flat_graph()["reducedimension1"]["arguments"]["reducer"]["process_graph"] == {
        "count1": {"process_id": "count", "arguments": expected_arguments, "result": True}
    }


@pytest.mark.parametrize(["process", "expected"], [
    (
            (lambda data: data.order()),
            {"order1": {"process_id": "order", "arguments": {"data": {"from_parameter": "data"}}, "result": True}},
    ),
    (
            (lambda x: x.order()),
            {"order1": {"process_id": "order", "arguments": {"data": {"from_parameter": "data"}}, "result": True}},
    ),
    (
            (lambda valyuez: valyuez.order()),
            {"order1": {"process_id": "order", "arguments": {"data": {"from_parameter": "data"}}, "result": True}},
    ),
    (
            (lambda data, context: data.order(asc=context)),
            {
                "order1": {
                    "process_id": "order",
                    "arguments": {"data": {"from_parameter": "data"}, "asc": {"from_parameter": "context"}},
                    "result": True,
                },
            },
    ),
    (
            (lambda data, ignore_nan=False: data.order()),
            {"order1": {"process_id": "order", "arguments": {"data": {"from_parameter": "data"}}, "result": True}},
    ),
])
def test_apply_dimension_lambda_and_context(con100, process, expected):
    im = con100.load_collection("S2")
    res = im.apply_dimension(process=process, dimension="bands")
    assert res.flat_graph()["applydimension1"]["arguments"]["process"]["process_graph"] == expected


def test_merge_cubes_add_str(con100):
    im1 = con100.load_collection("S2")
    im2 = con100.load_collection("MASK")
    res = im1.merge_cubes(other=im2, overlap_resolver="add")
    assert res.flat_graph() == load_json_resource('data/1.0.0/merge_cubes_add.json')


def test_merge_cubes_add_pgnode(con100):
    im1 = con100.load_collection("S2")
    im2 = con100.load_collection("MASK")
    res = im1.merge_cubes(
        other=im2,
        overlap_resolver=PGNode("add", x={"from_parameter": "x"}, y={"from_parameter": "y"})
    )
    assert res.flat_graph() == load_json_resource('data/1.0.0/merge_cubes_add.json')


def test_merge_cubes_add_callable(con100):
    im1 = con100.load_collection("S2")
    im2 = con100.load_collection("MASK")
    from openeo.processes import add
    res = im1.merge_cubes(other=im2, overlap_resolver=add)
    assert res.flat_graph() == load_json_resource('data/1.0.0/merge_cubes_add.json')


def test_merge_cubes_add_lambda(con100):
    im1 = con100.load_collection("S2")
    im2 = con100.load_collection("MASK")
    res = im1.merge_cubes(other=im2, overlap_resolver=lambda x, y: x + y)
    assert res.flat_graph() == load_json_resource('data/1.0.0/merge_cubes_add.json')


def test_merge_cubes_max_str(con100):
    im1 = con100.load_collection("S2")
    im2 = con100.load_collection("MASK")
    res = im1.merge_cubes(other=im2, overlap_resolver="max")
    assert res.flat_graph() == load_json_resource('data/1.0.0/merge_cubes_max.json')


def test_merge_cubes_max_pgnode(con100):
    im1 = con100.load_collection("S2")
    im2 = con100.load_collection("MASK")
    res = im1.merge_cubes(
        other=im2,
        overlap_resolver=PGNode("max", data=[{"from_parameter": "x"}, {"from_parameter": "y"}])
    )
    assert res.flat_graph() == load_json_resource('data/1.0.0/merge_cubes_max.json')


def test_merge_cubes_max_callable(con100):
    im1 = con100.load_collection("S2")
    im2 = con100.load_collection("MASK")
    from openeo.processes import max
    res = im1.merge_cubes(other=im2, overlap_resolver=max)
    assert res.flat_graph() == load_json_resource('data/1.0.0/merge_cubes_max.json')


def test_merge_cubes_max_lambda(con100):
    im1 = con100.load_collection("S2")
    im2 = con100.load_collection("MASK")
    res = im1.merge_cubes(other=im2, overlap_resolver=lambda data: data.max())
    assert res.flat_graph() == load_json_resource('data/1.0.0/merge_cubes_max.json')


def test_getitem_array_element_index(con100):
    im = con100.load_collection("S2")

    def callback(data: ProcessBuilder):
        return data[1] + data[2]

    res = im.reduce_dimension(reducer=callback, dimension="bands")

    assert res.flat_graph() == {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {"id": "S2", "spatial_extent": None, "temporal_extent": None},
        },
        "reducedimension1": {
            "process_id": "reduce_dimension",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "dimension": "bands",
                "reducer": {"process_graph": {
                    "arrayelement1": {
                        "process_id": "array_element",
                        "arguments": {"data": {"from_parameter": "data"}, "index": 1},
                    },
                    "arrayelement2": {
                        "process_id": "array_element",
                        "arguments": {"data": {"from_parameter": "data"}, "index": 2},
                    },
                    "add1": {
                        "process_id": "add",
                        "arguments": {"x": {"from_node": "arrayelement1"}, "y": {"from_node": "arrayelement2"}},
                        "result": True
                    },
                }}
            },
            "result": True
        }
    }


def test_getitem_array_element_label(con100):
    im = con100.load_collection("S2")

    def callback(data: ProcessBuilder):
        return data["red"] + data["green"]

    res = im.reduce_dimension(reducer=callback, dimension="bands")

    assert res.flat_graph() == {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {"id": "S2", "spatial_extent": None, "temporal_extent": None},
        },
        "reducedimension1": {
            "process_id": "reduce_dimension",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "dimension": "bands",
                "reducer": {"process_graph": {
                    "arrayelement1": {
                        "process_id": "array_element",
                        "arguments": {"data": {"from_parameter": "data"}, "label": "red"},
                    },
                    "arrayelement2": {
                        "process_id": "array_element",
                        "arguments": {"data": {"from_parameter": "data"}, "label": "green"},
                    },
                    "add1": {
                        "process_id": "add",
                        "arguments": {"x": {"from_node": "arrayelement1"}, "y": {"from_node": "arrayelement2"}},
                        "result": True
                    },
                }}
            },
            "result": True
        }
    }


def test_load_collection_properties_eq_process(con100):
    from openeo.processes import eq
    cube = con100.load_collection("S2", properties={"provider": lambda v: eq(v, "ESA")})
    expected = load_json_resource('data/1.0.0/load_collection_properties_eq.json')
    assert cube.flat_graph() == expected

    # Hack to swap x and y args in expected result
    args = expected["loadcollection1"]["arguments"]["properties"]["provider"]["process_graph"]["eq1"]["arguments"]
    args["x"], args["y"] = args["y"], args["x"]
    cube = con100.load_collection("S2", properties={"provider": lambda v: eq("ESA", v)})
    assert cube.flat_graph() == expected


def test_load_collection_properties_eq_operator(con100):
    cube = con100.load_collection("S2", properties={"provider": lambda v: v == "ESA"})
    expected = load_json_resource('data/1.0.0/load_collection_properties_eq.json')
    assert cube.flat_graph() == expected

    cube = con100.load_collection("S2", properties={"provider": lambda v: "ESA" == v})
    assert cube.flat_graph() == expected


def test_load_collection_properties_neq_process(con100):
    from openeo.processes import neq
    cube = con100.load_collection("S2", properties={"provider": lambda v: neq(v, "ESA")})
    expected = load_json_resource(
        'data/1.0.0/load_collection_properties_eq.json',
        preprocess=lambda s: s.replace('"eq', '"neq')
    )
    assert cube.flat_graph() == expected

    # Hack to swap x and y args in expected result
    args = expected["loadcollection1"]["arguments"]["properties"]["provider"]["process_graph"]["neq1"]["arguments"]
    args["x"], args["y"] = args["y"], args["x"]
    cube = con100.load_collection("S2", properties={"provider": lambda v: neq("ESA", v)})
    assert cube.flat_graph() == expected


def test_load_collection_properties_neq_operator(con100):
    cube = con100.load_collection("S2", properties={"provider": lambda v: v != "ESA"})
    expected = load_json_resource(
        'data/1.0.0/load_collection_properties_eq.json',
        preprocess=lambda s: s.replace('"eq', '"neq')
    )
    assert cube.flat_graph() == expected

    cube = con100.load_collection("S2", properties={"provider": lambda v: "ESA" != v})
    assert cube.flat_graph() == expected


@pytest.mark.parametrize("reducer", [
    builtins.sum,
    lambda data: builtins.sum(data),
    lambda data: builtins.sum(data) * 3 + 5,
    builtins.all,
    lambda data: not builtins.all(data),
    # TODO also test for `builtin.min`, `builtin.max` (when comparison is supported)
    # TODO also test for `builtins.any` (which, at the moment, doesn't work anyway due to another error)
])
def test_aggregate_temporal_builtin_sum(con100, reducer):
    """
    Using builtin `sum` in callback causes unintended infinite loop
    https://discuss.eodc.eu/t/reducing-masks-in-openeo/113
    """
    cube = con100.load_collection("S2")

    intervals = [["2019-01-01", "2020-01-01"], ["2020-01-02", "2021-01-01"]]
    with pytest.raises(RuntimeError, match="iteration limit"):
        cube.aggregate_temporal(intervals, reducer=reducer)


@pytest.mark.parametrize(["reducer", "expected_arguments"], [
    ("count", {"data": {"from_parameter": "data"}, "context": {"from_parameter": "context"}}),
    (openeo.processes.count, {"data": {"from_parameter": "data"}}),
    (lambda data: data.count(), {"data": {"from_parameter": "data"}}),
    (lambda data: data.count(condition=None), {"data": {"from_parameter": "data"}, "condition": None}),
    (lambda data: data.count(condition=False), {"data": {"from_parameter": "data"}, "condition": False}),
    (lambda data: data.count(condition=True), {"data": {"from_parameter": "data"}, "condition": True}),
    (
            lambda data: openeo.processes.count(data),
            {"data": {"from_parameter": "data"}}
    ),
    (
            lambda data: openeo.processes.count(data, condition=None),
            {"data": {"from_parameter": "data"}, "condition": None},
    ),
    (
            lambda data: openeo.processes.count(data, condition=False),
            {"data": {"from_parameter": "data"}, "condition": False},
    ),
    (
            lambda data: openeo.processes.count(data, condition=True),
            {"data": {"from_parameter": "data"}, "condition": True},
    ),
])
def test_reduce_dimension_count_condition_simple(con100, reducer, expected_arguments):
    """https://github.com/Open-EO/openeo-python-client/issues/317"""
    im = con100.load_collection("S2")
    res = im.reduce_dimension(reducer=reducer, dimension="t")
    assert res.flat_graph()["reducedimension1"]["arguments"]["reducer"]["process_graph"] == {
        "count1": {"process_id": "count", "arguments": expected_arguments, "result": True}
    }


@pytest.mark.parametrize(["reducer", "expected_arguments"], [
    (
            lambda data: data.count(condition=openeo.processes.is_valid),
            {
                "data": {"from_parameter": "data"},
                "condition": {"process_graph": {
                    "isvalid1": {
                        "process_id": "is_valid",
                        "arguments": {"x": {"from_parameter": "x"}},
                        "result": True,
                    }
                }}
            }
    ),
    (
            lambda data: data.count(condition=lambda x: x > 5),
            {
                "data": {"from_parameter": "data"},
                "condition": {"process_graph": {
                    "gt1": {
                        "process_id": "gt",
                        "arguments": {"x": {"from_parameter": "x"}, "y": 5},
                        "result": True,
                    }
                }}
            }
    ),
    (
            lambda data: data.count(condition=lambda x: openeo.processes.gt(x, 5)),
            {
                "data": {"from_parameter": "data"},
                "condition": {"process_graph": {
                    "gt1": {
                        "process_id": "gt",
                        "arguments": {"x": {"from_parameter": "x"}, "y": 5},
                        "result": True,
                    }
                }}
            }
    ),
    (
            functools.partial(openeo.processes.count, condition=lambda x: (x + 1) > 5),
            {
                "data": {"from_parameter": "data"},
                "condition": {"process_graph": {
                    "add1": {"process_id": "add", "arguments": {"x": {"from_parameter": "x"}, "y": 1}},
                    "gt1": {
                        "process_id": "gt",
                        "arguments": {"x": {"from_node": "add1"}, "y": 5},
                        "result": True,
                    }
                }}
            }
    ),
    (
            lambda data: openeo.processes.count(data=data, condition=lambda x: x > 5, context={"foo": "bar"}),
            {
                "data": {"from_parameter": "data"},
                "condition": {"process_graph": {
                    "gt1": {
                        "process_id": "gt",
                        "arguments": {"x": {"from_parameter": "x"}, "y": 5},
                        "result": True,
                    }
                }},
                "context": {"foo": "bar"},
            }
    ),
])
def test_reduce_dimension_count_condition_callback(con100, reducer, expected_arguments):
    """https://github.com/Open-EO/openeo-python-client/issues/317"""
    im = con100.load_collection("S2")
    res = im.reduce_dimension(reducer=reducer, dimension="t")
    assert res.flat_graph()["reducedimension1"]["arguments"]["reducer"]["process_graph"] == {
        "count1": {"process_id": "count", "arguments": expected_arguments, "result": True}
    }
