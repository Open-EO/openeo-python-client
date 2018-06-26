# -*- coding: utf-8 -*-

from graphviz import Digraph

def test_graph():
    graph = {'process_id': 'zonal_statistics',
             'args': {
                 'imagery': {'process_id': 'filter_bands',
                             'args': {
                                 'imagery':
                                 {'process_id': 'filter_bbox',
                                  'args': {
                                      'imagery': {'process_id': 'filter_daterange',
                                                  'args': {
                                                      'imagery': {'product_id': 'COPERNICUS/S2'},
                                                      'from': '2018-01-01',
                                                      'to': '2018-01-31'}},
                                      'left': 16.138916,
                                      'right': 16.524124,
                                      'top': 48.320647,
                                      'bottom': 48.1386,
                                      'srs': 'EPSG:4326'}},
                                 'bands': 'B8'}},
                 'regions': 'polygon.json',
                 'func': 'mean',
                 'scale': 1000,
                 'interval': 'day'}}
    # for part in iter_nodes(graph):
        # print(part)
    dot = Digraph(comment='graph')
    with dot.subgraph() as c:
        c.attr('node', shape='box', fillcolor='red:yellow', style='filled', gradientangle='90')
        build_graph(graph, c)
    import pdb; pdb.set_trace()
    dot.render('test_graph.gv', view=True)

def build_graph(graph, dot, prev_node=None):

    prev_node = None
    for node_id, arguments in iter_nodes(graph):
        if arguments is None:
            dot.attr('node', shape='ellipse')
            dot.node(node_id, '{}'.format(node_id))
        else:
            dot.attr('node', shape='box')
            argument_str = '<br />'.join(['{}: {}'.format(key, arguments[key]) for key in arguments])
            argument_str = '<FONT POINT-SIZE="10">{}</FONT>'.format(argument_str)
            dot.node(node_id, label='<{}<br />{}>'.format(node_id, argument_str))

        if prev_node is not None:
            dot.edge(prev_node, node_id)

        prev_node = node_id

def iter_nodes(graph):
    graph = dict(graph)
    if 'process_id' in graph:
        process_id = graph['process_id']
        node_id = process_id

    if 'product_id' in graph:
        product_id = graph['product_id']
        node_id = product_id

    orig_graph = dict(graph)
    arguments = graph.pop('args', None)
    if arguments is not None:
        imagery = arguments.pop('imagery', None)
        if imagery is not None:
            for node, arg in iter_nodes(imagery):
                yield node, arg
            yield node_id, arguments

    else:
        yield node_id, arguments
