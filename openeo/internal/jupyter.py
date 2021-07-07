import json

from openeo.rest import OpenEoApiError

SCRIPT_URL = 'https://cdn.jsdelivr.net/npm/@openeo/vue-components@2/assets/openeo.min.js'
COMPONENT_MAP = {
    'collection': 'data',
    'data-table': 'data',
    'file-format': 'format',
    'file-formats': 'formats',
    'item': 'data',
    'job-estimate': 'estimate',
    'service-type': 'service',
    'service-types': 'services',
    'udf-runtime': 'runtime',
    'udf-runtimes': 'runtimes',
}

TABLE_COLUMNS = {
    'jobs': {
        'id': {
            'name': 'ID',
            'primaryKey': True
        },
        'title': {
            'name': 'Title'
        },
        'status': {
            'name': 'Status',
#           'stylable': True
        },
        'created': {
            'name': 'Submitted',
            'format': 'Timestamp',
            'sort': 'desc'
        },
        'updated': {
            'name': 'Last update',
            'format': 'Timestamp'
        }
    },
    'services': {
        'id': {
            'name': 'ID',
            'primaryKey': True
        },
        'title': {
            'name': 'Title'
        },
        'type': {
            'name': 'Type',
#           'format': value => typeof value === 'string' ? value.toUpperCase() : value,
        },
        'enabled': {
            'name': 'Enabled'
        },
        'created': {
            'name': 'Submitted',
            'format': 'Timestamp',
            'sort': 'desc'
        }
    },
    'files': {
        'path': {
            'name': 'Path',
            'primaryKey': True,
#           'sortFn': Utils.sortByPath,
            'sort': 'asc'
        },
        'size': {
            'name': 'Size',
            'format': "FileSize",
            'filterable': False
        },
        'modified': {
            'name': 'Last modified',
            'format': 'Timestamp'
        }
    }
}


def render_component(component: str, data = None, parameters: dict = None):
    parameters = parameters or {}
    # Special handling for batch job results, show either item or collection depending on the data
    if component == "batch-job-result":
        component = "item" if data["type"] == "Feature" else "collection"
    elif component == "data-table":
        parameters['columns'] = TABLE_COLUMNS[parameters['columns']]

    # Set the data as the corresponding parameter in the Vue components
    key = COMPONENT_MAP.get(component, component)
    if data is not None:
        parameters[key] = data

    # Construct HTML, load Vue Components source files only if the openEO HTML tag is not yet defined
    return """
    <script>
    if (!window.customElements || !window.customElements.get('openeo-{component}')) {{
        var el = document.createElement('script');
        el.src = "{script}";
        document.head.appendChild(el);

        var font = document.createElement('font');
        font.as = "font";
        font.type = "font/woff2";
        font.crossOrigin = true;
        font.href = "https://use.fontawesome.com/releases/v5.13.0/webfonts/fa-solid-900.woff2"
        document.head.appendChild(font);
    }}
    </script>
    <openeo-{component}>
        <script type="application/json">{props}</script>
    </openeo-{component}>
    """.format(
        script=SCRIPT_URL,
        component=component,
        props=json.dumps(parameters)
    )


def render_error(error: OpenEoApiError):
    # ToDo: Once we have a dedicated log/error component, use that instead of description
    output = """## Error `{code}`\n\n{message}""".format(
        code=error.code,
        message=error.message
    )
    return render_component('description', data=output)


# These classes are proxies to visualize openEO responses nicely in Jupyter
# To show the actual list or dict in Jupyter, use repr() or print()

class VisualDict(dict):

    def __init__(self, component: str, data: dict, parameters: dict = None):
        dict.__init__(self, data)
        self.component = component
        self.parameters = parameters or {}

    def _repr_html_(self):
        return render_component(self.component, self, self.parameters)


class VisualList(list):

    def __init__(self, component: str, data: list, parameters: dict = None):
        list.__init__(self, data)
        self.component = component
        self.parameters = parameters or {}

    def _repr_html_(self):
        return render_component(self.component, self, self.parameters)
