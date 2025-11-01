import json
import os

from openeo.rest import OpenEoApiError

SCRIPT_URL = "https://cdn.jsdelivr.net/npm/@openeo/vue-components@2/assets/openeo.min.js"
COMPONENT_MAP = {
    "collection": "data",
    "data-table": "data",
    "file-format": "format",
    "file-formats": "formats",
    "item": "data",
    "job-estimate": "estimate",
    "model-builder": "value",
    "service-type": "service",
    "service-types": "services",
    "udf-runtime": "runtime",
    "udf-runtimes": "runtimes",
}

TABLE_COLUMNS = {
    "jobs": {
        "id": {
            "name": "ID",
            "primaryKey": True,
        },
        "title": {
            "name": "Title",
        },
        "status": {
            "name": "Status",
            #           'stylable': True
        },
        "created": {
            "name": "Submitted",
            "format": "Timestamp",
            "sort": "desc",
        },
        "updated": {
            "name": "Last update",
            "format": "Timestamp",
        },
    },
    "services": {
        "id": {
            "name": "ID",
            "primaryKey": True,
        },
        "title": {
            "name": "Title",
        },
        "type": {
            "name": "Type",
            #           'format': value => typeof value === 'string' ? value.toUpperCase() : value,
        },
        "enabled": {
            "name": "Enabled",
        },
        "created": {
            "name": "Submitted",
            "format": "Timestamp",
            "sort": "desc",
        },
    },
    "files": {
        "path": {
            "name": "Path",
            "primaryKey": True,
            #           'sortFn': Utils.sortByPath,
            "sort": "asc",
        },
        "size": {
            "name": "Size",
            "format": "FileSize",
            "filterable": False,
        },
        "modified": {
            "name": "Last modified",
            "format": "Timestamp",
        },
    },
}


def in_jupyter_context() -> bool:
    """Check if we are running in an interactive Jupyter notebook context."""
    try:
        from ipykernel.zmqshell import ZMQInteractiveShell
        from IPython.core.getipython import get_ipython
    except ImportError:
        return False
    return isinstance(get_ipython(), ZMQInteractiveShell)


def render_component(component: str, data=None, parameters: dict = None):
    parameters = parameters or {}
    # Special handling for batch job results, show either item or collection depending on the data
    if component == "batch-job-result":
        component = "item" if data["type"] == "Feature" else "collection"

    if component == "data-table":
        parameters["columns"] = TABLE_COLUMNS[parameters["columns"]]
    elif component in ["collection", "collections", "item", "items"]:
        url = os.environ.get("OPENEO_BASEMAP_URL")
        attribution = os.environ.get("OPENEO_BASEMAP_ATTRIBUTION")
        parameters["mapOptions"] = {}
        if url:
            parameters["mapOptions"]["basemap"] = url
        if attribution:
            parameters["mapOptions"]["attribution"] = attribution

    # Set the data as the corresponding parameter in the Vue components
    key = COMPONENT_MAP.get(component, component)
    if data is not None:
        if isinstance(data, list):
            # TODO: make this `to_dict` usage more explicit with an internal API?
            data = [(x.to_dict() if hasattr(x, "to_dict") else x) for x in data]
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
        script=SCRIPT_URL, component=component, props=json.dumps(parameters)
    )


def render_error(error: OpenEoApiError):
    # ToDo: Once we have a dedicated log/error component, use that instead of description
    output = """## Error `{code}`\n\n{message}""".format(code=error.code, message=error.message)
    return render_component("description", data=output)


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
