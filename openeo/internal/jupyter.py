from collections import Mapping
import json

# These classes are proxies to visualize openEO responses nicely in Jupyter
# To show the actual list or dict in Jupyter, use repr() or print()

SCRIPT_URL = 'https://cdn.jsdelivr.net/npm/@openeo/vue-components@2.0.0-beta.1/assets/openeo.js'
COMPONENT_MAP = {
    'file-format': 'format',
    'file-formats': 'formats',
    'service-type': 'service',
    'service-types': 'services',
    'udf-runtime': 'runtime',
    'udf-runtimes': 'runtimes',
}

class JupyterIntegration:

    def __init__(self, component: str, data = None, parameters: dict = {}):
        self.component = component
        self.parameters = parameters

        # Set the data as the corresponding parameter in the Vue components
        key = COMPONENT_MAP.get(component, component)
        if data != None:
            self.parameters[key] = data

    def _repr_html_(self):
        # Construct HTML, load Vue Components source files only if the openEO HTML tag is not yet defined
        return """
        <script>
        if (!window.customElements || !window.customElements.get('openeo-{component}')) {{
            var el = document.createElement('script');
            el.src = "{script}";
            document.head.appendChild(el);
        }}
        </script>
        <openeo-{component}>
            <script type="application/json">{props}</script>
        </openeo-{component}>
        """.format(
            script = SCRIPT_URL,
            component = self.component,
            props = json.dumps(self.parameters)
        )


class VisualDict(dict, JupyterIntegration):

    # The first entry of the dict is expected to always be the actual dict with the data
    def __init__(self, component: str, data : dict, parameters: dict = {}):
        JupyterIntegration.__init__(self, component, data, parameters)
        dict.__init__(self, data)


class VisualList(list, JupyterIntegration):

    # The first entry of the dict is expected to always be the actual list with the data
    def __init__(self, component: str, data : list, parameters: dict = {}):
        JupyterIntegration.__init__(self, component, data, parameters)
        list.__init__(self, data)