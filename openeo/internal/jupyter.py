from collections import Mapping
import json

# These classes are proxies to visualize openEO responses nicely in Jupyter
# To show the actual list or dict in Jupyter, use repr() or print()

SCRIPT_URL = 'https://cdn.jsdelivr.net/npm/@openeo/vue-components@2.0.0-beta.1/assets/openeo.js'

class JupyterIntegration:

    def __init__(self, component: str, parameters: dict = {}):
        self.component = component
        self.parameters = parameters

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
    def __init__(self, component: str, parameters: dict = {}):
        JupyterIntegration.__init__(self, component, parameters)

        data = {}
        if len(parameters) > 0:
            i = next(iter(parameters))
            if isinstance(parameters[i], Mapping):
                data = parameters[i]
            else:
                   raise ValueError("First value in the dict 'parameters' must be of type dict or Mapping")
        dict.__init__(self, data)


class VisualList(list, JupyterIntegration):

    # The first entry of the dict is expected to always be the actual list with the data
    def __init__(self, component: str, parameters: dict = {}):
        JupyterIntegration.__init__(self, component, parameters)

        data = []
        if len(parameters) > 0:
            i = next(iter(parameters))
            if isinstance(parameters[i], list):
                data = parameters[i]
            else:
                   raise ValueError("First value in the dict 'parameters' must be of type list")
        list.__init__(self, data)