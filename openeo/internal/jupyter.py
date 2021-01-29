import json

SCRIPT_URL = 'https://cdn.jsdelivr.net/npm/@openeo/vue-components@2.0.0-rc.2/assets/openeo.min.js'
COMPONENT_MAP = {
    'file-format': 'format',
    'file-formats': 'formats',
    'service-type': 'service',
    'service-types': 'services',
    'udf-runtime': 'runtime',
    'udf-runtimes': 'runtimes',
}

def render_component(component: str, data = None, parameters: dict = {}):
    # Set the data as the corresponding parameter in the Vue components
    key = COMPONENT_MAP.get(component, component)
    if data != None:
        parameters[key] = data

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
        component = component,
        props = json.dumps(parameters)
    )

# These classes are proxies to visualize openEO responses nicely in Jupyter
# To show the actual list or dict in Jupyter, use repr() or print()

class VisualDict(dict):

    def __init__(self, component: str, data : dict, parameters: dict = {}):
        dict.__init__(self, data)

        self.component = component
        self.parameters = parameters

    def _repr_html_(self):
        return render_component(self.component, self, self.parameters)


class VisualList(list):

    def __init__(self, component: str, data : list, parameters: dict = {}):
        list.__init__(self, data)

        self.component = component
        self.parameters = parameters

    def _repr_html_(self):
        return render_component(self.component, self, self.parameters)