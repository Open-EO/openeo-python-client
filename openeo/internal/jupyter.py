from string import Template
from collections import Mapping
import json

# These classes are proxies to visualize openEO responses nicely in Jupyter
# To show the actual list or dict in Jupyter, use repr() or print()

SCRIPT_URL = 'https://cdn.jsdelivr.net/npm/@openeo/vue-components@2.0.0-beta.1/assets/openeo.js'

class JupyterIntegration():

	# Class instance to store whether the script has been inserted into the DOM
	# TODO: This fails if a page is reloaded in Jupyter. The variable is still True, but the script doesn't exist in the page any longer.
	script_sent = False

	def __init__(self, component: str, parameters: dict = {}):
		self.component = component
		self.parameters = parameters

	def _repr_html_(self):
		# Output script tag only once, otherwise JS throws errors
		script = ''
		if JupyterIntegration.script_sent == False:
			script = '<script src="{}"></script>'.format(SCRIPT_URL)
			JupyterIntegration.script_sent = True

		# Construct HTML
		template = Template("""
		$script
		<openeo-$component>
			<script type="application/json">$props</script>
		</openeo-$component>
		""")
		return template.substitute(
			script = script,
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
	   			raise ValueError("First parameter must be a dict")
		dict.__init__(self, data)


class VisualList(list, JupyterIntegration):

	# The first entry of the dict is expected to always be the actual dict with the data
	def __init__(self, component: str, parameters: dict = {}):
		JupyterIntegration.__init__(self, component, parameters)

		data = []
		if len(parameters) > 0:
			i = next(iter(parameters))
			if isinstance(parameters[i], list):
				data = parameters[i]
			else:
	   			raise ValueError("First parameter must be a List[dict]")
		list.__init__(self, data)