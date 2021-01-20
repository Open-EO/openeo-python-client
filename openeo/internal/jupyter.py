from string import Template
from collections import Mapping
import json

SCRIPT_URL = 'https://cdn.jsdelivr.net/npm/@openeo/vue-components@2.0.0-beta.1/assets/openeo.js'

# This is a Proxy to visualize openEO responses nicely in Jupyter
# To show the actual dict in Jupyter, use repr() or print()
class VisualDict(dict):

	# Class instance to store whether the script has been inserted into the DOM
	# TODO: This fails if a page is reloaded in Jupyter. The variable is still True, but the script doesn't exist in the page any longer.
	script_sent = False

	# The first entry of the dict is expected to always be the actual dict with the data
	def __init__(self, component: str, parameters: dict = {}):
		data = {}
		if len(parameters) > 0:
			i = next(iter(parameters))
			if isinstance(parameters[i], Mapping):
				data = parameters[i]
		super().__init__(data)
		self.component = component
		self.parameters = parameters

	def _repr_html_(self):
		# Output script tag only once, otherwise JS throws errors
		script = ''
		if VisualDict.script_sent == False:
			script = '<script src="{}"></script>'.format(SCRIPT_URL)
			VisualDict.script_sent = True

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