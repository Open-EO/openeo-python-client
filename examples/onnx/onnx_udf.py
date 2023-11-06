import sys
from openeo.udf import XarrayDataCube
from typing import Dict
import xarray as xr
from openeo.udf.debug import inspect


def apply_datacube(cube: XarrayDataCube, context: Dict) -> XarrayDataCube:
    sys.path.insert(0, "tmp/extra_venv")
    import onnxruntime as ort

    input_data = cube.get_array().isel(t=0).values  # Only perform inference for the first date.
    input_data = input_data[None, ...]  # Neural network expects shape (1, 1, 256, 256)
    inspect(input_data, "input data")
    ort_session = ort.InferenceSession("tmp/extra_files/test_model.onnx")
    ort_inputs = {ort_session.get_inputs()[0].name: input_data}
    ort_outputs = ort_session.run(None, ort_inputs)
    output_data = xr.DataArray(ort_outputs[0])
    output_data = output_data.rename({"dim_0": "t", "dim_1": "bands", "dim_2": "y", "dim_3": "x"})
    inspect(output_data, "output data")

    return XarrayDataCube(output_data)
