import openeo

dependencies_url = "https://artifactory.vgt.vito.be:443/auxdata-public/openeo/onnx_dependencies.zip"
model_url = "https://artifactory.vgt.vito.be:443/auxdata-public/openeo/test_onnx_model.zip"
spatial_extent = dict(zip(["west", "south", "east", "north"], [8.908, 53.791, 8.96, 54.016]))
temporal_extent = ["2022-10-01", "2022-12-01"]

connection = openeo.connect("openeo.cloud").authenticate_oidc()

s2_cube = connection.load_collection(
    "TERRASCOPE_S2_TOC_V2",
    temporal_extent=temporal_extent,
    spatial_extent=spatial_extent,
    bands=["B04"],
    max_cloud_cover=20,
)

udf = openeo.UDF.from_file("onnx_udf.py")
s2_cube = s2_cube.apply_neighborhood(
    udf,
    size=[{"dimension": "x", "value": 256, "unit": "px"}, {"dimension": "y", "value": 256, "unit": "px"}],
    overlap=[{"dimension": "x", "value": 0, "unit": "px"}, {"dimension": "y", "value": 0, "unit": "px"}],
)

job_options = {
    "udf-dependency-archives": [
        f"{dependencies_url}#tmp/extra_venv",
        f"{model_url}#tmp/extra_files",
    ],
}
s2_cube.execute_batch("output.nc", job_options=job_options)
