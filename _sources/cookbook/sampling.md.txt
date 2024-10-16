
# Dataset sampling


A number of use cases do not require a full datacube to be computed,
but rather want to extract a result at specific locations.
Examples include extracting training data for model calibration, or computing the result for
areas where validation data is available.

An important constraint is that most implementations assume that sampling is an operation 
on relatively small areas, of for instance up to 512x512 pixels (but often much smaller). 
When extracting larger areas, it is recommended to look into running a separate job per 'sample'.

Sampling can be done for points or polygons:

- point extractions basically result in a 'vector cube', so can be exported into tabular formats.
- polygon extractions  can be stored to an individual netCDF per polygon so in this case the output is a sparse raster cube.

To indicate to openEO that we only want to compute the datacube for certain polygon features, we use the
`openeo.rest.datacube.DataCube.filter_spatial` method.

Next to that, we will also indicate that we want to write multiple output files. This is more convenient, as we will
want to have one or more raster outputs per sampling feature, for convenient further processing. To do this, we set
the 'sample_by_feature' output format property, which is available for the netCDF and GTiff output formats.

Combining all of this, results in the following sample code:

```python
s2_bands = auth_connection.load_collection(
    "SENTINEL2_L2A",
    bands=["B04"],
    temporal_extent=["2020-05-01", "2020-06-01"],
)
s2_bands = s2_bands.filter_spatial(
    "https://artifactory.vgt.vito.be/testdata-public/parcels/test_10.geojson",
)
job = s2_bands.create_job(
    title="Sentinel2",
    description="Sentinel-2 L2A bands",
    out_format="netCDF",
    sample_by_feature=True,
)
```


Sampling only works for batch jobs, because it results in multiple output files, which can not be conveniently transferred
in a synchronous call.

## Performance & scalability

It's important to note that dataset sampling is not necessarily a cheap operation, since creation of a sparse datacube still
may require accessing a large number of raw EO assets. Backends of course can and should optimize to restrict processing
to a minimum, but the size of the required input datasets is often a determining factor for cost and performance rather
than the size of the output dataset.

## Sampling at scale

When doing large scale (e.g. continental) sampling, it is usually not possible or impractical to run it as a single openEO
batch job. The recommendation here is to apply a spatial grouping to your sampling locations, with a single group covering
an area of around 100x100km. The optimal size of a group may be backend dependant. Also remember that when working with
data in the UTM projection, you may want to avoid covering multiple UTM zones in a single group.
