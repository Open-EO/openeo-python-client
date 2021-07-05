
Dataset sampling
----------------

EXPERIMENTAL

Tested on:

- Terrascope

A number of use cases do not require a full datacube to be computed,
but rather want to extract a result at specific locations.
Examples include extracting training data for model calibration, or computing the result for
areas where validation data is available.

Sampling a datacube in openEO currently requires polygons as sampling features. Other types of geometry, like lines
and points, should be converted into polygons first by applying a buffering operation. Using the spatial resolution
of the datacube as buffer size can be a way to approximate sampling at a point.

To indicate to openEO that we only want to compute the datacube for certain polygon features, we use the
:func:`~openeo.rest.datacube.DataCube.filter_spatial` method.

Next to that, we will also indicate that we want to write multiple output files. This is more convenient, as we will
want to have one or more raster outputs per sampling feature, for convenient further processing. To do this, we set
the 'sample_by_feature' output format property, which is available for the netCDF and GTiff output formats.

Combining all of this, results in the following sample code::

    s2_bands = auth_connection.load_collection("TERRASCOPE_S2_TOC_V2",
                                               bands=["B04"],
                                               temporal_extent=["2020-05-01","2020-06-01"]
                                               )
    s2_bands = s2_bands.filter_spatial("https://artifactory.vgt.vito.be/testdata-public/parcels/test_10.geojson")
    job = s2_bands.send_job(title="Sentinel2", description="Sentinel-2 L2A bands",out_format="netCDF",sample_by_feature=True)

Sampling only works for batch jobs, because it results in multiple output files, which can not be conveniently transferred
in a synchronous call.

