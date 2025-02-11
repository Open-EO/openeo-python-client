from openeo.rest.stac_resource import StacResource


class SaveResult(StacResource):
    """
    Alias for :py:class:`~openeo.rest.stac_resource.StacResource`,
    returned by methods corresponding to the openEO process ``save_result``, like
    :py:meth:`DataCube.save_result() <openeo.rest.datacube.DataCube.save_result>`
    and :py:meth:`VectorCube.save_result() <openeo.rest.vectorcube.VectorCube.save_result>`

    .. versionadded:: 0.39.0
    """
