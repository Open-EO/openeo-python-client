from openeo.rest.stac_resource import StacResource


class SaveResult(StacResource):
    """
    Handle for a process graph that represents the return value
    of the openEO process ``save_result``,
    as returned by methods like
    :py:meth:`DataCube.save_result() <openeo.rest.datacube.DataCube.save_result>`
    and :py:meth:`VectorCube.save_result() <openeo.rest.vectorcube.VectorCube.save_result>`.

    .. note ::
        This class is practically just a direct alias for
        :py:class:`~openeo.rest.stac_resource.StacResource`,
        but with a more self-explanatory name.

        Moreover, this additional abstraction layer also acts somewhat as an adapter between
        the incompatible return values from the ``save_result`` process
        in different versions of the official openeo-processes definitions:

        - in openeo-processes 1.x: ``save_result`` just returned a boolean,
          but that was not really useful to further build upon
          and was never properly exposed in the openEO Python client.
        - in openeo-processes 2.x: ``save_result`` returns a new concept:
          a "STAC resource" (object with subtype "stac")
          which is a more useful and flexible representation of an openEO result,
          allowing additional operations.

        The openEO Python client returns the same :py:class:`SaveResult` object
        in both cases however.
        It does that not only for simplicity,
        but also because it seems more useful (even in legacy openeo-processes 1.x use cases)
        to follow the new STAC resource based usage patterns
        than to strictly return some boolean wrapper nobody has use for.

    .. versionadded:: 0.39.0
    """
