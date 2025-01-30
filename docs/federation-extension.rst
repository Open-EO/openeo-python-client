
.. _federation-extension:

===========================
openEO Federation Extension
===========================


The `openEO Federation extension <https://github.com/Open-EO/openeo-api/tree/master/extensions/federation>`_
is a set of additional specifications,
on top of the standard openEO API specification,
to address the need for extra metadata in the context
of federated openEO processing,
where multiple (separately operated) openEO services are bundled together
behind a single API endpoint.


Accessing federation extension metadata
========================================

The openEO Python client library provides access to this additional metadata
in a couple of resources.

.. versionadded:: 0.38.0
    initial support to access federation extension related metadata.

Backend details
---------------

Participating backends in a federation are listed under the ``federation`` field
of the capabilities document (``GET /``) and can be inspected
using :py:meth:`OpenEoCapabilities.get_federation() <openeo.rest.capabilities.OpenEoCapabilities.get_federation>`:

.. code-block:: python

    import openeo
    connection = openeo.connect(url=...)
    capabilities = connection.capabilities()
    print("Federated backends:", capabilities.get_federation())


Unavailable backends (``federation:missing``)
----------------------------------------------

When listing resources like
collections (with :py:meth:`Connection.list_collections() <openeo.rest.connection.Connection.list_collections>`),
processes (with :py:meth:`Connection.list_processes() <openeo.rest.connection.Connection.list_processes>`),
jobs (with :py:meth:`Connection.list_jobs() <openeo.rest.connection.Connection.list_jobs>`),
etc.,
there might be items missing due to federation participants being temporarily unavailable.
These missing federation components are listed in the response under the ``federation:missing`` field
and can be inspected as follows:

.. code-block:: python

    import openeo
    connection = openeo.connect(url=...)
    collections = connection.list_collections()
    print("Number of collections:", len(collections))
    print("Missing federation components:", collections.ext_federation.missing)


Note that the ``collections`` object in this example, returned by
:py:meth:`Connection.list_collections() <openeo.rest.connection.Connection.list_collections>`,
acts at the surface as a simple list of dictionaries with collection metadata,
but also provides additional properties/methods like
:py:attr:`ext_federation <openeo.rest.models.general.CollectionListingResponse.ext_federation>`.
This is an accessor (an instance of :py:class:`FederationExtension <openeo.rest.models.federation_extension.FederationExtension>`)
