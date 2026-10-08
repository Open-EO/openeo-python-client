.. _api-openeo-extra-artifacts:

====================================
API: openeo.extra.artifacts
====================================
.. versionadded:: 0.45.0

.. warning::
    This is a new experimental API, subject to change.


.. important::
    The artifacts functionality relies on extra Python packages. They can be installed using:

    .. code-block:: shell

     pip install "openeo[artifacts]" --upgrade


When running openEO jobs it is not uncommon to require artifacts that should be accessible during job execution. This
requires the artifacts to be accessible from within the openEO processing environment. :py:mod:`openeo.extra.artifacts` tries
to perform the heavy lifting for this use case by allowing staging artifacts to a secure but temporary location using 3
simple steps:

1. Connect to your openEO backend
2. Create an artifact helper from your openEO connection
3. Upload your file using the artifact helper and optionally get a presigned URI

So in code this looks like:

.. code-block:: python

    import openeo
    from openeo.extra.artifacts import build_artifact_helper

    connection = openeo.connect("my-openeo.prod.example").authenticate_oidc()

    artifact_helper = build_artifact_helper(connection)
    storage_uri = artifact_helper.upload_file(path, object_name)
    presigned_uri = artifact_helper.get_presigned_url(storage_uri)

Note:

* The presigned_uri should be used for accessing the objects. It has authentication details embedded so if your data is
  sensitive you must make sure to keep this URL secret. You can lower expires_in_seconds in
  :py:meth:`openeo.extra.artifacts._artifact_helper_abc.ArtifactHelperABC.get_presigned_url`
  to limit the time window in which the URI can be used.

* The openEO backend must expose additional metadata in its capabilities doc to make this possible. Implementers of a
  backend can check the extra documentation :ref:`for-backend-providers`.


User facing API
===============


.. autofunction:: openeo.extra.artifacts.build_artifact_helper


.. autoclass:: openeo.extra.artifacts._artifact_helper_abc.ArtifactHelperABC
    :members: upload_file, get_presigned_url
    :no-index:


How does it work ?
==================

1) :py:meth:`openeo.extra.artifacts.build_artifact_helper` is a factory method that
   will create an artifact helper where the type is defined by the config type. The openEO connection object is used to
   see if the openEO backend advertises a preferred config.
2) :py:meth:`openeo.extra.artifacts._artifact_helper_abc.ArtifactHelperABC.upload_file` and
   :py:meth:`openeo.extra.artifacts._artifact_helper_abc.ArtifactHelperABC.get_presigned_url` do the heavy lifting to
   store your artifact in provider managed storage and to return references that can be used. In case the backend uses
   an Object storage that has an S3 API it will:

   1. Get temporary S3 credentials based on config advertised by the backend and the session from your connection
   2. Upload the file into object storage and return an S3 URI which the backend can resolve
   3. Optional the :py:meth:`openeo.extra.artifacts._artifact_helper_abc.ArtifactHelperABC.get_presigned_url` makes a
      URI signed with the temporary credentials such that it works standalone (Some tools and execution steps do not
      support handling of internal references. presigned URLs should work in any tool).


.. _for-backend-providers:

For backend providers
=====================

.. warning::
    Investigation is ongoing whether this would be a good fit for the workspace extension
    https://github.com/Open-EO/openeo-api/issues/566 which would mean a big overhaul for backend implementations. If you
    are a backend provider and interested in this feature please create an issue to allow collaboration.



Artifacts exceptions
--------------------

When using artifacts your interactions can result in the following exceptions.

.. autoexception:: openeo.extra.artifacts.exceptions.ArtifactsException
    :members:

.. autoexception:: openeo.extra.artifacts.exceptions.NoAdvertisedProviders
    :members:

.. autoexception:: openeo.extra.artifacts.exceptions.UnsupportedArtifactsType
    :members:

.. autoexception:: openeo.extra.artifacts.exceptions.NoDefaultConfig
    :members:

.. autoexception:: openeo.extra.artifacts.exceptions.InvalidProviderConfig
    :members:

.. autoexception:: openeo.extra.artifacts.exceptions.ProviderSpecificException
    :members:
