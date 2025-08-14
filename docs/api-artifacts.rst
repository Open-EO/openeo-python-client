.. _api-openeo-extra-artifacts:

====================================
API: openeo.extra.artifacts
====================================

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
  backend can check the extra documentation :ref:`advertising-capabilities`.


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


Documentation for backend providers
===================================

This section and its subsection is for engineers who operate an openEO backend. If you are a user of an openEO platform
this is unlikely to be of value to you.

.. _advertising-capabilities:

Advertising capabilities from the backend
-----------------------------------------

It is expected that the backend advertises in its capabilities a section on artifacts. The following is an example
for the S3STSConfig (of the :py:mod:`openeo.extra.artifacts._s3sts` package).

.. code-block:: json

    {
       // ...
       "artifacts": {
         "providers": [
           {
             // This id is a logical name
             "id": "s3",
             // The config type of the ArtifactHelper
             "type": "S3STSConfig"
             // The config block its keys can differ for other config types
             "config": {
               // The bucket where the artifacts will be stored
               "bucket": "openeo-artifacts",
               // The role that will be assumed via STS
               "role": "arn:aws:iam::000000000000:role/S3Access",
               // Where S3 API calls are sent
               "s3_endpoint": "https://my.s3.test",
              // Where STS API calls are sent
               "sts_endpoint": "https://my.sts.test"
             },
           }
         ]
       },
       // ...
    }


Extending support for other types of artifacts
----------------------------------------------

.. warning::
    This is a section for developers of the `openeo-python-client` Python package. If you want to walk this road it is
    best to create an issue on github and detail what support you are planning to add to get input on feasibility and
    whether it will be mergeable early on.

Ideally the user-interface is simple and stable. Unfortunately implementations themselves come with more complexity.
This section explains what is needed to provide support for additional types of artifacts. Below the steps we show
the API that is involved.

1. Create another internal package for the implementation. The following steps should be done inside that package.
   This package resides under :py:mod:`openeo.extra.artifacts`
2. Create a config implementation which extends :py:class:`openeo.extra.artifacts._config.ArtifactsStorageConfigABC`
   and should be a frozen dataclass. This class implements the logic to determine the configuration used by the
   implementation `_load_connection_provided_config(self, provider_config: ProviderConfig) -> None` is used for that.

   When this method is called explicit config is already put in place and if not provided default config is put in
   place.
   Because frozen dataclasses are used for config `object.__setattr__(self, ...)` must be used to manipulate the
   values.

   So per attribute the same pattern is used. For example an attribute `foo` which has a default `bar` that can be kept
   constant would be:

    .. code-block:: python

        if self.foo is None:
            try:
                object.__setattr__(self, "foo", provider_config["foo"])
            except NoDefaultConfig:
                object.__setattr__(self, "foo", "bar")

   Here we use :py:exc:`openeo.extra.artifacts.exceptions.NoDefaultConfig`

3. Create an implementation of :py:class:`openeo.extra.artifacts._uri.StorageURI` to model the internal URIs to the
   stored artifact
4. Create an ArtifactHelper implementation which extends :py:class:`openeo.extra.artifacts._artifact_helper_abc.ArtifactHelperABC`
5. Add a key value pair to the :py:obj:`openeo.extra.artifacts.artifact_helper.config_to_helper` dictionary. The key is
   the class created in 2 and the value is the class created in step 3

.. autoclass:: openeo.extra.artifacts._config.ArtifactsStorageConfigABC
    :members:
    :private-members: _load_connection_provided_config

.. autoclass:: openeo.extra.artifacts._artifact_helper_abc.ArtifactHelperABC
    :members:
    :private-members: _get_default_storage_config, _from_openeo_connection

.. autoclass:: openeo.extra.artifacts._uri.StorageURI
    :members:


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
