
===============
Configuration
===============

.. warning::
    Configuration files are an experimental feature
    and some details are subject to change.

.. versionadded:: 0.10.0


.. _configuration_files:

Configuration files
====================

Some functionality of the openEO Python client library can customized
through configuration files.


.. note::
    Note that these configuration files are different from the authentication secret/cache files
    discussed at :ref:`auth_configuration_files`.
    The latter are focussed on storing authentication secrets
    and are mostly managed automatically.
    The normal configuration files however should not contain secrets,
    are usually edited manually, can be placed at various locations
    and it is not uncommon to store them in version control where that makes sense.


Format
-------

At the moment, only INI-style configs are supported.
This is a simple configuration format, easy to maintain
and it is supported out of the box in Python (without additional libraries).

Example (note the use of sections and support for comments)::

    [General]
    # Print loaded configuration file and default back-end URLs in interactive mode
    verbose = auto

    [Connection]
    default_backend = openeo.cloud


.. _configuration_file_locations:

Location
---------

The following configuration locations are probed (in this order) for an existing configuration file. The first successful hit will be loaded:

- the path in environment variable ``OPENEO_CLIENT_CONFIG`` if it is set (filename must end with extension ``.ini``)
- the file ``openeo-client-config.ini`` in the current working directory
- the file ``${OPENEO_CONFIG_HOME}/openeo-client-config.ini`` if the environment variable ``OPENEO_CONFIG_HOME`` is set
- the file ``${XDG_CONFIG_HOME}/openeo-python-client/openeo-client-config.ini`` if environment variable ``XDG_CONFIG_HOME`` is set
- the file ``.openeo-client-config.ini`` in the home folder of the user


Configuration options
----------------------

.. list-table::
   :widths: 10 10 40
   :header-rows: 1

   * - Config Section
     - Config
     - Description and possible values
   * - ``General``
     - ``verbose``
     - Verbosity mode when important config values are used:
        + ``print``: always ``print()`` info
        + ``auto`` (default): only ``print()`` when in an interactive context
        + ``off``: don't print info
   * - ``Connection``
     - ``default_backend``
     - Default back-end to connect to when :py:func:`openeo.connect()`
       is used without explicit back-end URL.
       Also see :ref:`default_url_and_auto_auth`
   * - ``Connection``
     - ``default_backend.auto_authenticate``
     - Automatically authenticate in :py:func:`openeo.connect()` when  using the ``default_backend`` config. Allowed values:
        + ``basic`` for basic authentication
        + ``oidc`` for OpenID Connect authentication
        + ``off`` (default) for no authentication

       Also see :ref:`default_url_and_auto_auth`
   * - ``Connection``
     - ``auto_authenticate``
     - Automatically authenticate in :py:func:`openeo.connect()`.
       Allowed values: see ``default_backend.auto_authenticate``.
       Also see :ref:`default_url_and_auto_auth`
