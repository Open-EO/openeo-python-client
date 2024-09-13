.. _authentication_chapter:

*************************************
Authentication and Account Management
*************************************


While a couple of openEO operations can be done
anonymously, most of the interesting parts
of the API require you to identify as a registered
user.
The openEO API specifies two ways to authenticate
as a user:

*   OpenID Connect (recommended, but not always straightforward to use)
*   Basic HTTP Authentication (not recommended, but practically easier in some situations)

To illustrate how to authenticate with the openEO Python Client Library,
we start form a back-end connection::

    import openeo

    connection = openeo.connect("https://openeo.example.com")

Basic HTTP Auth
===============

Let's start with the easiest authentication method,
based on the Basic HTTP authentication scheme.
It is however *not recommended* for various reasons,
such as its limited *security* measures.
For example, if you are connecting to a back-end with a ``http://`` URL
instead of a ``https://`` one, you should certainly not use basic HTTP auth.

With these security related caveats out of the way, you authenticate
using your username and password like this::

    connection.authenticate_basic("john", "j0hn123")

Subsequent usage of the connection object ``connection`` will
use authenticated calls.
For example, show information about the authenticated user::

    >>> connection.describe_account()
    {'user_id': 'john'}



OpenID Connect Based Authentication
===================================

OpenID Connect (often abbreviated "OIDC") is an identity layer on top of the OAuth 2.0 protocol.
An in-depth discussion of the whole architecture would lead us too far here,
but some central OpenID Connect concepts are quite useful to understand
in the context of working with openEO:

*   There is **decoupling** between:

    *   the *OpenID Connect identity provider*
        which handles the authentication/authorization and stores user information
        (e.g. an organization Google, Github, Microsoft, your academic/research institution, ...)
    *   the *openEO back-end* which manages earth observation collections
        and executes your algorithms

    Instead of managing the authentication procedure itself,
    an openEO back-end forwards a user to the relevant OpenID Connect provider to authenticate
    and request access to basic profile information (e.g. email address).
    On return, when the user allowed this access,
    the openEO back-end receives the profile information and uses this to identify the user.

    Note that with this approach, the back-end does not have to
    take care of all the security and privacy challenges
    of properly handling user registration, passwords/authentication, etc.
    Also, it allows the user to securely reuse an existing account
    registered with an established organisation, instead of having
    to register yet another account with some web service.

*   Your openEO script or application acts as
    a so called **OpenID Connect client**, with an associated **client id**.
    In most cases, a default client (id) defined by the openEO back-end will be used automatically.
    For some applications a custom client might be necessary,
    but this is out of scope of this documentation.

*   OpenID Connect authentication can be done with different kind of "**flows**" (also called "grants")
    and picking the right flow depends on your specific use case.
    The most common OIDC flows using the openEO Python Client Library are:

    * :ref:`authenticate_oidc_device`
    * :ref:`authenticate_oidc_client_credentials`
    * :ref:`authenticate_oidc_refresh_token`


OpenID Connect is clearly more complex than Basic HTTP Auth.
In the sections below we will discuss the practical details of each flow.

General options
---------------

*   A back-end might support **multiple OpenID Connect providers**.
    The openEO Python Client Library will pick the first one by default,
    but another another provider can specified explicity with the ``provider_id`` argument, e.g.:

    .. code-block:: python

        connection.authenticate_oidc_device(
            provider_id="gl",
            ...
        )



.. _authenticate_oidc_device:

OIDC Authentication: Device Code Flow
======================================

The device code flow (also called device authorization grant)
is an interactive flow that requires a web browser for the authentication
with the OpenID Connect provider.
The nice things is that the browser doesn't have to run on
the same system or network as where you run your application,
you could even use a browser on your mobile phone.

Use :py:meth:`~openeo.rest.connection.Connection.authenticate_oidc_device` to initiate the flow:

.. code-block:: python

    connection.authenticate_oidc_device()

This will print a message like this:

.. code-block:: text

    Visit https://oidc.example.net/device
    and enter user code 'DTNY-KLNX' to authenticate.

Some OpenID Connect Providers use a slightly longer URL that already includes
the user code, and then you don't need to enter the user code in one of the next steps:

.. code-block:: text

    Visit https://oidc.example.net/device?user_code=DTNY-KLNX to authenticate.

You should now visit this URL in your browser of choice.
Usually, it is intentionally a short URL to make it feasible to type it
instead of copy-pasting it (e.g. on another device).

Authenticate with the OpenID Connect provider and, if requested, enter the user code
shown in the message.
When the URL already contains the user code, the page won't ask for this code.

Meanwhile, the openEO Python Client Library is actively polling the OpenID Connect
provider and when you successfully complete the authentication,
it will receive the necessary tokens for authenticated communication
with the back-end and print:

.. code-block:: text

    Authorized successfully.

In case of authentication failure, the openEO Python Client Library
will stop polling at some point and raise an exception.




.. _authenticate_oidc_refresh_token:

OIDC Authentication: Refresh Token Flow
========================================

When OpenID Connect authentication completes successfully,
the openID Python library receives an access token
to be used when doing authenticated calls to the back-end.
The access token usually has a short lifetime to reduce
the security risk when it would be stolen or intercepted.
The openID Python library also receives a *refresh token*
that can be used, through the Refresh Token flow,
to easily request a new access token,
without having to re-authenticate,
which makes it useful for **non-interactive uses cases**.


However, as it needs an existing refresh token,
the Refresh Token Flow requires
**first to authenticate with one of the other flows**
(but in practice this should not be done very often
because refresh tokens usually have a relatively long lifetime).
When doing the initial authentication,
you have to explicitly enable storage of the refresh token,
through the ``store_refresh_token`` argument, e.g.:

.. code-block:: python

    connection.authenticate_oidc_device(
        ...
        store_refresh_token=True



The refresh token will be stored in file in private file
in your home directory and will be used automatically
when authenticating with the Refresh Token Flow,
using :py:meth:`~openeo.rest.connection.Connection.authenticate_oidc_refresh_token`:

.. code-block:: python

    connection.authenticate_oidc_refresh_token()

You can also bootstrap the refresh token file
as described in :ref:`oidc_auth_get_refresh_token`



.. _authenticate_oidc_client_credentials:

OIDC Authentication: Client Credentials Flow
=============================================

The OIDC Client Credentials flow does not involve interactive authentication (e.g. through a web browser),
which makes it a useful option for **non-interactive use cases**.

.. important::
    This method requires a custom **OIDC client id** and **client secret**.
    It is out of scope of this general documentation to explain
    how to obtain these as it depends on the openEO back-end you are using
    and the OIDC provider that is in play.

    Also, your openEO back-end might not allow it, because technically
    you are authenticating a *client* instead of a *user*.

    Consult the support of the openEO back-end you want to use for more information.

In its most simple form, given your client id and secret,
you can authenticate with
:py:meth:`~openeo.rest.connection.Connection.authenticate_oidc_client_credentials`
as follows:

.. code-block:: python

    connection.authenticate_oidc_client_credentials(
        client_id=client_id,
        client_secret=client_secret,
    )

You might also have to pass a custom provider id (argument ``provider_id``)
if your OIDC client is associated with an OIDC provider that is different from the default provider.

.. caution::
    Make sure to *keep the client secret a secret* and avoid putting it directly in your source code
    or, worse, committing it to a version control system.
    Instead, fetch the secret from a protected source (e.g. a protected file, a database for sensitive data, ...)
    or from environment variables.

.. _authenticate_oidc_client_credentials_env_vars:

OIDC Client Credentials Using Environment Variables
----------------------------------------------------

Since version 0.18.0, the openEO Python Client Library has built-in support to get the client id,
secret (and provider id) from environment variables
``OPENEO_AUTH_CLIENT_ID``, ``OPENEO_AUTH_CLIENT_SECRET`` and ``OPENEO_AUTH_PROVIDER_ID`` respectively.
Just call :py:meth:`~openeo.rest.connection.Connection.authenticate_oidc_client_credentials`
without arguments.

Usage example assuming a Linux (Bash) shell context:

.. code-block:: console

    $ export OPENEO_AUTH_CLIENT_ID="my-client-id"
    $ export OPENEO_AUTH_CLIENT_SECRET="Cl13n7S3cr3t!?123"
    $ export OPENEO_AUTH_PROVIDER_ID="oidcprovider"
    $ python
    >>> import openeo
    >>> connection = openeo.connect("openeo.example.com")
    >>> connection.authenticate_oidc_client_credentials()
    <Connection to 'https://openeo.example.com/openeo/1.1/' with OidcBearerAuth>



.. _authenticate_oidc_automatic:

OIDC Authentication: Dynamic Method Selection
==============================================

The sections above discuss various authentication options, like
the :ref:`device code flow <authenticate_oidc_device>`,
:ref:`refresh tokens <authenticate_oidc_refresh_token>` and
:ref:`client credentials flow <authenticate_oidc_client_credentials>`,
but often you want to *dynamically* switch between these depending on the situation:
e.g. use a refresh token if you have an active one, and fallback on the device code flow otherwise.
Or you want to be able to run the same code in an interactive environment and automated in an unattended manner,
without having to switch authentication methods explicitly in code.

That is what :py:meth:`Connection.authenticate_oidc() <openeo.rest.connection.Connection.authenticate_oidc>` is for:

.. code-block:: python

    connection.authenticate_oidc() # is all you need

In a basic situation (without any particular environment variables set as discussed further),
this method will first try to authenticate with refresh tokens (if any)
and fall back on the device code flow otherwise.
Ideally, when valid refresh tokens are available, this works without interaction,
but occasionally, when the refresh tokens expire, one has to do the interactive device code flow.

Since version 0.18.0, the openEO Python Client Library also allows to trigger the
:ref:`client credentials flow <authenticate_oidc_client_credentials>`
from :py:meth:`~openeo.rest.connection.Connection.authenticate_oidc`
by setting environment variable ``OPENEO_AUTH_METHOD``
and the other :ref:`client credentials environment variables <authenticate_oidc_client_credentials_env_vars>`.
For example:

.. code-block:: shell

    $ export OPENEO_AUTH_METHOD="client_credentials"
    $ export OPENEO_AUTH_CLIENT_ID="my-client-id"
    $ export OPENEO_AUTH_CLIENT_SECRET="Cl13n7S3cr3t!?123"
    $ export OPENEO_AUTH_PROVIDER_ID="oidcprovider"
    $ python
    >>> import openeo
    >>> connection = openeo.connect("openeo.example.com")
    >>> connection.authenticate_oidc()
    <Connection to 'https://openeo.example.com/openeo/1.1/' with OidcBearerAuth>








.. _auth_configuration_files:

Auth config files and ``openeo-auth`` helper tool
====================================================

The openEO Python Client Library provides some features and tools
that ease the usability and security challenges
that come with authentication (especially in case of OpenID Connect).

Note that the code examples above contain quite some **passwords and other secrets**
that should be kept safe from prying eyes.
It is bad practice to define these kind of secrets directly
in your scripts and source code because that makes it quite hard
to responsibly share or reuse your code.
Even worse is storing these secrets in your version control system,
where it might be near impossible to remove them again.
A better solution is to keep **secrets in separate configuration or cache files**,
outside of your normal source code tree
(to avoid committing them accidentally).


The openEO Python Client Library supports config files to store:
user names, passwords, client IDs, client secrets, etc,
so you don't have to specify them always in your scripts and applications.

The openEO Python Client Library (when installed properly)
provides a command line tool ``openeo-auth`` to bootstrap and manage
these configs and secrets.
It is a command line tool that provides various "subcommands"
and has built-in help::

    $ openeo-auth -h
    usage: openeo-auth [-h] [--verbose]
                       {paths,config-dump,token-dump,add-basic,add-oidc,oidc-auth}
                       ...

    Tool to manage openEO related authentication and configuration.

    optional arguments:
      -h, --help            show this help message and exit

    Subcommands:
      {paths,config-dump,token-dump,add-basic,add-oidc,oidc-auth}
        paths               Show paths to config/token files.
        config-dump         Dump config file.
    ...



For example, to see the expected paths of the config files::

    $ openeo-auth paths
    openEO auth config: /home/john/.config/openeo-python-client/auth-config.json (perms: 0o600, size: 1414B)
    openEO OpenID Connect refresh token store: /home/john/.local/share/openeo-python-client/refresh-tokens.json (perms: 0o600, size: 846B)


With the ``config-dump`` and ``token-dump`` subcommands you can dump
the current configuration and stored refresh tokens, e.g.::

    $ openeo-auth config-dump
    ### /home/john/.config/openeo-python-client/auth-config.json ###############
    {
      "backends": {
        "https://openeo.example.com": {
          "basic": {
            "username": "john",
            "password": "<redacted>",
            "date": "2020-07-24T13:40:50Z"
    ...

The sensitive information (like passwords) are redacted by default.



Basic HTTP Auth config
-----------------------

With the ``add-basic`` subcommand you can add Basic HTTP Auth credentials
for a given back-end to the config.
It will interactively ask for username and password and
try if these credentials work::

    $ openeo-auth add-basic https://openeo.example.com/
    Enter username and press enter: john
    Enter password and press enter:
    Trying to authenticate with 'https://openeo.example.com'
    Successfully authenticated 'john'
    Saved credentials to '/home/john/.config/openeo-python-client/auth-config.json'

Now you can authenticate in your application without having to
specify username and password explicitly::

    connection.authenticate_basic()

OpenID Connect configs
-----------------------

Likewise, with the ``add-oidc`` subcommand you can add OpenID Connect
credentials to the config::

    $ openeo-auth add-oidc https://openeo.example.com/
    Using provider ID 'example' (issuer 'https://oidc.example.net/')
    Enter client_id and press enter: client-d7393fba
    Enter client_secret and press enter:
    Saved client information to '/home/john/.config/openeo-python-client/auth-config.json'

Now you can user OpenID Connect based authentication in your application
without having to specify the client ID and client secret explicitly,
like one of these calls::

    connection.authenticate_oidc_authorization_code()
    connection.authenticate_oidc_client_credentials()
    connection.authenticate_oidc_resource_owner_password_credentials(username=username, password=password)
    connection.authenticate_oidc_device()
    connection.authenticate_oidc_refresh_token()

Note that you still have to add additional options as required, like
``provider_id``, ``server_address``, ``store_refresh_token``, etc.


.. _oidc_auth_get_refresh_token:

OpenID Connect refresh tokens
`````````````````````````````

There is also a ``oidc-auth`` subcommand to execute an OpenID Connect
authentication flow and store the resulting refresh token.
This is intended to for bootstrapping the environment or system
on which you want to run openEO scripts or applications that use
the Refresh Token Flow for authentication.
For example::

    $ openeo-auth oidc-auth https://openeo.example.com
    Using config '/home/john/.config/openeo-python-client/auth-config.json'.
    Starting OpenID Connect device flow.
    To authenticate: visit https://oidc.example.net/device and enter the user code 'Q7ZNsy'.
    Authorized successfully.
    The OpenID Connect device flow was successful.
    Stored refresh token in '/home/john/.local/share/openeo-python-client/refresh-tokens.json'



.. _default_url_and_auto_auth:

Default openEO back-end URL and auto-authentication
=====================================================

.. versionadded:: 0.10.0


If you often use the same openEO back-end URL and authentication scheme,
it can be handy to put these in a configuration file as discussed at :ref:`configuration_files`.

.. note::
    Note that :ref:`these general configuration files <configuration_files>` are different
    from the auth config files discussed earlier under :ref:`auth_configuration_files`.
    The latter are for storing authentication related secrets
    and are mostly managed automatically (e.g. by the ``oidc-auth`` helper tool).
    The former are not for storing secrets and are usually edited manually.

For example, to define a default back-end and automatically use OpenID Connect authentication
add these configuration options to the :ref:`desired configuration file <configuration_file_locations>`::

    [Connection]
    default_backend = openeo.cloud
    default_backend.auto_authenticate = oidc

Getting an authenticated connection is now as simple as::

    >>> import openeo
    >>> connection = openeo.connect()
    Loaded openEO client config from openeo-client-config.ini
    Using default back-end URL 'openeo.cloud' (from config)
    Doing auto-authentication 'oidc' (from config)
    Authenticated using refresh token.


Authentication for long-running applications and non-interactive contexts
===========================================================================

With OpenID Connect authentication, the *access token*
(which is used in the authentication headers)
is typically short-lived (e.g. couple of minutes or hours).
This practically means that an authenticated connection could expire and become unusable
before a **long-running script or application** finishes its whole workflow.
Luckily, OpenID Connect also includes usage of *refresh tokens*,
which have a much longer expiry and allow request a new access token
to re-authenticate the connection.
Since version 0.10.1, the openEO Python Client Library will automatically
attempt to re-authenticate a connection when access token expiry is detected
and valid refresh tokens are available.

Likewise, refresh tokens can also be used for authentication in cases
where a script or application is **run automatically in the background on regular basis** (daily, weekly, ...).
If there is a non-expired refresh token available, the script can authenticate
without user interaction.

Guidelines and tips
--------------------

Some guidelines to get long-term and non-interactive authentication working for your use case:

-   If you run a workflow periodically, but the interval between runs
    is larger than the expiry time of the refresh token
    (e.g. a monthly job, while the refresh token expires after, say, 10 days),
    you could consider setting up a *custom OIDC client* with better suited
    refresh token timeout.
    The practical details of this heavily depend on the OIDC Identity Provider
    in play and are out of scope of this discussion.
-   Obtaining a refresh token requires manual/interactive authentication,
    but once it is stored on the necessary machine(s)
    in the refresh token store as discussed in :ref:`auth_configuration_files`,
    no further manual interaction should be necessary
    during the lifetime of the refresh token.
    To do so, use one of the following methods:

    -   Use the ``openeo-auth oidc-auth`` cli tool, for example to authenticate
        for openeo back-end openeo.example.com::

            $ openeo-auth oidc-auth openeo.example.com
            ...
            Stored refresh token in '/home/john/.local/share/openeo-python-client/refresh-tokens.json'


    -   Use a Python snippet to authenticate and store the refresh token::

            import openeo
            connection = openeo.connect("openeo.example.com")
            connection.authenticate_oidc_device(store_refresh_token=True)


    To verify that (and where) the refresh token is stored, use ``openeo-auth token-dump``::

            $ openeo-auth token-dump
            ### /home/john/.local/share/openeo-python-client/refresh-tokens.json #######
            {
              "https://oidc.example.net": {
                "default-client": {
                  "date": "2022-05-11T13:13:20Z",
                  "refresh_token": "<redacted>"
                },
            ...



Best Practices and Troubleshooting Tips
========================================

.. warning::

    Handle (OIDC) access and refresh tokens like secret, personal passwords.
    **Never share your access or refresh tokens** with other people,
    publicly, or for user support reasons.


Clear the refresh token file
----------------------------

When you have authentication or permission issues and you suspect
that your (locally cached) refresh tokens are the culprit:
remove your refresh token file in one of the following ways:

-   Locate the file with the ``openeo-auth`` command line tool::

        $ openeo-auth paths
        ...
        openEO OpenID Connect refresh token store: /home/john/.local/share/openeo-python-client/refresh-tokens.json (perms: 0o600, size: 846B)

    and remove it.
    Or, if you know what you are doing: remove the desired section from this JSON file.

-   Remove it directly with the ``token-clear`` subcommand of the ``openeo-auth`` command line tool::

        $ openeo-auth token-clear

-   Remove it with this Python snippet::

        from openeo.rest.auth.config import RefreshTokenStore
        RefreshTokenStore().remove()
