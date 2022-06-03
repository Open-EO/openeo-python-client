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

    con = openeo.connect("https://openeo.example.com")

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

    con.authenticate_basic("john", "j0hn123")

Subsequent usage of the connection object ``con`` will
use authenticated calls.
For example, show information about the authenticated user::

    >>> con.describe_account()
    {'user_id': 'john'}



OpenID Connect Based Authentication
===================================

OpenID Connect (often abbreviated "OIDC") is an identity layer on top of the OAuth 2.0 protocol.
It is a quite an extensive stack of interacting actors and protocols,
and an in-depth discussion of its architecture would lead us too far here.
However, in the context of working with openEO,
these OpenID Connect concepts are useful to understand:

*   There is **decoupling** between:

    *   the *OpenID Connect identity provider* (the platform
        that handles the authentication of the user)
    *   the *openEO back-end*, which manages earth observation collections
        and executes your algorithms

    Instead of managing the authentication procedure itself,
    a back-end first forwards a user to the log-in page of
    a OpenID Connect provider, such as an (external) organisation like Google or Microsoft.
    The user can log in there with an existing account (or create a new one)
    and then generally has to explicitly grant access
    to basic profile information (e.g. email address)
    that the back-end will use to identify the user.

    Note that with this approach, the back-end does not have to
    take care of all the security and privacy challenges
    of properly handling user registration, authentication, etc.
    Also, it allows the user to securely reuse an existing account
    registered with an established organisation, instead of having
    to register yet another account with some web service.

*   Your openEO script or application acts as
    a so called **OpenID Connect client**, with an associated **client ID**.
    This practically means that, apart from a user account,
    you need a client ID as well (and often a client secret too)
    when authenticating.

    The details of how to obtain the client ID and secret largely
    depend on the back-end and OpenID Connect provider:
    you might have to register a client yourself,
    or you might have to use an existing client ID.
    Consult the openEO back-end (documentation)
    about how to obtain client ID (and secret).

*   There are several possible "**flows**" (also called "grants")
    to complete the whole OpenID Connect authentication dance:

    * Authorization Code Flow
    * Device Flow
    * Client Credentials Flow
    * Resource Owner Password flow
    * Refresh Token Flow

    Picking the right flow highly depends on your use case and context:
    are you working interactively,
    are you working in a browser based environment,
    should your application be able to work
    without user interaction in the background,
    what does the OpenID Connect provider support,
    ...?


OpenID Connect is clearly more complex than Basic HTTP Auth.
In the sections below we will discuss the practical details of each flow.

General options
---------------

*   A back-end might support **multiple OpenID Connect providers**.
    If there is only one, the openEO Python Client Library will pick it automatically,
    but if there are multiple you might get an exception like this::

        OpenEoClientException: No provider_id given. Available: ['gl', 'ms'].

    Specify explicitly which provider to use with the ``provider_id`` argument, e.g.::

        con.authenticate_oidc_authorization_code(
            ...
            provider_id="gl",


Authorization Code Flow
------------------------

This is the most popular and widely supported OpenID Connect flow
in the general web development world.
However, it requires an environment that can be hard to get
right when using the openEO Python Client Library in your application:

*   You are working interactively (e.g. in a Jupyter notebook,
    in a Python/IPython shell or running a Python script
    manually)
*   You have access to a web browser
    (preferably on the same machine as your application),
    to authenticate with the OpenID Connect provider
*   That web browser has (network) access
    to a temporary web server that will be spawn
    by the openEO Python Client Library in your application.
*   The URL of the temporary web server is properly whitelisted
    in the OpenID client's "redirect URL" configuration
    at the OpenID Connect provider's side.

The hardest part are the two last items.
If you just run your application locally on your machine,
the whole procedure is doable (using a ``localhost`` based web server).
But if you are working remotely
(e.g. on a hosted Jupyter platform),
it can be challenging or even impossible
to get the network access part right.


Basic usage
```````````

The bare essentials to run the authorization code flow::

    con.authenticate_oidc_authorization_code(
        client_id=client_id,
        client_secret=client_secret,
    )

We assume here that you are running this locally
and that the OpenID Connect provider allows to use a wildcard ``*``
in the redirect URL whitelist.
The ``client_id`` and ``client_secret`` string variables hold
the client ID and secret as discussed above.

What happens when running that ``authenticate_oidc_authorization_code`` call:

*   the openEO Python Client Library will
    try to trigger your browser to open new window,
    pointing to a log-in page of the
    OpenID Connect provider (e.g. Google or Microsoft).
*   You have to authenticate on this page (unless you are logged in already)
    and allow the client (identified by ``client_id``) access to the
    basic account information, such as email address
    (unless you already did that).
*   Meanwhile, the openEO Python Client Library
    is running a short-living webserver in the background
    to serve a "redirect URL".
*   When you completed logging in and access granting
    on the OpenID Connect provider website,
    you are forwarded in your browser to this redirect URL.
*   Through the data provided in the request to the redirect URL,
    the openEO Python Client Library can obtain the desired
    tokens to set up authenticated communication with the back-end.

When the above procedure completed successfully, your connection
is authenticated, and you should be able
to inspect the "user" as seen by the back-end, e.g.::

    >>> con.describe_account()
    {'user_id': 'nIrHtS4rhk4ru7531RhtLHXd6Ou0AW3vHfg'}

The browser window should show a simple success page
that you can safely close.


Options and finetuning
``````````````````````

The above example only covers the bare foundation
of the OpenID Connect Authorization code flow.
In a practical use case, you will probably need
some of the following finetuning options:

*   The redirect URL is served by default on ``localhost``
    with a random port number.
    Most OpenID Connect providers however do not support wildcards
    in the redirect URL whitelist and require predefined fixed URLs.
    Also, your networking situation might require you to use
    a different hostname or IP address instead of ``localhost``
    to reach the short-living webserver.

    Both the redirect URL **hostname and port number** can be specified
    explicitly with the `server_address` argument, e.g.::

        con.authenticate_oidc_authorization_code(
            ...
            server_address=("myhost.example.com", 40878)

    In this example, the corresponding redirect URL to whitelist is::

        http://myhost.example.com:40878/callback

*   As noted above, the openEO Python Client Library tries
    to trigger your default browser
    (on the same machine that your application is running)
    to open a new window.
    If this does not work
    (e.g. you are working remotely in a non-graphical environment),
    or you want to use another browser on another machine,
    you can specify an alternative way to **"handle" the URL** that initiates
    the OpenID Connect flow with the ``webbrowser_open`` argument.
    For example, to just print the URL so you can visit it as you desire::

        con.authenticate_oidc_authorization_code(
            ...
            webbrowser_open=lambda url: print("Visit this:", url)

    Note that the web browser you use to visit that URL must be able
    to resolve and access the redirect URL
    served on the machine where your application is running.

*   The short-living webserver only waits up to a certain time
    for the request to the redirect URL.
    During that time, your application is actively waiting
    and not doing anything else.
    You can increase or decrease the maximum **waiting time** (in seconds)
    with the ``timeout`` argument.


Device Flow
-----------

The device flow (also called device authorization grant)
is a relatively new OpenID Connect flow
and it is not as widely supported across different OpenID Connect Providers
as the other flows.
It provides a nice alternative that is roughly comparable
to the authorization code flow but without the previously mentioned issues related
to short-living webservers, network access and browser redirects.

The device flow is only suited for interactive use cases
and requires a web browser for the authentication
with the OpenID Connect provider.
However, it can be any web browser, even one on your mobile phone.
There is no networking magic required to be able to access
any short-living background webserver like with the authorization code flow.

To illustrate the flow, this is how to initiate the authentication::

    con.authenticate_oidc_device(
        client_id=client_id,
        client_secret=client_secret
    )

This will print a message like this::

    To authenticate: visit https://oidc.example.net/device
    and enter the user code 'DTNY-KLNX'.

You should now visit this URL.
Usually it is intentionally a short URL to make it feasible to type it
instead of copy-pasting it (e.g. on another device).
Authenticate with the OpenID Connect provider and enter the user code
shown in the message.
Meanwhile, the openEO Python Client Library is actively polling the OpenID Connect
provider and when you successfully complete the authentication
and entering of the user code,
it will receive the necessary tokens for authenticated communication
with the back-end and print::

    Authorized successfully.

In case of authentication failure, the openEO Python Client Library
will stop polling at some point and raise an exception.


Some additional options for this flow:

*   By default, the messages containing the authentication URL, user code
    and success message are printed with standard Python ``print``.
    You can provide a custom function to display them with the ``display`` option, e.g.::

        con.authenticate_oidc_device(
            ...
            display=lambda msg: render_popup(msg)

*   The openEO Python Client Library waits actively
    for successful authentication, so your application is
    hanging for a certain time.
    You can increate or reduce this maximum polling time (in seconds)
    with the ``max_poll_time`` argument.



Client Credentials Flow
-----------------------

The Client Credentials flow directly uses the client ID and secret
to authenticate::

    con.authenticate_oidc_client_credentials(
        client_id=client_id,
        client_secret=client_secret,
    )


It does not involve interactive authentication through a web browser,
which makes it useful for **non-interactive use cases**.

The downside is of the Client Credentials flow is that it can
be challenging or even impossible with a given OpenID Connect provider,
to set up a client that supports this.
Also, your openEO back-end might not allow it, because technically
you are authenticating a *client*, and not a *user*.


Resource Owner Password flow
----------------------------

With the Resource Owner Password flow you directly pass
the user (and client) credentials::

    con.authenticate_oidc_resource_owner_password_credentials(
        client_id=client_id,
        client_secret=client_secret,
        username=username,
        password=password,
    )


Like the Client Credentials flow, it is useful for **non-interactive uses cases**.

However, usage of the Resource Owner Password flow is **generally discouraged**
because of its poor security features (e.g. OAuth/OIDC was designed
to avoid passing and storing user passwords unnecessarily).
It is also not widely supported across OpenID Connect providers,
probably due to its weak security measures.


Refresh Token Flow
------------------

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
through the ``store_refresh_token`` argument, e.g.::

    con.authenticate_oidc_authorization_code(
        ...
        store_refresh_token=True



The refresh token will be stored in file in private file
in your home directory and will be used automatically
when authenticating with the Refresh Token Flow like this::

    con.authenticate_oidc_refresh_token(
        client_secret=client_secret,
        client_id=client_id
    )

You can also bootstrap the refresh token file
as described in :ref:`oidc_auth_get_refresh_token`


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

    con.authenticate_basic()

OpenID Connect configs
-----------------------

Likewise, with the ``add-oidc`` subcommand you can add OpenID Connect
credentials to the config::

    $ openeo-auth add-oidc https://openeo.example.com/
    Using provider ID 'example' (issuer 'https://oidc.example.com/')
    Enter client_id and press enter: client-d7393fba
    Enter client_secret and press enter:
    Saved client information to '/home/john/.config/openeo-python-client/auth-config.json'

Now you can user OpenID Connect based authentication in your application
without having to specify the client ID and client secret explicitly,
like one of these calls::

    con.authenticate_oidc_authorization_code()
    con.authenticate_oidc_client_credentials()
    con.authenticate_oidc_resource_owner_password_credentials(username=username, password=password)
    con.authenticate_oidc_device()
    con.authenticate_oidc_refresh_token()

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
    >>> con = openeo.connect()
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
Since version 0.10.1, te openEO Python Client Library will automatically
attempt to re-authenticate a connection when access token expiry is detected
and valid refresh tokens are available.

Likewise, refresh tokens can also be used for authentication in cases
where a script or application is **run automatically in the background on regular basis** (daily, weekly, ...).
If there is a non-expired refresh token available, the script can authenticate
without user interaction.

Guidelines and tips
--------------------

Some guidelines get long-term and non-interactive authentication working for your use case:

-   If you run a workflow periodically, but the interval between runs
    is larger than the expiry time of the refresh token
    (e.g. a monthly job, while the refresh token expires after, say, 10 days),
    you could consider setting up a *custom OIDC client* with better suited
    refresh token timeout.
    The practical details of this heavily depend on the OIDC Identity Provider
    in play and are out of scope of this discussion.
-   Obtaining the initial refresh token requires manual/interactive authentication,
    but once it is stored on the necessary machine(s)
    in the refresh token store as discussed in :ref:`auth_configuration_files`,
    no further manual interaction should be necessary.
    To do so, use one of the following methods:

    -   Use the ``openeo-auth oidc-auth`` cli tool, for example to authenticate
        for openeo back-end openo.example.com::

            $ openeo-auth oidc-auth openeo.example.com
            ...
            Stored refresh token in '/home/john/.local/share/openeo-python-client/refresh-tokens.json'


    -   Use a Python snippet to authenticate and store the refresh token::

            import openeo
            con = openeo.connect("openeo.example.com")
            con.authenticate_oidc_device(store_refresh_token=True)


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

-   Authenticate in your script or application with the refresh token
    and make sure newer refresh tokens are stored as well::

        import openeo
        con = openeo.connect("openeo.example.com")
        con.authenticate_oidc_refresh_token(store_refresh_token=True)



Troubleshooting tips
====================

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

