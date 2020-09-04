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

To illustrate how to authenticate with the openEO Python client library,
we start form a backend connection::

    import openeo

    con = openeo.connect("https://openeo.example.com")

Basic HTTP Auth
===============

Let's start with the easiest authentication method,
based on the Basic HTTP authentication scheme.
It is however *not recommended* for various reasons,
such as its limited *security* measures.
For example, if you are connecting to a backend with a ``http://`` URL
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

OpenID Connect is an identity layer on top of the OAuth 2.0 protocol.
It is a quite an extensive stack of interacting actors and protocols,
and an in-depth discussion of its architecture would lead us too far here.
However, in the context of working with openEO,
these OpenID Connect concepts are useful to understand:

*   There is **decoupling** between:

    *   the *OpenID Connect identity provider* (the platform
        that handles the authentication of the user)
    *   the *openEO backend*, which manages earth observation collections
        and executes your algorithms

    Instead of managing the authentication procedure itself,
    a backend first forwards a user to the log-in page of
    a OpenID Connect provider, such as an (external) organisation like Google or Microsoft.
    The user can log in there with an existing account (or create a new one)
    and then generally has to explicitly grant access
    to basic profile information (e.g. email address)
    that the backend will use to identify the user.

    Note that with this approach, the backend does not have to
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
    depend on the backend and OpenID Connect provider:
    you might have to register a client yourself,
    or you might have to use an existing client ID.
    Consult the openEO backend (documentation)
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


OpenID Connect is clearly more complex than Basic HTTP Auth,
but let's try to break down the practical details of each flow.


Authorization Code Flow
------------------------

This is the most popular and widely supported OpenID Connect flow
in the general web development world.
However, it requires an environment that can be hard to get
right when using the openEO Python client library in your application:

*   You are working interactively (e.g. in a Jupyter notebook,
    in a Python/IPython shell or running a Python script
    manually)
*   You have access to a web browser
    (preferably on the same machine as your application),
    to authenticate with the OpenID Connect provider
*   That web browser has (network) access
    to a temporary web server that will be spawn
    by the openEO Python client library in your application.
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

*   the openEO Python client library will
    try to trigger your browser to open new window,
    pointing to a log-in page of the
    OpenID Connect provider (e.g. Google or Microsoft).
*   You have to authenticate on this page (unless you are logged in already)
    and allow the client (identified by ``client_id``) access to the
    basic account information, such as email address
    (unless you already did that).
*   Meanwhile, the openEO Python client library
    is running a short-living webserver in the background
    to serve a "redirect URL".
*   When you completed logging in and access granting
    on the OpenID Connect provider website,
    you are forwarded in your browser to this redirect URL.
*   Through the data provided in the request to the redirect URL,
    the openEO Python client library can obtain the desired
    tokens to set up authenticated communication with the backend.

When the above procedure completed successfully, your connection
is authenticated, and you should be able
to inspect the "user" as seen by the backend, e.g.::

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
            server_address=("myhost.example", 40878),

    In this example, the corresponding redirect URL to whitelist is::

        http://myhost.example:40878/callback

*   As noted above, the openEO Python client library tries
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
            webbrowser_open=lambda url: print("Visit this:", url),

    Note that the web browser you use to visit that URL must be able
    to resolve and access the redirect URL
    served on the machine where your application is running.

*   A backend might support **multiple OpenID Connect providers**.
    If there is only one, the openEO Python client library will pick it automatically,
    but if there are multiple you might get an exception like this::

        OpenEoClientException: No provider_id given. Available: ['gl', 'ms'].

    Specify explicitly which provider to use with the ``provider_id`` argument, e.g.::

        con.authenticate_oidc_authorization_code(
            ...
            provider_id="gl",

*   The short-living webserver only waits up to a certain time
    for the request to the redirect URL.
    During that time, your application is actively waiting
    and not doing anything else.
    You can increase or decrease the maximum **waiting time** (in seconds)
    with the ``timeout`` argument.


Device Flow
-----------

The device flow (also called device authorization grant)
is a recently added OpenID Connect flow.
It is not as widely supported across different OpenID Connect Providers
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

    To authenticate: visit https://provider.example/device
    and enter the user code 'DTNY-KLNX'.

You should now visit this URL.
Usually it is intentionally a short URL to make it feasible to type it
instead of copy-pasting it (e.g. on another device).
Authenticate with the OpenID Connect provider and enter the user code
shown in the message.
Meanwhile, the openEO Python client library is actively polling the OpenID Connect
provider and when you complete the authentication and entering of the user code,
it will receive the necessary tokens for authenticated communication
with the backend and print::

    Authorized successfully.

Some additional options for this flow:

*   By default, the messages containing the authentication URL, user code
    and success message are printed with standard Python ``print``.
    You can provide a custom function to display them with the ``display`` option, e.g.::

        con.authenticate_oidc_device(
            ...
            display=lambda msg: render_popup(msg)

*   The openEO Python client library waits actively
    for successful authentication, so your application is
    hanging for a certain time.
    You can increate or reduce this maximum polling time (in seconds)
    with the ``max_poll_time`` argument.



Client Credentials Flow
-----------------------



Resource Owner Password flow
----------------------------


Refresh Token Flow
------------------


.. TODO:
.. - config files, refresh token files, cli tool ...


