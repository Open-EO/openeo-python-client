*************************************
Authentication and Account Management
*************************************


While a couple of openEO operations can be done
anonymously, most of the interesting parts
of the API require you to identify as a registered
user.
The openEO API specifies two ways to authenticate
as a user:

* OpenID Connect: recommended, but not always straightforward to use
* Basic HTTP Auth: not recommended, but practically easier is some situations

Authentication Basics
======================

Let's start with the basics of how to authenticate
starting from a backend connection::

    import openeo

    con = openeo.connect("http://openeo.example.com")

OpenID Connect Based Authentication
------------------------------------

OpenID Connect is an identity layer on top of the OAuth 2.0 protocol.
It is a quite an extensive stack of interacting actors and protocols,
and an in-depth discussion of its architecture would lead us too far here.
However, some aspects that are relevant in the context of working
with openEO:

* Decoupling of the OpenID Connect *identity provider* (the platform
  that provides the authentication of the user)
  from the openEO backend (which manages earth observation collections
  and executes your algorithms).
  Instead of managing the authentication itself,
  a backend first forwards a user to a log-in page of
  an organisation like Google or Microsoft. The user can log-in
  with an existing account (or create a new one) and then generally
  has to grant access to basic profile information (e.g. email address)
  that allow the backend to identify the user.

* The openEO script or application that implements your openEO
  algorithm or workflow acts as an OpenID Connect *client*,
  with an associated *client ID*.
  This practically means that, apart from a user account,
  you also have to register your client with the identity provider
  in play, to get this client ID (and often a client secret too).

* There are several "flows" to complete the whole OpenID Connect
  authentication dance, and choosing a particular one highly depends
  on the use case and context:

  * Authorization Code Flow
  * Device Flow
  * Client Credentials Flow
  * Resource Owner Password flow
  * Refresh Token Flow





