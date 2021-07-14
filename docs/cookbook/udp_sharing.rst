====================================
Sharing of user-defined processes
====================================


.. warning::
    Beta feature -
    At the time of this writing (July 2021), sharing of :ref:`user-defined processes <user-defined-processes>`
    (publicly or among users) is not standardized in the openEO API.
    There are however some experimental sharing features in the openEO Python Client Library
    and some back-end providers that we are going to discuss here.

    Be warned that the details of this feature are subject to change.
    For more status information, consult GitHub ticket
    `Open-EO/openeo-api#310 <https://github.com/Open-EO/openeo-api/issues/310>`_.




Publicly publishing a user-defined process.
============================================

As discussed in :ref:`build_and_store_udp`, user-defined processes can be
stored with the :py:meth:`~openeo.rest.connection.Connection.save_user_defined_process` method
on a on a back-end :py:class:`~openeo.rest.connection.Connection`.
By default, these user-defined processes are private and only accessible by the user that saved it::

    from openeo.processes import subtract, divide
    from openeo.api.process import Parameter

    # Build user-defined process
    f = Parameter.number("f", description="Degrees Fahrenheit.")
    fahrenheit_to_celsius = divide(x=subtract(x=f, y=32), y=1.8)

    # Store user-defined process in openEO back-end.
    udp = connection.save_user_defined_process(
        "fahrenheit_to_celsius",
        fahrenheit_to_celsius,
        parameters=[f]
    )


Some back-ends, like the VITO/Terrascope back-end allow a user to flag a user-defined process as "public"
so that other users can access its description and metadata::

    udp = connection.save_user_defined_process(
        ...
        public=True
    )

The sharable, public URL of this user-defined process is available from the metadata given by
:py:meth:`RESTUserDefinedProcess.describe <openeo.rest.udp.RESTUserDefinedProcess.describe>`.
It's listed as "canonical" link::

    >>> udp.describe()
    {
        "id": "fahrenheit_to_celsius",
        "links": [
            {
                "rel": "canonical",
                "href": "https://openeo.vito.be/openeo/1.0/processes/u:johndoe/fahrenheit_to_celsius",
                "title": "Public URL for user-defined process fahrenheit_to_celsius"
            }
        ],
        ...


.. _udp_sharing_call_url_namespace:

Using a public UDP through URL based "namespace"
==================================================

Some back-ends, like the VITO/Terrascope back-end, allow to use a public UDP
through setting its public URL as the ``namespace`` property of the process graph node.

For example, based on the ``fahrenheit_to_celsius`` UDP created above,
the "flat graph" representation of a process graph could look like this::

    {
        ...
        "to_celsius": {
            "process_id": "fahrenheit_to_celsius",
            "namespace": "https://openeo.vito.be/openeo/1.0/processes/u:johndoe/fahrenheit_to_celsius",
            "arguments": {"f": 86}
        }


As a very basic illustration with the openEO Python Client library,
we can create and evaluate a process graph,
containing a ``fahrenheit_to_celsius`` call as single process,
with :meth:`Connection.datacube_from_process <openeo.rest.connection.Connection.datacube_from_process>` as follows::

    cube = connection.datacube_from_process(
        process_id="fahrenheit_to_celsius",
        namespace="https://openeo.vito.be/openeo/1.0/processes/u:johndoe/fahrenheit_to_celsius",
        f=86
    )
    print(cube.execute())
    # Prints: 30.0


Loading a published user-defined process as DataCube
======================================================


From the public URL of the user-defined process,
it is also possible for another user to construct, fully client-side,
a new :class:`~openeo.rest.datacube.DataCube`
with :meth:`Connection.datacube_from_json <openeo.rest.connection.Connection.datacube_from_json>`.

It is important to note that this approach is different from calling
a user-defined process as described in :ref:`evaluate_udp` and :ref:`udp_sharing_call_url_namespace`.
:meth:`Connection.datacube_from_json <openeo.rest.connection.Connection.datacube_from_json>`
breaks open the encapsulation of the user-defined process and "unrolls" the process graph inside
into a new :class:`~openeo.rest.datacube.DataCube`.
This also implies that parameters defined in the user-defined process have to be provided when calling
:meth:`Connection.datacube_from_json <openeo.rest.connection.Connection.datacube_from_json>` ::


    udp_url = "https://openeo.vito.be/openeo/1.0/processes/u:johndoe/fahrenheit_to_celsius"
    cube = connection.datacube_from_json(udp_url, parameters={"f": 86})
    print(cube.execute())
    # Prints: 30.0

For more information, also see :ref:`datacube_from_json`.


