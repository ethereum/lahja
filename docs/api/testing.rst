Testing
=======


.. warning:: This API is experimental and subject to breaking changes.

Tests for the ``lahja`` library can be written using the
**Runner/Engine/Driver** APIs.  These allow for constructing reusable
declarative tests against endpoints which can be run against different endpoint
implementations as well as different configurations of endpoints.


Runner
------

Runners are in charge of the outermost execution layer.  A ``Runner`` **must**
be a callable which accepts ``*args`` where each argument is a ``Driver``.


.. autoclass:: lahja.tools.runner.RunnerAPI
    :members:
    :undoc-members:
    :show-inheritance:


Engines
-------

Engines are in charge of abstracting away how each individual endpoint
implementation should be run.  An ``Engine`` **must** implement the following
API.

.. autoclass:: lahja.tools.engine.EngineAPI
    :members:
    :undoc-members:
    :show-inheritance:


Drivers
-------

Drivers are a declarative set of instructions for instrumenting the actions and
lifecycle of an endpoint.  A driver **must** be a coroutine which takes an
``Engine`` as a single argument and performs the actions declared by the driver.

Drivers should be constructed in a *functional* maner using the utilities
provided under ``lahja.tools.drivers``.


A driver is composed of a single *Initializer* followed by a variadic number of *Actions*.


.. autofunction:: lahja.tools.drivers.driver.driver


Initializers
~~~~~~~~~~~~

.. autofunction:: lahja.tools.drivers.initializers.serve_endpoint

.. autofunction:: lahja.tools.drivers.initializers.run_endpoint


Actions
~~~~~~~

.. autofunction:: lahja.tools.drivers.actions.broadcast

.. autofunction:: lahja.tools.drivers.actions.connect_to_endpoints

.. autofunction:: lahja.tools.drivers.actions.throws

.. autofunction:: lahja.tools.drivers.actions.wait_for

.. autofunction:: lahja.tools.drivers.actions.wait_until_any_endpoint_subscribed_to

.. autofunction:: lahja.tools.drivers.actions.wait_until_connected_to

.. autofunction:: lahja.tools.drivers.actions.wait_any_then_broadcast

.. autofunction:: lahja.tools.drivers.actions.serve_request

.. autofunction:: lahja.tools.drivers.actions.request

.. autofunction:: lahja.tools.drivers.actions.checkpoint


Examples
~~~~~~~~

Driver to run an endpoint as a server and wait for a client to connect.

.. code-block:: python

    from lahja.tools import drivers as d

    server_driver = d.driver(
        d.serve_endpoint(ConnectionConfig(...)),
        d.wait_until_connected_to('client'),
    )


Driver to run a client and connect to a server.

.. code-block:: python

    from lahja.tools import drivers as d

    server_config = ConnectionConfig(...)
    client_driver = d.driver(
        d.run_endpoint(ConnectionConfig(...)),
        d.connect_to_endpoints(server_config),
    )


We could then run these together against the ``trio`` implementation of the
endpoint like this.


.. code-block:: python

    from lahja.tools.runners import TrioRunner

    client_driver = ...
    server_driver = ...
    runner = TrioRunner()
    runner(client_driver, server_driver)
