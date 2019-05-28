Quickstart
==========


Install the library
~~~~~~~~~~~~~~~~~~~

.. code:: sh

   pip install lahja


Import ``Endpoint`` and ``BaseEvent``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. literalinclude:: ../examples/inter_process_ping_pong.py
   :language: python
   :end-before: class BaseExampleEvent(BaseEvent)


Setup application specific events
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. literalinclude:: ../examples/inter_process_ping_pong.py
   :language: python
   :pyobject: BaseExampleEvent

.. literalinclude:: ../examples/inter_process_ping_pong.py
   :language: python
   :pyobject: FirstThingHappened

.. literalinclude:: ../examples/inter_process_ping_pong.py
   :language: python
   :pyobject: SecondThingHappened


Setup first process to receive and broadcast events
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. literalinclude:: ../examples/inter_process_ping_pong.py
   :language: python
   :pyobject: run_proc1

.. literalinclude:: ../examples/inter_process_ping_pong.py
   :language: python
   :pyobject: proc1_worker


Setup second process to receive and broadcast events
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. literalinclude:: ../examples/inter_process_ping_pong.py
   :language: python
   :pyobject: run_proc2

.. literalinclude:: ../examples/inter_process_ping_pong.py
   :language: python
   :pyobject: proc2_worker


Start both processes
~~~~~~~~~~~~~~~~~~~~

.. literalinclude:: ../examples/inter_process_ping_pong.py
   :language: python
   :start-after: # Start two processes


Running the examples
====================

Example: Chatter between two processes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: sh

    python examples/inter_process_ping_pong.py

The output will look like this:


.. code:: sh

    INFO  05-29 11:31:45  Hello from proc2
    INFO  05-29 11:31:45  Hello from proc1
    INFO  05-29 11:31:45  Received via SUBSCRIBE API in proc2: Hit from proc1
    INFO  05-29 11:31:45  Received via STREAM API in proc2: Hit from proc1
    INFO  05-29 11:31:46  Hello from proc2
    INFO  05-29 11:31:46  Received via SUBSCRIBE API in proc1: Hit from proc2
    INFO  05-29 11:31:46  Hello from proc1
    INFO  05-29 11:31:47  Hello from proc2
    INFO  05-29 11:31:47  Hello from proc1
    INFO  05-29 11:31:48  Hello from proc2
    INFO  05-29 11:31:48  Received via SUBSCRIBE API in proc1: Hit from proc2
    INFO  05-29 11:31:48  Hello from proc1
    INFO  05-29 11:31:49  Hello from proc2
    INFO  05-29 11:31:49  Hello from proc1
    INFO  05-29 11:31:50  Hello from proc2
    INFO  05-29 11:31:50  Received via SUBSCRIBE API in proc1: Hit from proc2
    INFO  05-29 11:31:50  Hello from proc1
    INFO  05-29 11:31:50  Received via SUBSCRIBE API in proc2: Hit from proc1
    INFO  05-29 11:31:50  Received via STREAM API in proc2: Hit from proc1
    INFO  05-29 11:31:51  Hello from proc2
    INFO  05-29 11:31:51  Hello from proc1


Example: Request API
~~~~~~~~~~~~~~~~~~~~


.. code:: sh

    python examples/request_api.py


The output will look like this:

.. code:: sh

    Requesting
    Got answer: Yay
    Requesting
    Got answer: Yay
    Requesting
    Got answer: Yay

