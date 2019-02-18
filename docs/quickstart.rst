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

    Hello from proc1
    Hello from proc2
    Received via SUBSCRIBE API in proc1:  Hit from proc2 (1533887068.9261594)
    Hello from proc1
    Hello from proc2
    Hello from proc1
    Hello from proc2
    Received via SUBSCRIBE API in proc1:  Hit from proc2 (1533887070.9296985)
    Receiving own event:  Hit from proc1 (1533887070.9288142)
    Received via SUBSCRIBE API in proc2: Hit from proc1 (1533887070.9288142)
    Received via STREAM API in proc2:  Hit from proc1 (1533887070.9288142)
    Hello from proc1
    Hello from proc2
    Hello from proc1
    Hello from proc2
    Received via SUBSCRIBE API in proc1:  Hit from proc2 (1533887072.9331954)
    Hello from proc1
    Hello from proc2
    Hello from proc1
    Hello from proc2
    Received via SUBSCRIBE API in proc1:  Hit from proc2 (1533887074.937018)
    Hello from proc1
    Hello from proc2
    Received via SUBSCRIBE API in proc2: Hit from proc1 (1533887075.9378386)
    Received via STREAM API in proc2:  Hit from proc1 (1533887075.9378386)
    Receiving own event:  Hit from proc1 (1533887075.9378386)


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

