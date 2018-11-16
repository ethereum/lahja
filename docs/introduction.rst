Introduction
============

Lahja
~~~~~

.. warning::
  This is a very young project. It's used and validated mainly by the Python Ethereum client
  (Trinity) Lahja is alpha state software. Expect bugs.


*Lahja is a generic multi process event bus implementation written in Python 3.6+ that enables
lightweight inter-process communication, based on non-blocking asyncio.*

Goals
-----

Lahja is tailored around one primary use case: Enabling event-based communication between different
processes in moder Python applications using non-blocking asyncio.

Features:

- Non-blocking APIs based on ``asyncio``
- Broadcast events within a single process or across multiple processes.
- Multiple APIs to consume events that adapt to different use cases and styles
- lightweight and simple (e.g. no IPC pipe management etc)
- Easy event routing (e.g. route specific events to specific processes or process groups)


Further reading
---------------

Here are a couple more useful links to check out.

* `Source Code on GitHub <https://github.com/ethereum/lahja>`_
* `Examples <https://github.com/ethereum/lahja/tree/master/examples>`_