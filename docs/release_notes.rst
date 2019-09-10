Release Notes
=============

.. towncrier release notes start

Lahja 0.14.5 (2019-09-10)
-------------------------

Features
~~~~~~~~

- Ensure ``stream()`` does not suppress ``CancelledError`` (`#156 <https://github.com/ethereum/lahja/issues/156>`__)


Bugfixes
~~~~~~~~

- Fix that ensures ``asyncio`` streams are closed when an endpoint shuts down to prevent ``ResourceWarning`` warnings. (`#157 <https://github.com/ethereum/lahja/issues/157>`__)


Lahja 0.14.4 (2019-09-05)
-------------------------

No significant changes.


Lahja 0.14.3 (2019-08-28)
-------------------------

Improved Documentation
~~~~~~~~~~~~~~~~~~~~~~

- Fix broken RTD build by re-adding some info that is important for the latex job. (`#155 <https://github.com/ethereum/lahja/issues/155>`__)


Lahja 0.14.2 (2019-08-28)
-------------------------

Bugfixes
~~~~~~~~

- Raise ``ENDPOINT_CONNECT_TIMEOUT`` to ``30`` seconds to be more conservative about
  app specific expectations on the maximum time it could take for endpoints to become
  available upon connection attempts. (`#154 <https://github.com/ethereum/lahja/issues/154>`__)


Lahja 0.14.1 (2019-08-13)
-------------------------

Features
~~~~~~~~

- Add a ``TrioEndpoint`` as a trio based alternative to the ``AsyncioEndpoint``.  It can seamlessly operate with other endpoints both trio or asyncio based. (`#126 <https://github.com/ethereum/lahja/issues/126>`__)
- Convert run mechanism for ``RemoteEndpoint`` to be async context manager based. (`#131 <https://github.com/ethereum/lahja/issues/131>`__)


Bugfixes
~~~~~~~~

- Use the proper ``ConnectionAttemptRejected`` class in a code path that used
  a generic ``Exception`` before. (`#128 <https://github.com/ethereum/lahja/issues/128>`__)
- If for some reason the IPC file is missing during server shutdown,
  suppress the `FileNotFoundError` that is raised when we try to remove it. (`#144 <https://github.com/ethereum/lahja/issues/144>`__)
- Ensure cancellation of asyncio tasks is properly handled. (`#145 <https://github.com/ethereum/lahja/issues/145>`__)
- Fixed some syntax issues in the API docs that prevented them from building.
  Ensured the CI docs build catches these issues in the future. (`#147 <https://github.com/ethereum/lahja/issues/147>`__)


Improved Documentation
~~~~~~~~~~~~~~~~~~~~~~

- Setup towncrier to generate release notes from fragment files to ensure a higher standard
  for release notes. (`#147 <https://github.com/ethereum/lahja/issues/147>`__)
- Fix wrong title in docs as well as wrong info in license. (`#150 <https://github.com/ethereum/lahja/issues/150>`__)
- Rearrange the table of contents and move "Testing" under the API section. (`#151 <https://github.com/ethereum/lahja/issues/151>`__)
- Remove visual clutter from API docs
  Group methods and attributes in API docs (`#152 <https://github.com/ethereum/lahja/issues/152>`__)


Deprecations and Removals
~~~~~~~~~~~~~~~~~~~~~~~~~

- Remove ``connect_to_endpoint`` and ``connect_to_endpoints_nowait`` APIs. (`#137 <https://github.com/ethereum/lahja/issues/137>`__)


v0.14.0
-------

- Feature: Rename subscription wait APIs and ensure they also work well with local subscriptions

v0.13.0
-------

- Feature: Implement a standard API for endpoints to support non-asyncio based implementations (e.g. Trio)
- Feature: Improve flexibility of the APIs that allow waiting on subscriptions
- Bugfix: Get rid of warnings on shutdown
- Bugfix: Repair broken examples and add a CI job to ensure they don't break again
- Performance: Don't send events to endpoints that aren't subscribed to the specific event
- Performance: Reduce number of socket sends by precombinging length prefix
- Performance: Many small performance improvements in various code paths
- Performance: Use a faster request id implementation instead of using an uuid

v0.12.0
-------

- Change IPC backend from multiprocessing to asyncio
- Endpoint.broadcast() is now async
- Endpoint.broadcast_nowait() now exists, it schedules the message to be broadcast
- Endpoint.start_serving_nowait() no longer exists
- Endpoint.connect_to_endpoints_blocking() no longer exists
- Endpoint.stop() must be called or else some coroutines will be orphaned
- Endpoint can only be used from one event loop. It will remember the current event loop
  when an async method is first called, and throw an exception if another of its async
  methods is called from a different event loop.
- Messages will be compressed if python-snappy is installed
- Lahja previously silently dropped some exceptions, they are now propogated up

v0.11.2
-------

- Properly set up logger

v0.11.1
-------

- Turn exception that would be raised in a background task into a warning

v0.11.0
-------

- Performance: Connect endpoints directly without central coordinator (BREAKING CHANGE)

v0.10.2
-------

- Fix issue that can crash Endpoint

v0.10.1
-------

- Fix issue that can crash Endpoint

v0.10.0
-------

- Make `request` API accept a `BroadcastConfig`
- Add benchmarks

v0.9.0
------

- Implement "internal events"
- Rename `max` to `num_events`
- Ensure Futures are created on the correct event loop
- Ensure all consuming APIs handle cancellations well
- Don't try to propagate events after shutdown
