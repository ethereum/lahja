Release Notes
=============

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
