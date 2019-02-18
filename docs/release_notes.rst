Release Notes
=============

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
