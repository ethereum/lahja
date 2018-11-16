Release Notes
=============

v0.9.0
------

- Implement "internal events"
- Rename `max` to `num_events`
- Ensure Futures are created on the correct event loop
- Ensure all consuming APIs handle cancellations well
- Don't try to propagate events after shutdown
