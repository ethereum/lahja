# Lahja

**DISCLAIMER: This is a proof of concept. It's untested, has never been used anywhere in production and is probably full of bugs**

*Lahja is a generic event bus implementation written in Python 3.6+ to enable lightweight inter-process communication, based on non-blocking asyncio.*

## What is this for?

Lahja is tailored around one primary use case: enabling multi process Python applications to communicate through events between processes in a non-blocking
asyncio fashion.

Key facts:

- non-blocking APIs based on asyncio
- lightweight and simple (e.g no IPC pipes etc)
- easy multicasting of events (one event, many independent receivers)
- multiple consuming APIs to adapt to different use cases and styles


## Quickstart

### Install the library

```sh
pip install lahja
```

### Import `EventBus` and `Endpoint`.

```python
from lahja import (
    Endpoint,
    EventBus,
)
```

### Setup `EventBus` with `Endpoint`s and pass the `Endpoint`s into the different processes.

```python
event_bus = EventBus()
e1 = event_bus.create_endpoint('e1')
e2 = event_bus.create_endpoint('e2')
event_bus.start()

p1 = multiprocessing.Process(target=run_proc1, args=(e1,))
p1.start()

p2 = multiprocessing.Process(target=run_proc2, args=(e2,))
p2.start()
```

### Inside each process, `connect()` the endpoint with the event bus and start receiving and broadcasting events.

```Python
def run_proc1(endpoint):
    loop = asyncio.get_event_loop()
    endpoint.connect()
    endpoint.subscribe(SecondThingHappened, lambda event: 
        print("Received via SUBSCRIBE API in proc1: ", event.payload)
    )
    endpoint.subscribe(FirstThingHappened, lambda event: 
        print("Receiving own event: ", event.payload)
    )

    loop.run_until_complete(proc1_worker(endpoint))

async def proc1_worker(endpoint):
    while True:
        print("Hello from proc1")
        if is_nth_second(5):
            endpoint.broadcast(
                FirstThingHappened("Hit from proc1 ({})".format(time.time()))
            )
        await asyncio.sleep(1)
```

## API


### Broadcasting events

Events can be broadcasted via the `broadcast` API. Notice that events are internally handled via `asyncio`, which means, this is a non-blocking fire and forget API.

**Broadcast API**

`def broadcast(self, item: BaseEvent) -> None:`

*Example:*

```Python
endpoint.broadcast(
    FirstThingHappened("Hit from proc1 ({})".format(time.time()))
)
```

#### Limiting the scope of broadcasts

By default, broadcasted events are send into each and every endpoint. Since endpoints are usually living in different processes, broadcasting events comes with a performance cost. Often, it can be beneficial to reduce the scope of where events are broadcasted to.

*Example: Broadcasting into a specific endpoint*

```Python
endpoint.broadcast(
    FirstThingHappened("Hit from proc1 ({})".format(time.time())),
    BroadcastConfig(filter_endpoint="some_endpoint)
)
```

For scenarios where we only want to *answer* a specific request, a `BroadcastConfig` can be retrieved from an incoming event to send the response only to that specific call site that sent the request.

*Example: Broadcasting into a specific endpoint*

```Python
endpoint.subscribe(GetSomethingRequest, lambda event:
        # Send a response back to *only* who made that request
        endpoint.broadcast(DeliverSomethingResponse("Yay"), event.broadcast_config())
    )
```

### Listening to events

Events can be received in three different fashions. All APIs are non-blocking.

**Subscribe API**

`def subscribe(self, event_type: Type[BaseEvent], handler: Callable[[BaseEvent], None]) -> Subscription:`

*Example:*

```Python
subscription = endpoint.subscribe(SecondThingHappened, lambda event: 
    print("Received via SUBSCRIBE API in proc1: ", event.payload)
)
```

The handler will be called every time that a `SecondThingHappened` event is fired. Notice that the returned `Subscription` allows deregistering from the event at any later point in time.

*Example:*

```Python
subscription.unsubscribe()
```

**Stream API**

`async def stream(self, event_type: Type[BaseEvent]) -> AsyncIterable[BaseEvent]:`

*Example:*

```Python
async for event in endpoint.stream(SecondThingHappened):
    print(event.payload)
```

**Request API**

This API is combining the concepts of broadcasting an event and waiting for someone to respond on it. Notice that the response will be a routed event only delivered to the
callsite making the request.

`async def request(self, item: BaseEvent) -> BaseEvent:`

*Example:*

```Python
result = await endpoint.request(RequestSomething()):
print(result.payload)
```

## Running the full examples

**Example 1**

```Python
python3 examples/inter_process_ping_pong.py
```

The output will look like this:

```sh
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
```

**Example 2**

```Python
python3 examples/request_api.py
```

The output will look like this:

```sh
Yay
Yay
Yay
```


## TODOs

- Filter support (e.g. only subscribe to `EventX` from origin `y`)
- Push boundaries (don't push this into process x)
- Testing
- Performance analysis

## Developer Setup

If you would like to hack on lahja, please check out the
[Ethereum Development Tactical Manual](https://github.com/pipermerriam/ethereum-dev-tactical-manual)
for information on how we do:

- Testing
- Pull Requests
- Code Style
- Documentation

### Development Environment Setup

You can set up your dev environment with:

```sh
git clone https://github.com/cburgdorf/lahja
cd lahja
virtualenv -p python3 venv
. venv/bin/activate
pip install -e .[dev]
```

### Testing Setup

During development, you might like to have tests run on every file save.

Show flake8 errors on file change:

```sh
# Test flake8
when-changed -v -s -r -1 lahja/ tests/ -c "clear; flake8 lahja tests && echo 'flake8 success' || echo 'error'"
```

Run multi-process tests in one command, but without color:

```sh
# in the project root:
pytest --numprocesses=4 --looponfail --maxfail=1
# the same thing, succinctly:
pytest -n 4 -f --maxfail=1
```

Run in one thread, with color and desktop notifications:

```sh
cd venv
ptw --onfail "notify-send -t 5000 'Test failure ⚠⚠⚠⚠⚠' 'python 3 test on lahja failed'" ../tests ../lahja
```

### Release setup

For Debian-like systems:
```
apt install pandoc
```

To release a new version:

```sh
make release bump=$$VERSION_PART_TO_BUMP$$
```

#### How to bumpversion

The version format for this repo is `{major}.{minor}.{patch}` for stable, and
`{major}.{minor}.{patch}-{stage}.{devnum}` for unstable (`stage` can be alpha or beta).

To issue the next version in line, specify which part to bump,
like `make release bump=minor` or `make release bump=devnum`.

If you are in a beta version, `make release bump=stage` will switch to a stable.

To issue an unstable version when the current version is stable, specify the
new version explicitly, like `make release bump="--new-version 4.0.0-alpha.1 devnum"`
