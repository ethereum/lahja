# Lahja

[![Documentation Status](https://readthedocs.org/projects/lahja/badge/?version=latest)](http://lahja.readthedocs.io/en/latest/?badge=latest)

[Documentation hosted by ReadTheDocs](http://lahja.readthedocs.io/en/latest/)

**DISCLAIMER: This is alpha state software. Expect bugs.**

*Lahja is a generic multi process event bus implementation written in Python 3.6+ to enable lightweight inter-process communication, based on non-blocking asyncio.*

## What is this for?

Lahja is tailored around one primary use case: enabling multi process Python applications to communicate through events between processes in a non-blocking
asyncio fashion.

Key facts:

- non-blocking APIs based on asyncio
- lightweight and simple (e.g no IPC pipes etc)
- easy multicasting of events (one event, many independent receivers)
- easy event routing (e.g route event X only to process group Y)
- multiple consuming APIs to adapt to different use cases and styles


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
