Whoosh-Novo
===========
[![Docs](https://img.shields.io/website?url=https%3A%2F%2Fde-odex.github.io%2Fwhoosh-novo%2F&label=docs)](https://de-odex.github.io/whoosh-novo/)
[![PyPI version](https://img.shields.io/pypi/v/Whoosh-Novo.svg)](https://pypi.org/project/Whoosh-Novo/)
[![Python versions](https://img.shields.io/pypi/pyversions/Whoosh-Novo.svg)](https://pypi.org/project/Whoosh-Novo/)
[![License](https://img.shields.io/pypi/l/Whoosh-Novo)](https://github.com/de-odex/whoosh-novo/blob/main/LICENSE.txt)

[![Tests](https://github.com/de-odex/whoosh-novo/actions/workflows/nox-test.yml/badge.svg?branch=main)](https://github.com/de-odex/whoosh-novo/actions/workflows/nox-test.yml)
[![codecov](https://codecov.io/gh/de-odex/whoosh-novo/graph/badge.svg?token=RZ426ERMZD)](https://codecov.io/gh/de-odex/whoosh-novo)
[![CodeFactor](https://www.codefactor.io/repository/github/de-odex/whoosh-novo/badge)](https://www.codefactor.io/repository/github/de-odex/whoosh-novo)

[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://github.com/pre-commit/pre-commit)
[![lint: ruff](https://img.shields.io/badge/lint-ruff-informational?logo=ruff)](https://github.com/astral-sh/ruff)

<!-- -------------------------------------- -->

> [!IMPORTANT]
> This repository (**whoosh-novo**) is a fork and continuation of the Whoosh project, succeeding [Whoosh-Reloaded](https://github.com/Sygil-Dev/whoosh-reloaded) which is no longer maintained.

> [!WARNING]
> Compatibility will be best-effort, limited to maintained Python versions (>=3.10). I am a solo amateur developer; mistakes will probably be made more often than usual.

> [!NOTE]
> The first PyPI release is planned as `3.0.0a1`. This is an alpha intended to flush out old runtime bugs and any API drift introduced while adding typing annotations. If you depend on strict source compatibility, wait for a later prerelease.

### Motivation:
- Update Whoosh to have type annotations
  - Aim for 100% standard Pyright conformance, and as much strict Pyright conformance as possible
- Proposals for feature changes and additions
  - Probably will not be merged into `main` without external consultation

--------------------------------------

The original fork readme follows.

--------------------------------------

About Whoosh
============

Whoosh is a fast, featureful full-text indexing and searching library
implemented in pure Python. Programmers can use it to easily add search
functionality to their applications and websites. Every part of how Whoosh
works can be extended or replaced to meet your needs exactly.

Some of Whoosh's features include:

* Pythonic API.
* Pure-Python. No compilation or binary packages are needed, no mysterious crashes.
* Fielded indexing and search.
* Fast indexing and retrieval -- faster than any other pure-Python, scoring,
  full-text search solution I know of.
* Pluggable scoring algorithm (including BM25F), text analysis, storage,
  posting format, etc.
* Powerful query language.
* Pure Python spell-checker (as far as I know, the only one).

Whoosh might be useful in the following circumstances:

* Anywhere a pure-Python solution is desirable to avoid having to build/compile
  native libraries (or force users to build/compile them).
* As a research platform (at least for programmers who find Python easier to
  read and work with Java ;)
* When an easy-to-use Pythonic interface is more important to you than raw
  speed.

Whoosh was created by Matt Chaput. It was originally created for use in the online help system of Side Effects Software's 3D animation software Houdini. Side Effects Software Inc. graciously agreed to open-source the code.

This software is licensed under the terms of the simplified BSD (A.K.A. "two
clause" or "FreeBSD") license. See LICENSE.txt for information.

Installing Whoosh
=================

If you have ``pip`` installed, you can use it to download and install
Whoosh automatically::

    # Install the alpha release from PyPI
    $ pip install whoosh-novo

    # Install the development version from GitHub.
    $ pip install git+https://github.com/de-odex/whoosh-novo.git

Getting the source.
==================

You can check out the latest version of the source code on GitHub using git:

    $ git clone https://github.com/de-odex/whoosh-novo.git

Contributing
============

We use pre-commit to format the code and run some checks before committing to avoid common mistakes. To install it, run the following commands:

```bash
$ pip install pre-commit
$ pre-commit install
```

Learning more
=============

* Online Documentation:

  * [GitHub Pages](https://de-odex.github.io/whoosh-novo/)

* File bug reports and issues at https://github.com/de-odex/whoosh-novo/issues
