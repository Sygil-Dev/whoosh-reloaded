[![CodeFactor](https://www.codefactor.io/repository/github/de-odex/whoosh-novo/badge)](https://www.codefactor.io/repository/github/de-odex/whoosh-novo)
[![codecov](https://codecov.io/gh/de-odex/whoosh-novo/graph/badge.svg?token=RZ426ERMZD)](https://codecov.io/gh/de-odex/whoosh-novo)
<!-- [![Documentation Status](https://readthedocs.org/projects/whoosh-reloaded/badge/?version=latest)](https://whoosh-reloaded.readthedocs.io/en/latest/?badge=latest) -->
<!-- [![PyPI version](https://badge.fury.io/py/Whoosh-Reloaded.svg)](https://badge.fury.io/py/Whoosh-Reloaded) [![Downloads](https://pepy.tech/badge/whoosh-reloaded)](https://pepy.tech/project/whoosh-reloaded) [![License](https://img.shields.io/pypi/l/Whoosh-Reloaded)](https://pypi.org/project/Whoosh-Reloaded/) [![PyPI - Python Version](https://img.shields.io/pypi/pyversions/Whoosh-Reloaded)](https://pypi.org/project/Whoosh-Reloaded/) [![PyPI - Wheel](https://img.shields.io/pypi/wheel/Whoosh-Reloaded)](https://pypi.org/project/Whoosh-Reloaded/) [![PyPI - Format](https://img.shields.io/pypi/format/Whoosh-Reloaded)](https://pypi.org/project/Whoosh-Reloaded/) [![PyPI - Status](https://img.shields.io/pypi/status/Whoosh-Reloaded)](https://pypi.org/project/Whoosh-Reloaded/) -->

<!-- [![Lines of Code](https://sonarcloud.io/api/project_badges/measure?project=Sygil-Dev_whoosh-reloaded&metric=ncloc)](https://sonarcloud.io/summary/new_code?id=Sygil-Dev_whoosh-reloaded) -->
<!-- [![Code Smells](https://sonarcloud.io/api/project_badges/measure?project=Sygil-Dev_whoosh-reloaded&metric=code_smells)](https://sonarcloud.io/summary/new_code?id=Sygil-Dev_whoosh-reloaded) -->
<!-- [![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=Sygil-Dev_whoosh-reloaded&metric=sqale_rating)](https://sonarcloud.io/summary/new_code?id=Sygil-Dev_whoosh-reloaded) -->
<!-- [![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=Sygil-Dev_whoosh-reloaded&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=Sygil-Dev_whoosh-reloaded) -->
<!-- [![Bugs](https://sonarcloud.io/api/project_badges/measure?project=Sygil-Dev_whoosh-reloaded&metric=bugs)](https://sonarcloud.io/summary/new_code?id=Sygil-Dev_whoosh-reloaded) -->
<!-- [![Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=Sygil-Dev_whoosh-reloaded&metric=vulnerabilities)](https://sonarcloud.io/summary/new_code?id=Sygil-Dev_whoosh-reloaded) -->
<!-- [![Duplicated Lines (%)](https://sonarcloud.io/api/project_badges/measure?project=Sygil-Dev_whoosh-reloaded&metric=duplicated_lines_density)](https://sonarcloud.io/summary/new_code?id=Sygil-Dev_whoosh-reloaded) -->
<!-- [![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=Sygil-Dev_whoosh-reloaded&metric=reliability_rating)](https://sonarcloud.io/summary/new_code?id=Sygil-Dev_whoosh-reloaded) -->
<!-- [![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=Sygil-Dev_whoosh-reloaded&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=Sygil-Dev_whoosh-reloaded) -->
<!-- [![Technical Debt](https://sonarcloud.io/api/project_badges/measure?project=Sygil-Dev_whoosh-reloaded&metric=sqale_index)](https://sonarcloud.io/summary/new_code?id=Sygil-Dev_whoosh-reloaded) -->

<!-- -------------------------------------- -->

> [!IMPORTANT]
> This repository (**whoosh-novo**) is a fork and continuation of the Whoosh project, succeeding [Whoosh-Reloaded](https://github.com/Sygil-Dev/whoosh-reloaded) which is no longer maintained.

> [!WARNING]
> Compatibility will be best-effort, limited to maintained python versions (>=3.9). I am an amateur solo developer; mistakes will probably be made more often than usual.

### Motivation:
- update whoosh to have type annotations
  - aim for 100% standard pyright conformance, and as much strict pyright conformance as possible
- proposals for feature changes and additions
  - probably will not be merged into `main` without external consultation

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

    # Install the stable version from PyPI
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
