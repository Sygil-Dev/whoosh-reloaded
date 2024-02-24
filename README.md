[![CodeFactor](https://www.codefactor.io/repository/github/sygil-dev/whoosh-reloaded/badge/main)](https://www.codefactor.io/repository/github/sygil-dev/whoosh-reloaded/overview/main)
[![codecov](https://codecov.io/gh/Sygil-Dev/whoosh-reloaded/graph/badge.svg?token=O3Z2DFB8UA)](https://codecov.io/gh/Sygil-Dev/whoosh-reloaded)
[![Documentation Status](https://readthedocs.org/projects/whoosh-reloaded/badge/?version=latest)](https://whoosh-reloaded.readthedocs.io/en/latest/?badge=latest)
[![PyPI version](https://badge.fury.io/py/Whoosh-Reloaded.svg)](https://badge.fury.io/py/Whoosh-Reloaded) [![Downloads](https://pepy.tech/badge/whoosh-reloaded)](https://pepy.tech/project/whoosh-reloaded) [![License](https://img.shields.io/pypi/l/Whoosh-Reloaded)](https://pypi.org/project/Whoosh-Reloaded/) [![PyPI - Python Version](https://img.shields.io/pypi/pyversions/Whoosh-Reloaded)](https://pypi.org/project/Whoosh-Reloaded/) [![PyPI - Wheel](https://img.shields.io/pypi/wheel/Whoosh-Reloaded)](https://pypi.org/project/Whoosh-Reloaded/) [![PyPI - Format](https://img.shields.io/pypi/format/Whoosh-Reloaded)](https://pypi.org/project/Whoosh-Reloaded/) [![PyPI - Status](https://img.shields.io/pypi/status/Whoosh-Reloaded)](https://pypi.org/project/Whoosh-Reloaded/)

[![Lines of Code](https://sonarcloud.io/api/project_badges/measure?project=Sygil-Dev_whoosh-reloaded&metric=ncloc)](https://sonarcloud.io/summary/new_code?id=Sygil-Dev_whoosh-reloaded)
[![Code Smells](https://sonarcloud.io/api/project_badges/measure?project=Sygil-Dev_whoosh-reloaded&metric=code_smells)](https://sonarcloud.io/summary/new_code?id=Sygil-Dev_whoosh-reloaded)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=Sygil-Dev_whoosh-reloaded&metric=sqale_rating)](https://sonarcloud.io/summary/new_code?id=Sygil-Dev_whoosh-reloaded)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=Sygil-Dev_whoosh-reloaded&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=Sygil-Dev_whoosh-reloaded)
[![Bugs](https://sonarcloud.io/api/project_badges/measure?project=Sygil-Dev_whoosh-reloaded&metric=bugs)](https://sonarcloud.io/summary/new_code?id=Sygil-Dev_whoosh-reloaded)
[![Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=Sygil-Dev_whoosh-reloaded&metric=vulnerabilities)](https://sonarcloud.io/summary/new_code?id=Sygil-Dev_whoosh-reloaded)
[![Duplicated Lines (%)](https://sonarcloud.io/api/project_badges/measure?project=Sygil-Dev_whoosh-reloaded&metric=duplicated_lines_density)](https://sonarcloud.io/summary/new_code?id=Sygil-Dev_whoosh-reloaded)
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=Sygil-Dev_whoosh-reloaded&metric=reliability_rating)](https://sonarcloud.io/summary/new_code?id=Sygil-Dev_whoosh-reloaded)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=Sygil-Dev_whoosh-reloaded&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=Sygil-Dev_whoosh-reloaded)
[![Technical Debt](https://sonarcloud.io/api/project_badges/measure?project=Sygil-Dev_whoosh-reloaded&metric=sqale_index)](https://sonarcloud.io/summary/new_code?id=Sygil-Dev_whoosh-reloaded)

--------------------------------------

> **Notice:** This repository (**whoosh-reloaded**) is a fork and continuation of the Whoosh project.

> This fork **is actively maintained** by the Sygil-Dev Organization.

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

Whoosh was created by Matt Chaput and is maintained currently by the Sygil-Dev Organization. It was created for use in the online help system of Side Effects Software's 3D animation software Houdini. Side Effects Software Inc. graciously agreed to open-source the code.

This software is licensed under the terms of the simplified BSD (A.K.A. "two
clause" or "FreeBSD") license. See LICENSE.txt for information.

Installing Whoosh
=================

If you have ``setuptools`` or ``pip`` installed, you can use ``easy_install``
or ``pip`` to download and install Whoosh automatically::

    # Install the stable version from Pypi
    $ pip install whoosh-reloaded

    # Install the development version from GitHub.
    $ pip install git+https://github.com/Sygil-Dev/whoosh-reloaded.git

Getting the source.
==================

You can check out the latest version of the source code on GitHub using git:

    $ git clone https://github.com/Sygil-Dev/whoosh-reloaded.git

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

  *   [GitHub Pages](https://sygil-dev.github.io/whoosh-reloaded/)

  *   [Read the Docs](https://whoosh-reloaded.readthedocs.io/en/latest/)

* Read the old online documentation at https://docs.red-dove.com/whoosh-reloaded/ (Search work properly).

* Read the old online documentation at https://whoosh-reloaded.readthedocs.org/en/latest/ (Search DOES NOT work).

* File bug reports and issues at https://github.com/Sygil-Dev/whoosh-reloaded/issues

Maintainers
===========

* [Sygil-Dev Organization](https://github.com/Sygil-Dev)
* [ZeroCool940711](https://github.com/ZeroCool940711)

Discord Server
==============

- [Sygil-Dev - Resources](https://discord.gg/H5mftKP5S9)
