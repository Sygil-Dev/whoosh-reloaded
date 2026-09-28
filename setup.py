#!python

import os.path
import sys

from setuptools import find_packages, setup

sys.path.insert(0, os.path.abspath("src"))
from whoosh import versionstring


if __name__ == "__main__":
    setup(
        name="Whoosh-Reloaded",
        version=versionstring(),
        package_dir={"": "src"},
        packages=find_packages("src"),
        author="Matt Chaput",
        author_email="matt@whoosh.ca",
        maintainer="Sygil-Dev",
        description="Fast, pure-Python full text indexing, search, and spell checking library.",
        long_description=open("README.md").read(),
        long_description_content_type="text/markdown",
        license="Two-clause BSD license",
        keywords="index search text spell",
        url="https://github.com/Sygil-Dev/whoosh-reloaded",
        zip_safe=True,
        install_requires=[
            "cached-property==2.0.1",
            "loguru==0.7.3",
        ],
        python_requires=">=3.8",
        classifiers=[
            "Programming Language :: Python :: 3",
            "Development Status :: 5 - Production/Stable",
            "Intended Audience :: Developers",
            "License :: OSI Approved :: BSD License",
            "Natural Language :: English",
            "Operating System :: OS Independent",
            "Programming Language :: Python :: 3.8",
            "Programming Language :: Python :: 3.9",
            "Programming Language :: Python :: 3.10",
            "Programming Language :: Python :: 3.11",
            "Programming Language :: Python :: 3.12",
            "Programming Language :: Python :: 3.13",
            "Programming Language :: Python :: 3.14",
            "Topic :: Software Development :: Libraries :: Python Modules",
            "Topic :: Text Processing :: Indexing",
        ],
    )
