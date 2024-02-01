#!python

import os.path
import sys

from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand

try:
    import pytest
except ImportError:
    pytest = None

sys.path.insert(0, os.path.abspath("src"))
from whoosh_reloaded import versionstring


class PyTest(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        # import here, cause outside the eggs aren't loaded
        import pytest

        pytest.main(self.test_args)


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
        long_description=open("README.md", "r").read(),
        long_description_content_type="text/markdown",
        license="Two-clause BSD license",
        keywords="index search text spell",
        url="https://github.com/Sygil-Dev/whoosh-reloaded",
        zip_safe=True,
        install_requires=[
            "cached-property",
        ],
        tests_require=[
            "pytest",
            "nose",
        ],
        cmdclass={"test": PyTest},
        classifiers=[
            "Programming Language :: Python :: 3",
            "Development Status :: 5 - Production/Stable",
            "Intended Audience :: Developers",
            "License :: OSI Approved :: BSD License",
            "Natural Language :: English",
            "Operating System :: OS Independent",
            "Programming Language :: Python :: 2.7",
            "Programming Language :: Python :: 3.4",
            "Programming Language :: Python :: 3.5",
            "Programming Language :: Python :: 3.6",
            "Programming Language :: Python :: 3.7",
            "Programming Language :: Python :: 3.8",
            "Programming Language :: Python :: 3.9",
            "Programming Language :: Python :: 3.10",
            "Programming Language :: Python :: 3.11",
            "Programming Language :: Python :: 3.12",
            "Topic :: Software Development :: Libraries :: Python Modules",
            "Topic :: Text Processing :: Indexing",
        ],
    )
