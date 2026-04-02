# Copyright 2007 Matt Chaput. All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#    1. Redistributions of source code must retain the above copyright notice,
#       this list of conditions and the following disclaimer.
#
#    2. Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY MATT CHAPUT ``AS IS'' AND ANY EXPRESS OR
# IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
# EVENT SHALL MATT CHAPUT OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
# OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
# LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
# NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
# EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
# The views and conclusions contained in the software and documentation are
# those of the authors and should not be interpreted as representing official
# policies, either expressed or implied, of Matt Chaput.

import os.path
import shutil
import sys
import tempfile
from collections.abc import Generator
from collections.abc import Set as AbstractSet
from contextlib import contextmanager
from types import TracebackType
from typing import TYPE_CHECKING, Any

from whoosh.filedb.filestore import FileStorage
from whoosh.util import now, random_name

if TYPE_CHECKING:
    from whoosh.fields import Schema
    from whoosh.index import Index


SuppressedExceptions = AbstractSet[type[BaseException]]


class TempDir:
    basename: str
    parentdir: str | None
    dir: str
    suppress: SuppressedExceptions
    keepdir: bool

    def __init__(
        self,
        basename: str = "",
        parentdir: str | None = None,
        ext: str = ".whoosh",
        suppress: SuppressedExceptions = frozenset(),
        keepdir: bool = False,
    ) -> None:
        self.basename = basename or random_name(8)
        self.parentdir = parentdir

        dirname = parentdir or tempfile.mkdtemp(ext, self.basename)
        self.dir = os.path.abspath(dirname)
        self.suppress = suppress
        self.keepdir = keepdir

    def __enter__(self) -> str:
        if not os.path.exists(self.dir):
            os.makedirs(self.dir)
        return self.dir

    def cleanup(self) -> None:
        pass

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool | None:
        self.cleanup()
        if not self.keepdir:
            try:
                shutil.rmtree(self.dir)
            except OSError:
                pass
                # e = sys.exc_info()[1]
                # sys.stderr.write("Can't remove temp dir: " + str(e) + "\n")
                # if exc_type is None:
                #    raise

        if exc_type is not None:
            if self.keepdir:
                sys.stderr.write("Temp dir=" + self.dir + "\n")
            if exc_type not in self.suppress:
                return False


class TempStorage:
    basename: str
    dir: TempDir
    store: FileStorage | None

    def __init__(
        self,
        basename: str = "",
        parentdir: str | None = None,
        ext: str = ".whoosh",
        suppress: SuppressedExceptions = frozenset(),
        keepdir: bool = False,
        debug: bool = False,
    ) -> None:
        self.dir = TempDir(basename, parentdir, ext, suppress, keepdir)
        self.basename = self.dir.basename
        self._debug = debug
        self.store = None

    def __enter__(self) -> FileStorage:
        dirpath = self.dir.__enter__()
        try:
            self.store = FileStorage(dirpath, debug=self._debug)
            return self.store
        except BaseException:
            self.dir.__exit__(*sys.exc_info())
            raise

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool | None:
        store = self.store
        self.store = None
        try:
            if store is not None:
                store.close()
        except BaseException:
            if not self.dir.__exit__(*sys.exc_info()):
                raise
            return True
        return self.dir.__exit__(exc_type, exc_val, exc_tb)


class TempIndex:
    basename: str
    storage: TempStorage
    schema: "Schema"
    index: "Index | None"

    def __init__(
        self,
        schema: "Schema",
        ixname: str = "",
        storage_debug: bool = False,
        parentdir: str | None = None,
        ext: str = ".whoosh",
        suppress: SuppressedExceptions = frozenset(),
        keepdir: bool = False,
    ) -> None:
        self.storage = TempStorage(
            basename=ixname,
            parentdir=parentdir,
            ext=ext,
            suppress=suppress,
            keepdir=keepdir,
            debug=storage_debug,
        )
        self.basename = self.storage.basename
        self.schema = schema
        self.index = None

    def __enter__(self) -> "Index":
        store = self.storage.__enter__()
        try:
            self.index = store.create_index(self.schema, indexname=self.basename)
            return self.index
        except BaseException:
            self.storage.__exit__(*sys.exc_info())
            raise

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool | None:
        index = self.index
        self.index = None
        try:
            if index is not None:
                index.close()
        except BaseException:
            if not self.storage.__exit__(*sys.exc_info()):
                raise
            return True
        return self.storage.__exit__(exc_type, exc_val, exc_tb)


def is_abstract_method(attr: object) -> bool:
    """Returns True if the given object has __isabstractmethod__ == True."""

    return bool(getattr(attr, "__isabstractmethod__", False))


def check_abstract_methods(base: type[Any], subclass: type[Any]) -> None:
    """Raises AssertionError if ``subclass`` does not override a method on
    ``base`` that is marked as an abstract method.
    """

    for attrname in dir(base):
        if attrname.startswith("_"):
            continue
        attr = getattr(base, attrname)
        if is_abstract_method(attr):
            oattr = getattr(subclass, attrname)
            if is_abstract_method(oattr):
                raise Exception(f"{subclass.__name__}.{attrname} not overridden")


@contextmanager
def timing(name: str | None = None) -> Generator[None, None, None]:
    t = now()
    yield
    t = now() - t
    print(f"{name or ''}: {t:0.06f} s")
