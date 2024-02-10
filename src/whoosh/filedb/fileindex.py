# ===============================================================================
# Copyright 2009 Matt Chaput
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ===============================================================================

import pickle
import re
from bisect import bisect_right
from threading import Lock
from time import time

from whoosh import __version__
from whoosh.fields import Schema
from whoosh.index import (
    _DEF_INDEX_NAME,
    EmptyIndexError,
    Index,
    IndexVersionError,
    LockError,
    OutOfDateError,
)
from whoosh.system import _FLOAT_SIZE, _INT_SIZE

_INDEX_VERSION = -105


# A mix-in that adds methods for deleting
# documents from self.segments. These methods are on IndexWriter as
# well as Index for convenience, so they're broken out here.


class SegmentDeletionMixin:
    """Mix-in for classes that support deleting documents from self.segments."""

    def delete_document(self, docnum, delete=True):
        """Deletes a document by number."""
        self.segments.delete_document(docnum, delete=delete)

    def deleted_count(self):
        """Returns the total number of deleted documents in this index."""
        return self.segments.deleted_count()

    def is_deleted(self, docnum):
        """Returns True if a given document number is deleted but
        not yet optimized out of the index.
        """
        return self.segments.is_deleted(docnum)

    def has_deletions(self):
        """Returns True if this index has documents that are marked
        deleted but haven't been optimized out of the index yet.
        """
        return self.segments.has_deletions()


class FileIndex(SegmentDeletionMixin, Index):
    def __init__(self, storage, schema, create=False, indexname=_DEF_INDEX_NAME):
        """
        Represents an index stored in a file-based storage.

        Args:
            storage (Storage): The storage object used to store the index files.
            schema (Schema): The schema object defining the fields and their types in the index.
            create (bool, optional): Whether to create a new index. Defaults to False.
            indexname (str, optional): The name of the index. Defaults to _DEF_INDEX_NAME.

        Raises:
            ValueError: If the provided schema is not a Schema object.
            IndexError: If create is True but no schema is specified.
            EmptyIndexError: If the index does not exist in the storage.
        """
        self.storage = storage
        self.indexname = indexname

        if schema is not None and not isinstance(schema, Schema):
            raise ValueError(f"{schema!r} is not a Schema object")

        self.generation = self.latest_generation()

        if create:
            if schema is None:
                raise IndexError("To create an index you must specify a schema")

            self.schema = schema
            self.generation = 0
            self.segment_counter = 0
            self.segments = SegmentSet()

            # Clear existing files
            prefix = f"_{self.indexname}_"
            for filename in self.storage:
                if filename.startswith(prefix):
                    storage.delete_file(filename)

            self._write()
        elif self.generation >= 0:
            self._read(schema)
        else:
            raise EmptyIndexError(
                f"No index named {indexname!r} in storage {storage!r}"
            )

        # Open a reader for this index. This is used by the
        # deletion methods, but mostly it's to keep the underlying
        # files open so they don't get deleted from underneath us.
        self._acquire_readlocks()

        self.segment_num_lock = None

    def __repr__(self):
        """
        Returns a string representation of the FileIndex object.

        Returns:
            str: The string representation of the FileIndex object.
        """
        return f"{self.__class__.__name__}({self.storage!r}, {self.indexname!r})"

    def _acquire_readlocks(self):
        """
        Acquires read locks on the segment files.

        This is used to keep the underlying files open so they don't get deleted from underneath us.
        """
        self._readlocks = [
            self.storage.open_file(name, mapped=False)
            for name in self.segments.filenames()
            if self.storage.file_exists(name)
        ]

    def _release_readlocks(self):
        """
        Releases the read locks on the segment files.
        """
        (f.close() for f in self._readlocks)
        self._readlocks = []

    def close(self):
        """
        Closes the FileIndex object by releasing the read locks on the segment files.
        """
        self._release_readlocks()

    def latest_generation(self):
        """
        Returns the latest generation number of the index files.

        Returns:
            int: The latest generation number of the index files.
        """
        pattern = _toc_pattern(self.indexname)

        maximum = -1
        for filename in self.storage:
            m = pattern.match(filename)
            if m:
                num = int(m.group(1))
                if num > maximum:
                    maximum = num
        return maximum

    def refresh(self):
        """
        Refreshes the FileIndex object by creating a new instance with the same storage and schema.

        Returns:
            FileIndex: The refreshed FileIndex object.
        """
        if not self.up_to_date():
            return self.__class__(self.storage, self.schema, indexname=self.indexname)
        else:
            return self

    def up_to_date(self):
        """
        Checks if the FileIndex object is up to date.

        Returns:
            bool: True if the FileIndex object is up to date, False otherwise.
        """
        return self.generation == self.latest_generation()

    def _write(self):
        """
        Writes the content of this index to the .toc file.
        """
        self.schema.clean()
        # stream = self.storage.create_file(self._toc_filename())

        # Use a temporary file for atomic write.
        tocfilename = self._toc_filename()
        tempfilename = f"{tocfilename}.{time()}"
        stream = self.storage.create_file(tempfilename)

        stream.write_varint(_INT_SIZE)
        stream.write_varint(_FLOAT_SIZE)
        stream.write_int(-12345)

        stream.write_int(_INDEX_VERSION)
        for num in __version__[:3]:
            stream.write_varint(num)

        stream.write_string(pickle.dumps(self.schema, -1))
        stream.write_int(self.generation)
        stream.write_int(self.segment_counter)
        stream.write_pickle(self.segments)
        stream.close()

        # Rename temporary file to the proper filename
        self.storage.rename_file(tempfilename, self._toc_filename(), safe=True)

    def _read(self, schema):
        """
        Reads the content of this index from the .toc file.

        Args:
            schema (Schema): The schema object to use. If None, the pickled schema from the saved index will be loaded.

        Raises:
            IndexError: If the index was created on an architecture with different data sizes.
            IndexError: If there is a byte order problem.
            IndexVersionError: If the format of the index is not supported.

        """
        stream = self.storage.open_file(self._toc_filename())

        if stream.read_varint() != _INT_SIZE or stream.read_varint() != _FLOAT_SIZE:
            raise IndexError(
                "Index was created on an architecture with different data sizes"
            )

        if stream.read_int() != -12345:
            raise IndexError("Number misread: byte order problem")

        version = stream.read_int()
        if version != _INDEX_VERSION:
            raise IndexVersionError(f"Can't read format {version}", version)
        self.version = version
        self.release = (
            stream.read_varint(),
            stream.read_varint(),
            stream.read_varint(),
        )

        # If the user supplied a schema object with the constructor, don't load
        # the pickled schema from the saved index.
        if schema:
            self.schema = schema
            stream.skip_string()
        else:
            self.schema = pickle.loads(stream.read_string())

        generation = stream.read_int()
        assert generation == self.generation
        self.segment_counter = stream.read_int()
        self.segments = stream.read_pickle()
        stream.close()

    def _next_segment_name(self):
        """
        Returns the name of the next segment in sequence.

        Returns:
            str: The name of the next segment in sequence.

        Raises:
            LockError: If the segment number lock cannot be acquired.
        """
        if self.segment_num_lock is None:
            self.segment_num_lock = Lock()

        if self.segment_num_lock.acquire():
            try:
                self.segment_counter += 1
                return f"_{self.indexname}_{self.segment_counter}"
            finally:
                self.segment_num_lock.release()
        else:
            raise LockError

    def _toc_filename(self):
        """
        Returns the computed filename of the TOC (Table of Contents) for this index name and generation.

        Returns:
            str: The computed filename of the TOC for this index name and generation.
        """
        return f"_{self.indexname}_{self.generation}.toc"

    def last_modified(self):
        """
        Returns the last modified timestamp of the TOC file.

        Returns:
            float: The last modified timestamp of the TOC file.
        """
        return self.storage.file_modified(self._toc_filename())

    def is_empty(self):
        """
        Checks if the index is empty.

        Returns:
            bool: True if the index is empty, False otherwise.
        """
        return len(self.segments) == 0

    def segment_count(self):
        """
        Returns the number of segments in the index.

        Returns:
            int: The number of segments in the index.
        """
        return len(self.segments)

    def optimize(self):
        """
        Optimizes the index by merging segments if necessary.

        This operation improves search performance.

        Note:
            This method only performs optimization if there are more than 1 segments and no deletions.

        """
        if len(self.segments) < 2 and not self.segments.has_deletions():
            return

        from whoosh.filedb.filewriting import OPTIMIZE

        w = self.writer()
        w.commit(OPTIMIZE)

    def commit(self, new_segments=None):
        """
        Commits changes to the index.

        Args:
            new_segments (SegmentSet, optional): The new segments to replace the existing segments in the index.

        Raises:
            OutOfDateError: If the index is not up to date.
            ValueError: If new_segments is provided but is not a SegmentSet.

        """
        self._release_readlocks()

        if not self.up_to_date():
            raise OutOfDateError

        if new_segments:
            if not isinstance(new_segments, SegmentSet):
                raise ValueError(
                    "FileIndex.commit() called with something other than a SegmentSet: %r"
                    % new_segments
                )
            self.segments = new_segments

        self.generation += 1
        self._write()
        self._clean_files()

        self._acquire_readlocks()

    def _clean_files(self):
        """
        Attempts to remove unused index files.

        This method is called when a new generation is created.
        If existing Index and/or reader objects have the files open, they may not be deleted immediately (i.e. on Windows)
        but will probably be deleted eventually by a later call to clean_files.
        """
        storage = self.storage
        current_segment_names = {s.name for s in self.segments}

        tocpattern = _toc_pattern(self.indexname)
        segpattern = _segment_pattern(self.indexname)

        todelete = set()
        for filename in storage:
            tocm = tocpattern.match(filename)
            segm = segpattern.match(filename)
            if tocm:
                if int(tocm.group(1)) != self.generation:
                    todelete.add(filename)
            elif segm:
                name = segm.group(1)
                if name not in current_segment_names:
                    todelete.add(filename)

        for filename in todelete:
            try:
                storage.delete_file(filename)
            except OSError:
                # Another process still has this file open
                pass

    def doc_count_all(self):
        """
        Returns the total number of documents in the index, including deleted documents.

        Returns:
            int: The total number of documents in the index, including deleted documents.
        """
        return self.segments.doc_count_all()

    def doc_count(self):
        """
        Returns the number of non-deleted documents in the index.

        Returns:
            int: The number of non-deleted documents in the index.
        """
        return self.segments.doc_count()

    def field_length(self, fieldnum):
        """
        Returns the total length of a field in the index.

        Args:
            fieldnum (int): The field number.

        Returns:
            int: The total length of the field in the index.
        """
        return sum(s.field_length(fieldnum) for s in self.segments)

    def reader(self):
        """
        Returns a reader object for the index.

        Returns:
            IndexReader: The reader object for the index.
        """
        return self.segments.reader(self.storage, self.schema)

    def writer(self, **kwargs):
        """
        Returns a writer object for the index.

        Args:
            **kwargs: Additional keyword arguments to pass to the writer constructor.

        Returns:
            IndexWriter: The writer object for the index.
        """
        from whoosh.filedb.filewriting import SegmentWriter

        return SegmentWriter(self, **kwargs)


# SegmentSet object


class SegmentSet:
    """
    This class is used by the Index object to keep track of the segments in the index.

    Attributes:
        segments (list): A list of segments in the index.
        _doc_offsets (list): A list of document offsets for each segment.

    Methods:
        __init__(segments=None): Initializes a new instance of the SegmentSet class.
        __repr__(): Returns a string representation of the segments in the set.
        __len__(): Returns the number of segments in this set.
        __iter__(): Returns an iterator over the segments in this set.
        __getitem__(n): Returns the segment at the specified index.
        append(segment): Adds a segment to this set.
        _document_segment(docnum): Returns the index.Segment object containing the given document number.
        _segment_and_docnum(docnum): Returns an (index.Segment, segment_docnum) pair for the segment containing the given document number.
        copy(): Returns a deep copy of this set.
        filenames(): Returns a set of filenames associated with the segments in this set.
        doc_offsets(): Recomputes the document offset list.
        doc_count_all(): Returns the total number of documents, DELETED or UNDELETED, in this set.
        doc_count(): Returns the number of undeleted documents in this set.
        has_deletions(): Returns True if this index has documents that are marked deleted but haven't been optimized out of the index yet.
        delete_document(docnum, delete=True): Deletes a document by number.
        deleted_count(): Returns the total number of deleted documents in this index.
        is_deleted(docnum): Returns True if a given document number is deleted but not yet optimized out of the index.
        reader(storage, schema): Returns a reader object for accessing the segments in this set.

    """

    def __init__(self, segments=None):
        if segments is None:
            self.segments = []
        else:
            self.segments = segments

        self._doc_offsets = self.doc_offsets()

    def __repr__(self):
        return repr(self.segments)

    def __len__(self):
        """
        Returns the number of segments in this set.

        Returns:
            int: The number of segments in this set.
        """
        return len(self.segments)

    def __iter__(self):
        return iter(self.segments)

    def __getitem__(self, n):
        return self.segments.__getitem__(n)

    def append(self, segment):
        """
        Adds a segment to this set.

        Args:
            segment (object): The segment to be added.
        """
        self.segments.append(segment)
        self._doc_offsets = self.doc_offsets()

    def _document_segment(self, docnum):
        """
        Returns the index.Segment object containing the given document number.

        Args:
            docnum (int): The document number.

        Returns:
            int: The index of the segment containing the document.
        """
        offsets = self._doc_offsets
        if len(offsets) == 1:
            return 0
        return bisect_right(offsets, docnum) - 1

    def _segment_and_docnum(self, docnum):
        """
        Returns an (index.Segment, segment_docnum) pair for the segment containing the given document number.

        Args:
            docnum (int): The document number.

        Returns:
            tuple: A tuple containing the index.Segment object and the segment_docnum.
        """
        segmentnum = self._document_segment(docnum)
        offset = self._doc_offsets[segmentnum]
        segment = self.segments[segmentnum]
        return segment, docnum - offset

    def copy(self):
        """
        Returns a deep copy of this set.

        Returns:
            SegmentSet: A deep copy of this set.
        """
        return self.__class__([s.copy() for s in self.segments])

    def filenames(self):
        """
        Returns a set of filenames associated with the segments in this set.

        Returns:
            set: A set of filenames.
        """
        nameset = set()
        for segment in self.segments:
            nameset |= segment.filenames()
        return nameset

    def doc_offsets(self):
        """
        Recomputes the document offset list. This must be called if you change self.segments.

        Returns:
            list: A list of document offsets.
        """
        offsets = []
        base = 0
        for s in self.segments:
            offsets.append(base)
            base += s.doc_count_all()
        return offsets

    def doc_count_all(self):
        """
        Returns the total number of documents, DELETED or UNDELETED, in this set.

        Returns:
            int: The total number of documents.
        """
        return sum(s.doc_count_all() for s in self.segments)

    def doc_count(self):
        """
        Returns the number of undeleted documents in this set.

        Returns:
            int: The number of undeleted documents.
        """
        return sum(s.doc_count() for s in self.segments)

    def has_deletions(self):
        """
        Returns True if this index has documents that are marked deleted but haven't been optimized out of the index yet.

        Returns:
            bool: True if there are deleted documents, False otherwise.
        """
        return any(s.has_deletions() for s in self.segments)

    def delete_document(self, docnum, delete=True):
        """
        Deletes a document by number.

        Args:
            docnum (int): The document number.
            delete (bool, optional): Whether to mark the document as deleted. Defaults to True.
        """
        segment, segdocnum = self._segment_and_docnum(docnum)
        segment.delete_document(segdocnum, delete=delete)

    def deleted_count(self):
        """
        Returns the total number of deleted documents in this index.

        Returns:
            int: The total number of deleted documents.
        """
        return sum(s.deleted_count() for s in self.segments)

    def is_deleted(self, docnum):
        """
        Returns True if a given document number is deleted but not yet optimized out of the index.

        Args:
            docnum (int): The document number.

        Returns:
            bool: True if the document is deleted, False otherwise.
        """
        segment, segdocnum = self._segment_and_docnum(docnum)
        return segment.is_deleted(segdocnum)

    def reader(self, storage, schema):
        """
        Returns a reader object for accessing the segments in this set.

        Args:
            storage (object): The storage object.
            schema (object): The schema object.

        Returns:
            object: A reader object.
        """
        from whoosh.filedb.filereading import SegmentReader

        segments = self.segments
        if len(segments) == 1:
            return SegmentReader(storage, segments[0], schema)
        else:
            from whoosh.reading import MultiReader

            readers = [SegmentReader(storage, segment, schema) for segment in segments]
            return MultiReader(readers, schema)


class Segment:
    """Represents a segment in the index.

    Segments are used by the Index object to hold information about a segment.
    A segment is a real reverse index that stores a subset of the documents in the index.
    Multiple segments allow for quick incremental indexing and efficient searching.

    Attributes:
        name (str): The name of the segment.
        doccount (int): The maximum document number in the segment.
        fieldlength_totals (dict): A dictionary mapping field numbers to the total number of terms in that field across all documents in the segment.
        fieldlength_maxes (dict): A dictionary mapping field numbers to the maximum length of the field in any of the documents in the segment.
        deleted (set): A set of deleted document numbers, or None if no deleted documents exist in this segment.

    Methods:
        __init__(name, doccount, fieldlength_totals, fieldlength_maxes, deleted=None): Initializes a Segment object.
        __repr__(): Returns a string representation of the Segment object.
        copy(): Creates a copy of the Segment object.
        filenames(): Returns a set of filenames associated with the segment.
        doc_count_all(): Returns the total number of documents, deleted or undeleted, in this segment.
        doc_count(): Returns the number of undeleted documents in this segment.
        has_deletions(): Returns True if any documents in this segment are deleted.
        deleted_count(): Returns the total number of deleted documents in this segment.
        field_length(fieldnum, default=0): Returns the total number of terms in the given field across all documents in this segment.
        max_field_length(fieldnum, default=0): Returns the maximum length of the given field in any of the documents in the segment.
        delete_document(docnum, delete=True): Deletes or undeletes a document in the segment.
        is_deleted(docnum): Returns True if the given document number is deleted.
    """

    EXTENSIONS = {
        "fieldlengths": "dci",
        "storedfields": "dcz",
        "termsindex": "tiz",
        "termposts": "pst",
        "vectorindex": "fvz",
        "vectorposts": "vps",
    }

    def __init__(
        self, name, doccount, fieldlength_totals, fieldlength_maxes, deleted=None
    ):
        """
        Initializes a Segment object.

        Args:
            name (str): The name of the segment (the Index object computes this from its name and the generation).
            doccount (int): The maximum document number in the segment.
            fieldlength_totals (dict): A dictionary mapping field numbers to the total number of terms in that field across all documents in the segment.
            fieldlength_maxes (dict): A dictionary mapping field numbers to the maximum length of the field in any of the documents in the segment.
            deleted (set, optional): A set of deleted document numbers, or None if no deleted documents exist in this segment.
        """

        self.name = name
        self.doccount = doccount
        self.fieldlength_totals = fieldlength_totals
        self.fieldlength_maxes = fieldlength_maxes
        self.deleted = deleted

        self._filenames = set()
        for attr, ext in self.EXTENSIONS.items():
            fname = f"{self.name}.{ext}"
            setattr(self, attr + "_filename", fname)
            self._filenames.add(fname)

    def __repr__(self):
        """
        Returns a string representation of the Segment object.

        Returns:
            str: A string representation of the Segment object.
        """
        return f"{self.__class__.__name__}({self.name!r})"

    def copy(self):
        """
        Creates a copy of the Segment object.

        Returns:
            Segment: A copy of the Segment object.
        """
        if self.deleted:
            deleted = set(self.deleted)
        else:
            deleted = None
        return Segment(
            self.name,
            self.doccount,
            self.fieldlength_totals,
            self.fieldlength_maxes,
            deleted,
        )

    def filenames(self):
        """
        Returns a set of filenames associated with the segment.

        Returns:
            set: A set of filenames associated with the segment.
        """
        return self._filenames

    def doc_count_all(self):
        """
        Returns the total number of documents, deleted or undeleted, in this segment.

        Returns:
            int: The total number of documents in this segment.
        """
        return self.doccount

    def doc_count(self):
        """
        Returns the number of undeleted documents in this segment.

        Returns:
            int: The number of undeleted documents in this segment.
        """
        return self.doccount - self.deleted_count()

    def has_deletions(self):
        """
        Returns True if any documents in this segment are deleted.

        Returns:
            bool: True if any documents in this segment are deleted, False otherwise.
        """
        return self.deleted_count() > 0

    def deleted_count(self):
        """
        Returns the total number of deleted documents in this segment.

        Returns:
            int: The total number of deleted documents in this segment.
        """
        if self.deleted is None:
            return 0
        return len(self.deleted)

    def field_length(self, fieldnum, default=0):
        """
        Returns the total number of terms in the given field across all documents in this segment.

        Args:
            fieldnum (int): The internal number of the field.
            default (int, optional): The default value to return if the field number is not found.

        Returns:
            int: The total number of terms in the given field across all documents in this segment.
        """
        return self.fieldlength_totals.get(fieldnum, default)

    def max_field_length(self, fieldnum, default=0):
        """
        Returns the maximum length of the given field in any of the documents in the segment.

        Args:
            fieldnum (int): The internal number of the field.
            default (int, optional): The default value to return if the field number is not found.

        Returns:
            int: The maximum length of the given field in any of the documents in the segment.
        """
        return self.fieldlength_maxes.get(fieldnum, default)

    def delete_document(self, docnum, delete=True):
        """
        Deletes or undeletes the given document number.

        The document is not actually removed from the index until it is optimized.

        Args:
            docnum (int): The document number to delete or undelete.
            delete (bool, optional): If True, deletes the document. If False, undeletes a deleted document.

        Raises:
            KeyError: If the document number is already deleted or not deleted.
        """
        if delete:
            if self.deleted is None:
                self.deleted = set()
            elif docnum in self.deleted:
                raise KeyError(
                    f"Document {docnum} in segment {self.name!r} is already deleted"
                )

            self.deleted.add(docnum)
        else:
            if self.deleted is None or docnum not in self.deleted:
                raise KeyError(f"Document {docnum} is not deleted")

            self.deleted.clear(docnum)

    def is_deleted(self, docnum):
        """
        Returns True if the given document number is deleted.

        Args:
            docnum (int): The document number.

        Returns:
            bool: True if the given document number is deleted, False otherwise.
        """
        if self.deleted is None:
            return False
        return docnum in self.deleted


# Utility functions


def _toc_pattern(indexname):
    """
    Returns a regular expression object that matches TOC filenames.

    Parameters:
    indexname (str): The name of the index.

    Returns:
    re.Pattern: A regular expression object that matches TOC filenames.

    Example:
    >>> pattern = _toc_pattern("myindex")
    >>> pattern.match("_myindex_1.toc")
    <re.Match object; span=(0, 13), match='_myindex_1.toc'>
    >>> pattern.match("_myindex_2.toc")
    <re.Match object; span=(0, 13), match='_myindex_2.toc'>
    >>> pattern.match("_otherindex_1.toc")
    None
    """
    return re.compile(f"_{indexname}_([0-9]+).toc")


def _segment_pattern(indexname):
    """
    Returns a regular expression object that matches segment filenames.

    Args:
        indexname (str): The name of the index.

    Returns:
        re.Pattern: A regular expression object that matches segment filenames.

    Example:
        >>> pattern = _segment_pattern("my_index")
        >>> pattern.match("_my_index_001.fdt")
        <re.Match object; span=(0, 14), match='_my_index_001.fdt'>
    """
    return re.compile(f"(_{indexname}_[0-9]+).({Segment.EXTENSIONS.values()})")
