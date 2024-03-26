# Copyright 2012 Matt Chaput. All rights reserved.
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


from bisect import bisect_left
from threading import Lock

from whoosh.codec import base
from whoosh.matching import ListMatcher
from whoosh.reading import SegmentReader, TermInfo, TermNotFound
from whoosh.writing import SegmentWriter


class MemWriter(SegmentWriter):
    """
    A class for writing segments to memory.

    This class extends the `SegmentWriter` class and provides functionality
    for writing segments to memory instead of a file.

    Usage:
    writer = MemWriter()
    writer.commit()

    Args:
    mergetype (str, optional): The type of merge to perform during commit.
        Defaults to None.
    optimize (bool, optional): Whether to optimize the index during commit.
        Defaults to False.
    merge (bool, optional): Whether to perform a merge during commit.
        Defaults to True.
    """

    def commit(self, mergetype=None, optimize=False, merge=True):
        """
        Commits the changes made to the segment.

        This method finalizes the segment and performs any necessary
        operations, such as merging and optimization.

        Args:
        mergetype (str, optional): The type of merge to perform during commit.
            Defaults to None.
        optimize (bool, optional): Whether to optimize the index during commit.
            Defaults to False.
        merge (bool, optional): Whether to perform a merge during commit.
            Defaults to True.
        """
        self._finalize_segment()


class MemoryCodec(base.Codec):
    """
    Codec implementation for in-memory storage.

    This codec provides an in-memory storage solution for the Whoosh library.
    It uses a RamStorage object to store the index data.

    Usage:
    codec = MemoryCodec()
    writer = codec.writer(schema)
    reader = codec.reader(schema)
    per_doc_writer = codec.per_document_writer(storage, segment)
    field_writer = codec.field_writer(storage, segment)
    per_doc_reader = codec.per_document_reader(storage, segment)
    terms_reader = codec.terms_reader(storage, segment)
    new_segment = codec.new_segment(storage, indexname)
    """

    def __init__(self):
        """
        Initializes a MemoryCodec object.

        This method creates a RamStorage object to be used as the storage for the index data.
        It also initializes a MemSegment object.

        Parameters:
        None

        Returns:
        None
        """
        from whoosh.filedb.filestore import RamStorage

        self.storage = RamStorage()
        self.segment = MemSegment(self, "blah")

    def writer(self, schema):
        """
        Creates a writer object for the index.

        This method creates a MemWriter object for the given schema and returns it.

        Parameters:
        - schema (whoosh.fields.Schema): The schema for the index.

        Returns:
        - writer (MemWriter): The writer object for the index.
        """
        ix = self.storage.create_index(schema)
        return MemWriter(ix, _lk=False, codec=self, docbase=self.segment._doccount)

    def reader(self, schema):
        """
        Creates a reader object for the index.

        This method creates a SegmentReader object for the given schema and returns it.

        Parameters:
        - schema (whoosh.fields.Schema): The schema for the index.

        Returns:
        - reader (SegmentReader): The reader object for the index.
        """
        return SegmentReader(self.storage, schema, self.segment, codec=self)

    def per_document_writer(self, storage, segment):
        """
        Creates a per-document writer object.

        This method creates a MemPerDocWriter object for the given storage and segment and returns it.

        Parameters:
        - storage (RamStorage): The storage object for the index.
        - segment (MemSegment): The segment object for the index.

        Returns:
        - per_doc_writer (MemPerDocWriter): The per-document writer object.
        """
        return MemPerDocWriter(self.storage, self.segment)

    def field_writer(self, storage, segment):
        """
        Creates a field writer object.

        This method creates a MemFieldWriter object for the given storage and segment and returns it.

        Parameters:
        - storage (RamStorage): The storage object for the index.
        - segment (MemSegment): The segment object for the index.

        Returns:
        - field_writer (MemFieldWriter): The field writer object.
        """
        return MemFieldWriter(self.storage, self.segment)

    def per_document_reader(self, storage, segment):
        """
        Creates a per-document reader object.

        This method creates a MemPerDocReader object for the given storage and segment and returns it.

        Parameters:
        - storage (RamStorage): The storage object for the index.
        - segment (MemSegment): The segment object for the index.

        Returns:
        - per_doc_reader (MemPerDocReader): The per-document reader object.
        """
        return MemPerDocReader(self.storage, self.segment)

    def terms_reader(self, storage, segment):
        """
        Creates a terms reader object.

        This method creates a MemTermsReader object for the given storage and segment and returns it.

        Parameters:
        - storage (RamStorage): The storage object for the index.
        - segment (MemSegment): The segment object for the index.

        Returns:
        - terms_reader (MemTermsReader): The terms reader object.
        """
        return MemTermsReader(self.storage, self.segment)

    def new_segment(self, storage, indexname):
        """
        Creates a new segment object.

        This method returns the existing segment object.

        Parameters:
        - storage (RamStorage): The storage object for the index.
        - indexname (str): The name of the index.

        Returns:
        - segment (MemSegment): The segment object.
        """
        return self.segment


class MemPerDocWriter(base.PerDocWriterWithColumns):
    """
    A class that writes per-document data to memory.

    This class is responsible for writing per-document data, such as stored fields, field lengths, and vectors,
    to memory. It is used by the `MemoryCodec` to store document data in memory.

    Attributes:
        _storage (Storage): The storage object used to create files for storing column data.
        _segment (Segment): The segment object to which the per-document data is written.
        is_closed (bool): Indicates whether the writer has been closed.
        _colwriters (dict): A dictionary that maps field names to column writers.
        _doccount (int): The total number of documents written.

    Methods:
        _has_column(fieldname): Checks if a column with the given field name exists.
        _create_column(fieldname, column): Creates a new column for the given field name.
        _get_column(fieldname): Retrieves the column writer for the given field name.
        start_doc(docnum): Starts writing data for a new document.
        add_field(fieldname, fieldobj, value, length): Adds a field value and length to the current document.
        add_vector_items(fieldname, fieldobj, items): Adds vector items to the current document.
        finish_doc(): Finishes writing data for the current document.
        close(): Closes the writer and finishes writing any remaining data.
    """

    def __init__(self, storage, segment):
        """
        Initializes a new instance of the MemPerDocWriter class.

        Args:
            storage (Storage): The storage object used to create files for storing column data.
            segment (Segment): The segment object to which the per-document data is written.
        """
        self._storage = storage
        self._segment = segment
        self.is_closed = False
        self._colwriters = {}
        self._doccount = 0

    def _has_column(self, fieldname):
        """
        Checks if a column with the given field name exists.

        Args:
            fieldname (str): The name of the field.

        Returns:
            bool: True if the column exists, False otherwise.
        """
        return fieldname in self._colwriters

    def _create_column(self, fieldname, column):
        """
        Creates a new column for the given field name.

        Args:
            fieldname (str): The name of the field.
            column (Column): The column object used to write data to the column file.
        """
        colfile = self._storage.create_file(f"{fieldname}.c")
        self._colwriters[fieldname] = (colfile, column.writer(colfile))

    def _get_column(self, fieldname):
        """
        Retrieves the column writer for the given field name.

        Args:
            fieldname (str): The name of the field.

        Returns:
            ColumnWriter: The column writer object.
        """
        return self._colwriters[fieldname][1]

    def start_doc(self, docnum):
        """
        Starts writing data for a new document.

        Args:
            docnum (int): The document number.
        """
        self._doccount += 1
        self._docnum = docnum
        self._stored = {}
        self._lengths = {}
        self._vectors = {}

    def add_field(self, fieldname, fieldobj, value, length):
        """
        Adds a field value and length to the current document.

        Args:
            fieldname (str): The name of the field.
            fieldobj (Field): The field object.
            value: The field value.
            length: The field length.
        """
        if value is not None:
            self._stored[fieldname] = value
        if length is not None:
            self._lengths[fieldname] = length

    def add_vector_items(self, fieldname, fieldobj, items):
        """
        Adds vector items to the current document.

        Args:
            fieldname (str): The name of the field.
            fieldobj (Field): The field object.
            items (list): The vector items.
        """
        self._vectors[fieldname] = tuple(items)

    def finish_doc(self):
        """
        Finishes writing data for the current document.
        """
        with self._segment._lock:
            docnum = self._docnum
            self._segment._stored[docnum] = self._stored
            self._segment._lengths[docnum] = self._lengths
            self._segment._vectors[docnum] = self._vectors

    def close(self):
        """
        Closes the writer and finishes writing any remaining data.
        """
        colwriters = self._colwriters
        for fieldname in colwriters:
            colfile, colwriter = colwriters[fieldname]
            colwriter.finish(self._doccount)
            colfile.close()
        self.is_closed = True


class MemPerDocReader(base.PerDocumentReader):
    """
    A class that provides read access to per-document data stored in memory.

    This class is responsible for reading per-document data from a memory storage
    and a specific segment. It provides methods to retrieve information about the
    documents, columns, field lengths, vectors, and stored fields.

    Usage:
    1. Create an instance of MemPerDocReader by passing the storage and segment.
    2. Use the various methods to access the desired information.

    Example:
    ```
    storage = MemoryStorage()
    segment = MemorySegment()
    reader = MemPerDocReader(storage, segment)
    doc_count = reader.doc_count()
    has_deletions = reader.has_deletions()
    stored_fields = reader.stored_fields(0)
    reader.close()
    ```

    Note:
    - The storage object should implement the necessary methods for file operations.
    - The segment object should provide access to the per-document data.

    """

    def __init__(self, storage, segment):
        """
        Initialize a MemPerDocReader instance.

        Args:
        - storage: The storage object that provides file operations.
        - segment: The segment object that provides access to the per-document data.
        """
        self._storage = storage
        self._segment = segment

    def doc_count(self):
        """
        Get the number of documents in the segment.

        Returns:
        - The number of documents in the segment.
        """
        return self._segment.doc_count()

    def doc_count_all(self):
        """
        Get the total number of documents, including deleted documents.

        Returns:
        - The total number of documents.
        """
        return self._segment.doc_count_all()

    def has_deletions(self):
        """
        Check if the segment has deleted documents.

        Returns:
        - True if the segment has deleted documents, False otherwise.
        """
        return self._segment.has_deletions()

    def is_deleted(self, docnum):
        """
        Check if a document is deleted.

        Args:
        - docnum: The document number.

        Returns:
        - True if the document is deleted, False otherwise.
        """
        return self._segment.is_deleted(docnum)

    def deleted_docs(self):
        """
        Get the set of deleted document numbers.

        Returns:
        - A set containing the numbers of deleted documents.
        """
        return self._segment.deleted_docs()

    def supports_columns(self):
        """
        Check if the segment supports columns.

        Returns:
        - True if the segment supports columns, False otherwise.
        """
        return True

    def has_column(self, fieldname):
        """
        Check if a column exists for a given field.

        Args:
        - fieldname: The name of the field.

        Returns:
        - True if the column exists, False otherwise.
        """
        filename = f"{fieldname}.c"
        return self._storage.file_exists(filename)

    def column_reader(self, fieldname, column):
        """
        Get a reader for a specific column of a field.

        Args:
        - fieldname: The name of the field.
        - column: The column object.

        Returns:
        - A reader for the column.
        """
        filename = f"{fieldname}.c"
        colfile = self._storage.open_file(filename)
        length = self._storage.file_length(filename)
        return column.reader(colfile, 0, length, self._segment.doc_count_all())

    def doc_field_length(self, docnum, fieldname, default=0):
        """
        Get the length of a field in a specific document.

        Args:
        - docnum: The document number.
        - fieldname: The name of the field.
        - default: The default value to return if the field is not found.

        Returns:
        - The length of the field in the document, or the default value if not found.
        """
        return self._segment._lengths[docnum].get(fieldname, default)

    def field_length(self, fieldname):
        """
        Get the total length of a field across all documents.

        Args:
        - fieldname: The name of the field.

        Returns:
        - The total length of the field.
        """
        return sum(lens.get(fieldname, 0) for lens in self._segment._lengths.values())

    def min_field_length(self, fieldname):
        """
        Get the minimum length of a field across all documents.

        Args:
        - fieldname: The name of the field.

        Returns:
        - The minimum length of the field.
        """
        return min(
            lens[fieldname]
            for lens in self._segment._lengths.values()
            if fieldname in lens
        )

    def max_field_length(self, fieldname):
        """
        Get the maximum length of a field across all documents.

        Args:
        - fieldname: The name of the field.

        Returns:
        - The maximum length of the field.
        """
        return max(
            lens[fieldname]
            for lens in self._segment._lengths.values()
            if fieldname in lens
        )

    def has_vector(self, docnum, fieldname):
        """
        Check if a document has a vector for a given field.

        Args:
        - docnum: The document number.
        - fieldname: The name of the field.

        Returns:
        - True if the document has a vector for the field, False otherwise.
        """
        return (
            docnum in self._segment._vectors
            and fieldname in self._segment._vectors[docnum]
        )

    def vector(self, docnum, fieldname, format_):
        """
        Get a vector for a specific document and field.

        Args:
        - docnum: The document number.
        - fieldname: The name of the field.
        - format_: The format of the vector.

        Returns:
        - A ListMatcher object representing the vector.
        """
        items = self._segment._vectors[docnum][fieldname]
        ids, weights, values = zip(*items)
        return ListMatcher(ids, weights, values, format_)

    def stored_fields(self, docnum):
        """
        Get the stored fields of a specific document.

        Args:
        - docnum: The document number.

        Returns:
        - A dictionary containing the stored fields of the document.
        """
        return self._segment._stored[docnum]

    def close(self):
        """
        Close the MemPerDocReader.

        This method is intentionally left empty.
        """
        pass


class MemFieldWriter(base.FieldWriter):
    """
    The MemFieldWriter class is responsible for writing field data to memory.

    It provides methods for starting and finishing fields, terms, and adding data to the field.

    Attributes:
    - _storage: The storage object used for storing the field data.
    - _segment: The segment object representing the segment being written to.
    - _fieldname: The name of the current field being written.
    - _btext: The binary representation of the current term being written.
    - is_closed: A flag indicating whether the writer has been closed.

    Methods:
    - start_field(fieldname, fieldobj): Starts a new field.
    - start_term(btext): Starts a new term within the current field.
    - add(docnum, weight, vbytes, length): Adds data to the current term.
    - finish_term(): Finishes the current term.
    - finish_field(): Finishes the current field.
    - close(): Closes the writer.

    Usage:
    1. Create an instance of MemFieldWriter with the storage and segment objects.
    2. Call start_field() to start a new field.
    3. Call start_term() to start a new term within the field.
    4. Call add() to add data to the term.
    5. Call finish_term() to finish the term.
    6. Repeat steps 3-5 for additional terms within the field.
    7. Call finish_field() to finish the field.
    8. Repeat steps 2-7 for additional fields.
    9. Call close() to close the writer.

    Example:
    storage = ...
    segment = ...
    writer = MemFieldWriter(storage, segment)
    writer.start_field("title", fieldobj)
    writer.start_term(b"hello")
    writer.add(1, 0.5, 10, 5)
    writer.finish_term()
    writer.finish_field()
    writer.close()
    """

    def __init__(self, storage, segment):
        self._storage = storage
        self._segment = segment
        self._fieldname = None
        self._btext = None
        self.is_closed = False

    def start_field(self, fieldname, fieldobj):
        """
        Starts a new field.

        Args:
        - fieldname: The name of the field.
        - fieldobj: The field object representing the field.

        Raises:
        - ValueError: If start_field is called within a field.
        """
        if self._fieldname is not None:
            raise ValueError("Called start_field in a field")

        with self._segment._lock:
            invindex = self._segment._invindex
            if fieldname not in invindex:
                invindex[fieldname] = {}

        self._fieldname = fieldname
        self._fieldobj = fieldobj

    def start_term(self, btext):
        """
        Starts a new term within the current field.

        Args:
        - btext: The binary representation of the term.

        Raises:
        - ValueError: If start_term is called within a term.
        """
        if self._btext is not None:
            raise ValueError("Called start_term in a term")
        fieldname = self._fieldname

        fielddict = self._segment._invindex[fieldname]
        terminfos = self._segment._terminfos
        with self._segment._lock:
            if btext not in fielddict:
                fielddict[btext] = []

            if (fieldname, btext) not in terminfos:
                terminfos[fieldname, btext] = TermInfo()

        self._postings = fielddict[btext]
        self._terminfo = terminfos[fieldname, btext]
        self._btext = btext

    def add(self, docnum, weight, vbytes, length):
        """
        Adds data to the current term.

        Args:
        - docnum: The document number.
        - weight: The weight of the term in the document.
        - vbytes: The number of bytes used to store the term's value.
        - length: The length of the term.

        Raises:
        - ValueError: If add is called outside a term.
        """
        if self._btext is None:
            raise ValueError("Called add outside a term")

        self._postings.append((docnum, weight, vbytes))
        self._terminfo.add_posting(docnum, weight, length)

    def finish_term(self):
        """
        Finishes the current term.

        Raises:
        - ValueError: If finish_term is called outside a term.
        """
        if self._btext is None:
            raise ValueError("Called finish_term outside a term")

        self._postings = None
        self._btext = None
        self._terminfo = None

    def finish_field(self):
        """
        Finishes the current field.

        Raises:
        - ValueError: If finish_field is called outside a field.
        """
        if self._fieldname is None:
            raise ValueError("Called finish_field outside a field")
        self._fieldname = None
        self._fieldobj = None

    def close(self):
        """
        Closes the writer.
        """
        self.is_closed = True


class MemTermsReader(base.TermsReader):
    """
    A terms reader implementation for in-memory storage.

    This class provides methods to access and retrieve terms, term information,
    and matchers from an in-memory index segment.

    Args:
        storage (object): The storage object used for the index.
        segment (object): The index segment object.

    Attributes:
        _storage (object): The storage object used for the index.
        _segment (object): The index segment object.
        _invindex (dict): The inverted index of the segment.

    """

    def __init__(self, storage, segment):
        self._storage = storage
        self._segment = segment
        self._invindex = segment._invindex

    def __contains__(self, term):
        """
        Check if a term exists in the segment.

        Args:
            term (str): The term to check.

        Returns:
            bool: True if the term exists, False otherwise.

        """
        return term in self._segment._terminfos

    def terms(self):
        """
        Get an iterator over all terms in the segment.

        Yields:
            tuple: A tuple containing the field name and term.

        """
        for fieldname in self._invindex:
            for btext in self._invindex[fieldname]:
                yield (fieldname, btext)

    def terms_from(self, fieldname, prefix):
        """
        Get an iterator over terms starting with a given prefix in a specific field.

        Args:
            fieldname (str): The field name.
            prefix (str): The prefix to match.

        Yields:
            tuple: A tuple containing the field name and term.

        Raises:
            TermNotFound: If the field name is unknown.

        """
        if fieldname not in self._invindex:
            raise TermNotFound(f"Unknown field {fieldname!r}")
        terms = sorted(self._invindex[fieldname])
        if not terms:
            return
        start = bisect_left(terms, prefix)
        for i in range(start, len(terms)):
            yield (fieldname, terms[i])

    def term_info(self, fieldname, text):
        """
        Get the term information for a specific term in a field.

        Args:
            fieldname (str): The field name.
            text (str): The term.

        Returns:
            object: The term information object.

        """
        return self._segment._terminfos[fieldname, text]

    def matcher(self, fieldname, btext, format_, scorer=None):
        """
        Get a matcher for a specific term in a field.

        Args:
            fieldname (str): The field name.
            btext (bytes): The term as bytes.
            format_ (object): The format object.
            scorer (object, optional): The scorer object. Defaults to None.

        Returns:
            object: The matcher object.

        """
        items = self._invindex[fieldname][btext]
        ids, weights, values = zip(*items)
        return ListMatcher(ids, weights, values, format_, scorer=scorer)

    def indexed_field_names(self):
        """
        Returns a list of field names that have been indexed.

        This method retrieves the keys from the inverted index dictionary
        and returns them as a list. Each key represents a field name that
        has been indexed.

        Returns:
            list: A list of field names that have been indexed.
        """
        return self._invindex.keys()

    def close(self):
        """
        Close the terms reader.

        This method is intentionally left empty.

        """
        pass


class MemSegment(base.Segment):
    """
    In-memory implementation of a segment for the Whoosh search engine.

    This class represents a segment of an index stored in memory. It provides methods for managing
    documents, storing and retrieving data, and handling deletions.

    Attributes:
        _codec (Codec): The codec used for encoding and decoding data.
        _doccount (int): The total number of documents in the segment.
        _stored (dict): A dictionary mapping document numbers to stored data.
        _lengths (dict): A dictionary mapping document numbers to the length of the stored data.
        _vectors (dict): A dictionary mapping document numbers to term vectors.
        _invindex (dict): A dictionary mapping terms to inverted index entries.
        _terminfos (dict): A dictionary mapping terms to term information.
        _lock (Lock): A lock used for thread-safety.

    Methods:
        codec(): Returns the codec used by the segment.
        set_doc_count(doccount): Sets the total number of documents in the segment.
        doc_count(): Returns the number of stored documents.
        doc_count_all(): Returns the total number of documents in the segment, including deleted ones.
        delete_document(docnum, delete=True): Deletes a document from the segment.
        has_deletions(): Checks if the segment has any deleted documents.
        is_deleted(docnum): Checks if a document is deleted.
        deleted_docs(): Returns an iterator over the document numbers of deleted documents.
        should_assemble(): Checks if the segment should be assembled.

    """

    def __init__(self, codec, indexname):
        """
        Initializes a new instance of the MemSegment class.

        Args:
            codec (Codec): The codec used for encoding and decoding data.
            indexname (str): The name of the index.

        """
        base.Segment.__init__(self, indexname)
        self._codec = codec
        self._doccount = 0
        self._stored = {}
        self._lengths = {}
        self._vectors = {}
        self._invindex = {}
        self._terminfos = {}
        self._lock = Lock()

    def codec(self):
        """
        Returns the codec used by the segment.

        Returns:
            Codec: The codec used by the segment.

        """
        return self._codec

    def set_doc_count(self, doccount):
        """
        Sets the total number of documents in the segment.

        Args:
            doccount (int): The total number of documents.

        """
        self._doccount = doccount

    def doc_count(self):
        """
        Returns the number of stored documents.

        Returns:
            int: The number of stored documents.

        """
        return len(self._stored)

    def doc_count_all(self):
        """
        Returns the total number of documents in the segment, including deleted ones.

        Returns:
            int: The total number of documents.

        """
        return self._doccount

    def delete_document(self, docnum, delete=True):
        """
        Deletes a document from the segment.

        Args:
            docnum (int): The document number.
            delete (bool): Whether to permanently delete the document. Default is True.

        Raises:
            ValueError: If delete is False, as MemoryCodec does not support undeleting.

        """
        if not delete:
            raise ValueError("MemoryCodec can't undelete")
        with self._lock:
            del self._stored[docnum]
            del self._lengths[docnum]
            del self._vectors[docnum]

    def has_deletions(self):
        """
        Checks if the segment has any deleted documents.

        Returns:
            bool: True if there are deleted documents, False otherwise.

        """
        with self._lock:
            return self._doccount - len(self._stored)

    def is_deleted(self, docnum):
        """
        Checks if a document is deleted.

        Args:
            docnum (int): The document number.

        Returns:
            bool: True if the document is deleted, False otherwise.

        """
        return docnum not in self._stored

    def deleted_docs(self):
        """
        Returns an iterator over the document numbers of deleted documents.

        Yields:
            int: The document number of a deleted document.

        """
        stored = self._stored
        for docnum in range(self.doc_count_all()):
            if docnum not in stored:
                yield docnum

    def should_assemble(self):
        """
        Checks if the segment should be assembled.

        Returns:
            bool: True if the segment should be assembled, False otherwise.

        """
        return False
