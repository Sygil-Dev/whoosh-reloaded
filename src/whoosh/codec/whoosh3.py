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

"""
This module implements a "codec" for writing/reading Whoosh X indexes.
"""

import struct
from array import array
from collections import defaultdict
from pickle import dumps, loads

from whoosh import columns, formats
from whoosh.codec import base
from whoosh.filedb import compound, filetables
from whoosh.matching import LeafMatcher, ListMatcher, ReadTooFar
from whoosh.reading import TermInfo, TermNotFound
from whoosh.system import (
    _FLOAT_SIZE,
    _INT_SIZE,
    _LONG_SIZE,
    _SHORT_SIZE,
    emptybytes,
    pack_int,
    pack_long,
    pack_ushort,
    unpack_int,
    unpack_long,
    unpack_ushort,
)
from whoosh.util.numeric import byte_to_length, length_to_byte
from whoosh.util.numlists import delta_decode, delta_encode

try:
    import zlib
except ImportError:
    zlib = None


# This byte sequence is written at the start of a posting list to identify the
# codec/version
WHOOSH3_HEADER_MAGIC = b"W3Bl"

# Column type to store field length info
LENGTHS_COLUMN = columns.NumericColumn("B", default=0)
# Column type to store pointers to vector posting lists
VECTOR_COLUMN = columns.NumericColumn("I")
# Column type to store vector posting list lengths
VECTOR_LEN_COLUMN = columns.NumericColumn("i")
# Column type to store values of stored fields
STORED_COLUMN = columns.PickleColumn(columns.CompressedBytesColumn())


class W3Codec(base.Codec):
    """
    Codec implementation for the Whoosh 3 index format.

    This codec provides methods for reading and writing various components of the index,
    such as term indexes, term postings, vector postings, and per-document value columns.

    Parameters:
    - blocklimit (int): The maximum number of postings to store in a block. Defaults to 128.
    - compression (int): The level of compression to use for the postings. Defaults to 3.
    - inlinelimit (int): The maximum number of postings to inline in the term info object. Defaults to 1.

    """

    # File extensions
    TERMS_EXT = ".trm"  # Term index
    POSTS_EXT = ".pst"  # Term postings
    VPOSTS_EXT = ".vps"  # Vector postings
    COLUMN_EXT = ".col"  # Per-document value columns

    def __init__(self, blocklimit=128, compression=3, inlinelimit=1):
        """
        Initialize a new instance of the W3Codec class.

        Parameters:
        - blocklimit (int): The maximum number of postings to store in a block. Defaults to 128.
        - compression (int): The level of compression to use for the postings. Defaults to 3.
        - inlinelimit (int): The maximum number of postings to inline in the term info object. Defaults to 1.

        """
        self._blocklimit = blocklimit
        self._compression = compression
        self._inlinelimit = inlinelimit

    # Per-document value writer
    def per_document_writer(self, storage, segment):
        """
        Create a per-document value writer for the given storage and segment.

        Parameters:
        - storage (Storage): The storage object for the index.
        - segment (Segment): The segment object for the index.

        Returns:
        - W3PerDocWriter: The per-document value writer.

        """
        return W3PerDocWriter(self, storage, segment)

    def field_writer(self, storage, segment):
        """
        Create an inverted index writer for the given storage and segment.

        Parameters:
        - storage (Storage): The storage object for the index.
        - segment (Segment): The segment object for the index.

        Returns:
        - W3FieldWriter: The inverted index writer.

        """
        return W3FieldWriter(self, storage, segment)

    def postings_writer(self, dbfile, byteids=False):
        """
        Create a postings writer for the given database file.

        Parameters:
        - dbfile (File): The file object for the postings.
        - byteids (bool): Whether to use byte-based document ids. Defaults to False.

        Returns:
        - W3PostingsWriter: The postings writer.

        """
        return W3PostingsWriter(
            dbfile,
            blocklimit=self._blocklimit,
            byteids=byteids,
            compression=self._compression,
            inlinelimit=self._inlinelimit,
        )

    def postings_reader(self, dbfile, terminfo, format_, term=None, scorer=None):
        """
        Create a postings reader for the given database file and term info.

        Parameters:
        - dbfile (File): The file object for the postings.
        - terminfo (TermInfo): The term info object for the term.
        - format_ (str): The format of the postings.
        - term (str): The term to read the postings for. Defaults to None.
        - scorer (Scorer): The scorer object for scoring the postings. Defaults to None.

        Returns:
        - Matcher: The postings reader.

        """
        if terminfo.is_inlined():
            # If the postings were inlined into the terminfo object, pull them
            # out and use a ListMatcher to wrap them in a Matcher interface
            ids, weights, values = terminfo.inlined_postings()
            m = ListMatcher(
                ids,
                weights,
                values,
                format_,
                scorer=scorer,
                term=term,
                terminfo=terminfo,
            )
        else:
            offset, length = terminfo.extent()
            m = W3LeafMatcher(dbfile, offset, length, format_, term=term, scorer=scorer)
        return m

    def per_document_reader(self, storage, segment):
        """
        Create a per-document value reader for the given storage and segment.

        Parameters:
        - storage (Storage): The storage object for the index.
        - segment (Segment): The segment object for the index.

        Returns:
        - W3PerDocReader: The per-document value reader.

        """
        return W3PerDocReader(storage, segment)

    def terms_reader(self, storage, segment):
        """
        Create a terms reader for the given storage and segment.

        Parameters:
        - storage (Storage): The storage object for the index.
        - segment (Segment): The segment object for the index.

        Returns:
        - W3TermsReader: The terms reader.

        """
        tiname = segment.make_filename(self.TERMS_EXT)
        tilen = storage.file_length(tiname)
        tifile = storage.open_file(tiname)

        postfile = segment.open_file(storage, self.POSTS_EXT)

        return W3TermsReader(self, tifile, tilen, postfile)

    # Graph methods provided by CodecWithGraph
    def supports_columns(self):
        """
        Check if the codec supports per-document value columns.

        Returns:
        - bool: True if per-document value columns are supported, False otherwise.

        """
        return True

    @classmethod
    def column_filename(cls, segment, fieldname):
        """
        Get the filename for the per-document value column of the given field in the segment.

        Parameters:
        - segment (Segment): The segment object for the index.
        - fieldname (str): The name of the field.

        Returns:
        - str: The filename for the per-document value column.

        """
        ext = "".join((".", fieldname, cls.COLUMN_EXT))
        return segment.make_filename(ext)

    # Segments and generations
    def new_segment(self, storage, indexname):
        """
        Create a new segment for the given storage and index name.

        Parameters:
        - storage (Storage): The storage object for the index.
        - indexname (str): The name of the index.

        Returns:
        - W3Segment: The new segment.

        """
        return W3Segment(self, indexname)


# Common functions


def _vecfield(fieldname):
    """
    Returns the vector field name for a given field.

    Parameters:
    fieldname (str): The name of the field.

    Returns:
    str: The vector field name.

    Example:
    >>> _vecfield("title")
    '_title_vec'

    This function takes a field name as input and returns the corresponding vector field name.
    The vector field name is constructed by adding underscores before and after the field name.
    """
    return f"_{fieldname}_vec"


def _lenfield(fieldname):
    """
    Returns the length field name for a given field.

    Parameters:
    - fieldname (str): The name of the field.

    Returns:
    - str: The length field name.

    Example:
    >>> _lenfield("title")
    '_title_len'

    This function is used to generate the length field name for a given field. The length field name is used in the Whoosh codec to store the length of a variable-length field. It appends "_len" to the field name to create the length field name.

    Usage:
    >>> length_field = _lenfield("content")
    >>> print(length_field)
    '_content_len'
    """
    return f"_{fieldname}_len"


# Per-doc information writer
class W3PerDocWriter(base.PerDocWriterWithColumns):
    """
    This class is responsible for writing per-document data to the index for the Whoosh3 codec.

    It provides methods for adding fields, vectors, and other per-document information to the index.

    Usage:
    ------
    1. Create an instance of W3PerDocWriter by passing the codec, storage, and segment parameters to the constructor.
    2. Use the start_doc() method to indicate the start of a new document.
    3. Use the add_field() method to add a field to the document with its corresponding value and length.
    4. Use the add_vector_items() method to add vector items (text, weight, and vbytes) to the document.
    5. Use the finish_doc() method to indicate the end of the current document.
    6. Repeat steps 2-5 for each document.
    7. Call the close() method to finish writing the per-document data to the index.

    Note:
    -----
    The close() method must be called after writing all the documents to the index.

    Attributes:
    -----------
    - is_closed: A boolean attribute indicating whether the writer has been closed.

    Methods:
    --------
    - start_doc(docnum): Indicates the start of a new document.
    - add_field(fieldname, fieldobj, value, length): Adds a field to the document with its corresponding value and length.
    - add_vector_items(fieldname, fieldobj, items): Adds vector items to the document.
    - finish_doc(): Indicates the end of the current document.
    - cancel_doc(): Cancels the current document.
    - close(): Finishes writing the per-document data to the index.

    """

    def __init__(self, codec, storage, segment):
        """
        Initializes a new instance of W3PerDocWriter.

        Parameters:
        -----------
        - codec: The codec used for encoding and decoding data.
        - storage: The storage object used for storing the index files.
        - segment: The segment object representing the current segment of the index.
        """
        self._codec = codec
        self._storage = storage
        self._segment = segment

        tempst = storage.temp_storage(f"{segment.indexname}.tmp")
        self._cols = compound.CompoundWriter(tempst)
        self._colwriters = {}
        self._create_column("_stored", STORED_COLUMN)

        self._fieldlengths = defaultdict(int)
        self._doccount = 0
        self._docnum = None
        self._storedfields = None
        self._indoc = False
        self.is_closed = False

        # We'll wait to create the vector file until someone actually tries
        # to add a vector
        self._vpostfile = None

    def _create_file(self, ext):
        """
        Creates a new file with the given extension in the current segment.

        Parameters:
        -----------
        - ext: The extension of the file.

        Returns:
        --------
        The created file object.
        """
        return self._segment.create_file(self._storage, ext)

    def _has_column(self, fieldname):
        """
        Checks if a column with the given fieldname has been added.

        Parameters:
        -----------
        - fieldname: The name of the field/column.

        Returns:
        --------
        True if the column exists, False otherwise.
        """
        return fieldname in self._colwriters

    def _create_column(self, fieldname, column):
        """
        Creates a new column with the given fieldname.

        Parameters:
        -----------
        - fieldname: The name of the field/column.
        - column: The column object.

        Raises:
        -------
        ValueError: If a column with the same fieldname has already been added.
        """
        writers = self._colwriters
        if fieldname in writers:
            raise ValueError(f"Already added column {fieldname!r}")

        f = self._cols.create_file(fieldname)
        writers[fieldname] = column.writer(f)

    def _get_column(self, fieldname):
        return self._colwriters[fieldname]

    def _prep_vectors(self):
        self._vpostfile = self._create_file(W3Codec.VPOSTS_EXT)
        # We'll use offset==0 as a marker for "no vectors", so we can't start
        # postings at position 0, so just write a few header bytes :)
        self._vpostfile.write(b"VPST")

    def start_doc(self, docnum):
        if self._indoc:
            raise ValueError("Called start_doc when already in a doc")
        if docnum != self._doccount:
            raise ValueError(
                f"Called start_doc({docnum!r}) was expecting {self._doccount!r}"
            )

        self._docnum = docnum
        self._doccount += 1
        self._storedfields = {}
        self._indoc = True

    def add_field(self, fieldname, fieldobj, value, length):
        if value is not None:
            self._storedfields[fieldname] = value
        if length:
            # Add byte to length column
            lenfield = _lenfield(fieldname)
            lb = length_to_byte(length)
            self.add_column_value(lenfield, LENGTHS_COLUMN, lb)
            # Add length to total field length
            self._fieldlengths[fieldname] += length

    def add_vector_items(self, fieldname, fieldobj, items):
        if not items:
            # Don't do anything if the list of items is empty
            return

        if self._vpostfile is None:
            self._prep_vectors()

        # Write vector postings
        vpostwriter = self._codec.postings_writer(self._vpostfile, byteids=True)
        vpostwriter.start_postings(fieldobj.vector, W3TermInfo())
        for text, weight, vbytes in items:
            vpostwriter.add_posting(text, weight, vbytes)
        # finish_postings() returns terminfo object
        vinfo = vpostwriter.finish_postings()

        # Add row to vector lookup column
        vecfield = _vecfield(fieldname)  # Compute vector column name
        offset, length = vinfo.extent()
        assert offset != 0
        self.add_column_value(vecfield, VECTOR_COLUMN, offset)
        self.add_column_value(vecfield + "L", VECTOR_LEN_COLUMN, length)

    def finish_doc(self):
        sf = self._storedfields
        if sf:
            self.add_column_value("_stored", STORED_COLUMN, sf)
            sf.clear()
        self._indoc = False

    def cancel_doc(self):
        self._doccount -= 1
        self._indoc = False

    def _column_filename(self, fieldname):
        return W3Codec.column_filename(self._segment, fieldname)

    def close(self):
        if self._indoc is not None:
            # Called close without calling finish_doc
            self.finish_doc()

        self._segment._fieldlengths = self._fieldlengths

        # Finish open columns and close the columns writer
        for writer in self._colwriters.values():
            writer.finish(self._doccount)
        self._cols.save_as_files(self._storage, self._column_filename)

        # If vectors were written, close the vector writers
        if self._vpostfile:
            self._vpostfile.close()

        self.is_closed = True


class W3FieldWriter(base.FieldWriter):
    """
    Writes field data to the index for the Whoosh3 codec.

    This class is responsible for writing field data, including terms and postings, to the index.
    It is used internally by the Whoosh3 codec and should not be instantiated directly.

    Parameters:
    - codec (Codec): The codec used for encoding and decoding data.
    - storage (Storage): The storage object used for creating files.
    - segment (Segment): The segment object representing the current segment.

    Attributes:
    - _codec (Codec): The codec used for encoding and decoding data.
    - _storage (Storage): The storage object used for creating files.
    - _segment (Segment): The segment object representing the current segment.
    - _fieldname (str): The name of the current field being written.
    - _fieldid (int): The ID of the current field being written.
    - _btext (bytes): The binary representation of the current term being written.
    - _fieldobj (Field): The field object associated with the current field being written.
    - _format (Format): The format object associated with the current field being written.
    - _tindex (OrderedHashWriter): The ordered hash writer for the terms index.
    - _fieldmap (dict): A dictionary mapping field names to field IDs.
    - _postfile (File): The file object for writing postings data.
    - _postwriter (PostingsWriter): The postings writer for the current field being written.
    - _infield (bool): Indicates whether the writer is currently inside a field.
    - is_closed (bool): Indicates whether the writer has been closed.

    Methods:
    - _create_file(ext): Creates a file with the given extension.
    - start_field(fieldname, fieldobj): Starts writing a new field.
    - start_term(btext): Starts writing a new term.
    - add(docnum, weight, vbytes, length): Adds a posting to the current term.
    - finish_term(): Finishes writing the current term.
    - finish_field(): Finishes writing the current field.
    - close(): Closes the writer and releases any resources.
    """

    def __init__(self, codec, storage, segment):
        self._codec = codec
        self._storage = storage
        self._segment = segment

        self._fieldname = None
        self._fieldid = None
        self._btext = None
        self._fieldobj = None
        self._format = None

        _tifile = self._create_file(W3Codec.TERMS_EXT)
        self._tindex = filetables.OrderedHashWriter(_tifile)
        self._fieldmap = self._tindex.extras["fieldmap"] = {}

        self._postfile = self._create_file(W3Codec.POSTS_EXT)

        self._postwriter = None
        self._infield = False
        self.is_closed = False

    def _create_file(self, ext):
        """
        Creates a file with the given extension.

        Parameters:
        - ext (str): The file extension.

        Returns:
        - File: The created file object.
        """
        return self._segment.create_file(self._storage, ext)

    def start_field(self, fieldname, fieldobj):
        """
        Starts writing a new field.

        Parameters:
        - fieldname (str): The name of the field.
        - fieldobj (Field): The field object.

        Raises:
        - ValueError: If called before start_field.

        """
        fmap = self._fieldmap
        if fieldname in fmap:
            self._fieldid = fmap[fieldname]
        else:
            self._fieldid = len(fmap)
            fmap[fieldname] = self._fieldid

        self._fieldname = fieldname
        self._fieldobj = fieldobj
        self._format = fieldobj.format
        self._infield = True

        # Start a new postwriter for this field
        self._postwriter = self._codec.postings_writer(self._postfile)

    def start_term(self, btext):
        """
        Starts writing a new term.

        Parameters:
        - btext (bytes): The binary representation of the term.

        Raises:
        - ValueError: If called before start_field.
        """
        if self._postwriter is None:
            raise ValueError("Called start_term before start_field")
        self._btext = btext
        self._postwriter.start_postings(self._fieldobj.format, W3TermInfo())

    def add(self, docnum, weight, vbytes, length):
        """
        Adds a posting to the current term.

        Parameters:
        - docnum (int): The document number.
        - weight (float): The weight of the posting.
        - vbytes (int): The number of bytes used to encode the posting value.
        - length (int): The length of the posting.

        """
        self._postwriter.add_posting(docnum, weight, vbytes, length)

    def finish_term(self):
        """
        Finishes writing the current term.

        """
        terminfo = self._postwriter.finish_postings()

        # Add row to term info table
        keybytes = pack_ushort(self._fieldid) + self._btext
        valbytes = terminfo.to_bytes()
        self._tindex.add(keybytes, valbytes)

    def finish_field(self):
        """
        Finishes writing the current field.

        Raises:
        - ValueError: If called before start_field.
        """
        if not self._infield:
            raise ValueError("Called finish_field before start_field")
        self._infield = False
        self._postwriter = None

    def close(self):
        """
        Closes the writer and releases any resources.

        """
        self._tindex.close()
        self._postfile.close()
        self.is_closed = True


# Reader objects
class W3PerDocReader(base.PerDocumentReader):
    def __init__(self, storage, segment):
        self._storage = storage
        self._segment = segment
        self._doccount = segment.doc_count_all()

        self._vpostfile = None
        self._colfiles = {}
        self._readers = {}
        self._minlengths = {}
        self._maxlengths = {}

    def close(self):
        for colfile, _, _ in self._colfiles.values():
            colfile.close()
        if self._vpostfile:
            self._vpostfile.close()

    def doc_count(self):
        return self._doccount - self._segment.deleted_count()

    def doc_count_all(self):
        return self._doccount

    # Deletions

    def has_deletions(self):
        return self._segment.has_deletions()

    def is_deleted(self, docnum):
        return self._segment.is_deleted(docnum)

    def deleted_docs(self):
        return self._segment.deleted_docs()

    # Columns

    def has_column(self, fieldname):
        filename = W3Codec.column_filename(self._segment, fieldname)
        return self._storage.file_exists(filename)

    def _get_column_file(self, fieldname):
        filename = W3Codec.column_filename(self._segment, fieldname)
        length = self._storage.file_length(filename)
        colfile = self._storage.open_file(filename)
        return colfile, 0, length

    def column_reader(self, fieldname, column):
        if fieldname not in self._colfiles:
            self._colfiles[fieldname] = self._get_column_file(fieldname)
        colfile, offset, length = self._colfiles[fieldname]
        return column.reader(colfile, offset, length, self._doccount)

    # Lengths

    def _cached_reader(self, fieldname, column):
        if fieldname in self._readers:
            return self._readers[fieldname]
        else:
            if not self.has_column(fieldname):
                return None

            reader = self.column_reader(fieldname, column)
            self._readers[fieldname] = reader
            return reader

    def doc_field_length(self, docnum, fieldname, default=0):
        if docnum > self._doccount:
            raise IndexError("Asked for docnum %r of %d" % (docnum, self._doccount))

        lenfield = _lenfield(fieldname)
        reader = self._cached_reader(lenfield, LENGTHS_COLUMN)
        if reader is None:
            return default

        lbyte = reader[docnum]
        if lbyte:
            return byte_to_length(lbyte)

    def field_length(self, fieldname):
        return self._segment._fieldlengths.get(fieldname, 0)

    def _minmax_length(self, fieldname, op, cache):
        if fieldname in cache:
            return cache[fieldname]

        lenfield = _lenfield(fieldname)
        reader = self._cached_reader(lenfield, LENGTHS_COLUMN)
        length = byte_to_length(op(reader))
        cache[fieldname] = length
        return length

    def min_field_length(self, fieldname):
        return self._minmax_length(fieldname, min, self._minlengths)

    def max_field_length(self, fieldname):
        return self._minmax_length(fieldname, max, self._maxlengths)

    # Vectors

    def _prep_vectors(self):
        f = self._segment.open_file(self._storage, W3Codec.VPOSTS_EXT)
        self._vpostfile = f

    def _vector_extent(self, docnum, fieldname):
        if docnum > self._doccount:
            raise IndexError("Asked for document %r of %d" % (docnum, self._doccount))
        vecfield = _vecfield(fieldname)  # Compute vector column name

        # Get the offset from the vector offset column
        offset = self._cached_reader(vecfield, VECTOR_COLUMN)[docnum]

        # Get the length from the length column, if it exists, otherwise return
        # -1 for the length (backwards compatibility with old dev versions)
        lreader = self._cached_reader(vecfield + "L", VECTOR_COLUMN)
        if lreader:
            length = lreader[docnum]
        else:
            length = -1

        return offset, length

    def has_vector(self, docnum, fieldname):
        if self.has_column(_vecfield(fieldname)):
            offset, _ = self._vector_extent(docnum, fieldname)
            return offset != 0
        return False

    def vector(self, docnum, fieldname, format_):
        if self._vpostfile is None:
            self._prep_vectors()
        offset, length = self._vector_extent(docnum, fieldname)
        if not offset:
            raise ValueError(f"Field {fieldname!r} has no vector in docnum {docnum}")
        m = W3LeafMatcher(self._vpostfile, offset, length, format_, byteids=True)
        return m

    # Stored fields

    def stored_fields(self, docnum):
        reader = self._cached_reader("_stored", STORED_COLUMN)
        v = reader[docnum]
        if v is None:
            v = {}
        return v


class W3FieldCursor(base.FieldCursor):
    """Cursor for iterating over the terms in a field in a Whoosh 3 index.

    This cursor provides methods for iterating over the terms in a specific field
    in a Whoosh 3 index. It allows you to navigate through the terms in the field,
    retrieve the text representation of the current term, and access additional
    information about the term.

    Attributes:
        _tindex (TIndex): The TIndex object representing the index.
        _fieldname (str): The name of the field.
        _keycoder (callable): The function used to encode the field name and term
            into a key.
        _keydecoder (callable): The function used to decode a key into the field name
            and term.
        _fieldobj (Field): The Field object representing the field.

    Methods:
        __init__(tindex, fieldname, keycoder, keydecoder, fieldobj): Initializes the
            W3FieldCursor object.
        first(): Moves the cursor to the first term in the field and returns the text
            representation of the term.
        find(term): Moves the cursor to the specified term in the field and returns the
            text representation of the term.
        next(): Moves the cursor to the next term in the field and returns the text
            representation of the term.
        text(): Returns the text representation of the current term.
        term_info(): Returns additional information about the current term.
        is_valid(): Returns True if the cursor is currently pointing to a valid term,
            False otherwise.
    """

    def __init__(self, tindex, fieldname, keycoder, keydecoder, fieldobj):
        """
        Initializes a new instance of the W3FieldCursor class.

        Args:
            tindex (TIndex): The TIndex object representing the index.
            fieldname (str): The name of the field.
            keycoder (callable): The function used to encode the field name and term
                into a key.
            keydecoder (callable): The function used to decode a key into the field name
                and term.
            fieldobj (Field): The Field object representing the field.
        """
        self._tindex = tindex
        self._fieldname = fieldname
        self._keycoder = keycoder
        self._keydecoder = keydecoder
        self._fieldobj = fieldobj

        prefixbytes = keycoder(fieldname, b"")
        self._startpos = self._tindex.closest_key_pos(prefixbytes)

        self._pos = self._startpos
        self._text = None
        self._datapos = None
        self._datalen = None
        self.next()

    def first(self):
        """
        Moves the cursor to the first term in the field and returns the text
        representation of the term.

        Returns:
            str: The text representation of the first term in the field.
        """
        self._pos = self._startpos
        return self.next()

    def find(self, term):
        """
        Moves the cursor to the specified term in the field and returns the text
        representation of the term.

        Args:
            term (bytes or str): The term to find in the field.

        Returns:
            str: The text representation of the found term.
        """
        if not isinstance(term, bytes):
            term = self._fieldobj.to_bytes(term)
        key = self._keycoder(self._fieldname, term)
        self._pos = self._tindex.closest_key_pos(key)
        return self.next()

    def next(self):
        """
        Moves the cursor to the next term in the field and returns the text
        representation of the term.

        Returns:
            str: The text representation of the next term in the field.
        """
        if self._pos is not None:
            keyrng = self._tindex.key_and_range_at(self._pos)
            if keyrng is not None:
                keybytes, datapos, datalen = keyrng
                fname, text = self._keydecoder(keybytes)
                if fname == self._fieldname:
                    self._pos = datapos + datalen
                    self._text = self._fieldobj.from_bytes(text)
                    self._datapos = datapos
                    self._datalen = datalen
                    return self._text

        self._text = self._pos = self._datapos = self._datalen = None
        return None

    def text(self):
        """
        Returns the text representation of the current term.

        Returns:
            str: The text representation of the current term.
        """
        return self._text

    def term_info(self):
        """
        Returns additional information about the current term.

        Returns:
            W3TermInfo: An object containing additional information about the current term.
        """
        if self._pos is None:
            return None

        databytes = self._tindex.dbfile.get(self._datapos, self._datalen)
        return W3TermInfo.from_bytes(databytes)

    def is_valid(self):
        """
        Returns True if the cursor is currently pointing to a valid term, False otherwise.

        Returns:
            bool: True if the cursor is currently pointing to a valid term, False otherwise.
        """
        return self._pos is not None


class W3TermsReader(base.TermsReader):
    """
    A terms reader for the Whoosh3 codec.

    This class is responsible for reading and retrieving terms, term information, and posting lists from the index.

    Parameters:
    - codec (Codec): The codec associated with the index.
    - dbfile (file-like object): The file-like object representing the terms index.
    - length (int): The length of the terms index.
    - postfile (file-like object): The file-like object representing the posting lists.

    Attributes:
    - _codec (Codec): The codec associated with the index.
    - _dbfile (file-like object): The file-like object representing the terms index.
    - _tindex (OrderedHashReader): The ordered hash reader for the terms index.
    - _fieldmap (dict): A dictionary mapping field names to field numbers.
    - _postfile (file-like object): The file-like object representing the posting lists.
    - _fieldunmap (list): A list mapping field numbers to field names.

    """

    def __init__(self, codec, dbfile, length, postfile):
        """
        Initialize a Whoosh3 object.

        Parameters:
        - codec (object): The codec object used for encoding and decoding data.
        - dbfile (str): The path to the database file.
        - length (int): The length of the database file.
        - postfile (str): The path to the postfile.

        This method initializes a Whoosh3 object by setting the codec, database file,
        length, postfile, fieldmap, and fieldunmap attributes. The fieldmap is a
        dictionary that maps field names to field numbers, and the fieldunmap is a
        list that maps field numbers to field names.

        Example usage:
        codec = MyCodec()
        dbfile = "/path/to/database.db"
        length = 1000
        postfile = "/path/to/postfile"
        whoosh3 = Whoosh3(codec, dbfile, length, postfile)
        """
        self._codec = codec
        self._dbfile = dbfile
        self._tindex = filetables.OrderedHashReader(dbfile, length)
        self._fieldmap = self._tindex.extras["fieldmap"]
        self._postfile = postfile

        self._fieldunmap = [None] * len(self._fieldmap)
        for fieldname, num in self._fieldmap.items():
            self._fieldunmap[num] = fieldname

    def _keycoder(self, fieldname, tbytes):
        """
        Encode the field name and term bytes into a key.

        Parameters:
        - fieldname (str): The name of the field.
        - tbytes (bytes): The term bytes.

        Returns:
        - bytes: The encoded key.

        """
        assert isinstance(tbytes, bytes), f"tbytes={tbytes!r}"
        fnum = self._fieldmap.get(fieldname, 65535)
        return pack_ushort(fnum) + tbytes

    def _keydecoder(self, keybytes):
        """
        Decode the key bytes into the field name and term bytes.

        Parameters:
        - keybytes (bytes): The key bytes.

        Returns:
        - Tuple[str, bytes]: The field name and term bytes.

        """
        fieldid = unpack_ushort(keybytes[:_SHORT_SIZE])[0]
        return self._fieldunmap[fieldid], keybytes[_SHORT_SIZE:]

    def _range_for_key(self, fieldname, tbytes):
        """
        Get the range of positions in the terms index for the given field name and term bytes.

        Parameters:
        - fieldname (str): The name of the field.
        - tbytes (bytes): The term bytes.

        Returns:
        - Tuple[int, int]: The start and end positions in the terms index.

        """
        return self._tindex.range_for_key(self._keycoder(fieldname, tbytes))

    def __contains__(self, term):
        """
        Check if the given term is present in the terms index.

        Parameters:
        - term (Tuple[str, bytes]): The field name and term bytes.

        Returns:
        - bool: True if the term is present, False otherwise.

        """
        return self._keycoder(*term) in self._tindex

    def indexed_field_names(self):
        """
        Get the names of the fields that are indexed.

        Returns:
        - KeysView: A view object containing the names of the indexed fields.

        """
        return self._fieldmap.keys()

    def cursor(self, fieldname, fieldobj):
        """
        Create a cursor for iterating over the terms in the given field.

        Parameters:
        - fieldname (str): The name of the field.
        - fieldobj (Field): The field object.

        Returns:
        - W3FieldCursor: The cursor object.

        """
        tindex = self._tindex
        coder = self._keycoder
        decoder = self._keydecoder
        return W3FieldCursor(tindex, fieldname, coder, decoder, fieldobj)

    def terms(self):
        """
        Get an iterator over all the terms in the index.

        Yields:
        - Tuple[str, bytes]: The field name and term bytes.

        """
        keydecoder = self._keydecoder
        return (keydecoder(keybytes) for keybytes in self._tindex.keys())

    def terms_from(self, fieldname, prefix):
        """
        Get an iterator over the terms in the given field starting from the specified prefix.

        Parameters:
        - fieldname (str): The name of the field.
        - prefix (bytes): The prefix bytes.

        Yields:
        - Tuple[str, bytes]: The field name and term bytes.

        """
        prefixbytes = self._keycoder(fieldname, prefix)
        keydecoder = self._keydecoder
        return (
            keydecoder(keybytes) for keybytes in self._tindex.keys_from(prefixbytes)
        )

    def items(self):
        """
        Get an iterator over all the (term, term info) pairs in the index.

        Yields:
        - Tuple[Tuple[str, bytes], W3TermInfo]: The (field name, term bytes) and term info.

        """
        tidecoder = W3TermInfo.from_bytes
        keydecoder = self._keydecoder
        return (
            (keydecoder(keybytes), tidecoder(valbytes))
            for keybytes, valbytes in self._tindex.items()
        )

    def items_from(self, fieldname, prefix):
        """
        Get an iterator over the (term, term info) pairs in the given field starting from the specified prefix.

        Parameters:
        - fieldname (str): The name of the field.
        - prefix (bytes): The prefix bytes.

        Yields:
        - Tuple[Tuple[str, bytes], W3TermInfo]: The (field name, term bytes) and term info.

        """
        prefixbytes = self._keycoder(fieldname, prefix)
        tidecoder = W3TermInfo.from_bytes
        keydecoder = self._keydecoder
        return (
            (keydecoder(keybytes), tidecoder(valbytes))
            for keybytes, valbytes in self._tindex.items_from(prefixbytes)
        )

    def term_info(self, fieldname, tbytes):
        """
        Get the term info for the given field name and term bytes.

        Parameters:
        - fieldname (str): The name of the field.
        - tbytes (bytes): The term bytes.

        Returns:
        - W3TermInfo: The term info.

        Raises:
        - TermNotFound: If the term is not found.

        """
        key = self._keycoder(fieldname, tbytes)
        try:
            return W3TermInfo.from_bytes(self._tindex[key])
        except KeyError:
            raise TermNotFound(f"No term {fieldname}:{tbytes!r}")

    def frequency(self, fieldname, tbytes):
        """
        Get the frequency of the given term in the specified field.

        Parameters:
        - fieldname (str): The name of the field.
        - tbytes (bytes): The term bytes.

        Returns:
        - int: The term frequency.

        """
        datapos = self._range_for_key(fieldname, tbytes)[0]
        return W3TermInfo.read_weight(self._dbfile, datapos)

    def doc_frequency(self, fieldname, tbytes):
        """
        Get the document frequency of the given term in the specified field.

        Parameters:
        - fieldname (str): The name of the field.
        - tbytes (bytes): The term bytes.

        Returns:
        - int: The document frequency.

        """
        datapos = self._range_for_key(fieldname, tbytes)[0]
        return W3TermInfo.read_doc_freq(self._dbfile, datapos)

    def matcher(self, fieldname, tbytes, format_, scorer=None):
        """
        Create a matcher for the given term in the specified field.

        Parameters:
        - fieldname (str): The name of the field.
        - tbytes (bytes): The term bytes.
        - format_ (str): The format of the posting lists.
        - scorer (Scorer, optional): The scorer object.

        Returns:
        - Matcher: The matcher object.

        """
        terminfo = self.term_info(fieldname, tbytes)
        m = self._codec.postings_reader(
            self._postfile, terminfo, format_, term=(fieldname, tbytes), scorer=scorer
        )
        return m

    def close(self):
        """
        Close the terms reader and associated resources.

        """
        self._tindex.close()
        self._postfile.close()


# Postings


class W3PostingsWriter(base.PostingsWriter):
    """This object writes posting lists to the postings file. It groups postings
    into blocks and tracks block level statistics to makes it easier to skip
    through the postings.

    Parameters:
    - postfile (file-like object): The file-like object to write the posting lists to.
    - blocklimit (int): The maximum number of postings to buffer before writing them to the file.
    - byteids (bool, optional): Whether the IDs should be stored as bytes or integers. Defaults to False.
    - compression (int, optional): The compression level to use. Defaults to 3.
    - inlinelimit (int, optional): The maximum number of postings to inline into the terminfo object. Defaults to 1.
    """

    def __init__(
        self, postfile, blocklimit, byteids=False, compression=3, inlinelimit=1
    ):
        self._postfile = postfile
        self._blocklimit = blocklimit
        self._byteids = byteids
        self._compression = compression
        self._inlinelimit = inlinelimit

        self._blockcount = 0
        self._format = None
        self._terminfo = None

    def written(self):
        """Check if any blocks have been written to the file.

        Returns:
        bool: True if blocks have been written, False otherwise.
        """
        return self._blockcount > 0

    def start_postings(self, format_, terminfo):
        """Start a new term.

        Parameters:
        - format_ (formats.Format): The format object for the term.
        - terminfo (Terminfo): The terminfo object for the term.

        Raises:
        ValueError: If called while already in a term.
        """
        if self._terminfo:
            # If self._terminfo is not None, that means we are already in a term
            raise ValueError("Called start in a term")

        assert isinstance(format_, formats.Format)
        self._format = format_
        # Reset block count
        self._blockcount = 0
        # Reset block bufferg
        self._new_block()
        # Remember terminfo object passed to us
        self._terminfo = terminfo
        # Remember where we started in the posting file
        self._startoffset = self._postfile.tell()

    def add_posting(self, id_, weight, vbytes, length=None):
        """Add a posting to the buffered block.

        Parameters:
        - id_ (str or int): The ID of the posting.
        - weight (int or float): The weight of the posting.
        - vbytes (bytes): The encoded payload of the posting.
        - length (int, optional): The length of the field. Defaults to None.

        Raises:
        AssertionError: If the types of the parameters are incorrect.
        """
        # buffered block and reset before adding this one
        if len(self._ids) >= self._blocklimit:
            self._write_block()

        # Check types
        if self._byteids:
            assert isinstance(id_, str), f"id_={id_!r}"
        else:
            assert isinstance(id_, int), f"id_={id_!r}"
        assert isinstance(weight, (int, float)), f"weight={weight!r}"
        assert isinstance(vbytes, bytes), f"vbytes={vbytes!r}"
        assert length is None or isinstance(length, int)

        self._ids.append(id_)
        self._weights.append(weight)

        if weight > self._maxweight:
            self._maxweight = weight
        if vbytes:
            self._values.append(vbytes)
        if length:
            minlength = self._minlength
            if minlength is None or length < minlength:
                self._minlength = length
            if length > self._maxlength:
                self._maxlength = length

    def finish_postings(self):
        """Finish writing the postings for the term.

        If there are fewer than "inlinelimit" postings in this posting list,
        the postings are inlined into the terminfo object instead of writing them to the posting file.

        Returns:
        Terminfo: The current terminfo object.

        Raises:
        AssertionError: If the types of the parameters are incorrect.
        """
        terminfo = self._terminfo

        # the posting file
        if not self.written() and len(self) < self._inlinelimit:
            terminfo.add_block(self)
            terminfo.set_inline(self._ids, self._weights, self._values)
        else:
            # If there are leftover items in the current block, write them out
            if self._ids:
                self._write_block(last=True)
            startoffset = self._startoffset
            length = self._postfile.tell() - startoffset
            terminfo.set_extent(startoffset, length)

        # Clear self._terminfo to indicate we're between terms
        self._terminfo = None
        # Return the current terminfo object
        return terminfo

    def _new_block(self):
        """Reset the block buffer."""
        # List of IDs (docnums for regular posting list, terms for vector PL)
        self._ids = [] if self._byteids else array("I")
        # List of weights
        self._weights = array("f")
        # List of encoded payloads
        self._values = []
        # Statistics
        self._minlength = None
        self._maxlength = 0
        self._maxweight = 0

    def _write_block(self, last=False):
        """Write the buffered block to the postings file.

        Parameters:
        - last (bool, optional): Whether this is the last block. Defaults to False.
        """
        # If this is the first block, write a small header first
        if not self._blockcount:
            self._postfile.write(WHOOSH3_HEADER_MAGIC)

        # Add this block's statistics to the terminfo object, which tracks the
        # overall statistics for all term postings
        self._terminfo.add_block(self)

        # Minify the IDs, weights, and values, and put them in a tuple
        data = (self._mini_ids(), self._mini_weights(), self._mini_values())
        # Pickle the tuple
        databytes = dumps(data, 2)
        # If the pickle is less than 20 bytes, don't bother compressing
        if len(databytes) < 20:
            comp = 0
        # Compress the pickle (if self._compression > 0)
        if self._compression > 0:
            comp = self._compression
        if comp:
            databytes = zlib.compress(databytes, comp)

        # Make a tuple of block info. The posting reader can check this info
        # and decide whether to skip the block without having to decompress the
        # full block data
        #
        # - Number of postings in block
        # - Last ID in block
        # - Maximum weight in block
        # - Compression level
        # - Minimum length byte
        # - Maximum length byte
        ids = self._ids
        infobytes = dumps(
            (
                len(ids),
                ids[-1],
                self._maxweight,
                comp,
                length_to_byte(self._minlength),
                length_to_byte(self._maxlength),
            ),
            2,
        )

        # Write block length
        postfile = self._postfile
        blocklength = len(infobytes) + len(databytes)
        if last:
            # If this is the last block, use a negative number
            blocklength *= -1
        postfile.write_int(blocklength)
        # Write block info
        postfile.write(infobytes)
        # Write block data
        postfile.write(databytes)

        self._blockcount += 1
        # Reset block buffer
        self._new_block()

    # Methods to reduce the byte size of the various lists
    def _mini_ids(self):
        """Minify the IDs."""
        ids = self._ids
        if not self._byteids:
            ids = delta_encode(ids)
        return tuple(ids)

    def _mini_weights(self):
        """Minify the weights."""
        weights = self._weights

        if all(w == 1.0 for w in weights):
            return None
        elif all(w == weights[0] for w in weights):
            return weights[0]
        else:
            return tuple(weights)

    def _mini_values(self):
        """Minify the values."""
        fixedsize = self._format.fixed_value_size()
        values = self._values

        if fixedsize is None or fixedsize < 0:
            vs = tuple(values)
        elif fixedsize == 0:
            vs = None
        else:
            vs = emptybytes.join(values)
        return vs

    # Block stats methods
    def __len__(self):
        """Return the number of unwritten buffered postings.

        Returns:
        int: The number of unwritten buffered postings.
        """
        return len(self._ids)

    def min_id(self):
        """Return the first ID in the buffered block.

        Returns:
        str or int: The first ID in the buffered block.
        """
        return self._ids[0]

    def max_id(self):
        """Return the last ID in the buffered block.

        Returns:
        str or int: The last ID in the buffered block.
        """
        return self._ids[-1]

    def min_length(self):
        """Return the shortest field length in the buffered block.

        Returns:
        int or None: The shortest field length in the buffered block.
        """
        return self._minlength

    def max_length(self):
        """Return the longest field length in the buffered block.

        Returns:
        int: The longest field length in the buffered block.
        """
        return self._maxlength

    def max_weight(self):
        """Return the highest weight in the buffered block.

        Returns:
        int or float: The highest weight in the buffered block.
        """
        return self._maxweight


class W3LeafMatcher(LeafMatcher):
    """Reads on-disk postings from the postings file and presents the
    :class:`whoosh.matching.Matcher` interface.

    Parameters:
    - postfile (file-like object): The file-like object representing the postings file.
    - startoffset (int): The starting offset of the postings in the file.
    - length (int): The length of the postings.
    - format_ (CodecFormat): The format of the postings.
    - term (bytes, optional): The term associated with the postings. Defaults to None.
    - byteids (bool, optional): Whether the IDs in the postings are stored as bytes. Defaults to None.
    - scorer (Scorer, optional): The scorer to use for scoring the postings. Defaults to None.

    Attributes:
    - _postfile (file-like object): The file-like object representing the postings file.
    - _startoffset (int): The starting offset of the postings in the file.
    - _length (int): The length of the postings.
    - format (CodecFormat): The format of the postings.
    - _term (bytes): The term associated with the postings.
    - _byteids (bool): Whether the IDs in the postings are stored as bytes.
    - scorer (Scorer): The scorer to use for scoring the postings.
    - _fixedsize (int): The fixed size of the values in the postings.
    - _baseoffset (int): The base offset of the postings (start of postings, after the header).
    - _blocklength (int): The length of the current block of postings.
    - _maxid (int): The maximum ID in the current block of postings.
    - _maxweight (float): The maximum weight in the current block of postings.
    - _compression (bool): Whether the block of postings is compressed.
    - _minlength (int): The minimum length of the values in the current block of postings.
    - _maxlength (int): The maximum length of the values in the current block of postings.
    - _lastblock (bool): Whether the current block of postings is the last block.
    - _atend (bool): Whether the matcher has reached the end of the postings.
    - _data (tuple): The data tuple of the current block of postings.
    - _ids (tuple): The IDs in the current block of postings.
    - _weights (array): The weights in the current block of postings.
    - _values (tuple): The values in the current block of postings.
    - _i (int): The current position in the block of postings.

    Methods:
    - _read_header(): Reads the header tag at the start of the postings.
    - reset(): Resets the matcher to read the first block of postings.
    - _goto(position): Reads the posting block at the given position.
    - _next_block(): Moves to the next block of postings.
    - _skip_to_block(skipwhile): Skips blocks as long as the skipwhile() function returns True.
    - is_active(): Checks if the matcher is active (not at the end of the postings).
    - id(): Returns the current ID (docnum for regular postings, term for vector).
    - weight(): Returns the weight for the current posting.
    - value(): Returns the value for the current posting.
    - next(): Moves to the next posting.
    - skip_to(targetid): Skips to the next ID equal to or greater than the given target ID.
    - skip_to_quality(minquality): Skips blocks until finding one that might exceed the given minimum quality.
    - block_min_id(): Returns the minimum ID in the current block of postings.
    - block_max_id(): Returns the maximum ID in the current block of postings.
    - block_min_length(): Returns the minimum length of the values in the current block of postings.
    - block_max_length(): Returns the maximum length of the values in the current block of postings.
    - block_max_weight(): Returns the maximum weight in the current block of postings.
    - _read_data(): Loads the block data tuple from disk.
    - _read_ids(): Loads the IDs from the block data.
    - _read_weights(): Loads the weights from the block data.
    - _read_values(): Loads the values from the block data.
    """


class W3LeafMatcher(LeafMatcher):
    """Reads on-disk postings from the postings file and presents the
    :class:`whoosh.matching.Matcher` interface.
    """

    def __init__(
        self,
        postfile,
        startoffset,
        length,
        format_,
        term=None,
        byteids=None,
        scorer=None,
    ):
        """
        Initialize a Whoosh3 object.

        Args:
            postfile (file-like object): The file-like object representing the postings file.
            startoffset (int): The starting offset of the postings in the file.
            length (int): The length of the postings in bytes.
            format_ (CodecFormat): The codec format used for encoding and decoding the postings.
            term (bytes, optional): The term associated with the postings. Defaults to None.
            byteids (list of int, optional): The byte IDs associated with the postings. Defaults to None.
            scorer (Scorer, optional): The scorer used for scoring the postings. Defaults to None.

        Attributes:
            _postfile (file-like object): The file-like object representing the postings file.
            _startoffset (int): The starting offset of the postings in the file.
            _length (int): The length of the postings in bytes.
            format (CodecFormat): The codec format used for encoding and decoding the postings.
            _term (bytes): The term associated with the postings.
            _byteids (list of int): The byte IDs associated with the postings.
            scorer (Scorer): The scorer used for scoring the postings.
            _fixedsize (int): The fixed size of each posting value.
        """
        self._postfile = postfile
        self._startoffset = startoffset
        self._length = length
        self.format = format_
        self._term = term
        self._byteids = byteids
        self.scorer = scorer

        self._fixedsize = self.format.fixed_value_size()
        # Read the header tag at the start of the postings
        self._read_header()
        # "Reset" to read the first block
        self.reset()

    def _read_header(self):
        """
        Reads and verifies the header of the postings file.

        This method seeks to the start of the postings file, reads the header tag, and verifies its correctness.
        It also sets the base offset to the current position in the file, which represents the start of the postings
        after the header.

        Raises:
            ValueError: If the header tag is incorrect.

        Usage:
            Call this method to read and verify the header of the postings file before accessing the postings data.

        """
        postfile = self._postfile

        postfile.seek(self._startoffset)
        magic = postfile.read(4)
        if magic != WHOOSH3_HEADER_MAGIC:
            raise ValueError(f"Block tag error {magic!r}")

        # Remember the base offset (start of postings, after the header)
        self._baseoffset = postfile.tell()

    def reset(self):
        """
        Reset the codec's internal state.

        This method resets the block stats, including block length, maximum ID, maximum weight,
        compression, minimum length, and maximum length. It also resets the flags indicating the
        last block and whether the codec is at the end.

        After resetting the internal state, the method consumes the first block by calling the
        `_goto` method with the base offset.

        Usage:
            codec.reset()

        """
        self._blocklength = None
        self._maxid = None
        self._maxweight = None
        self._compression = None
        self._minlength = None
        self._maxlength = None

        self._lastblock = False
        self._atend = False
        # Consume first block
        self._goto(self._baseoffset)

    def _goto(self, position):
        """
        Move the pointer to the given position in the posting file and load the block data.

        Args:
            position (int): The position in the posting file to move the pointer to.

        Returns:
            None

        Raises:
            None

        This method is responsible for moving the pointer to the specified position in the posting file
        and loading the block data from that position. It performs the following steps:
        1. Resets the block data attributes to None.
        2. Resets the pointer into the block to 0.
        3. Seeks to the start of the block in the posting file.
        4. Reads the length of the block.
        5. If the length is negative, sets the `_lastblock` attribute to True and makes the length positive.
        6. Remembers the offset of the next block.
        7. Reads the pickled block info tuple.
        8. Remembers the offset of the block's data.
        9. Decomposes the info tuple to set the current block info.

        Note:
            This method assumes that the posting file is already open and assigned to the `_postfile` attribute.
        """
        postfile = self._postfile

        # Reset block data -- we'll lazy load the data from the new block as
        # needed
        self._data = None
        self._ids = None
        self._weights = None
        self._values = None
        # Reset pointer into the block
        self._i = 0

        # Seek to the start of the block
        postfile.seek(position)
        # Read the block length
        length = postfile.read_int()
        # If the block length is negative, that means this is the last block
        if length < 0:
            self._lastblock = True
            length *= -1

        # Remember the offset of the next block
        self._nextoffset = position + _INT_SIZE + length
        # Read the pickled block info tuple
        info = postfile.read_pickle()
        # Remember the offset of the block's data
        self._dataoffset = postfile.tell()

        # Decompose the info tuple to set the current block info
        (
            self._blocklength,
            self._maxid,
            self._maxweight,
            self._compression,
            mnlen,
            mxlen,
        ) = info
        self._minlength = byte_to_length(mnlen)
        self._maxlength = byte_to_length(mxlen)

    def _next_block(self):
        """
        Move to the next block in the postings.

        This method is responsible for advancing the cursor to the next block in the postings.
        It handles cases where the cursor is already at the end, reached the end of the postings,
        or needs to move to the next block.

        Raises:
            ValueError: If there is no next block.

        Usage:
            Call this method to move the cursor to the next block in the postings.

        """
        if self._atend:
            # We were already at the end, and yet somebody called _next_block()
            # again, so something is wrong somewhere
            raise ValueError("No next block")
        elif self._lastblock:
            # Reached the end of the postings
            self._atend = True
        else:
            # Go to the next block
            self._goto(self._nextoffset)

    def _skip_to_block(self, skipwhile):
        """
        Skips blocks in the codec as long as the skipwhile() function returns True.

        Parameters:
        - skipwhile (function): A function that takes no arguments and returns a boolean value.
            It is called at each block to determine whether to skip to the next block or not.

        Returns:
        - skipped (int): The number of blocks skipped.

        Notes:
        - This method is used internally by the codec to skip blocks based on a condition.
        - The skipwhile() function should return True if the current block should be skipped,
            and False if the current block should not be skipped.

        Example usage:
        ```
        def skip_condition():
                # Skip blocks until a certain condition is met
                return some_condition()

        skipped_blocks = _skip_to_block(skip_condition)
        ```
        """
        skipped = 0
        while self.is_active() and skipwhile():
            self._next_block()
            skipped += 1
        return skipped

    def is_active(self):
        """
        Check if the current position in the file is active.

        Returns:
            bool: True if the current position is active, False otherwise.
        """
        return not self._atend and self._i < self._blocklength

    def id(self):
        """
        Get the current ID.

        This method returns the current ID, which can be either the docnum for regular postings or the term for vectors.

        Returns:
            int: The current ID.

        Raises:
            ValueError: If the block IDs have not been loaded yet.
        """

        # If we haven't loaded the block IDs yet, load them now
        if self._ids is None:
            self._read_ids()

        return self._ids[self._i]

    def weight(self):
        """
        Get the weight for the current posting.

        This method retrieves the weight associated with the current posting.
        If the block weights have not been loaded yet, it loads them before
        returning the weight.

        Returns:
            float: The weight of the current posting.

        Raises:
            Exception: If the block weights cannot be loaded.
        """
        # If we haven't loaded the block weights yet, load them now
        if self._weights is None:
            self._read_weights()

        return self._weights[self._i]

    def value(self):
        """
        Get the value for the current posting.

        If the block values have not been loaded yet, this method will load them.

        Returns:
            The value for the current posting.

        Raises:
            IndexError: If the current posting index is out of range.
        """
        # If we haven't loaded the block values yet, load them now
        if self._values is None:
            self._read_values()

        return self._values[self._i]

    def next(self):
        """
        Move to the next posting.

        This method increments the in-block pointer by 1. If the pointer reaches the end of the block,
        it moves to the next block and returns True. Otherwise, it returns False.

        Returns:
            bool: True if the pointer reached the end of the block and moved to the next block, False otherwise.
        """
        # Increment the in-block pointer
        self._i += 1
        # If we reached the end of the block, move to the next block
        if self._i == self._blocklength:
            self._next_block()
            return True
        else:
            return False

    def skip_to(self, targetid):
        """
        Skip to the next ID equal to or greater than the given target ID.

        Args:
            targetid (int): The target ID to skip to.

        Raises:
            ReadTooFar: If the skip operation is attempted when the reader is not active.

        Notes:
            - If the reader is already at or past the target ID, no skipping is performed.
            - The method skips to the block that would contain the target ID.
            - If the target ID is greater than the maximum ID in the current block, the method
              skips to the next block that would contain the target ID.
            - The method iterates through the IDs in the block until it finds or passes the target ID.

        """
        if not self.is_active():
            raise ReadTooFar

        # If we're already at or past target ID, do nothing
        if targetid <= self.id():
            return

        # Skip to the block that would contain the target ID
        block_max_id = self.block_max_id
        if targetid > block_max_id():
            self._skip_to_block(lambda: targetid > block_max_id())

        # Iterate through the IDs in the block until we find or pass the
        # target
        while self.is_active() and self.id() < targetid:
            self.next()

    def skip_to_quality(self, minquality):
        """
        Skips to the next block with a quality greater than or equal to the given minimum quality.

        Parameters:
        - minquality (float): The minimum quality threshold.

        Returns:
        - int: The number of blocks skipped.

        Notes:
        - This method is used to skip blocks in a search index until a block with a quality greater than or equal to the given minimum quality is found.
        - The block quality is determined by the `block_quality` attribute of the current object.
        - If the quality of the current block is already higher than the minimum quality, no blocks are skipped.
        - Blocks are skipped until a block with a quality greater than or equal to the minimum quality is found.
        """
        block_quality = self.block_quality

        # If the quality of this block is already higher than the minimum,
        # do nothing
        if block_quality() > minquality:
            return 0

        # Skip blocks as long as the block quality is not greater than the
        # minimum
        return self._skip_to_block(lambda: block_quality() <= minquality)

    def block_min_id(self):
        """
        Returns the minimum ID of the block.

        This method retrieves the minimum ID of the block. If the IDs have not been
        read yet, it reads them from the source.

        Returns:
            int: The minimum ID of the block.

        """
        if self._ids is None:
            self._read_ids()
        return self._ids[0]

    def block_max_id(self):
        """
        Returns the maximum ID of the block.

        This method returns the maximum ID of the block. The ID represents the highest
        value assigned to a block.

        Returns:
            int: The maximum ID of the block.

        Example:
            >>> codec = WhooshCodec()
            >>> codec.block_max_id()
            10
        """
        return self._maxid

    def block_min_length(self):
        """
        Returns the minimum length of a block.

        This method returns the minimum length of a block used by the codec.
        The block length is an important parameter that affects the indexing
        and searching process. It determines the size of the data chunks that
        are read and written during these operations.

        Returns:
            int: The minimum length of a block.

        """
        return self._minlength

    def block_max_length(self):
        """
        Returns the maximum length of a block in the codec.

        This method returns the maximum length of a block in the codec. A block is a unit of data used in the codec's
        internal operations. The maximum length of a block can affect the performance and memory usage of the codec.

        Returns:
            int: The maximum length of a block in the codec.

        Example:
            >>> codec = WhooshCodec()
            >>> codec.block_max_length()
            4096

        Note:
            The value returned by this method is determined by the codec implementation and may vary between different
            codecs.

        """
        return self._maxlength

    def block_max_weight(self):
        """
        Returns the maximum weight of a block in the codec.

        This method returns the maximum weight that a block can have in the codec.
        The weight of a block is a measure of its importance or relevance.

        Returns:
            int: The maximum weight of a block.

        Example:
            >>> codec = WhooshCodec()
            >>> codec.block_max_weight()
            100

        Note:
            The maximum weight can be used to determine the importance of a block
            when performing operations such as scoring or ranking.
        """
        return self._maxweight

    def _read_data(self):
        """
        Reads and loads the block data tuple from disk.

        This method reads the block data tuple from the disk, decompresses it if necessary,
        and unpickles the data tuple. The unpickled data tuple is then saved in the `_data`
        attribute of the object.

        Returns:
            None

        Raises:
            None
        """
        datalen = self._nextoffset - self._dataoffset
        b = self._postfile.get(self._dataoffset, datalen)

        # Decompress the pickled data if necessary
        if self._compression:
            b = zlib.decompress(b)

        # Unpickle the data tuple and save it in an attribute
        self._data = loads(b)

    def _read_ids(self):
        """
        Reads and initializes the document IDs from disk.

        This method loads the document IDs from disk if they haven't been loaded yet.
        It then de-minifies the IDs if necessary and sets the `_ids` attribute.

        Returns:
            None

        Raises:
            Any exceptions that occur during the data loading process.

        Usage:
            Call this method to load and initialize the document IDs before using them.
        """
        # If we haven't loaded the data from disk yet, load it now
        if self._data is None:
            self._read_data()
        ids = self._data[0]

        # De-minify the IDs
        if not self._byteids:
            ids = tuple(delta_decode(ids))

        self._ids = ids

    def _read_weights(self):
        """
        Reads and initializes the weights for the index.

        If the data has not been loaded from disk yet, it loads it first.
        The weights are then de-minified and stored in the `_weights` attribute.

        Returns:
            None

        Raises:
            None

        Usage:
            _read_weights()
        """
        # If we haven't loaded the data from disk yet, load it now
        if self._data is None:
            self._read_data()
        weights = self._data[1]

        # De-minify the weights
        postcount = self._blocklength
        if weights is None:
            self._weights = array("f", (1.0 for _ in range(postcount)))
        elif isinstance(weights, float):
            self._weights = array("f", (weights for _ in range(postcount)))
        else:
            self._weights = weights

    def _read_values(self):
        """
        Reads and de-minifies the values from the data.

        If the data has not been loaded from disk yet, it will be loaded before processing.

        Parameters:
            None

        Returns:
            None

        Raises:
            None

        Usage:
            Call this method to read and de-minify the values from the data.
            It is recommended to call this method before accessing the values.

        Example:
            _read_values()
        """
        # If we haven't loaded the data from disk yet, load it now
        if self._data is None:
            self._read_data()

        # De-minify the values
        fixedsize = self._fixedsize
        vs = self._data[2]
        if fixedsize is None or fixedsize < 0:
            self._values = vs
        elif fixedsize == 0:
            self._values = (None,) * self._blocklength
        else:
            assert isinstance(vs, bytes)
            self._values = tuple(
                vs[i : i + fixedsize] for i in range(0, len(vs), fixedsize)
            )


# Term info implementation
class W3TermInfo(TermInfo):
    """
    Represents term information for the Whoosh3 codec.

    This class is responsible for storing and manipulating term information such as
    weights, document frequencies, lengths, and IDs. It provides methods to add blocks
    of information, set extents, inline postings, and convert the term info to bytes.

    Attributes:
        _struct (struct.Struct): The struct format used to pack and unpack the term info.
        _offset (int): The offset of the term info in the posting file.
        _length (int): The length of the term info in the posting file.
        _inlined (tuple): A tuple containing the inlined postings (IDs, weights, values).

    """

    _struct = struct.Struct("!BfIBBfII")

    def __init__(self, *args, **kwargs):
        """
        Initializes a new instance of the W3TermInfo class.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        """
        TermInfo.__init__(self, *args, **kwargs)
        self._offset = None
        self._length = None
        self._inlined = None

    def add_block(self, block):
        """
        Adds a block of information to the term info.

        This method updates the total weight, document frequency, minimum length,
        maximum length, maximum weight, minimum ID, and maximum ID based on the
        information in the given block.

        Args:
            block (Block): The block of information to add.

        """
        self._weight += sum(block._weights)
        self._df += len(block)

        ml = block.min_length()
        if self._minlength is None:
            self._minlength = ml
        else:
            self._minlength = min(self._minlength, ml)

        self._maxlength = max(self._maxlength, block.max_length())
        self._maxweight = max(self._maxweight, block.max_weight())
        if self._minid is None:
            self._minid = block.min_id()
        self._maxid = block.max_id()

    def set_extent(self, offset, length):
        """
        Sets the extent of the term info in the posting file.

        This method sets the offset and length of the term info in the posting file.

        Args:
            offset (int): The offset of the term info.
            length (int): The length of the term info.

        """
        self._offset = offset
        self._length = length

    def extent(self):
        """
        Returns the extent of the term info in the posting file.

        Returns:
            tuple: A tuple containing the offset and length of the term info.

        """
        return self._offset, self._length

    def set_inlined(self, ids, weights, values):
        """
        Sets the inlined postings for the term info.

        This method sets the inlined postings, which are represented as tuples of IDs,
        weights, and values.

        Args:
            ids (tuple): A tuple of IDs.
            weights (tuple): A tuple of weights.
            values (tuple): A tuple of values.

        """
        self._inlined = (tuple(ids), tuple(weights), tuple(values))

    def is_inlined(self):
        """
        Checks if the term info has inlined postings.

        Returns:
            bool: True if the term info has inlined postings, False otherwise.

        """
        return self._inlined is not None

    def inlined_postings(self):
        """
        Returns the inlined postings for the term info.

        Returns:
            tuple: A tuple containing the inlined postings (IDs, weights, values).

        """
        return self._inlined

    def to_bytes(self):
        """
        Converts the term info to bytes.

        Returns:
            bytes: The term info encoded as bytes.

        """
        isinlined = self.is_inlined()

        # Encode the lengths as 0-255 values
        minlength = 0 if self._minlength is None else length_to_byte(self._minlength)
        maxlength = length_to_byte(self._maxlength)
        # Convert None values to the out-of-band NO_ID constant so they can be
        # stored as unsigned ints
        minid = 0xFFFFFFFF if self._minid is None else self._minid
        maxid = 0xFFFFFFFF if self._maxid is None else self._maxid

        # Pack the term info into bytes
        st = self._struct.pack(
            isinlined,
            self._weight,
            self._df,
            minlength,
            maxlength,
            self._maxweight,
            minid,
            maxid,
        )

        if isinlined:
            # Postings are inlined - dump them using the pickle protocol
            postbytes = dumps(self._inlined, 2)
        else:
            postbytes = pack_long(self._offset) + pack_int(self._length)
        st += postbytes
        return st

    @classmethod
    def from_bytes(cls, s):
        """
        Creates a new W3TermInfo instance from bytes.

        Args:
            s (bytes): The bytes representing the term info.

        Returns:
            W3TermInfo: A new instance of the W3TermInfo class.

        """
        st = cls._struct
        vals = st.unpack(s[: st.size])
        terminfo = cls()

        flags = vals[0]
        terminfo._weight = vals[1]
        terminfo._df = vals[2]
        terminfo._minlength = byte_to_length(vals[3])
        terminfo._maxlength = byte_to_length(vals[4])
        terminfo._maxweight = vals[5]
        terminfo._minid = None if vals[6] == 0xFFFFFFFF else vals[6]
        terminfo._maxid = None if vals[7] == 0xFFFFFFFF else vals[7]

        if flags:
            # Postings are stored inline
            terminfo._inlined = loads(s[st.size :])
        else:
            # Last bytes are pointer into posting file and length
            offpos = st.size
            lenpos = st.size + _LONG_SIZE
            terminfo._offset = unpack_long(s[offpos:lenpos])[0]
            terminfo._length = unpack_int(s[lenpos : lenpos + _INT_SIZE])

        return terminfo

    @classmethod
    def read_weight(cls, dbfile, datapos):
        """
        Reads the weight from the database file.

        Args:
            dbfile (DatabaseFile): The database file.
            datapos (int): The position of the weight in the file.

        Returns:
            float: The weight.

        """
        return dbfile.get_float(datapos + 1)

    @classmethod
    def read_doc_freq(cls, dbfile, datapos):
        """
        Reads the document frequency from the database file.

        Args:
            dbfile (DatabaseFile): The database file.
            datapos (int): The position of the document frequency in the file.

        Returns:
            int: The document frequency.

        """
        return dbfile.get_uint(datapos + 1 + _FLOAT_SIZE)

    @classmethod
    def read_min_and_max_length(cls, dbfile, datapos):
        """
        Reads the minimum and maximum length from the database file.

        Args:
            dbfile (DatabaseFile): The database file.
            datapos (int): The position of the lengths in the file.

        Returns:
            tuple: A tuple containing the minimum and maximum length.

        """
        lenpos = datapos + 1 + _FLOAT_SIZE + _INT_SIZE
        ml = byte_to_length(dbfile.get_byte(lenpos))
        xl = byte_to_length(dbfile.get_byte(lenpos + 1))
        return ml, xl

    @classmethod
    def read_max_weight(cls, dbfile, datapos):
        """
        Reads the maximum weight from the database file.

        Args:
            dbfile (DatabaseFile): The database file.
            datapos (int): The position of the maximum weight in the file.

        Returns:
            float: The maximum weight.

        """
        weightspos = datapos + 1 + _FLOAT_SIZE + _INT_SIZE + 2
        return dbfile.get_float(weightspos)


# Segment implementation
class W3Segment(base.Segment):
    """
    Represents a segment in the Whoosh index.

    Args:
        codec (Codec): The codec used for encoding and decoding the segment.
        indexname (str): The name of the index.
        doccount (int, optional): The number of documents in the segment. Defaults to 0.
        segid (str, optional): The unique identifier for the segment. If not provided, a random ID will be generated.
        deleted (set, optional): A set of deleted document numbers. Defaults to None.

    Attributes:
        indexname (str): The name of the index.
        segid (str): The unique identifier for the segment.
        compound (bool): Indicates whether the segment is a compound segment.
        _codec (Codec): The codec used for encoding and decoding the segment.
        _doccount (int): The number of documents in the segment.
        _deleted (set): A set of deleted document numbers.

    """

    def __init__(self, codec, indexname, doccount=0, segid=None, deleted=None):
        self.indexname = indexname
        self.segid = self._random_id() if segid is None else segid

        self._codec = codec
        self._doccount = doccount
        self._deleted = deleted
        self.compound = False

    def codec(self, **kwargs):
        """
        Returns the codec used for encoding and decoding the segment.

        Returns:
            Codec: The codec used for the segment.

        """
        return self._codec

    def set_doc_count(self, dc):
        """
        Sets the number of documents in the segment.

        Args:
            dc (int): The number of documents.

        """
        self._doccount = dc

    def doc_count_all(self):
        """
        Returns the total number of documents in the segment.

        Returns:
            int: The total number of documents.

        """
        return self._doccount

    def deleted_count(self):
        """
        Returns the number of deleted documents in the segment.

        Returns:
            int: The number of deleted documents.

        """
        if self._deleted is None:
            return 0
        return len(self._deleted)

    def deleted_docs(self):
        """
        Returns an iterator over the deleted document numbers in the segment.

        Returns:
            Iterator[int]: An iterator over the deleted document numbers.

        """
        if self._deleted is None:
            return ()
        else:
            return iter(self._deleted)

    def delete_document(self, docnum, delete=True):
        """
        Marks a document as deleted in the segment.

        Args:
            docnum (int): The document number to delete.
            delete (bool, optional): Whether to delete the document. Defaults to True.

        """
        if delete:
            if self._deleted is None:
                self._deleted = set()
            self._deleted.add(docnum)
        elif self._deleted is not None and docnum in self._deleted:
            self._deleted.clear(docnum)

    def is_deleted(self, docnum):
        """
        Checks if a document is marked as deleted in the segment.

        Args:
            docnum (int): The document number to check.

        Returns:
            bool: True if the document is marked as deleted, False otherwise.

        """
        if self._deleted is None:
            return False
        return docnum in self._deleted
