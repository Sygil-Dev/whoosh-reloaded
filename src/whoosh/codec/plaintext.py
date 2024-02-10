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

from ast import literal_eval
from pickle import dumps, loads

from whoosh.codec import base
from whoosh.matching import ListMatcher
from whoosh.reading import TermInfo, TermNotFound

_reprable = (bytes, str, int, float)


# Mixin classes for producing and consuming the simple text format


class LineWriter:
    """
    A class for writing lines to a file with specified indentation and command.

    Attributes:
        _dbfile (file): The file object to write the lines to.

    Methods:
        _print_line(indent, command, **kwargs): Writes a line to the file with the specified indentation, command, and keyword arguments.
    """

    def _print_line(self, indent, command, **kwargs):
        """
        Writes a line to the file with the specified indentation, command, and keyword arguments.

        Args:
            indent (int): The number of indentation levels for the line.
            command (str): The command to write.
            **kwargs: Additional keyword arguments to include in the line.

        Raises:
            TypeError: If a keyword argument value is not of a valid type.

        Returns:
            None
        """
        self._dbfile.write(b"  " * indent)
        self._dbfile.write(command.encode("latin1"))
        for k, v in kwargs.items():
            if isinstance(v, memoryview):
                v = bytes(v)
            if v is not None and not isinstance(v, _reprable):
                raise TypeError(type(v))
            self._dbfile.write(f"\t{k}={v!r}".encode("latin1"))
        self._dbfile.write(b"\n")


class LineReader:
    """A class for reading lines from a file and performing line-based operations."""

    def __init__(self, dbfile):
        """
        Initialize a LineReader object.

        Parameters:
        - dbfile (file): The file object to read lines from.
        """
        self._dbfile = dbfile

    def _reset(self):
        """
        Reset the file pointer to the beginning of the file.
        """
        self._dbfile.seek(0)

    def _find_line(self, indent, command, **kwargs):
        """
        Find the first line that matches the given indent, command, and keyword arguments.

        Parameters:
        - indent (int): The indentation level of the line.
        - command (str): The command to match.
        - kwargs (dict): Keyword arguments to match against the line's arguments.

        Returns:
        - tuple: A tuple containing the indent, command, and arguments of the matched line.
        """
        for largs in self._find_lines(indent, command, **kwargs):
            return largs

    def _find_lines(self, indent, command, **kwargs):
        """
        Find all lines that match the given indent, command, and keyword arguments.

        Parameters:
        - indent (int): The indentation level of the lines.
        - command (str): The command to match.
        - kwargs (dict): Keyword arguments to match against the lines' arguments.

        Yields:
        - tuple: A tuple containing the indent, command, and arguments of each matched line.
        """
        while True:
            line = self._dbfile.readline()
            if not line:
                return

            c = self._parse_line(line)
            if c is None:
                return

            lindent, lcommand, largs = c
            if lindent == indent and lcommand == command:
                matched = True
                if kwargs:
                    for k in kwargs:
                        if kwargs[k] != largs.get(k):
                            matched = False
                            break

                if matched:
                    yield largs
            elif lindent < indent:
                return

    def _parse_line(self, line):
        """
        Parse a line and extract the indent, command, and arguments.

        Parameters:
        - line (str): The line to parse.

        Returns:
        - tuple: A tuple containing the indent, command, and arguments of the line.
        """
        line = line.decode("latin1")
        line = line.rstrip()
        l = len(line)
        line = line.lstrip()
        if not line or line.startswith("#"):
            return None

        indent = (l - len(line)) // 2

        parts = line.split("\t")
        command = parts[0]
        args = {}
        for i in range(1, len(parts)):
            n, v = parts[i].split("=")
            args[n] = literal_eval(v)
        return (indent, command, args)

    def _find_root(self, command):
        """
        Find the root section with the given command.

        Parameters:
        - command (str): The command to match.

        Returns:
        - tuple: A tuple containing the indent, command, and arguments of the root section.

        Raises:
        - ValueError: If no root section with the given command is found.
        """
        self._reset()
        c = self._find_line(0, command)
        if c is None:
            raise ValueError(f"No root section {command}")


# Codec class
class PlainTextCodec(base.Codec):
    """
    Codec for storing and retrieving plain text documents in Whoosh.

    This codec provides the necessary methods for reading and writing plain text documents
    in Whoosh. It is responsible for handling the storage, segmentation, and retrieval of
    plain text data.

    Usage:
    ------
    codec = PlainTextCodec()
    per_doc_writer = codec.per_document_writer(storage, segment)
    field_writer = codec.field_writer(storage, segment)
    per_doc_reader = codec.per_document_reader(storage, segment)
    terms_reader = codec.terms_reader(storage, segment)
    segment = codec.new_segment(storage, indexname)
    """

    length_stats = False

    def per_document_writer(self, storage, segment):
        """
        Returns a per-document writer for the given storage and segment.

        Parameters:
        -----------
        storage : Storage
            The storage object used for storing the documents.
        segment : Segment
            The segment object representing the current segment.

        Returns:
        --------
        PlainPerDocWriter
            The per-document writer for the given storage and segment.
        """
        return PlainPerDocWriter(storage, segment)

    def field_writer(self, storage, segment):
        """
        Returns a field writer for the given storage and segment.

        Parameters:
        -----------
        storage : Storage
            The storage object used for storing the documents.
        segment : Segment
            The segment object representing the current segment.

        Returns:
        --------
        PlainFieldWriter
            The field writer for the given storage and segment.
        """
        return PlainFieldWriter(storage, segment)

    def per_document_reader(self, storage, segment):
        """
        Returns a per-document reader for the given storage and segment.

        Parameters:
        -----------
        storage : Storage
            The storage object used for retrieving the documents.
        segment : Segment
            The segment object representing the current segment.

        Returns:
        --------
        PlainPerDocReader
            The per-document reader for the given storage and segment.
        """
        return PlainPerDocReader(storage, segment)

    def terms_reader(self, storage, segment):
        """
        Returns a terms reader for the given storage and segment.

        Parameters:
        -----------
        storage : Storage
            The storage object used for retrieving the terms.
        segment : Segment
            The segment object representing the current segment.

        Returns:
        --------
        PlainTermsReader
            The terms reader for the given storage and segment.
        """
        return PlainTermsReader(storage, segment)

    def new_segment(self, storage, indexname):
        """
        Creates a new segment for the given storage and index name.

        Parameters:
        -----------
        storage : Storage
            The storage object used for storing the segment.
        indexname : str
            The name of the index.

        Returns:
        --------
        PlainSegment
            The new segment for the given storage and index name.
        """
        return PlainSegment(indexname)


class PlainPerDocWriter(base.PerDocumentWriter, LineWriter):
    """
    A class that writes per-document data in plain text format.

    This class is responsible for writing per-document data, such as document fields, column values, and vector items,
    in a plain text format. It inherits from the `PerDocumentWriter` and `LineWriter` classes.

    Usage:
    1. Create an instance of `PlainPerDocWriter` by providing a storage object and a segment object.
    2. Call the `start_doc` method to indicate the start of a new document.
    3. Call the `add_field` method to add a field to the document.
    4. Call the `add_column_value` method to add a column value to the document.
    5. Call the `add_vector_items` method to add vector items to the document.
    6. Call the `finish_doc` method to indicate the end of the current document.
    7. Call the `close` method to close the writer.

    Attributes:
    - `_dbfile`: The file object used for writing per-document data.
    - `is_closed`: A boolean indicating whether the writer has been closed.
    """

    def __init__(self, storage, segment):
        """
        Initializes a new instance of the PlainPerDocWriter class.

        Parameters:
        - `storage`: The storage object used for creating the per-document data file.
        - `segment`: The segment object representing the current segment.

        Returns:
        None.
        """
        self._dbfile = storage.create_file(segment.make_filename(".dcs"))
        self._print_line(0, "DOCS")
        self.is_closed = False

    def start_doc(self, docnum):
        """
        Indicates the start of a new document.

        Parameters:
        - `docnum`: The document number.

        Returns:
        None.
        """
        self._print_line(1, "DOC", dn=docnum)

    def add_field(self, fieldname, fieldobj, value, length):
        """
        Adds a field to the current document.

        Parameters:
        - `fieldname`: The name of the field.
        - `fieldobj`: The field object.
        - `value`: The value of the field.
        - `length`: The length of the field value.

        Returns:
        None.
        """
        if value is not None:
            value = dumps(value, 2)
        self._print_line(2, "DOCFIELD", fn=fieldname, v=value, len=length)

    def add_column_value(self, fieldname, columnobj, value):
        """
        Adds a column value to the current document.

        Parameters:
        - `fieldname`: The name of the field.
        - `columnobj`: The column object.
        - `value`: The value of the column.

        Returns:
        None.
        """
        self._print_line(2, "COLVAL", fn=fieldname, v=value)

    def add_vector_items(self, fieldname, fieldobj, items):
        """
        Adds vector items to the current document.

        Parameters:
        - `fieldname`: The name of the field.
        - `fieldobj`: The field object.
        - `items`: A list of vector items, where each item is a tuple containing the text, weight, and vector bytes.

        Returns:
        None.
        """
        self._print_line(2, "VECTOR", fn=fieldname)
        for text, weight, vbytes in items:
            self._print_line(3, "VPOST", t=text, w=weight, v=vbytes)

    def finish_doc(self):
        """
        Indicates the end of the current document.

        Returns:
        None.
        """
        # This method is intentionally left empty.
        pass

    def close(self):
        """
        Closes the writer.

        Returns:
        None.
        """
        self._dbfile.close()
        self.is_closed = True


class PlainPerDocReader(base.PerDocumentReader, LineReader):
    """
    A reader for plain text per-document data in Whoosh index.

    This class provides methods to read per-document data stored in plain text format in a Whoosh index.
    It inherits from the `PerDocumentReader` and `LineReader` classes.

    Attributes:
        _dbfile (File): The file object representing the per-document data file.
        _segment (Segment): The segment object representing the segment containing the per-document data.
        is_closed (bool): Indicates whether the reader is closed or not.

    Methods:
        doc_count(): Returns the number of documents in the segment.
        doc_count_all(): Returns the total number of documents in the segment.
        has_deletions(): Returns False, indicating that the segment does not have any deleted documents.
        is_deleted(docnum): Returns False, indicating that the specified document is not deleted.
        deleted_docs(): Returns an empty frozenset, indicating that there are no deleted documents.
        _find_doc(docnum): Internal method to find a document by its number.
        _iter_docs(): Internal method to iterate over the document numbers in the segment.
        _iter_docfields(fieldname): Internal method to iterate over the lines of a specific field in the document.
        _iter_lengths(fieldname): Internal method to iterate over the lengths of a specific field in the document.
        doc_field_length(docnum, fieldname, default=0): Returns the length of a specific field in the document.
        _column_values(fieldname): Internal method to iterate over the column values of a specific field in the document.
        has_column(fieldname): Returns True if the specified field has column values in the document, False otherwise.
        column_reader(fieldname, column): Returns a list of column values for a specific field in the document.
        field_length(fieldname): Returns the total length of a specific field in the document.
        min_field_length(fieldname): Returns the minimum length of a specific field in the document.
        max_field_length(fieldname): Returns the maximum length of a specific field in the document.
        has_vector(docnum, fieldname): Returns True if the document has a vector for the specified field, False otherwise.
        vector(docnum, fieldname, format_): Returns a ListMatcher object representing the vector for the specified field in the document.
        _read_stored_fields(): Internal method to read the stored fields of the document.
        stored_fields(docnum): Returns a dictionary containing the stored fields of the document.
        iter_docs(): Returns an iterator over the document numbers and their stored fields in the segment.
        all_stored_fields(): Returns an iterator over the stored fields of all documents in the segment.
        close(): Closes the reader and releases any associated resources.
    """

    def __init__(self, storage, segment):
        """
        Initializes a new instance of the PlainPerDocReader class.

        Args:
            storage (Storage): The storage object representing the index storage.
            segment (Segment): The segment object representing the segment containing the per-document data.
        """
        self._dbfile = storage.open_file(segment.make_filename(".dcs"))
        self._segment = segment
        self.is_closed = False

    def doc_count(self):
        """
        Returns the number of documents in the segment.

        Returns:
            int: The number of documents in the segment.
        """
        return self._segment.doc_count()

    def doc_count_all(self):
        """
        Returns the total number of documents in the segment.

        Returns:
            int: The total number of documents in the segment.
        """
        return self._segment.doc_count()

    def has_deletions(self):
        """
        Returns False, indicating that the segment does not have any deleted documents.

        Returns:
            bool: False, indicating that the segment does not have any deleted documents.
        """
        return False

    def is_deleted(self, docnum):
        """
        Returns False, indicating that the specified document is not deleted.

        Args:
            docnum (int): The document number.

        Returns:
            bool: False, indicating that the specified document is not deleted.
        """
        return False

    def deleted_docs(self):
        """
        Returns an empty frozenset, indicating that there are no deleted documents.

        Returns:
            frozenset: An empty frozenset, indicating that there are no deleted documents.
        """
        return frozenset()

    def _find_doc(self, docnum):
        """
        Internal method to find a document by its number.

        Args:
            docnum (int): The document number.

        Returns:
            bool: True if the document is found, False otherwise.
        """
        self._find_root("DOCS")
        c = self._find_line(1, "DOC")
        while c is not None:
            dn = c["dn"]
            if dn == docnum:
                return True
            elif dn > docnum:
                return False
            c = self._find_line(1, "DOC")
        return False

    def _iter_docs(self):
        """
        Internal method to iterate over the document numbers in the segment.

        Yields:
            int: The document number.
        """
        self._find_root("DOCS")
        c = self._find_line(1, "DOC")
        while c is not None:
            yield c["dn"]
            c = self._find_line(1, "DOC")

    def _iter_docfields(self, fieldname):
        """
        Internal method to iterate over the lines of a specific field in the document.

        Args:
            fieldname (str): The name of the field.

        Yields:
            dict: A dictionary representing a line of the field in the document.
        """
        for _ in self._iter_docs():
            yield from self._find_lines(2, "DOCFIELD", fn=fieldname)

    def _iter_lengths(self, fieldname):
        """
        Internal method to iterate over the lengths of a specific field in the document.

        Args:
            fieldname (str): The name of the field.

        Yields:
            int: The length of the field in the document.
        """
        return (c.get("len", 0) for c in self._iter_docfields(fieldname))

    def doc_field_length(self, docnum, fieldname, default=0):
        """
        Returns the length of a specific field in the document.

        Args:
            docnum (int): The document number.
            fieldname (str): The name of the field.
            default (int, optional): The default length to return if the field is not found. Defaults to 0.

        Returns:
            int: The length of the field in the document, or the default length if the field is not found.
        """
        for dn in self._iter_docs():
            if dn == docnum:
                c = self._find_line(2, "DOCFIELD", fn=fieldname)
                if c is not None:
                    return c.get("len", default)
            elif dn > docnum:
                break

        return default

    def _column_values(self, fieldname):
        """
        Internal method to iterate over the column values of a specific field in the document.

        Args:
            fieldname (str): The name of the field.

        Yields:
            Any: The column value.
        """
        for i, docnum in enumerate(self._iter_docs()):
            if i != docnum:
                raise ValueError(f"Missing column value for field {fieldname} doc {i}?")

            c = self._find_line(2, "COLVAL", fn=fieldname)
            if c is None:
                raise ValueError(
                    f"Missing column value for field {fieldname} doc {docnum}"
                )

            yield c.get("v")

    def has_column(self, fieldname):
        """
        Returns True if the specified field has column values in the document, False otherwise.

        Args:
            fieldname (str): The name of the field.

        Returns:
            bool: True if the specified field has column values in the document, False otherwise.
        """
        for _ in self._column_values(fieldname):
            return True
        return False

    def column_reader(self, fieldname, column):
        """
        Returns a list of column values for a specific field in the document.

        Args:
            fieldname (str): The name of the field.
            column (int): The column number.

        Returns:
            list: A list of column values for the specified field in the document.
        """
        return list(self._column_values(fieldname))

    def field_length(self, fieldname):
        """
        Returns the total length of a specific field in the document.

        Args:
            fieldname (str): The name of the field.

        Returns:
            int: The total length of the field in the document.
        """
        return sum(self._iter_lengths(fieldname))

    def min_field_length(self, fieldname):
        """
        Returns the minimum length of a specific field in the document.

        Args:
            fieldname (str): The name of the field.

        Returns:
            int: The minimum length of the field in the document.
        """
        return min(self._iter_lengths(fieldname))

    def max_field_length(self, fieldname):
        """
        Returns the maximum length of a specific field in the document.

        Args:
            fieldname (str): The name of the field.

        Returns:
            int: The maximum length of the field in the document.
        """
        return max(self._iter_lengths(fieldname))

    def has_vector(self, docnum, fieldname):
        """
        Returns True if the document has a vector for the specified field, False otherwise.

        Args:
            docnum (int): The document number.
            fieldname (str): The name of the field.

        Returns:
            bool: True if the document has a vector for the specified field, False otherwise.
        """
        if self._find_doc(docnum) and self._find_line(2, "VECTOR"):
            return True
        return False

    def vector(self, docnum, fieldname, format_):
        """
        Returns a ListMatcher object representing the vector for the specified field in the document.

        Args:
            docnum (int): The document number.
            fieldname (str): The name of the field.
            format_ (str): The format of the vector.

        Returns:
            ListMatcher: A ListMatcher object representing the vector for the specified field in the document.
        """
        if not self._find_doc(docnum):
            raise ValueError("Document not found.")
        if not self._find_line(2, "VECTOR"):
            raise ValueError("Vector not found.")

        ids = []
        weights = []
        values = []
        c = self._find_line(3, "VPOST")
        while c is not None:
            ids.append(c["t"])
            weights.append(c["w"])
            values.append(c["v"])
            c = self._find_line(3, "VPOST")

        return ListMatcher(
            ids,
            weights,
            values,
            format_,
        )

    def _read_stored_fields(self):
        """
        Internal method to read the stored fields of the document.

        Returns:
            dict: A dictionary containing the stored fields of the document.
        """
        sfs = {}
        c = self._find_line(2, "DOCFIELD")
        while c is not None:
            v = c.get("v")
            if v is not None:
                v = loads(v)
            sfs[c["fn"]] = v
            c = self._find_line(2, "DOCFIELD")
        return sfs

    def stored_fields(self, docnum):
        """
        Returns a dictionary containing the stored fields of the document.

        Args:
            docnum (int): The document number.

        Returns:
            dict: A dictionary containing the stored fields of the document.
        """
        if not self._find_doc(docnum):
            raise ValueError("Document not found.")
        return self._read_stored_fields()

    def iter_docs(self):
        """
        Returns an iterator over the document numbers and their stored fields in the segment.

        Yields:
            tuple: A tuple containing the document number and its stored fields.
        """
        return enumerate(self.all_stored_fields())

    def all_stored_fields(self):
        """
        Returns an iterator over the stored fields of all documents in the segment.

        Yields:
            dict: A dictionary containing the stored fields of a document.
        """
        for _ in self._iter_docs():
            yield self._read_stored_fields()

    def close(self):
        """
        Closes the reader and releases any associated resources.
        """
        self._dbfile.close()
        self.is_closed = True


class PlainFieldWriter(base.FieldWriter, LineWriter):
    """
    A class that writes field data in plain text format.

    This class is responsible for writing field data to a storage file in plain text format.
    It implements the necessary methods to handle field, term, and posting information.

    Attributes:
        _dbfile (File): The storage file for the field data.
        _fieldobj (Field): The field object being written.
        _terminfo (TermInfo): The term information being written.

    Methods:
        __init__(self, storage, segment): Initializes a PlainFieldWriter instance.
        is_closed(self): Checks if the writer is closed.
        start_field(self, fieldname, fieldobj): Starts writing a new field.
        start_term(self, btext): Starts writing a new term.
        add(self, docnum, weight, vbytes, length): Adds a posting to the current term.
        finish_term(self): Finishes writing the current term.
        add_spell_word(self, fieldname, text): Adds a spell word to the current field.
        close(self): Closes the writer and the storage file.
    """

    def __init__(self, storage, segment):
        """
        Initializes a PlainFieldWriter instance.

        Args:
            storage (Storage): The storage object for the field data.
            segment (Segment): The segment object for the field data.
        """
        self._dbfile = storage.create_file(segment.make_filename(".trm"))
        self._print_line(0, "TERMS")

    @property
    def is_closed(self):
        """
        Checks if the writer is closed.

        Returns:
            bool: True if the writer is closed, False otherwise.
        """
        return self._dbfile.is_closed

    def start_field(self, fieldname, fieldobj):
        """
        Starts writing a new field.

        Args:
            fieldname (str): The name of the field.
            fieldobj (Field): The field object.
        """
        self._fieldobj = fieldobj
        self._print_line(1, "TERMFIELD", fn=fieldname)

    def start_term(self, btext):
        """
        Starts writing a new term.

        Args:
            btext (bytes): The term text in bytes.
        """
        self._terminfo = TermInfo()
        self._print_line(2, "BTEXT", t=btext)

    def add(self, docnum, weight, vbytes, length):
        """
        Adds a posting to the current term.

        Args:
            docnum (int): The document number.
            weight (float): The weight of the posting.
            vbytes (int): The number of bytes in the posting.
            length (int): The length of the posting.
        """
        self._terminfo.add_posting(docnum, weight, length)
        self._print_line(3, "POST", dn=docnum, w=weight, v=vbytes)

    def finish_term(self):
        """
        Finishes writing the current term.
        """
        ti = self._terminfo
        self._print_line(
            3,
            "TERMINFO",
            df=ti.doc_frequency(),
            weight=ti.weight(),
            minlength=ti.min_length(),
            maxlength=ti.max_length(),
            maxweight=ti.max_weight(),
            minid=ti.min_id(),
            maxid=ti.max_id(),
        )

    def add_spell_word(self, fieldname, text):
        """
        Adds a spell word to the current field.

        Args:
            fieldname (str): The name of the field.
            text (str): The spell word text.
        """
        self._print_line(2, "SPELL", fn=fieldname, t=text)

    def close(self):
        """
        Closes the writer and the storage file.
        """
        self._dbfile.close()


class PlainTermsReader(base.TermsReader, LineReader):
    """
    A reader for plain text terms in a Whoosh index.

    This class provides methods to read and retrieve terms, term information,
    and perform term matching in a plain text index.

    Parameters:
    - storage (Storage): The storage object representing the index.
    - segment (Segment): The segment object representing the index segment.

    Attributes:
    - _dbfile (File): The file object representing the terms file.
    - _segment (Segment): The segment object representing the index segment.
    - is_closed (bool): Indicates whether the reader is closed or not.

    """

    def __init__(self, storage, segment):
        """
        Initializes a PlainTermsReader object.

        Parameters:
        - storage (Storage): The storage object representing the index.
        - segment (Segment): The segment object representing the index segment.

        """
        self._dbfile = storage.open_file(segment.make_filename(".trm"))
        self._segment = segment
        self.is_closed = False

    def _find_field(self, fieldname):
        """
        Finds the field with the given name in the terms file.

        Parameters:
        - fieldname (str): The name of the field to find.

        Raises:
        - TermNotFound: If the field with the given name is not found.

        """
        self._find_root("TERMS")
        if self._find_line(1, "TERMFIELD", fn=fieldname) is None:
            raise TermNotFound(f"No field {fieldname!r}")

    def _iter_fields(self):
        """
        Iterates over the field names in the terms file.

        Yields:
        - str: The name of each field.

        """
        self._find_root()
        c = self._find_line(1, "TERMFIELD")
        while c is not None:
            yield c["fn"]
            c = self._find_line(1, "TERMFIELD")

    def _iter_btexts(self):
        """
        Iterates over the binary texts in the terms file.

        Yields:
        - bytes: The binary text of each term.

        """
        c = self._find_line(2, "BTEXT")
        while c is not None:
            yield c["t"]
            c = self._find_line(2, "BTEXT")

    def _find_term(self, fieldname, btext):
        """
        Finds a term with the given field name and binary text in the terms file.

        Parameters:
        - fieldname (str): The name of the field.
        - btext (bytes): The binary text of the term.

        Returns:
        - bool: True if the term is found, False otherwise.

        """
        self._find_field(fieldname)
        for t in self._iter_btexts():
            if t == btext:
                return True
            elif t > btext:
                break
        return False

    def _find_terminfo(self):
        """
        Finds the term information in the terms file.

        Returns:
        - TermInfo: The term information.

        """
        c = self._find_line(3, "TERMINFO")
        return TermInfo(**c)

    def __contains__(self, term):
        """
        Checks if a term is present in the terms file.

        Parameters:
        - term (tuple): A tuple containing the field name and binary text of the term.

        Returns:
        - bool: True if the term is present, False otherwise.

        """
        fieldname, btext = term
        return self._find_term(fieldname, btext)

    def indexed_field_names(self):
        """
        Returns the names of the indexed fields in the terms file.

        Returns:
        - Iterator[str]: An iterator over the field names.

        """
        return self._iter_fields()

    def terms(self):
        """
        Returns an iterator over all the terms in the terms file.

        Yields:
        - tuple: A tuple containing the field name and binary text of each term.

        """
        for fieldname in self._iter_fields():
            for btext in self._iter_btexts():
                yield (fieldname, btext)

    def terms_from(self, fieldname, prefix):
        """
        Returns an iterator over the terms with the given field name and prefix.

        Parameters:
        - fieldname (str): The name of the field.
        - prefix (bytes): The prefix of the terms.

        Yields:
        - tuple: A tuple containing the field name and binary text of each term.

        """
        self._find_field(fieldname)
        for btext in self._iter_btexts():
            if btext < prefix:
                continue
            yield (fieldname, btext)

    def items(self):
        """
        Returns an iterator over the terms and their corresponding term information.

        Yields:
        - tuple: A tuple containing the term (field name and binary text) and its term information.

        """
        for fieldname, btext in self.terms():
            yield (fieldname, btext), self._find_terminfo()

    def items_from(self, fieldname, prefix):
        """
        Returns an iterator over the terms with the given field name and prefix, and their corresponding term information.

        Parameters:
        - fieldname (str): The name of the field.
        - prefix (bytes): The prefix of the terms.

        Yields:
        - tuple: A tuple containing the term (field name and binary text) and its term information.

        """
        for fieldname, btext in self.terms_from(fieldname, prefix):
            yield (fieldname, btext), self._find_terminfo()

    def term_info(self, fieldname, btext):
        """
        Retrieves the term information for the given field name and binary text.

        Parameters:
        - fieldname (str): The name of the field.
        - btext (bytes): The binary text of the term.

        Returns:
        - TermInfo: The term information.

        Raises:
        - TermNotFound: If the term is not found.

        """
        if not self._find_term(fieldname, btext):
            raise TermNotFound((fieldname, btext))
        return self._find_terminfo()

    def matcher(self, fieldname, btext, format_, scorer=None):
        """
        Creates a matcher for the given field name and binary text.

        Parameters:
        - fieldname (str): The name of the field.
        - btext (bytes): The binary text of the term.
        - format_ (int): The format of the matcher.
        - scorer (Scorer): The scorer object to use for scoring the matches.

        Returns:
        - ListMatcher: The matcher object.

        Raises:
        - TermNotFound: If the term is not found.

        """
        if not self._find_term(fieldname, btext):
            raise TermNotFound((fieldname, btext))

        ids = []
        weights = []
        values = []
        c = self._find_line(3, "POST")
        while c is not None:
            ids.append(c["dn"])
            weights.append(c["w"])
            values.append(c["v"])
            c = self._find_line(3, "POST")

        return ListMatcher(ids, weights, values, format_, scorer=scorer)

    def close(self):
        """
        Closes the PlainTermsReader object.

        """
        self._dbfile.close()
        self.is_closed = True


class PlainSegment(base.Segment):
    """
    Represents a segment in a plain text index.

    This class is responsible for managing a segment in a plain text index.
    It keeps track of the document count and provides methods to interact
    with the segment.

    Attributes:
        _doccount (int): The number of documents in the segment.
    """

    def __init__(self, indexname):
        """
        Initializes a PlainSegment object.

        Args:
            indexname (str): The name of the index.

        """
        base.Segment.__init__(self, indexname)
        self._doccount = 0

    def codec(self):
        """
        Returns the codec associated with the segment.

        Returns:
            PlainTextCodec: The codec associated with the segment.

        """
        return PlainTextCodec()

    def set_doc_count(self, doccount):
        """
        Sets the document count for the segment.

        Args:
            doccount (int): The number of documents in the segment.

        """
        self._doccount = doccount

    def doc_count(self):
        """
        Returns the document count for the segment.

        Returns:
            int: The number of documents in the segment.

        """
        return self._doccount

    def should_assemble(self):
        """
        Determines whether the segment should be assembled.

        Returns:
            bool: True if the segment should be assembled, False otherwise.

        """
        return False
