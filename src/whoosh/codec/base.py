# Copyright 2011 Matt Chaput. All rights reserved.
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
This module contains base classes/interfaces for "codec" objects.
"""

from abc import abstractmethod
from bisect import bisect_right

from whoosh import columns
from whoosh.automata import lev
from whoosh.filedb.compound import CompoundStorage
from whoosh.system import emptybytes
from whoosh.util import random_name

# Exceptions


class OutOfOrderError(Exception):
    """
    Exception raised when encountering out-of-order data during decoding.

    This exception is raised when the codec encounters data that is out of order
    during the decoding process. It typically indicates a corruption or
    inconsistency in the data being decoded.

    Attributes:
        message -- explanation of the error
    """

    pass


# Base classes


class Codec:
    """
    The base class for defining codecs in Whoosh.

    A codec is responsible for defining how data is stored and retrieved from the index.
    It provides implementations for various operations such as per-document value writing,
    inverted index writing, postings writing and reading, index readers, and segment and
    generation management.

    Subclasses of Codec should implement the abstract methods to provide the specific
    functionality required by the codec.

    Attributes:
        length_stats (bool): Indicates whether length statistics should be enabled for the codec.

    """

    length_stats = True

    @abstractmethod
    def per_document_writer(self, storage, segment):
        """
        Returns a per-document value writer for the given storage and segment.

        Args:
            storage (Storage): The storage object for the index.
            segment (Segment): The segment object representing a portion of the index.

        Returns:
            PerDocumentWriter: The per-document value writer.

        Raises:
            NotImplementedError: If the method is not implemented by the subclass.

        """

        raise NotImplementedError

    @abstractmethod
    def field_writer(self, storage, segment):
        """
        Returns an inverted index writer for the given storage and segment.

        Args:
            storage (Storage): The storage object for the index.
            segment (Segment): The segment object representing a portion of the index.

        Returns:
            FieldWriter: The inverted index writer.

        Raises:
            NotImplementedError: If the method is not implemented by the subclass.

        """

        raise NotImplementedError

    @abstractmethod
    def postings_writer(self, dbfile, byteids=False):
        """
        Returns a postings writer for the given database file.

        Args:
            dbfile (File): The file object representing the database file.
            byteids (bool, optional): Indicates whether the postings should be written using byte IDs.

        Returns:
            PostingsWriter: The postings writer.

        Raises:
            NotImplementedError: If the method is not implemented by the subclass.

        """

        raise NotImplementedError

    @abstractmethod
    def postings_reader(self, dbfile, terminfo, format_, term=None, scorer=None):
        """
        Returns a postings reader for the given database file.

        Args:
            dbfile (File): The file object representing the database file.
            terminfo (TermInfo): The term information object.
            format_ (str): The format of the postings.
            term (Term, optional): The term to read the postings for.
            scorer (Scorer, optional): The scorer object for scoring the postings.

        Returns:
            PostingsReader: The postings reader.

        Raises:
            NotImplementedError: If the method is not implemented by the subclass.

        """

        raise NotImplementedError

    def automata(self, storage, segment):
        """
        Returns an automata object for the given storage and segment.

        Args:
            storage (Storage): The storage object for the index.
            segment (Segment): The segment object representing a portion of the index.

        Returns:
            Automata: The automata object.

        """

        _ = storage, segment  # Unused arguments
        return Automata()

    @abstractmethod
    def terms_reader(self, storage, segment):
        """
        Returns a terms reader for the given storage and segment.

        Args:
            storage (Storage): The storage object for the index.
            segment (Segment): The segment object representing a portion of the index.

        Returns:
            TermsReader: The terms reader.

        Raises:
            NotImplementedError: If the method is not implemented by the subclass.

        """

        raise NotImplementedError

    @abstractmethod
    def per_document_reader(self, storage, segment):
        """
        Returns a per-document value reader for the given storage and segment.

        Args:
            storage (Storage): The storage object for the index.
            segment (Segment): The segment object representing a portion of the index.

        Returns:
            PerDocumentReader: The per-document value reader.

        Raises:
            NotImplementedError: If the method is not implemented by the subclass.

        """

        raise NotImplementedError

    @abstractmethod
    def new_segment(self, storage, indexname):
        """
        Creates a new segment for the given storage and index name.

        Args:
            storage (Storage): The storage object for the index.
            indexname (str): The name of the index.

        Returns:
            Segment: The new segment.

        Raises:
            NotImplementedError: If the method is not implemented by the subclass.

        """

        raise NotImplementedError


class WrappingCodec(Codec):
    """
    A codec that wraps another codec.

    This codec delegates all the method calls to the wrapped codec.
    It can be used to extend or modify the behavior of an existing codec.

    Parameters:
    - child (Codec): The codec to be wrapped.

    """

    def __init__(self, child):
        """
        Initializes a new instance of the WrappingCodec class.

        Parameters:
        - child (Codec): The codec to be wrapped.

        """
        self._child = child

    def per_document_writer(self, storage, segment):
        """
        Returns a per-document writer for the given storage and segment.

        Parameters:
        - storage (Storage): The storage object.
        - segment (Segment): The segment object.

        Returns:
        - PerDocumentWriter: The per-document writer.

        """
        return self._child.per_document_writer(storage, segment)

    def field_writer(self, storage, segment):
        """
        Returns a field writer for the given storage and segment.

        Parameters:
        - storage (Storage): The storage object.
        - segment (Segment): The segment object.

        Returns:
        - FieldWriter: The field writer.

        """
        return self._child.field_writer(storage, segment)

    def postings_writer(self, dbfile, byteids=False):
        """
        Returns a postings writer for the given dbfile.

        Parameters:
        - dbfile (DBFile): The dbfile object.
        - byteids (bool): Whether to use byteids.

        Returns:
        - PostingsWriter: The postings writer.

        """
        return self._child.postings_writer(dbfile, byteids=byteids)

    def postings_reader(self, dbfile, terminfo, format_, term=None, scorer=None):
        """
        Returns a postings reader for the given dbfile, terminfo, format, term, and scorer.

        Parameters:
        - dbfile (DBFile): The dbfile object.
        - terminfo (TermInfo): The terminfo object.
        - format_ (str): The format.
        - term (Term): The term object.
        - scorer (Scorer): The scorer object.

        Returns:
        - PostingsReader: The postings reader.

        """
        return self._child.postings_reader(
            dbfile, terminfo, format_, term=term, scorer=scorer
        )

    def automata(self, storage, segment):
        """
        Returns an automata object for the given storage and segment.

        Parameters:
        - storage (Storage): The storage object.
        - segment (Segment): The segment object.

        Returns:
        - Automata: The automata object.

        """
        return self._child.automata(storage, segment)

    def terms_reader(self, storage, segment):
        """
        Returns a terms reader for the given storage and segment.

        Parameters:
        - storage (Storage): The storage object.
        - segment (Segment): The segment object.

        Returns:
        - TermsReader: The terms reader.

        """
        return self._child.terms_reader(storage, segment)

    def per_document_reader(self, storage, segment):
        """
        Returns a per-document reader for the given storage and segment.

        Parameters:
        - storage (Storage): The storage object.
        - segment (Segment): The segment object.

        Returns:
        - PerDocumentReader: The per-document reader.

        """
        return self._child.per_document_reader(storage, segment)

    def new_segment(self, storage, indexname):
        """
        Returns a new segment for the given storage and indexname.

        Parameters:
        - storage (Storage): The storage object.
        - indexname (str): The indexname.

        Returns:
        - Segment: The new segment.

        """
        return self._child.new_segment(storage, indexname)


# Writer classes


class PerDocumentWriter:
    """
    The PerDocumentWriter class is an abstract base class that defines the interface for writing per-document data
    during the indexing process.

    Subclasses of PerDocumentWriter must implement the following methods:
    - start_doc(docnum): Called at the beginning of writing a new document.
    - add_field(fieldname, fieldobj, value, length): Called to add a field and its value to the document.
    - add_column_value(fieldname, columnobj, value): Called to add a column value to the document.
    - add_vector_items(fieldname, fieldobj, items): Called to add vector items to the document.

    The PerDocumentWriter class also provides default implementations for the following methods:
    - add_vector_matcher(fieldname, fieldobj, vmatcher): Adds vector items to the document using a vector matcher.
    - finish_doc(): Called at the end of writing a document.
    - close(): Called to close the writer.

    Usage:
    1. Create a subclass of PerDocumentWriter.
    2. Implement the required methods.
    3. Use the subclass to write per-document data during the indexing process.

    Example:
    ```python
    class MyDocumentWriter(PerDocumentWriter):
        def start_doc(self, docnum):
            # Implementation goes here

        def add_field(self, fieldname, fieldobj, value, length):
            # Implementation goes here

        def add_column_value(self, fieldname, columnobj, value):
            # Implementation goes here

        def add_vector_items(self, fieldname, fieldobj, items):
            # Implementation goes here

    writer = MyDocumentWriter()
    writer.start_doc(1)
    writer.add_field("title", fieldobj, "Sample Title", 1)
    writer.finish_doc()
    writer.close()
    ```
    """

    @abstractmethod
    def start_doc(self, docnum):
        """
        Called at the beginning of writing a new document.

        Parameters:
        - docnum (int): The document number.

        Raises:
        - NotImplementedError: If the method is not implemented by the subclass.
        """
        raise NotImplementedError

    @abstractmethod
    def add_field(self, fieldname, fieldobj, value, length):
        """
        Called to add a field and its value to the document.

        Parameters:
        - fieldname (str): The name of the field.
        - fieldobj: The field object.
        - value: The value of the field.
        - length (int): The length of the field.

        Raises:
        - NotImplementedError: If the method is not implemented by the subclass.
        """
        raise NotImplementedError

    @abstractmethod
    def add_column_value(self, fieldname, columnobj, value):
        """
        Called to add a column value to the document.

        Parameters:
        - fieldname (str): The name of the field.
        - columnobj: The column object.
        - value: The value of the column.

        Raises:
        - NotImplementedError: If the method is not implemented by the subclass.
        """
        raise NotImplementedError("Codec does not implement writing columns")

    @abstractmethod
    def add_vector_items(self, fieldname, fieldobj, items):
        """
        Called to add vector items to the document.

        Parameters:
        - fieldname (str): The name of the field.
        - fieldobj: The field object.
        - items: An iterable of vector items.

        Raises:
        - NotImplementedError: If the method is not implemented by the subclass.
        """
        raise NotImplementedError

    def add_vector_matcher(self, fieldname, fieldobj, vmatcher):
        """
        Adds vector items to the document using a vector matcher.

        Parameters:
        - fieldname (str): The name of the field.
        - fieldobj: The field object.
        - vmatcher: The vector matcher.

        Note:
        This method provides a default implementation that reads vector items from the vector matcher
        and calls the add_vector_items method.

        Raises:
        - NotImplementedError: If the add_vector_items method is not implemented by the subclass.
        """

        def readitems():
            while vmatcher.is_active():
                text = vmatcher.id()
                weight = vmatcher.weight()
                valuestring = vmatcher.value()
                yield (text, weight, valuestring)
                vmatcher.next()

        self.add_vector_items(fieldname, fieldobj, readitems())

    def finish_doc(self):
        """
        Called at the end of writing a document.

        Note:
        This method is intentionally left empty.

        Usage:
        Subclasses can override this method to perform any necessary cleanup or finalization steps.
        """
        pass

    def close(self):
        """
        Called to close the writer.

        Note:
        This method is intentionally left empty.

        Usage:
        Subclasses can override this method to perform any necessary cleanup or closing steps.
        """
        pass


class FieldWriter:
    """
    The FieldWriter class is responsible for translating a generator of postings into calls to various methods
    such as start_field(), start_term(), add(), finish_term(), finish_field(), etc. It is used in the process
    of writing fields and terms to an index.

    Usage:
    1. Create an instance of FieldWriter.
    2. Implement the abstract methods: start_field(), start_term(), add(), finish_term().
    3. Optionally, implement the add_spell_word() method if you need to add spelling words.
    4. Use the add_postings() method to process a generator of postings and write them to the index.
    5. Call the close() method to perform any necessary cleanup.

    Example:
    ```python
    class MyFieldWriter(FieldWriter):
        def start_field(self, fieldname, fieldobj):
            # Implementation goes here

        def start_term(self, text):
            # Implementation goes here

        def add(self, docnum, weight, vbytes, length):
            # Implementation goes here

        def finish_term(self):
            # Implementation goes here

        def add_spell_word(self, fieldname, text):
            # Implementation goes here

    writer = MyFieldWriter()
    writer.add_postings(schema, lengths, items)
    writer.close()
    ```

    Note: The finish_field() method is intentionally left empty and does not need to be implemented.
    """

    def add_postings(self, schema, lengths, items):
        """
        Translates a generator of (fieldname, btext, docnum, w, v) postings into calls to start_field(), start_term(),
        add(), finish_term(), finish_field(), etc.

        Parameters:
        - schema (Schema): The schema object that defines the fields in the index.
        - lengths (Lengths): The lengths object that provides the document field lengths.
        - items (generator): A generator of (fieldname, btext, docnum, weight, value) postings.

        Raises:
        - OutOfOrderError: If the postings are out of order.

        Returns:
        - None
        """
        start_field = self.start_field
        start_term = self.start_term
        add = self.add
        finish_term = self.finish_term
        finish_field = self.finish_field

        if lengths:
            dfl = lengths.doc_field_length
        else:
            dfl = lambda docnum, fieldname: 0

        # The fieldname of the previous posting
        lastfn = None
        # The bytes text of the previous posting
        lasttext = None
        # The (fieldname, btext) of the previous spelling posting
        # lastspell = None
        # The field object for the current field
        fieldobj = None
        for fieldname, btext, docnum, weight, value in items:
            # Check for out-of-order postings. This is convoluted because Python
            # 3 removed the ability to compare a string to None
            if lastfn is not None and fieldname < lastfn:
                raise OutOfOrderError(f"Field {lastfn!r} .. {fieldname!r}")
            if fieldname == lastfn and lasttext and btext < lasttext:
                raise OutOfOrderError(
                    f"Term {lastfn}:{lasttext!r} .. {fieldname}:{btext!r}"
                )

            # If the fieldname of this posting is different from the last one,
            # tell the writer we're starting a new field
            if fieldname != lastfn:
                if lasttext is not None:
                    finish_term()
                if lastfn is not None and fieldname != lastfn:
                    finish_field()
                fieldobj = schema[fieldname]
                start_field(fieldname, fieldobj)
                lastfn = fieldname
                lasttext = None

            # HACK: items where docnum == -1 indicate words that should be added
            # to the spelling graph, not the postings
            if docnum == -1:
                # spellterm = (fieldname, btext)
                # # There can be duplicates of spelling terms, so only add a spell
                # # term if it's greater than the last one
                # if lastspell is None or spellterm > lastspell:
                #     spellword = fieldobj.from_bytes(btext)
                #     self.add_spell_word(fieldname, spellword)
                #     lastspell = spellterm
                continue

            # If this term is different from the term in the previous posting,
            # tell the writer to start a new term
            if btext != lasttext:
                if lasttext is not None:
                    finish_term()
                start_term(btext)
                lasttext = btext

            # Add this posting
            length = dfl(docnum, fieldname)
            if value is None:
                value = emptybytes
            add(docnum, weight, value, length)

        if lasttext is not None:
            finish_term()
        if lastfn is not None:
            finish_field()

    @abstractmethod
    def start_field(self, fieldname, fieldobj):
        """
        This method is called when starting to process a new field during indexing or searching.

        Parameters:
        - fieldname (str): The name of the field being processed.
        - fieldobj: The field object representing the field being processed.

        Raises:
        - NotImplementedError: This method should be implemented by subclasses.

        Notes:
        - This method is typically used for initializing any necessary resources or state for processing the field.
        - Subclasses should override this method to provide their own implementation.
        """
        raise NotImplementedError

    @abstractmethod
    def start_term(self, text):
        """
        This method is called to indicate the start of a term during indexing or searching.

        Parameters:
        - text (str): The text of the term.

        Raises:
        - NotImplementedError: This method should be implemented by subclasses.

        """
        raise NotImplementedError

    @abstractmethod
    def add(self, docnum, weight, vbytes, length):
        """
        Adds a document to the codec.

        Args:
            docnum (int): The document number.
            weight (float): The weight of the document.
            vbytes (bytes): The encoded document data.
            length (int): The length of the document in bytes.

        Raises:
            NotImplementedError: This method should be implemented by a subclass.

        """
        raise NotImplementedError

    def add_spell_word(self, fieldname, text):
        """
        Adds a spell word to the specified field.

        Args:
            fieldname (str): The name of the field to add the spell word to.
            text (str): The spell word to add.

        Raises:
            NotImplementedError: This method is not implemented in the base class.
        """
        raise NotImplementedError

    @abstractmethod
    def finish_term(self):
        """
        Finish processing the current term.

        This method is called to finalize the processing of the current term. Subclasses should implement this method
        to perform any necessary cleanup or finalization steps for the term.

        Raises:
            NotImplementedError: This method is meant to be overridden by subclasses.
        """
        raise NotImplementedError

    def finish_field(self):
        """
        Finish processing the current field.

        This method is called after all the terms in a field have been processed.
        It can be overridden in subclasses to perform any necessary finalization
        steps for the field.

        Usage:
            codec = BaseCodec()
            codec.finish_field()

        """
        # This method is intentionally left empty.
        pass

    def close(self):
        """
        Closes the codec.

        This method is called when the codec needs to be closed. It should release any resources
        held by the codec and perform any necessary cleanup.

        Example usage:
        codec = MyCodec()
        # ... do some operations with the codec ...
        codec.close()
        """
        pass


# Postings
class PostingsWriter:
    """Abstract base class for writing postings lists to disk.

    This class defines the interface for writing postings lists to disk in a specific format.
    Subclasses must implement the abstract methods to provide the necessary functionality.

    Attributes:
        None

    Methods:
        start_postings(format_, terminfo): Start writing a new postings list.
        add_posting(id_, weight, vbytes, length=None): Add a posting to the current postings list.
        finish_postings(): Finish writing the current postings list.
        written(): Check if this object has already written to disk.

    """

    @abstractmethod
    def start_postings(self, format_, terminfo):
        """Start writing a new postings list.

        Args:
            format_ (str): The format of the postings list.
            terminfo (object): The term information associated with the postings list.

        Returns:
            None

        Raises:
            NotImplementedError: This method must be implemented by subclasses.

        """

        raise NotImplementedError

    @abstractmethod
    def add_posting(self, id_, weight, vbytes, length=None):
        """Add a posting to the current postings list.

        Args:
            id_ (int): The identifier of the posting.
            weight (float): The weight of the posting.
            vbytes (bytes): The encoded bytes of the posting.
            length (int, optional): The length of the posting. Defaults to None.

        Returns:
            None

        Raises:
            NotImplementedError: This method must be implemented by subclasses.

        """

        raise NotImplementedError

    def finish_postings(self):
        """Finish writing the current postings list.

        This method is intentionally left empty.

        Args:
            None

        Returns:
            None

        """

        pass

    @abstractmethod
    def written(self):
        """Check if this object has already written to disk.

        Args:
            None

        Returns:
            bool: True if this object has already written to disk, False otherwise.

        Raises:
            NotImplementedError: This method must be implemented by subclasses.

        """

        raise NotImplementedError


# Reader classes
class FieldCursor:
    """A cursor for navigating through a field's data.

    This class provides methods for navigating through a field's data,
    such as moving to the first position, finding a specific string,
    moving to the next position, and retrieving the current term.

    Usage:
        cursor = FieldCursor()
        cursor.first()  # Move to the first position
        cursor.find("example")  # Find the position of the string "example"
        cursor.next()  # Move to the next position
        term = cursor.term()  # Retrieve the current term

    Note:
        This class is meant to be subclassed and the methods should be
        implemented according to the specific requirements of the field's
        data format.
    """

    def first(self):
        """Move the cursor to the first position.

        Raises:
            NotImplementedError: This method should be implemented by
                subclasses.
        """
        raise NotImplementedError

    def find(self, string):
        """Find the position of a specific string.

        Args:
            string (str): The string to find.

        Raises:
            NotImplementedError: This method should be implemented by
                subclasses.
        """
        raise NotImplementedError

    def next(self):
        """Move the cursor to the next position.

        Raises:
            NotImplementedError: This method should be implemented by
                subclasses.
        """
        raise NotImplementedError

    def term(self):
        """Retrieve the current term.

        Returns:
            str: The current term.

        Raises:
            NotImplementedError: This method should be implemented by
                subclasses.
        """
        raise NotImplementedError


class TermsReader:
    """A base class for reading terms and their associated information from an index.

    This class provides methods for retrieving terms, term frequencies, document frequencies,
    and creating term matchers for querying the index.

    Subclasses of `TermsReader` should implement the abstract methods to provide the necessary
    functionality for reading terms from a specific index format.

    """

    @abstractmethod
    def __contains__(self, term):
        """Check if a term exists in the index.

        Args:
            term (str): The term to check.

        Returns:
            bool: True if the term exists in the index, False otherwise.

        Raises:
            NotImplementedError: If the method is not implemented by the subclass.

        """
        raise NotImplementedError

    @abstractmethod
    def cursor(self, fieldname, fieldobj):
        """Get a cursor for iterating over the terms in a field.

        Args:
            fieldname (str): The name of the field.
            fieldobj (object): The field object.

        Returns:
            object: A cursor object for iterating over the terms in the field.

        Raises:
            NotImplementedError: If the method is not implemented by the subclass.

        """
        raise NotImplementedError

    @abstractmethod
    def terms(self):
        """Get a list of all terms in the index.

        Returns:
            list: A list of all terms in the index.

        Raises:
            NotImplementedError: If the method is not implemented by the subclass.

        """
        raise NotImplementedError

    @abstractmethod
    def terms_from(self, fieldname, prefix):
        """Get a list of terms starting with a given prefix in a specific field.

        Args:
            fieldname (str): The name of the field.
            prefix (str): The prefix to match.

        Returns:
            list: A list of terms starting with the given prefix in the field.

        Raises:
            NotImplementedError: If the method is not implemented by the subclass.

        """
        raise NotImplementedError

    @abstractmethod
    def items(self):
        """Get a list of all (fieldname, term) pairs in the index.

        Returns:
            list: A list of all (fieldname, term) pairs in the index.

        Raises:
            NotImplementedError: If the method is not implemented by the subclass.

        """
        raise NotImplementedError

    @abstractmethod
    def items_from(self, fieldname, prefix):
        """Get a list of (fieldname, term) pairs starting with a given prefix in a specific field.

        Args:
            fieldname (str): The name of the field.
            prefix (str): The prefix to match.

        Returns:
            list: A list of (fieldname, term) pairs starting with the given prefix in the field.

        Raises:
            NotImplementedError: If the method is not implemented by the subclass.

        """
        raise NotImplementedError

    @abstractmethod
    def term_info(self, fieldname, text):
        """Get the term information for a specific term in a field.

        Args:
            fieldname (str): The name of the field.
            text (str): The term to get information for.

        Returns:
            object: The term information object for the specified term in the field.

        Raises:
            NotImplementedError: If the method is not implemented by the subclass.

        """
        raise NotImplementedError

    @abstractmethod
    def frequency(self, fieldname, text):
        """Get the term frequency for a specific term in a field.

        Args:
            fieldname (str): The name of the field.
            text (str): The term to get the frequency for.

        Returns:
            int: The term frequency for the specified term in the field.

        Raises:
            NotImplementedError: If the method is not implemented by the subclass.

        """
        return self.term_info(fieldname, text).weight()

    @abstractmethod
    def doc_frequency(self, fieldname, text):
        """Get the document frequency for a specific term in a field.

        Args:
            fieldname (str): The name of the field.
            text (str): The term to get the document frequency for.

        Returns:
            int: The document frequency for the specified term in the field.

        Raises:
            NotImplementedError: If the method is not implemented by the subclass.

        """
        return self.term_info(fieldname, text).doc_frequency()

    @abstractmethod
    def matcher(self, fieldname, text, format_, scorer=None):
        """Create a term matcher for a specific term in a field.

        Args:
            fieldname (str): The name of the field.
            text (str): The term to create the matcher for.
            format_ (object): The format object for the field.
            scorer (object, optional): The scorer object to use for scoring the matches.

        Returns:
            object: A term matcher for the specified term in the field.

        Raises:
            NotImplementedError: If the method is not implemented by the subclass.

        """
        raise NotImplementedError

    @abstractmethod
    def indexed_field_names(self):
        """Get a list of all field names in the index.

        Returns:
            list: A list of all field names in the index.

        Raises:
            NotImplementedError: If the method is not implemented by the subclass.

        """
        raise NotImplementedError

    def close(self):
        """Close the terms reader.

        This method is intentionally left empty.

        """
        pass


class Automata:
    """
    The Automata class provides methods for working with automata used in string matching operations.
    """

    @staticmethod
    def levenshtein_dfa(uterm, maxdist, prefix=0):
        """
        Generates a deterministic finite automaton (DFA) for performing approximate string matching using the Levenshtein distance algorithm.

        Args:
            uterm (str): The target term to match against.
            maxdist (int): The maximum allowed edit distance between the target term and the matched terms.
            prefix (int, optional): The length of the common prefix between the target term and the matched terms. Defaults to 0.

        Returns:
            DFA: The generated DFA for performing approximate string matching.
        """
        return lev.levenshtein_automaton(uterm, maxdist, prefix).to_dfa()

    @staticmethod
    def find_matches(dfa, cur):
        """
        Finds all matches in a given cursor using a DFA.

        Args:
            dfa (DFA): The DFA used for matching.
            cur (Cursor): The cursor to search for matches.

        Yields:
            str: The matched terms found in the cursor.
        """
        unull = chr(0)

        term = cur.text()
        if term is None:
            return

        match = dfa.next_valid_string(term)
        while match:
            cur.find(match)
            term = cur.text()
            if term is None:
                return
            if match == term:
                yield match
                term += unull
            match = dfa.next_valid_string(term)

    def terms_within(self, fieldcur, uterm, maxdist, prefix=0):
        """
        Finds all terms within a given cursor that are within a specified edit distance of a target term.

        Args:
            fieldcur (Cursor): The cursor representing the field to search within.
            uterm (str): The target term to match against.
            maxdist (int): The maximum allowed edit distance between the target term and the matched terms.
            prefix (int, optional): The length of the common prefix between the target term and the matched terms. Defaults to 0.

        Returns:
            Generator[str]: A generator that yields the matched terms found within the cursor.
        """
        dfa = self.levenshtein_dfa(uterm, maxdist, prefix)
        return self.find_matches(dfa, fieldcur)


# Per-doc value reader
class PerDocumentReader:
    """
    The PerDocumentReader class represents a base class for reading per-document data in a search index.

    This class provides methods for accessing and manipulating per-document data, such as deletions, columns, bitmaps,
    lengths, vectors, and stored fields.

    Subclasses of PerDocumentReader should implement the abstract methods to provide the specific functionality
    required for a particular codec.

    Usage:
    1. Create an instance of a subclass of PerDocumentReader.
    2. Use the provided methods to access and manipulate per-document data.

    Example:
    ```
    reader = MyPerDocumentReader()
    count = reader.doc_count()
    print(f"Total number of documents: {count}")
    ```
    """

    def close(self):
        """
        Closes the PerDocumentReader and releases any resources associated with it.

        This method should be called when the PerDocumentReader is no longer needed.
        """

        pass

    @abstractmethod
    def doc_count(self):
        """
        Returns the number of documents in the reader.

        Raises:
            NotImplementedError: If the method is not implemented by the subclass.
        """

        raise NotImplementedError

    @abstractmethod
    def doc_count_all(self):
        """
        Returns the total number of documents, including deleted documents, in the reader.

        Raises:
            NotImplementedError: If the method is not implemented by the subclass.
        """

        raise NotImplementedError

    # Deletions

    @abstractmethod
    def has_deletions(self):
        """
        Returns True if the reader has deletions, False otherwise.

        Raises:
            NotImplementedError: If the method is not implemented by the subclass.
        """

        raise NotImplementedError

    @abstractmethod
    def is_deleted(self, docnum):
        """
        Returns True if the document with the given docnum is deleted, False otherwise.

        Args:
            docnum (int): The document number.

        Raises:
            NotImplementedError: If the method is not implemented by the subclass.
        """

        raise NotImplementedError

    @abstractmethod
    def deleted_docs(self):
        """
        Returns a set of document numbers that are deleted.

        Raises:
            NotImplementedError: If the method is not implemented by the subclass.
        """

        raise NotImplementedError

    def all_doc_ids(self):
        """
        Returns an iterator of all (undeleted) document IDs in the reader.

        Returns:
            An iterator of document IDs.

        Example:
        ```
        for doc_id in reader.all_doc_ids():
            print(doc_id)
        ```
        """

        is_deleted = self.is_deleted
        return (
            docnum for docnum in range(self.doc_count_all()) if not is_deleted(docnum)
        )

    def iter_docs(self):
        """
        Returns an iterator over all (undeleted) documents in the reader.

        Yields:
            Tuple[int, dict]: A tuple containing the document number and the stored fields of the document.

        Example:
        ```
        for docnum, fields in reader.iter_docs():
            print(f"Document {docnum}: {fields}")
        ```
        """

        for docnum in self.all_doc_ids():
            yield docnum, self.stored_fields(docnum)

    # Columns

    def supports_columns(self):
        """
        Returns True if the reader supports columns, False otherwise.

        Returns:
            bool: True if the reader supports columns, False otherwise.
        """

        return False

    def has_column(self, fieldname):
        """
        Returns True if the reader has a column with the given fieldname, False otherwise.

        Args:
            fieldname (str): The name of the column field.

        Returns:
            bool: True if the reader has the column, False otherwise.
        """

        _ = fieldname  # Unused argument
        return False

    def list_columns(self):
        """
        Returns a list of all column names in the reader.

        Raises:
            NotImplementedError: If the method is not implemented by the subclass.
        """

        raise NotImplementedError

    # Don't need to override this if supports_columns() returns False
    def column_reader(self, fieldname, column):
        """
        Returns a reader for accessing the values in the specified column.

        Args:
            fieldname (str): The name of the column field.
            column (str): The name of the column.

        Raises:
            NotImplementedError: If the method is not implemented by the subclass.
        """

        raise NotImplementedError

    # Bitmaps

    def field_docs(self, fieldname):
        """
        Returns the bitmap of documents that have a value for the specified field.

        Args:
            fieldname (str): The name of the field.

        Returns:
            Bitmap or None: The bitmap of documents or None if the field does not exist.

        Raises:
            NotImplementedError: If the method is not implemented by the subclass.
        """

        _ = fieldname  # Unused argument
        return None

    # Lengths

    @abstractmethod
    def doc_field_length(self, docnum, fieldname, default=0):
        """
        Returns the length of the specified field in the specified document.

        Args:
            docnum (int): The document number.
            fieldname (str): The name of the field.
            default (int, optional): The default length to return if the field does not exist. Defaults to 0.

        Raises:
            NotImplementedError: If the method is not implemented by the subclass.
        """

        raise NotImplementedError

    @abstractmethod
    def field_length(self, fieldname):
        """
        Returns the total length of the specified field across all documents.

        Args:
            fieldname (str): The name of the field.

        Raises:
            NotImplementedError: If the method is not implemented by the subclass.
        """

        raise NotImplementedError

    @abstractmethod
    def min_field_length(self, fieldname):
        """
        Returns the minimum length of the specified field across all documents.

        Args:
            fieldname (str): The name of the field.

        Raises:
            NotImplementedError: If the method is not implemented by the subclass.
        """

        raise NotImplementedError

    @abstractmethod
    def max_field_length(self, fieldname):
        """
        Returns the maximum length of the specified field across all documents.

        Args:
            fieldname (str): The name of the field.

        Raises:
            NotImplementedError: If the method is not implemented by the subclass.
        """

        raise NotImplementedError

    # Vectors

    def has_vector(self, docnum, fieldname):
        """
        Returns True if the specified document has a vector for the specified field, False otherwise.

        Args:
            docnum (int): The document number.
            fieldname (str): The name of the field.

        Returns:
            bool: True if the document has a vector, False otherwise.
        """

        _ = docnum, fieldname  # Unused arguments
        return False

    # Don't need to override this if has_vector() always returns False
    def vector(self, docnum, fieldname, format_):
        """
        Returns the vector for the specified document and field.

        Args:
            docnum (int): The document number.
            fieldname (str): The name of the field.
            format_ (str): The format of the vector.

        Raises:
            NotImplementedError: If the method is not implemented by the subclass.
        """

        raise NotImplementedError

    # Stored

    @abstractmethod
    def stored_fields(self, docnum):
        """
        Returns the stored fields of the specified document.

        Args:
            docnum (int): The document number.

        Raises:
            NotImplementedError: If the method is not implemented by the subclass.
        """

        raise NotImplementedError

    def all_stored_fields(self):
        """
        Returns an iterator over the stored fields of all (undeleted) documents in the reader.

        Yields:
            dict: The stored fields of a document.

        Example:
        ```
        for fields in reader.all_stored_fields():
            print(fields)
        ```
        """

        for docnum in self.all_doc_ids():
            yield self.stored_fields(docnum)


# Segment base class
class Segment:
    """Do not instantiate this object directly. It is used by the Index object
    to hold information about a segment. A list of objects of this class are
    pickled as part of the TOC file.

    The TOC file stores a minimal amount of information -- mostly a list of
    Segment objects. Segments are the real reverse indexes. Having multiple
    segments allows quick incremental indexing: just create a new segment for
    the new documents, and have the index overlay the new segment over previous
    ones for purposes of reading/search. "Optimizing" the index combines the
    contents of existing segments into one (removing any deleted documents
    along the way).
    """

    # Extension for compound segment files
    COMPOUND_EXT = ".seg"

    # self.indexname
    # self.segid

    def __init__(self, indexname):
        """
        Initializes a Segment object.

        :param indexname: The name of the index.
        """
        self.indexname = indexname
        self.segid = self._random_id()
        self.compound = False

    @classmethod
    def _random_id(cls, size=16):
        """
        Generates a random ID for the segment.

        :param size: The size of the random ID. Default is 16.
        :return: The random ID.
        """
        return random_name(size=size)

    def __repr__(self):
        """
        Returns a string representation of the Segment object.

        :return: The string representation.
        """
        return f"<{self.__class__.__name__} {self.segment_id()}>"

    def __eq__(self, other):
        """
        Checks if two Segment objects are equal.

        :param other: The other Segment object to compare.
        :return: True if the objects are equal, False otherwise.
        """
        return isinstance(other, type(self)) and self.segment_id() == other.segment_id()

    def __hash__(self):
        """
        Returns the hash value of the Segment object.

        :return: The hash value.
        """
        return hash(self.segment_id())

    def codec(self):
        """
        Returns the codec used by the segment.

        :return: The codec used by the segment.
        """
        raise NotImplementedError

    def index_name(self):
        """
        Returns the name of the index.

        :return: The name of the index.
        """
        return self.indexname

    def segment_id(self):
        """
        Returns the ID of the segment.

        :return: The ID of the segment.
        """
        if hasattr(self, "name"):
            # Old segment class
            return self.name
        else:
            return f"{self.index_name()}_{self.segid}"

    def is_compound(self):
        """
        Checks if the segment is a compound segment.

        :return: True if the segment is compound, False otherwise.
        """
        if not hasattr(self, "compound"):
            return False
        return self.compound

    # File convenience methods

    def make_filename(self, ext):
        """
        Creates a filename for the segment with the given extension.

        :param ext: The extension of the filename.
        :return: The filename.
        """
        return f"{self.segment_id()}{ext}"

    def list_files(self, storage):
        """
        Lists the files associated with the segment in the given storage.

        :param storage: The storage object.
        :return: A list of file names.
        """
        prefix = f"{self.segment_id()}."
        return [name for name in storage.list() if name.startswith(prefix)]

    def create_file(self, storage, ext, **kwargs):
        """
        Creates a new file in the given storage with the segment's ID and the given extension.

        :param storage: The storage object.
        :param ext: The extension of the file.
        :param kwargs: Additional keyword arguments passed to the storage's create_file method.
        :return: The created file object.
        """
        fname = self.make_filename(ext)
        return storage.create_file(fname, **kwargs)

    def open_file(self, storage, ext, **kwargs):
        """
        Opens a file in the given storage with the segment's ID and the given extension.

        :param storage: The storage object.
        :param ext: The extension of the file.
        :param kwargs: Additional keyword arguments passed to the storage's open_file method.
        :return: The opened file object.
        """
        fname = self.make_filename(ext)
        return storage.open_file(fname, **kwargs)

    def create_compound_file(self, storage):
        """
        Creates a compound file in the given storage by combining the segment's files.

        :param storage: The storage object.
        """
        segfiles = self.list_files(storage)
        assert not any(name.endswith(self.COMPOUND_EXT) for name in segfiles)
        cfile = self.create_file(storage, self.COMPOUND_EXT)
        CompoundStorage.assemble(cfile, storage, segfiles)
        for name in segfiles:
            storage.delete_file(name)
        self.compound = True

    def open_compound_file(self, storage):
        """
        Opens the compound file associated with the segment in the given storage.

        :param storage: The storage object.
        :return: The opened compound file object.
        """
        name = self.make_filename(self.COMPOUND_EXT)
        dbfile = storage.open_file(name)
        return CompoundStorage(dbfile, use_mmap=storage.supports_mmap)

    # Abstract methods

    @abstractmethod
    def doc_count_all(self):
        """
        Returns the total number of documents, DELETED OR UNDELETED, in this
        segment.
        """
        raise NotImplementedError

    def doc_count(self):
        """
        Returns the number of (undeleted) documents in this segment.
        """
        return self.doc_count_all() - self.deleted_count()

    def set_doc_count(self, doccount):
        """
        Sets the number of documents in the segment.

        :param doccount: The number of documents.
        """
        raise NotImplementedError

    def has_deletions(self):
        """
        Checks if any documents in this segment are deleted.

        :return: True if there are deleted documents, False otherwise.
        """
        return self.deleted_count() > 0

    @abstractmethod
    def deleted_count(self):
        """
        Returns the total number of deleted documents in this segment.
        """
        raise NotImplementedError

    @abstractmethod
    def deleted_docs(self):
        """
        Returns a list of deleted document numbers in this segment.
        """
        raise NotImplementedError

    @abstractmethod
    def delete_document(self, docnum, delete=True):
        """
        Deletes or undeletes the given document number.

        :param docnum: The document number to delete or undelete.
        :param delete: If False, undeletes the document. Default is True.
        """
        raise NotImplementedError

    @abstractmethod
    def is_deleted(self, docnum):
        """
        Checks if the given document number is deleted.

        :param docnum: The document number.
        :return: True if the document is deleted, False otherwise.
        """
        raise NotImplementedError

    def should_assemble(self):
        """
        Checks if the segment should be assembled.

        :return: True if the segment should be assembled, False otherwise.
        """
        return True


# Wrapping Segment
class WrappingSegment(Segment):
    """
    A segment that wraps another segment.

    This class serves as a wrapper around another segment, providing a way to modify or extend its behavior.

    Args:
        child (Segment): The segment to be wrapped.

    """

    def __init__(self, child):
        self._child = child

    def codec(self):
        """
        Get the codec used by the wrapped segment.

        Returns:
            Codec: The codec used by the wrapped segment.

        """
        return self._child.codec()

    def index_name(self):
        """
        Get the name of the index associated with the wrapped segment.

        Returns:
            str: The name of the index associated with the wrapped segment.

        """
        return self._child.index_name()

    def segment_id(self):
        """
        Get the unique identifier of the wrapped segment.

        Returns:
            str: The unique identifier of the wrapped segment.

        """
        return self._child.segment_id()

    def is_compound(self):
        """
        Check if the wrapped segment is a compound segment.

        Returns:
            bool: True if the wrapped segment is a compound segment, False otherwise.

        """
        return self._child.is_compound()

    def should_assemble(self):
        """
        Check if the wrapped segment should be assembled.

        Returns:
            bool: True if the wrapped segment should be assembled, False otherwise.

        """
        return self._child.should_assemble()

    def make_filename(self, ext):
        """
        Generate a filename for the wrapped segment with the given extension.

        Args:
            ext (str): The file extension.

        Returns:
            str: The generated filename for the wrapped segment.

        """
        return self._child.make_filename(ext)

    def list_files(self, storage):
        """
        List all files associated with the wrapped segment in the given storage.

        Args:
            storage: The storage object.

        Returns:
            list: A list of filenames associated with the wrapped segment.

        """
        return self._child.list_files(storage)

    def create_file(self, storage, ext, **kwargs):
        """
        Create a new file for the wrapped segment with the given extension.

        Args:
            storage: The storage object.
            ext (str): The file extension.
            **kwargs: Additional keyword arguments.

        Returns:
            File: The created file object.

        """
        return self._child.create_file(storage, ext, **kwargs)

    def open_file(self, storage, ext, **kwargs):
        """
        Open an existing file for the wrapped segment with the given extension.

        Args:
            storage: The storage object.
            ext (str): The file extension.
            **kwargs: Additional keyword arguments.

        Returns:
            File: The opened file object.

        """
        return self._child.open_file(storage, ext, **kwargs)

    def create_compound_file(self, storage):
        """
        Create a compound file for the wrapped segment in the given storage.

        Args:
            storage: The storage object.

        Returns:
            CompoundFile: The created compound file object.

        """
        return self._child.create_compound_file(storage)

    def open_compound_file(self, storage):
        """
        Open a compound file for the wrapped segment in the given storage.

        Args:
            storage: The storage object.

        Returns:
            CompoundFile: The opened compound file object.

        """
        return self._child.open_compound_file(storage)

    def delete_document(self, docnum, delete=True):
        """
        Delete a document from the wrapped segment.

        Args:
            docnum (int): The document number.
            delete (bool): Whether to mark the document as deleted or not. Default is True.

        Returns:
            bool: True if the document was successfully deleted, False otherwise.

        """
        return self._child.delete_document(docnum, delete=delete)

    def has_deletions(self):
        """
        Check if the wrapped segment has any deleted documents.

        Returns:
            bool: True if the wrapped segment has deleted documents, False otherwise.

        """
        return self._child.has_deletions()

    def deleted_count(self):
        """
        Get the number of deleted documents in the wrapped segment.

        Returns:
            int: The number of deleted documents.

        """
        return self._child.deleted_count()

    def deleted_docs(self):
        """
        Get a list of deleted document numbers in the wrapped segment.

        Returns:
            list: A list of deleted document numbers.

        """
        return self._child.deleted_docs()

    def is_deleted(self, docnum):
        """
        Check if a document with the given number is deleted in the wrapped segment.

        Args:
            docnum (int): The document number.

        Returns:
            bool: True if the document is deleted, False otherwise.

        """
        return self._child.is_deleted(docnum)

    def set_doc_count(self, doccount):
        """
        Set the total number of documents in the wrapped segment.

        Args:
            doccount (int): The total number of documents.

        """
        self._child.set_doc_count(doccount)

    def doc_count(self):
        """
        Get the total number of documents in the wrapped segment.

        Returns:
            int: The total number of documents.

        """
        return self._child.doc_count()

    def doc_count_all(self):
        """
        Get the total number of documents, including deleted ones, in the wrapped segment.

        Returns:
            int: The total number of documents.

        """
        return self._child.doc_count_all()


# Multi per doc reader
class MultiPerDocumentReader(PerDocumentReader):
    """
    A reader that combines multiple per-document readers into a single reader.

    This class is used to read documents from multiple per-document readers and present them as a single reader.
    It provides methods to access document counts, check for deletions, access columns, and retrieve field lengths.

    Parameters:
    - readers (list): A list of per-document readers to be combined.
    - offset (int): The offset to be applied to the document numbers of each reader.

    Attributes:
    - _readers (list): The list of per-document readers.
    - _doc_offsets (list): The list of document offsets for each reader.
    - _doccount (int): The total number of documents across all readers.
    - is_closed (bool): Indicates whether the reader is closed.

    """

    def __init__(self, readers, offset=0):
        """
        Initializes a MultiPerDocumentReader instance.

        Parameters:
        - readers (list): A list of per-document readers to be combined.
        - offset (int): The offset to be applied to the document numbers of each reader.

        """
        self._readers = readers

        self._doc_offsets = []
        self._doccount = 0
        for pdr in readers:
            self._doc_offsets.append(self._doccount)
            self._doccount += pdr.doc_count_all()

        self.is_closed = False

    def close(self):
        """
        Closes the reader and releases any resources.

        """
        for r in self._readers:
            r.close()
        self.is_closed = True

    def doc_count_all(self):
        """
        Returns the total number of documents across all readers.

        Returns:
        - int: The total number of documents.

        """
        return self._doccount

    def doc_count(self):
        """
        Returns the number of non-deleted documents across all readers.

        Returns:
        - int: The number of non-deleted documents.

        """
        total = 0
        for r in self._readers:
            total += r.doc_count()
        return total

    def _document_reader(self, docnum):
        """
        Returns the index of the reader that contains the specified document number.

        Parameters:
        - docnum (int): The document number.

        Returns:
        - int: The index of the reader.

        """
        return max(0, bisect_right(self._doc_offsets, docnum) - 1)

    def _reader_and_docnum(self, docnum):
        """
        Returns the reader index and the document number within the reader for the specified document number.

        Parameters:
        - docnum (int): The document number.

        Returns:
        - tuple: A tuple containing the reader index and the document number within the reader.

        """
        rnum = self._document_reader(docnum)
        offset = self._doc_offsets[rnum]
        return rnum, docnum - offset

    def has_deletions(self):
        """
        Checks if any of the readers have deletions.

        Returns:
        - bool: True if any of the readers have deletions, False otherwise.

        """
        return any(r.has_deletions() for r in self._readers)

    def is_deleted(self, docnum):
        """
        Checks if the specified document number is deleted.

        Parameters:
        - docnum (int): The document number.

        Returns:
        - bool: True if the document is deleted, False otherwise.

        """
        x, y = self._reader_and_docnum(docnum)
        return self._readers[x].is_deleted(y)

    def deleted_docs(self):
        """
        Yields the document numbers of all deleted documents across all readers.

        Yields:
        - int: The document number of a deleted document.

        """
        for r, offset in zip(self._readers, self._doc_offsets):
            for docnum in r.deleted_docs():
                yield docnum + offset

    def all_doc_ids(self):
        """
        Yields all document numbers across all readers.

        Yields:
        - int: The document number.

        """
        for r, offset in zip(self._readers, self._doc_offsets):
            for docnum in r.all_doc_ids():
                yield docnum + offset

    def has_column(self, fieldname):
        """
        Checks if any of the readers have the specified column.

        Parameters:
        - fieldname (str): The name of the column.

        Returns:
        - bool: True if any of the readers have the column, False otherwise.

        """
        return any(r.has_column(fieldname) for r in self._readers)

    def column_reader(self, fieldname, column):
        """
        Returns a column reader for the specified fieldname and column.

        Parameters:
        - fieldname (str): The name of the field.
        - column (Column): The column object.

        Returns:
        - ColumnReader: The column reader.

        Raises:
        - ValueError: If none of the readers have the specified column.

        """
        if not self.has_column(fieldname):
            raise ValueError(f"No column {fieldname!r}")

        default = column.default_value()
        colreaders = []
        for r in self._readers:
            if r.has_column(fieldname):
                cr = r.column_reader(fieldname, column)
            else:
                cr = columns.EmptyColumnReader(default, r.doc_count_all())
            colreaders.append(cr)

        if len(colreaders) == 1:
            return colreaders[0]
        else:
            return columns.MultiColumnReader(colreaders)

    def doc_field_length(self, docnum, fieldname, default=0):
        """
        Returns the length of the specified field in the specified document.

        Parameters:
        - docnum (int): The document number.
        - fieldname (str): The name of the field.
        - default (int): The default value to return if the field is not found.

        Returns:
        - int: The length of the field in the document.

        """
        x, y = self._reader_and_docnum(docnum)
        return self._readers[x].doc_field_length(y, fieldname, default)

    def field_length(self, fieldname):
        """
        Returns the total length of the specified field across all readers.

        Parameters:
        - fieldname (str): The name of the field.

        Returns:
        - int: The total length of the field.

        """
        total = 0
        for r in self._readers:
            total += r.field_length(fieldname)
        return total

    def min_field_length(self):
        """
        Returns the minimum field length across all readers.

        Returns:
        - int: The minimum field length.

        """
        return min(r.min_field_length() for r in self._readers)

    def max_field_length(self):
        """
        Returns the maximum field length across all readers.

        Returns:
        - int: The maximum field length.

        """
        return max(r.max_field_length() for r in self._readers)


# Extended base classes
class PerDocWriterWithColumns(PerDocumentWriter):
    """
    A subclass of PerDocumentWriter that supports columns for storing additional data per document.

    This class provides methods for adding and retrieving column values for a given fieldname.

    Attributes:
        _storage (object): The storage object used for storing the column data.
        _segment (object): The segment object representing the current segment.
        _docnum (int): The document number.

    Methods:
        _has_column(fieldname): Checks if a column with the given fieldname exists.
        _create_column(fieldname, column): Creates a new column with the given fieldname and column object.
        _get_column(fieldname): Retrieves the column object for the given fieldname.
        add_column_value(fieldname, column, value): Adds a value to the column for the given fieldname.

    """

    def __init__(self):
        PerDocumentWriter.__init__(self)
        self._storage = None
        self._segment = None
        self._docnum = None

    @abstractmethod
    def _has_column(self, fieldname):
        """
        Checks if a column with the given fieldname exists.

        Args:
            fieldname (str): The name of the field.

        Returns:
            bool: True if the column exists, False otherwise.

        Raises:
            NotImplementedError: This method should be implemented by subclasses.
        """
        raise NotImplementedError

    @abstractmethod
    def _create_column(self, fieldname, column):
        """
        Creates a new column with the given fieldname and column object.

        Args:
            fieldname (str): The name of the field.
            column (object): The column object.

        Raises:
            NotImplementedError: This method should be implemented by subclasses.
        """
        raise NotImplementedError

    @abstractmethod
    def _get_column(self, fieldname):
        """
        Retrieves the column object for the given fieldname.

        Args:
            fieldname (str): The name of the field.

        Returns:
            object: The column object.

        Raises:
            NotImplementedError: This method should be implemented by subclasses.
        """
        raise NotImplementedError

    def add_column_value(self, fieldname, column, value):
        """
        Adds a value to the column for the given fieldname.

        If the column does not exist, it will be created.

        Args:
            fieldname (str): The name of the field.
            column (object): The column object.
            value (object): The value to be added to the column.
        """
        if not self._has_column(fieldname):
            self._create_column(fieldname, column)
        self._get_column(fieldname).add(self._docnum, value)


# FieldCursor implementations
class EmptyCursor(FieldCursor):
    """A cursor implementation that represents an empty cursor.

    This cursor is used when there are no matching terms in the index.
    It provides methods to navigate through the non-existent terms and
    retrieve information about them.

    Note: This class is intended for internal use within the Whoosh library
    and should not be instantiated directly by users.

    """

    def first(self):
        """Move the cursor to the first term.

        Returns:
            None: Always returns None as there are no terms to move to.

        """
        return None

    def find(self, term):
        """Find a specific term in the index.

        Args:
            term (str): The term to find.

        Returns:
            None: Always returns None as the term does not exist.

        """
        return None

    def next(self):
        """Move the cursor to the next term.

        Returns:
            None: Always returns None as there are no terms to move to.

        """
        return None

    def text(self):
        """Get the text of the current term.

        Returns:
            None: Always returns None as there are no terms.

        """
        return None

    def term_info(self):
        """Get information about the current term.

        Returns:
            None: Always returns None as there are no terms.

        """
        return None

    def is_valid(self):
        """Check if the cursor is valid.

        Returns:
            bool: Always returns False as the cursor is not valid.

        """
        return False
