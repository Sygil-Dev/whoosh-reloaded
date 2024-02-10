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

from marshal import loads
from threading import Lock

from whoosh.fields import FieldConfigurationError
from whoosh.filedb import misc
from whoosh.filedb.filepostings import FilePostingReader
from whoosh.filedb.filetables import (
    FileListReader,
    FileTableReader,
    LengthReader,
    StructHashReader,
)

# from whoosh.postings import Exclude
from whoosh.reading import IndexReader, TermNotFound
from whoosh.util import byte_to_length, protected

# Reader class


class SegmentReader(IndexReader):
    """
    A class for reading data from a segment in a Whoosh index.

    This class provides methods for accessing various information and data stored in a segment of a Whoosh index.
    It is used internally by the Whoosh library and should not be instantiated directly by users.

    Parameters:
    - storage (Storage): The storage object representing the index storage.
    - segment (Segment): The segment object representing the segment to read from.
    - schema (Schema): The schema object representing the index schema.

    Attributes:
    - storage (Storage): The storage object representing the index storage.
    - segment (Segment): The segment object representing the segment being read from.
    - schema (Schema): The schema object representing the index schema.
    - termsindex (FileTableReader): The file table reader for the term index.
    - postfile (File): The file object for the term postings file.
    - vectorindex (StructHashReader): The struct hash reader for the vector index.
    - vpostfile (File): The file object for the vector postings file.
    - storedfields (FileListReader): The file list reader for the stored fields file.
    - fieldlengths (list): A list of field lengths.
    - has_deletions (bool): Indicates whether the segment has deletions.
    - is_deleted (callable): A callable object that checks if a document is deleted.
    - doc_count (int): The number of documents in the segment.
    - dc (int): The total number of documents in the segment, including deleted documents.
    - is_closed (bool): Indicates whether the segment reader is closed.
    - _sync_lock (Lock): A lock object for synchronization.

    Methods:
    - _open_vectors(): Opens the vector index and vector postings file.
    - _open_postfile(): Opens the term postings file.
    - close(): Closes the segment reader.
    - doc_count_all(): Returns the total number of documents in the segment.
    - stored_fields(docnum): Returns the stored fields for a given document number.
    - all_stored_fields(): Returns an iterator over all stored fields in the segment.
    - field_length(fieldnum): Returns the length of a field in the segment.
    - doc_field_length(docnum, fieldnum, default=0): Returns the length of a field in a document.
    - max_field_length(fieldnum): Returns the maximum length of a field in the segment.
    - has_vector(docnum, fieldnum): Checks if a document has a vector for a given field.
    - __iter__(): Returns an iterator over the terms in the segment.
    - iter_from(fieldnum, text): Returns an iterator over the terms starting from a given field and text.
    - _term_info(fieldnum, text): Returns the term info for a given field and text.
    - doc_frequency(fieldid, text): Returns the document frequency of a term in a field.
    - frequency(fieldid, text): Returns the frequency of a term in a field.
    - lexicon(fieldid): Returns an iterator over the terms in a field.
    - expand_prefix(fieldid, prefix): Returns an iterator over the terms with a given prefix in a field.
    - postings(fieldid, text, exclude_docs=frozenset()): Returns a posting reader for a term in a field.
    - vector(docnum, fieldid): Returns a vector reader for a document and field.

    """

    def __init__(self, storage, segment, schema):
        """
        Initialize a Filereading object.

        Args:
            storage (Storage): The storage object used to access the index files.
            segment (Segment): The segment object representing a segment of the index.
            schema (Schema): The schema object representing the index schema.

        Attributes:
            storage (Storage): The storage object used to access the index files.
            segment (Segment): The segment object representing a segment of the index.
            schema (Schema): The schema object representing the index schema.
            termsindex (FileTableReader): The file table reader for the term index.
            postfile (None or FileTableReader): The file table reader for the term postings file.
            vectorindex (None or FileTableReader): The file table reader for the vector index.
            vpostfile (None or FileTableReader): The file table reader for the vector postings file.
            storedfields (FileListReader): The file list reader for the stored fields file.
            fieldlengths (list): The list of field lengths.
            has_deletions (bool): Indicates if the segment has deletions.
            is_deleted (function): Function to check if a document is deleted.
            doc_count (int): The number of documents in the segment.
            dc (int): The total number of documents in the segment, including deleted documents.
            is_closed (bool): Indicates if the Filereading object is closed.
            _sync_lock (Lock): Lock object for synchronization.

        Note:
            The Filereading object provides access to various index files and information related to a segment of the index.
            It is used internally by the Whoosh library and should not be instantiated directly by the user.
        """
        self.storage = storage
        self.segment = segment
        self.schema = schema

        storedfieldnames = schema.stored_field_names()

        def decode_storedfields(value):
            return dict(zip(storedfieldnames, loads(value)))

        # Term index
        tf = storage.open_file(segment.termsindex_filename)
        self.termsindex = FileTableReader(
            tf,
            keycoder=misc.encode_termkey,
            keydecoder=misc.decode_termkey,
            valuedecoder=misc.decode_terminfo,
        )

        # Term postings file, vector index, and vector postings: lazy load
        self.postfile = None
        self.vectorindex = None
        self.vpostfile = None

        # Stored fields file
        sf = storage.open_file(segment.storedfields_filename, mapped=False)
        self.storedfields = FileListReader(sf, valuedecoder=decode_storedfields)

        # Field length file
        scorables = schema.scorable_fields()
        if scorables:
            self.indices = {fieldnum: i for i, fieldnum in enumerate(scorables)}
            lengthcount = segment.doc_count_all() * len(self.indices)
            flf = storage.open_file(segment.fieldlengths_filename)
            self.fieldlengths = flf.read_array("B", lengthcount)
            flf.close()
        else:
            self.fieldlengths = []

        # Copy methods from underlying segment
        self.has_deletions = segment.has_deletions
        self.is_deleted = segment.is_deleted
        self.doc_count = segment.doc_count

        self.dc = segment.doc_count_all()
        self.is_closed = False
        self._sync_lock = Lock()

    def _open_vectors(self):
        """
        Opens the vector index and vector postings file.

        This method is responsible for opening the vector index and vector postings file
        associated with the current storage and segment. It initializes the `vectorindex`
        attribute with a StructHashReader object for reading the vector index, and sets
        the `vpostfile` attribute to the opened vector postings file.

        Note:
            This method assumes that the `vectorindex_filename` and `vectorposts_filename`
            attributes of the segment object have been properly set.

        Args:
            None

        Returns:
            None

        Raises:
            None
        """
        if self.vectorindex:
            return

        storage, segment = self.storage, self.segment

        # Vector index
        vf = storage.open_file(segment.vectorindex_filename)
        self.vectorindex = StructHashReader(vf, "!IH", "!I")

        # Vector postings file
        self.vpostfile = storage.open_file(segment.vectorposts_filename, mapped=False)

    def _open_postfile(self):
        """
        Opens the postfile for reading.

        This method is responsible for opening the postfile associated with the segment
        for reading. If the postfile is already open, this method does nothing.

        Returns:
            None

        Raises:
            None

        Usage:
            _open_postfile()
        """
        if self.postfile:
            return
        self.postfile = self.storage.open_file(
            self.segment.termposts_filename, mapped=False
        )

    def __repr__(self):
        """
        Return a string representation of the object.

        This method returns a string that represents the object in a unique and
        human-readable format. It is used primarily for debugging and logging
        purposes.

        Returns:
            str: A string representation of the object.
        """
        return f"{self.__class__.__name__}({self.segment})"

    @protected
    def __contains__(self, term):
        """
        Check if a term is present in the index.

        Args:
            term (tuple): A tuple representing the term to be checked. The tuple should
                          contain two elements: the first element is the term's numeric
                          representation, and the second element is the term's string
                          representation.

        Returns:
            bool: True if the term is present in the index, False otherwise.
        """
        return (self.schema.to_number(term[0]), term[1]) in self.termsindex

    def close(self):
        """
        Closes the file reader and releases any associated resources.

        This method closes the stored fields, terms index, post file, and vector index
        if they are open. It also marks the file reader as closed.

        Note:
            If the `fieldlengths` attribute is uncommented, it will also be closed.

        Usage:
            Call this method when you are finished using the file reader to release
            any resources it holds. After calling this method, the file reader should
            not be used again.

        """
        self.storedfields.close()
        self.termsindex.close()
        if self.postfile:
            self.postfile.close()
        if self.vectorindex:
            self.vectorindex.close()
        # if self.fieldlengths:
        #    self.fieldlengths.close()
        self.is_closed = True

    def doc_count_all(self):
        """
        Returns the total number of documents in the index.

        This method retrieves the document count from the index and returns it.

        Returns:
            int: The total number of documents in the index.

        Example:
            >>> reader = FileReader()
            >>> reader.doc_count_all()
            100
        """
        return self.dc

    @protected
    def stored_fields(self, docnum):
        """
        Retrieve the stored fields for a given document number.

        Parameters:
            docnum (int): The document number for which to retrieve the stored fields.

        Returns:
            dict: A dictionary containing the stored fields for the specified document number.

        Raises:
            IndexError: If the specified document number is out of range.

        Example:
            >>> reader = FileReading()
            >>> reader.stored_fields(0)
            {'title': 'Sample Document', 'author': 'John Doe', 'content': 'This is a sample document.'}
        """
        return self.storedfields[docnum]

    @protected
    def all_stored_fields(self):
        """
        Generator that yields the stored fields of all non-deleted documents in the segment.

        Yields:
            dict: A dictionary containing the stored fields of a document.

        Notes:
            - This method iterates over all document numbers in the segment and checks if each document is deleted.
            - If a document is not deleted, it yields the stored fields of that document.
            - The stored fields are returned as a dictionary.

        Example:
            >>> reader = FileReading()
            >>> for fields in reader.all_stored_fields():
            ...     print(fields)
            {'title': 'Document 1', 'content': 'This is the content of document 1'}
            {'title': 'Document 2', 'content': 'This is the content of document 2'}
            {'title': 'Document 3', 'content': 'This is the content of document 3'}
            ...
        """
        is_deleted = self.segment.is_deleted
        for docnum in range(self.segment.doc_count_all()):
            if not is_deleted(docnum):
                yield self.storedfields[docnum]

    def field_length(self, fieldnum):
        """
        Returns the length of a field in the segment.

        Parameters:
        - fieldnum (int): The field number.

        Returns:
        - int: The length of the field.

        Raises:
        - ValueError: If the field number is invalid.

        This method retrieves the length of a field in the segment. The field number
        should be a valid field number. If the field number is invalid, a ValueError
        is raised.

        Example usage:
        >>> segment = Segment()
        >>> field_length = segment.field_length(0)
        >>> print(field_length)
        10
        """
        return self.segment.field_length(fieldnum)

    @protected
    def doc_field_length(self, docnum, fieldnum, default=0):
        """
        Returns the length of a field in a document.

        Parameters:
        - docnum (int): The document number.
        - fieldnum (int): The field number.
        - default (int, optional): The default value to return if the field length is not found. Defaults to 0.

        Returns:
        - int: The length of the field in the document.

        Raises:
        - IndexError: If the field number is out of range.
        - IndexError: If the document number is out of range.

        This method retrieves the length of a field in a document from the internal data structure.
        It uses the document number and field number to calculate the position in the fieldlengths array,
        and then converts the byte value at that position to the corresponding length using the byte_to_length function.

        Example usage:
        ```
        reader = FileReader()
        length = reader.doc_field_length(10, 2)
        print(length)  # Output: 42
        ```
        """
        index = self.indices[fieldnum]
        pos = index * self.dc + docnum
        return byte_to_length(self.fieldlengths[pos])

    def max_field_length(self, fieldnum):
        """
        Returns the maximum length of a field in the segment.

        Parameters:
            fieldnum (int): The field number.

        Returns:
            int: The maximum length of the field.

        Raises:
            ValueError: If the field number is invalid.

        This method retrieves the maximum length of a field in the segment. The field number
        should be a valid field number within the segment. If the field number is invalid,
        a ValueError is raised.

        Example usage:
            segment = Segment()
            field_length = segment.max_field_length(0)
            print(field_length)  # Output: 100
        """
        return self.segment.max_field_length(fieldnum)

    @protected
    def has_vector(self, docnum, fieldnum):
        """
        Check if a vector exists for a given document number and field number.

        Parameters:
            docnum (int): The document number.
            fieldnum (int): The field number.

        Returns:
            bool: True if the vector exists, False otherwise.

        Raises:
            None

        Notes:
            - This method assumes that the vectors have been opened using the _open_vectors() method.
            - The vectorindex is a dictionary that stores the document and field numbers as keys, and the vectors as values.
        """
        self._open_vectors()
        return (docnum, fieldnum) in self.vectorindex

    @protected
    def __iter__(self):
        """
        Iterate over the terms index and yield tuples containing file name, term, post count, and total frequency.

        Yields:
            tuple: A tuple containing the file name, term, post count, and total frequency.

        Notes:
            This method is used to iterate over the terms index in the `filereading` module. The terms index is a list of
            tuples, where each tuple contains information about a term in the index. The tuple structure is as follows:
            ((file_name, term), (total_frequency, _, post_count)).

            The method iterates over each tuple in the terms index and yields a tuple containing the file name, term,
            post count, and total frequency.

        Example:
            >>> reader = FileReader()
            >>> for file_name, term, post_count, total_freq in reader:
            ...     print(file_name, term, post_count, total_freq)
        """
        for (fn, t), (totalfreq, _, postcount) in self.termsindex:
            yield (fn, t, postcount, totalfreq)

    @protected
    def iter_from(self, fieldnum, text):
        """
        Iterates over the terms index starting from a specific field number and text.

        Args:
            fieldnum (int): The field number to start iterating from.
            text (str): The text to start iterating from.

        Yields:
            tuple: A tuple containing the field number, term, postcount, and total frequency.

        """
        tt = self.termsindex
        for (fn, t), (totalfreq, _, postcount) in tt.items_from((fieldnum, text)):
            yield (fn, t, postcount, totalfreq)

    @protected
    def _term_info(self, fieldnum, text):
        """
        Retrieve the term information for a given field and text.

        This method returns the term information (e.g., frequency, positions) for a specific term in a specific field.
        It looks up the term in the termsindex dictionary, which is a mapping of (fieldnum, text) tuples to term information.

        Parameters:
        - fieldnum (int): The field number of the term.
        - text (str): The text of the term.

        Returns:
        - TermInfo: An object containing the term information.

        Raises:
        - TermNotFound: If the term is not found in the termsindex dictionary.

        Usage:
        term_info = _term_info(fieldnum, text)
        """

        try:
            return self.termsindex[(fieldnum, text)]
        except KeyError:
            raise TermNotFound(f"{fieldnum}:{text!r}")

    def doc_frequency(self, fieldid, text):
        """
        Returns the document frequency of a given term in a specific field.

        Parameters:
        - fieldid (str): The ID of the field.
        - text (str): The term to calculate the document frequency for.

        Returns:
        - int: The document frequency of the term in the field.

        Raises:
        - TermNotFound: If the term is not found in the field.

        This method calculates the document frequency of a given term in a specific field.
        It first converts the field ID to a field number using the schema.
        Then, it retrieves the term information using the field number and the term.
        Finally, it returns the document frequency from the term information.

        Example usage:
        ```
        field_id = "content"
        term = "python"
        frequency = doc_frequency(field_id, term)
        print(f"The document frequency of '{term}' in field '{field_id}' is {frequency}.")
        ```
        """
        try:
            fieldnum = self.schema.to_number(fieldid)
            return self._term_info(fieldnum, text)[2]
        except TermNotFound:
            return 0

    def frequency(self, fieldid, text):
        """
        Returns the frequency of a given term in a specified field.

        Args:
            fieldid (str): The ID of the field.
            text (str): The term to get the frequency for.

        Returns:
            int: The frequency of the term in the field.

        Raises:
            TermNotFound: If the term is not found in the field.

        Example:
            >>> frequency("title", "python")
            3
        """
        try:
            fieldnum = self.schema.to_number(fieldid)
            return self._term_info(fieldnum, text)[0]
        except TermNotFound:
            return 0

    @protected
    def lexicon(self, fieldid):
        """
        Returns an iterator over the terms in the lexicon for the specified field.

        Args:
            fieldid (str): The field identifier.

        Yields:
            str: The terms in the lexicon for the specified field.

        Raises:
            None.

        Notes:
            - This method overrides the base class implementation to use FileTableReader.keys_from()
              for faster performance.
            - The lexicon is a collection of unique terms in a field.
            - The terms are yielded in lexicographic order.

        Example:
            reader = FileTableReader()
            for term in reader.lexicon("content"):
                print(term)
        """
        tt = self.termsindex
        fieldid = self.schema.to_number(fieldid)
        for fn, t in tt.keys_from((fieldid, "")):
            if fn != fieldid:
                return
            yield t

    @protected
    def expand_prefix(self, fieldid, prefix):
        """
        Expand a prefix in a specific field.

        This method expands a given prefix in a specific field of the index. It uses the `FileTableReader.keys_from()` method for faster performance compared to the base class implementation.

        Parameters:
        - fieldid (str): The ID of the field to expand the prefix in.
        - prefix (str): The prefix to expand.

        Yields:
        - str: The expanded terms that match the given prefix in the specified field.

        Note:
        - The `fieldid` parameter should be a valid field ID defined in the schema.
        - The `prefix` parameter should be a string representing the prefix to expand.

        Example:
        ```
        reader = FileTableReader()
        for term in reader.expand_prefix("title", "comp"):
            print(term)
        ```

        This will print all the terms in the "title" field that start with the prefix "comp".
        """
        tt = self.termsindex
        fieldid = self.schema.to_number(fieldid)
        for fn, t in tt.keys_from((fieldid, prefix)):
            if fn != fieldid or not t.startswith(prefix):
                return
            yield t

    def postings(self, fieldid, text, exclude_docs=frozenset()):
        """
        Returns a postreader object that allows iterating over the postings (document ids) for a given field and text.

        Args:
            fieldid (str): The field identifier.
            text (str): The text to search for in the field.
            exclude_docs (frozenset, optional): A set of document ids to exclude from the postings. Defaults to an empty set.

        Returns:
            FilePostingReader: A postreader object that provides access to the postings.

        Raises:
            TermNotFound: If the specified term (fieldid:text) is not found in the index.

        Note:
            The postreader object returned by this method allows efficient iteration over the postings (document ids) for a given field and text.
            It is important to note that the postreader object is not thread-safe and should not be shared across multiple threads.

        Example:
            # Create an index and add documents
            ix = create_in("indexdir", schema)
            writer = ix.writer()
            writer.add_document(title="Document 1", content="This is the first document.")
            writer.add_document(title="Document 2", content="This is the second document.")
            writer.commit()

            # Get the postreader for the "title" field and the term "document"
            postreader = ix.reader().postings("title", "document")

            # Iterate over the postings
            for docnum in postreader:
                print(f"Document ID: {docnum}")

        """
        schema = self.schema
        fieldnum = schema.to_number(fieldid)
        format_schema = schema[fieldnum].format

        try:
            offset = self.termsindex[(fieldnum, text)][1]
        except KeyError:
            raise TermNotFound(f"{fieldid}:{text!r}")

        if self.segment.deleted and exclude_docs:
            exclude_docs = self.segment.deleted | exclude_docs
        elif self.segment.deleted:
            exclude_docs = self.segment.deleted

        self._open_postfile()
        postreader = FilePostingReader(self.postfile, offset, format_schema)
        # if exclude_docs:
        #    postreader = Exclude(postreader, exclude_docs)
        return postreader

    def vector(self, docnum, fieldid):
        """
        Returns the vector representation of a document's field.

        Args:
            docnum (int): The document number.
            fieldid (str): The field identifier.

        Returns:
            FilePostingReader: The reader object for accessing the vector representation of the field.

        Raises:
            ValueError: If no vectors are stored for the specified field or if no vector is found for the specified document and field.
        """
        schema = self.schema
        fieldnum = schema.to_number(fieldid)
        vformat = schema[fieldnum].vector
        if not vformat:
            raise ValueError(f"No vectors are stored for field {fieldid!r}")

        self._open_vectors()
        offset = self.vectorindex.get((docnum, fieldnum))
        if offset is None:
            raise ValueError(f"No vector found for document {docnum} field {fieldid!r}")

        return FilePostingReader(self.vpostfile, offset, vformat, stringids=True)
