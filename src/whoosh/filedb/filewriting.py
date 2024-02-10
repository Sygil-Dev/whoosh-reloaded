# ===============================================================================
# Copyright 2007 Matt Chaput
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

from collections import defaultdict
from marshal import dumps

from whoosh.fields import UnknownFieldError
from whoosh.filedb import misc
from whoosh.filedb.fileindex import Segment, SegmentDeletionMixin, SegmentSet
from whoosh.filedb.filepostings import FilePostingWriter
from whoosh.filedb.filetables import (
    FileListWriter,
    FileTableWriter,
    LengthWriter,
    StructHashWriter,
)
from whoosh.filedb.pools import MultiPool, TempfilePool
from whoosh.index import LockError
from whoosh.support import unicode
from whoosh.util import fib
from whoosh.util.filelock import try_for
from whoosh.writing import IndexWriter

# Merge policies

# A merge policy is a callable that takes the Index object, the SegmentWriter
# object, and the current SegmentSet (not including the segment being written),
# and returns an updated SegmentSet (not including the segment being written).


def NO_MERGE(ix, writer, segments):
    """
    This policy does not merge any existing segments.

    Parameters:
    - ix (Index): The index object.
    - writer (IndexWriter): The index writer object.
    - segments (list): The list of existing segments.

    Returns:
    - list: The list of existing segments, unchanged.

    Usage:
    - Use this policy when you want to prevent any merging of existing segments in the index.
    - This can be useful in scenarios where you want to maintain the original segment structure without any merging.
    """
    _ = ix, writer
    return segments


def MERGE_SMALL(ix, writer, segments):
    """
    Merge small segments based on a heuristic using the Fibonacci sequence.

    This policy merges small segments, where "small" is defined using a heuristic based on the Fibonacci sequence.
    The segments are sorted based on their document count, and then merged according to the heuristic.

    Parameters:
    - ix (Index): The Whoosh index object.
    - writer (IndexWriter): The writer object used for merging segments.
    - segments (list): A list of segments to be merged.

    Returns:
    - newsegments (SegmentSet): The merged segments.

    Usage:
    - Call this function to merge small segments in an index. Pass the index object, writer object, and the list of segments to be merged.
    - The function will merge the segments based on the Fibonacci sequence heuristic and return the merged segments.

    Example:
    ```
    ix = Index("/path/to/index")
    writer = ix.writer()
    segments = [segment1, segment2, segment3]
    newsegments = MERGE_SMALL(ix, writer, segments)
    ```
    """
    from whoosh.filedb.filereading import SegmentReader

    newsegments = SegmentSet()
    sorted_segment_list = sorted((s.doc_count_all(), s) for s in segments)
    total_docs = 0
    for i, (count, seg) in enumerate(sorted_segment_list):
        if count > 0:
            total_docs += count
            if total_docs < fib(i + 5):
                writer.add_reader(SegmentReader(ix.storage, seg, ix.schema))
            else:
                newsegments.append(seg)
    return newsegments


def OPTIMIZE(ix, writer, segments):
    """
    Merge all existing segments into a single segment.

    This function merges all the segments specified in the `segments` list into a single segment.
    It uses the `writer` object to add a reader for each segment, and then returns an empty `SegmentSet`.

    Parameters:
    - ix (Index): The index object.
    - writer (IndexWriter): The index writer object.
    - segments (list): A list of segment names to be merged.

    Returns:
    - SegmentSet: An empty `SegmentSet` object.

    Example:
    >>> ix = Index(...)
    >>> writer = IndexWriter(...)
    >>> segments = ['segment1', 'segment2', 'segment3']
    >>> OPTIMIZE(ix, writer, segments)
    <SegmentSet object at 0x...>

    Note:
    - This function assumes that the `SegmentReader` class is imported from `whoosh.filedb.filereading`.
    - The `SegmentSet` object returned by this function is not used or modified further in the code snippet provided.
    """
    from whoosh.filedb.filereading import SegmentReader

    for seg in segments:
        writer.add_reader(SegmentReader(ix.storage, seg, ix.schema))
    return SegmentSet()


class SegmentWriter(SegmentDeletionMixin, IndexWriter):
    """A class for writing segments in an index.

    This class is responsible for writing segments in an index. It handles the creation
    of temporary segment files, writing term indexes, term postings, vector indexes,
    vector postings, stored fields, and field lengths.

    Parameters:
    - ix (Index): The index to write the segment to.
    - poolclass (class, optional): The class to use for the pool. Defaults to None.
    - procs (int, optional): The number of processes to use for the pool. Defaults to 0.
    - blocklimit (int, optional): The block limit for the posting writer. Defaults to 128.
    - timeout (float, optional): The timeout for acquiring the lock. Defaults to 0.0.
    - delay (float, optional): The delay between attempts to acquire the lock. Defaults to 0.1.
    - **poolargs: Additional keyword arguments to pass to the pool class.

    Attributes:
    - lock (Lock): The lock object used to acquire the lock for writing the segment.
    - index (Index): The index to write the segment to.
    - segments (list): The list of segments in the index.
    - blocklimit (int): The block limit for the posting writer.
    - schema (Schema): The schema of the index.
    - name (str): The name of the segment.
    - _searcher (Searcher): The searcher object for the index.
    - docnum (int): The document number.
    - fieldlength_totals (defaultdict): The total field lengths.
    - termsindex (FileTableWriter): The file table writer for the terms index.
    - postwriter (FilePostingWriter): The file posting writer for the term postings.
    - vectorindex (StructHashWriter): The struct hash writer for the vector index.
    - vpostwriter (FilePostingWriter): The file posting writer for the vector postings.
    - storedfields (FileListWriter): The file list writer for the stored fields.
    - fieldlengths (File): The file for the field lengths.
    - pool (Pool): The pool object for the field lengths.

    Methods:
    - searcher(): Returns a searcher object for the index.
    - add_reader(reader): Adds a reader object to the segment writer.
    - add_document(**fields): Adds a document to the segment writer.
    - _add_stored_fields(storeddict): Adds stored fields to the segment writer.
    - _add_vector(fieldnum, vlist): Adds a vector to the segment writer.
    - _close_all(): Closes all files used by the segment writer.
    - commit(mergetype=MERGE_SMALL): Commits the segment writer and releases the lock.
    - cancel(): Cancels the segment writer and releases the lock.

    Usage:
    1. Create an instance of SegmentWriter by providing the index to write the segment to.
    2. Optionally, you can specify the pool class, the number of processes to use for the pool,
       the block limit for the posting writer, the timeout for acquiring the lock, and the delay
       between attempts to acquire the lock.
    3. Use the various methods provided by SegmentWriter to add documents, stored fields, and vectors
       to the segment writer.
    4. Call the commit() method to commit the segment writer and release the lock.
    5. If needed, you can cancel the segment writer and release the lock by calling the cancel() method.

    Example:
    ```python
    from whoosh import index
    from whoosh.filedb.filewriting import SegmentWriter

    # Open an existing index
    ix = index.open_dir("my_index")

    # Create a SegmentWriter
    writer = SegmentWriter(ix)

    # Add a document to the segment writer
    writer.add_document(title="Example Document", content="This is an example document.")

    # Commit the segment writer
    writer.commit()
    ```
    """

    def __init__(
        self,
        ix,
        poolclass=None,
        procs=0,
        blocklimit=128,
        timeout=0.0,
        delay=0.1,
        **poolargs,
    ):
        """
        Initialize a FileWriter object.

        Parameters:
        - ix (Index): The index object to write to.
        - poolclass (class, optional): The class to use for multiprocessing. If not provided, it defaults to MultiPool if procs > 1, otherwise TempfilePool.
        - procs (int, optional): The number of processes to use for multiprocessing. Defaults to 0, which means no multiprocessing.
        - blocklimit (int, optional): The maximum number of documents to write in a single block. Defaults to 128.
        - timeout (float, optional): The maximum time to wait for acquiring the lock. Defaults to 0.0, which means no timeout.
        - delay (float, optional): The delay between attempts to acquire the lock. Defaults to 0.1 seconds.
        - **poolargs (dict, optional): Additional keyword arguments to pass to the poolclass constructor.

        Raises:
        - LockError: If the lock cannot be acquired within the specified timeout.

        Usage:
        - Create an instance of FileWriter by passing an Index object.
        - Optionally, specify the poolclass, procs, blocklimit, timeout, delay, and additional poolargs.
        - Use the FileWriter object to write documents to the index.
        """
        self.lock = ix.storage.lock(ix.indexname + "_LOCK")
        if not try_for(self.lock.acquire, timeout=timeout, delay=delay):
            raise LockError

        self.index = ix
        self.segments = ix.segments.copy()
        self.blocklimit = 128

        self.schema = ix.schema
        self.name = ix._next_segment_name()

        # Create a temporary segment to use its .*_filename attributes
        segment = Segment(self.name, 0, 0, None, None)

        self._searcher = ix.searcher()
        self.docnum = 0
        self.fieldlength_totals = defaultdict(int)

        storedfieldnames = ix.schema.stored_field_names()

        def encode_storedfields(fielddict):
            return dumps([fielddict.get(k) for k in storedfieldnames])

        storage = ix.storage

        # Terms index
        tf = storage.create_file(segment.termsindex_filename)
        self.termsindex = FileTableWriter(
            tf, keycoder=misc.encode_termkey, valuecoder=misc.encode_terminfo
        )

        # Term postings file
        pf = storage.create_file(segment.termposts_filename)
        self.postwriter = FilePostingWriter(self.schema, pf, blocklimit=blocklimit)

        if ix.schema.has_vectored_fields():
            # Vector index
            vf = storage.create_file(segment.vectorindex_filename)
            self.vectorindex = StructHashWriter(vf, "!IH", "!I")

            # Vector posting file
            vpf = storage.create_file(segment.vectorposts_filename)
            self.vpostwriter = FilePostingWriter(vpf, stringids=True)
        else:
            self.vectorindex = None
            self.vpostwriter = None

        # Stored fields file
        sf = storage.create_file(segment.storedfields_filename)
        self.storedfields = FileListWriter(sf, valuecoder=encode_storedfields)

        # Field length file
        self.fieldlengths = storage.create_file(segment.fieldlengths_filename)

        # Create the pool
        if poolclass is None:
            if procs > 1:
                poolclass = MultiPool
            else:
                poolclass = TempfilePool
        self.pool = poolclass(self.fieldlengths, procs=procs, **poolargs)

    def searcher(self):
        """
        Returns a searcher object for the index.

        This method creates and returns a searcher object that can be used to search the index.
        The searcher object provides methods for executing queries and retrieving search results.

        Returns:
            Searcher: A searcher object for the index.

        Example:
            >>> index = Index()
            >>> writer = index.writer()
            >>> # ... add documents to the index ...
            >>> searcher = writer.searcher()
            >>> results = searcher.search(Query("hello"))
        """
        return self.index.searcher()

    def add_reader(self, reader):
        """
        Adds documents from the given reader to the index.

        Parameters:
            - reader (Reader): The reader object containing the documents to be added.

        This method adds stored documents, vectors, and field lengths from the given reader
        to the index. It also handles deletions, if any, and updates the document mapping accordingly.

        Note:
            - The reader object must implement the following methods:
                - `has_deletions()`: Returns True if the reader has deleted documents, False otherwise.
                - `doc_count_all()`: Returns the total number of documents in the reader.
                - `is_deleted(docnum)`: Returns True if the document with the given docnum is deleted, False otherwise.
                - `stored_fields(docnum)`: Returns the stored fields of the document with the given docnum.
                - `scorable_fields()`: Returns a list of field numbers that are scorable.
                - `doc_field_length(docnum, fieldnum)`: Returns the length of the field with the given fieldnum in the document with the given docnum.
                - `has_vector(docnum, fieldnum)`: Returns True if the document with the given docnum has a vector for the field with the given fieldnum, False otherwise.
                - `vector(docnum, fieldnum)`: Returns the vector for the field with the given fieldnum in the document with the given docnum.
                - `postings(fieldnum, text)`: Returns a Postings object for the given fieldnum and text.

        Returns:
            None

        Raises:
            None
        """
        startdoc = self.docnum

        has_deletions = reader.has_deletions()
        if has_deletions:
            docmap = {}

        schema = self.schema
        vectored_fieldnums = schema.vectored_fields()
        scorable_fieldnums = schema.scorable_fields()

        # Add stored documents, vectors, and field lengths
        for docnum in range(reader.doc_count_all()):
            if (not has_deletions) or (not reader.is_deleted(docnum)):
                stored = reader.stored_fields(docnum)
                self._add_stored_fields(stored)

                if has_deletions:
                    docmap[docnum] = self.docnum

                for fieldnum in scorable_fieldnums:
                    self.pool.add_field_length(
                        self.docnum, fieldnum, reader.doc_field_length(docnum, fieldnum)
                    )
                for fieldnum in vectored_fieldnums:
                    if reader.has_vector(docnum, fieldnum):
                        self._add_vector(
                            fieldnum, reader.vector(docnum, fieldnum).items()
                        )
                self.docnum += 1

        current_fieldnum = None
        decoder = None
        for fieldnum, text, _, _ in reader:
            if fieldnum != current_fieldnum:
                current_fieldnum = fieldnum
                decoder = schema[fieldnum].format.decode_frequency

            postreader = reader.postings(fieldnum, text)
            for docnum, valuestring in postreader.all_items():
                if has_deletions:
                    newdoc = docmap[docnum]
                else:
                    newdoc = startdoc + docnum

                # TODO: Is there a faster way to do this?
                freq = decoder(valuestring)
                self.pool.add_posting(fieldnum, text, newdoc, freq, valuestring)

    def add_document(self, **fields):
        """
        Add a document to the index.

        Args:
            **fields: Keyword arguments representing the fields of the document.
                      The field names should match the names defined in the schema.

        Raises:
            UnknownFieldError: If a field name provided does not exist in the schema.

        Notes:
            - The fields are sorted based on their order in the schema.
            - The indexed fields are added to the index.
            - The vector fields are processed and added to the index.
            - The stored fields are stored in the index.

        Example:
            schema = Schema(title=TEXT(stored=True), content=TEXT)
            writer = IndexWriter(index, schema)
            writer.add_document(title="Document 1", content="This is the content of Document 1")
        """
        schema = self.schema
        name2num = schema.name_to_number

        # Sort the keys by their order in the schema
        fieldnames = [name for name in fields.keys() if not name.startswith("_")]
        fieldnames.sort(key=name2num)

        # Check if the caller gave us a bogus field
        for name in fieldnames:
            if name not in schema:
                raise UnknownFieldError(f"There is no field named {name!r}")

        storedvalues = {}

        docnum = self.docnum
        for name in fieldnames:
            value = fields.get(name)
            if value:
                fieldnum = name2num(name)
                field = schema.field_by_number(fieldnum)

                if field.indexed:
                    self.pool.add_content(docnum, fieldnum, field, value)

                vformat = field.vector
                if vformat:
                    vlist = sorted(
                        (w, valuestring)
                        for w, freq, valuestring in vformat.word_values(
                            value, mode="index"
                        )
                    )
                    self._add_vector(fieldnum, vlist)

                if field.stored:
                    # Caller can override the stored value by including a key
                    # _stored_<fieldname>
                    storedname = "_stored_" + name
                    if storedname in fields:
                        storedvalues[name] = fields[storedname]
                    else:
                        storedvalues[name] = value

        self._add_stored_fields(storedvalues)
        self.docnum += 1

    def _add_stored_fields(self, storeddict):
        """
        Adds a stored field dictionary to the list of stored fields.

        Args:
            storeddict (dict): A dictionary containing the stored field data.

        Returns:
            None

        Notes:
            - The stored field dictionary should contain key-value pairs representing the field name and its value.
            - The stored fields are used to store additional data associated with a document.
            - The stored fields can be retrieved later during search or retrieval operations.

        Example:
            storeddict = {"title": "Sample Document", "author": "John Doe"}
            _add_stored_fields(storeddict)
        """
        self.storedfields.append(storeddict)

    def _add_vector(self, fieldnum, vlist):
        """
        Add a vector to the index for a given field.

        Args:
            fieldnum (int): The field number.
            vlist (list): A list of tuples containing the text and valuestring for each vector.

        Raises:
            AssertionError: If the text is not of type unicode.

        Notes:
            This method adds a vector to the index for a specific field. It takes a list of tuples, where each tuple contains the text and valuestring for a vector. The text should be of type unicode.

            The method uses the vpostwriter to write the vectors to the index file. It starts by obtaining the vformat from the schema for the given field. It then iterates over the vlist and writes each vector to the vpostwriter. Finally, it finishes writing the vectors and adds the vector offset to the vectorindex.

        Example:
            vlist = [(u"example text", "valuestring1"), (u"another text", "valuestring2")]
            _add_vector(0, vlist)
        """
        vpostwriter = self.vpostwriter
        vformat = self.schema[fieldnum].vector

        offset = vpostwriter.start(vformat)
        for text, valuestring in vlist:
            assert isinstance(text, unicode), f"{text!r} is not unicode"
            vpostwriter.write(text, valuestring)
        vpostwriter.finish()

        self.vectorindex.add((self.docnum, fieldnum), offset)

    def _close_all(self):
        """
        Closes all the file resources used by the writer.

        This method is responsible for closing the terms index, post writer, vector index,
        vpost writer, stored fields, and field lengths. It ensures that all the resources
        are properly closed to prevent any data corruption or resource leaks.

        Usage:
            Call this method when you are done writing to the index and want to release
            the file resources. It is important to call this method to ensure that all
            changes are persisted and the files are closed properly.

        Note:
            - If the vector index or vpost writer is not initialized, they will not be closed.
            - The field lengths are only closed if they are not already closed.

        Raises:
            None

        Returns:
            None
        """
        self.termsindex.close()
        self.postwriter.close()
        if self.vectorindex:
            self.vectorindex.close()
        if self.vpostwriter:
            self.vpostwriter.close()
        self.storedfields.close()
        if not self.fieldlengths.is_closed:
            self.fieldlengths.close()

    def commit(self, mergetype=MERGE_SMALL):
        """
        Commits the changes made by the writer to the index.

        This method finalizes the changes made by the writer and commits them to the index.
        It performs the following steps:
        1. Calls the merge policy function to determine if any segments need to be merged into the writer's pool.
        2. Informs the pool to add its accumulated data to the terms index and posting file.
        3. Creates a new segment object for the segment created by this writer and adds it to the list of remaining segments.
        4. Closes all files, writes a new TOC (Table of Contents) with the updated segment list, and releases the lock.

        Parameters:
        - mergetype (optional): The merge policy function to be used for determining which segments to merge. Default is MERGE_SMALL.

        Returns:
        None
        """
        # Call the merge policy function. The policy may choose to merge other
        # segments into this writer's pool
        new_segments = mergetype(self.index, self, self.segments)

        # Tell the pool we're finished adding information, it should add its
        # accumulated data to the terms index and posting file.
        self.pool.finish(self.schema, self.docnum, self.termsindex, self.postwriter)

        # Create a Segment object for the segment created by this writer and
        # add it to the list of remaining segments returned by the merge policy
        # function
        thissegment = Segment(
            self.name,
            self.docnum,
            self.pool.fieldlength_totals(),
            self.pool.fieldlength_maxes(),
        )
        new_segments.append(thissegment)

        # Close all files, tell the index to write a new TOC with the new
        # segment list, and release the lock.
        self._close_all()
        self.index.commit(new_segments)
        self.lock.release()

    def cancel(self):
        """
        Cancels the current operation and releases any acquired resources.

        This method cancels the current operation by calling the `cancel` method of the underlying
        thread pool. It also closes all open file handles and releases the lock held by the current
        thread.

        Note:
            This method should be called if the current operation needs to be canceled or if any
            acquired resources need to be released.

        Example:
            >>> writer.cancel()

        """
        self.pool.cancel()
        self._close_all()
        self.lock.release()
