# ===============================================================================
# Copyright 2010 Matt Chaput
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

import os
import shutil
import tempfile
import time
from collections import defaultdict
from heapq import heapify, heappop, heappush
from marshal import dump, load
from multiprocessing import Process, Queue
from struct import Struct

from whoosh.filedb.filetables import LengthWriter
from whoosh.util import length_to_byte

_2int_struct = Struct("!II")
pack2ints = _2int_struct.pack
unpack2ints = _2int_struct.unpack

_length_struct = Struct("!IHB")  # Docnum, fieldnum, lengthbyte
pack_length = _length_struct.pack
unpack_length = _length_struct.unpack


# def encode_posting(fieldNum, text, doc, freq, datastring):
#    """Encodes a posting as a string, for sorting.
#    """
#
#    return "".join((pack_ushort(fieldNum),
#                    utf8encode(text)[0],
#                    chr(0),
#                    pack2ints(doc, freq),
#                    datastring))
#
# def decode_posting(posting):
#    """Decodes an encoded posting string into a
#    (field_number, text, document_number, datastring) tuple.
#    """
#
#    field_num = unpack_ushort(posting[:_SHORT_SIZE])[0]
#
#    zero = posting.find(chr(0), _SHORT_SIZE)
#    text = utf8decode(posting[_SHORT_SIZE:zero])[0]
#
#    metastart = zero + 1
#    metaend = metastart + _INT_SIZE * 2
#    doc, freq = unpack2ints(posting[metastart:metaend])
#
#    datastring = posting[metaend:]
#
#    return field_num, text, doc, freq, datastring


def imerge(iterators):
    """
    Merge multiple sorted iterators into a single sorted iterator.

    This function takes a list of sorted iterators and merges them into a single
    sorted iterator. It uses a heap data structure to efficiently merge the
    iterators.

    Parameters:
    - iterators (list): A list of sorted iterators to be merged.

    Yields:
    - item: The next item in the merged sorted iterator.

    Example:
    ```
    iterators = [iter([1, 3, 5]), iter([2, 4, 6]), iter([7, 8, 9])]
    merged_iterator = imerge(iterators)
    for item in merged_iterator:
        print(item)
    # Output: 1, 2, 3, 4, 5, 6, 7, 8, 9
    ```
    """
    current = []
    for g in iterators:
        try:
            current.append((next(g), g))
        except StopIteration:
            pass
    heapify(current)

    while len(current) > 1:
        item, gen = heappop(current)
        yield item
        try:
            heappush(current, (next(gen), gen))
        except StopIteration:
            pass

    if current:
        item, gen = current[0]
        yield item
        for item in gen:
            yield item


def bimerge(iter1, iter2):
    """
    Merge two sorted iterators into a single sorted iterator.

    This function takes two sorted iterators, `iter1` and `iter2`, and merges them into a single sorted iterator.
    The merged iterator will contain all the elements from both `iter1` and `iter2`, in ascending order.

    Parameters:
    - iter1 (iterator): The first sorted iterator.
    - iter2 (iterator): The second sorted iterator.

    Returns:
    - iterator: A merged iterator containing all the elements from `iter1` and `iter2`, in ascending order.

    Example:
    ```
    >>> iter1 = iter([1, 3, 5])
    >>> iter2 = iter([2, 4, 6])
    >>> merged_iter = bimerge(iter1, iter2)
    >>> list(merged_iter)
    [1, 2, 3, 4, 5, 6]
    ```
    """
    try:
        p1 = iter1.next()
    except StopIteration:
        for p2 in iter2:
            yield p2
        return

    try:
        p2 = iter2.next()
    except StopIteration:
        for p1 in iter1:
            yield p1
        return

    while True:
        if p1 < p2:
            yield p1
            try:
                p1 = iter1.next()
            except StopIteration:
                for p2 in iter2:
                    yield p2
                return
        else:
            yield p2
            try:
                p2 = iter2.next()
            except StopIteration:
                for p1 in iter1:
                    yield p1
                return


def dividemerge(iters):
    """
    Divides a list of iterators into smaller sublists recursively and merges them using bimerge.

    Parameters:
    - iters (list): A list of iterators to be divided and merged.

    Returns:
    - merged_iter (iterator): An iterator that merges the divided sublists.

    Example:
    >>> iters = [iter([1, 2, 3]), iter([4, 5, 6]), iter([7, 8, 9])]
    >>> merged_iter = dividemerge(iters)
    >>> list(merged_iter)
    [1, 2, 3, 4, 5, 6, 7, 8, 9]
    """

    length = len(iters)
    if length == 0:
        return []
    if length == 1:
        return iters[0]

    mid = length >> 1
    return bimerge(dividemerge(iters[:mid]), dividemerge(iters[mid:]))


def read_run(filename, count):
    """
    Read and yield objects from a binary file.

    Args:
        filename (str): The path to the binary file.
        count (int): The number of objects to read.

    Yields:
        object: The loaded object from the file.

    Raises:
        FileNotFoundError: If the specified file does not exist.

    Example:
        >>> for obj in read_run("data.bin", 3):
        ...     print(obj)
    """
    f = open(filename, "rb")
    try:
        while count:
            count -= 1
            yield load(f)
    finally:
        f.close()


def write_postings(schema, termtable, postwriter, postiter):
    """
    Writes postings to the posting file and adds terms to the term table.

    This method pulls postings out of the posting pool (built up as documents are added)
    and writes them to the posting file. Each time it encounters a posting for a new term,
    it writes the previous term to the term index. By waiting to write the term entry,
    we can easily count the document frequency and sum the terms by looking at the postings.

    Args:
        schema (Schema): The schema object that defines the fields and their properties.
        termtable (TermTable): The term table object that stores the term entries.
        postwriter (PostWriter): The post writer object that writes postings to the posting file.
        postiter (iterable): An iterable that provides the postings in (field number, lexical) order.

    Raises:
        ValueError: If the postings are out of order.

    Returns:
        None
    """
    current_fieldnum = None  # Field number of the current term
    current_text = None  # Text of the current term
    first = True
    current_freq = 0
    offset = None

    # Loop through the postings in the pool. Postings always come out of
    # the pool in (field number, lexical) order.
    for fieldnum, text, docnum, freq, valuestring in postiter:
        # Is this the first time through, or is this a new term?
        if first or fieldnum > current_fieldnum or text > current_text:
            if first:
                first = False
            else:
                # This is a new term, so finish the postings and add the
                # term to the term table
                postcount = postwriter.finish()
                termtable.add(
                    (current_fieldnum, current_text), (current_freq, offset, postcount)
                )

            # Reset the post writer and the term variables
            current_fieldnum = fieldnum
            current_text = text
            current_freq = 0
            offset = postwriter.start(fieldnum)

        elif fieldnum < current_fieldnum or (
            fieldnum == current_fieldnum and text < current_text
        ):
            # This should never happen!
            raise ValueError(
                f"Postings are out of order: {current_fieldnum}:{current_text} .. {fieldnum}:{text}"
            )

        # Write a posting for this occurrence of the current term
        current_freq += freq
        postwriter.write(docnum, valuestring)

    # If there are still "uncommitted" postings at the end, finish them off
    if not first:
        postcount = postwriter.finish()
        termtable.add(
            (current_fieldnum, current_text), (current_freq, offset, postcount)
        )


class LengthSpool:
    """
    A class for managing a spool file that stores length information.

    The LengthSpool class provides methods to create a spool file, add length information
    for documents and fields, finish writing to the spool file, and read back the length
    information.

    Usage:
        spool = LengthSpool(filename)
        spool.create()
        spool.add(docnum, fieldnum, length)
        spool.finish()
        for length_info in spool.readback():
            # Process length_info

    Args:
        filename (str): The path to the spool file.

    Attributes:
        filename (str): The path to the spool file.
        file (file object): The file object representing the spool file.

    Methods:
        create(): Creates the spool file for writing.
        add(docnum, fieldnum, length): Adds length information for a document and field to the spool file.
        finish(): Finishes writing to the spool file and closes it.
        readback(): Reads back the length information from the spool file.

    """

    def __init__(self, filename):
        self.filename = filename
        self.file = None

    def create(self):
        """
        Creates the spool file for writing.

        This method opens the spool file in write binary mode.

        """
        self.file = open(self.filename, "wb")

    def add(self, docnum, fieldnum, length):
        """
        Adds length information for a document and field to the spool file.

        This method writes the packed length information to the spool file.

        Args:
            docnum (int): The document number.
            fieldnum (int): The field number.
            length (int): The length of the field.

        """
        self.file.write(pack_length(docnum, fieldnum, length_to_byte(length)))

    def finish(self):
        """
        Finishes writing to the spool file and closes it.

        This method closes the spool file after writing is complete.

        """
        self.file.close()
        self.file = None

    def readback(self):
        """
        Reads back the length information from the spool file.

        This method opens the spool file in read binary mode and reads the length information
        in chunks of the specified size. It yields each unpacked length information.

        Yields:
            tuple: A tuple containing the document number, field number, and length.

        """
        f = open(self.filename, "rb")
        size = _length_struct.size
        while True:
            data = f.read(size)
            if not data:
                break
            yield unpack_length(data)
        f.close()


class PoolBase:
    """
    Base class for pool implementations.

    A pool is responsible for managing resources, such as file handles or connections,
    that need to be reused across multiple operations. This class provides a basic
    implementation for managing the pool directory, field length totals, and field length maxes.

    Attributes:
        _dir (str): The directory path where the pool is located.
        _fieldlength_totals (defaultdict): A dictionary that stores the total field lengths for each field.
        _fieldlength_maxes (dict): A dictionary that stores the maximum field lengths for each field.

    Methods:
        __init__(self, dir): Initializes the PoolBase instance with the specified directory.
        _filename(self, name): Returns the full path of a file within the pool directory.
        cancel(self): Cancels any pending operations or releases any acquired resources.
        fieldlength_totals(self): Returns a dictionary containing the total field lengths for each field.
        fieldlength_maxes(self): Returns a dictionary containing the maximum field lengths for each field.
    """

    def __init__(self, dir):
        self._dir = dir
        self._fieldlength_totals = defaultdict(int)
        self._fieldlength_maxes = {}

    def _filename(self, name):
        """
        Returns the full path of a file within the pool directory.

        Args:
            name (str): The name of the file.

        Returns:
            str: The full path of the file within the pool directory.
        """
        return os.path.join(self._dir, name)

    def cancel(self):
        """
        Cancels any pending operations or releases any acquired resources.
        """
        pass

    def fieldlength_totals(self):
        """
        Returns a dictionary containing the total field lengths for each field.

        Returns:
            dict: A dictionary where the keys are field names and the values are the total field lengths.
        """
        return dict(self._fieldlength_totals)

    def fieldlength_maxes(self):
        """
        Returns a dictionary containing the maximum field lengths for each field.

        Returns:
            dict: A dictionary where the keys are field names and the values are the maximum field lengths.
        """
        return self._fieldlength_maxes


class TempfilePool(PoolBase):
    """
    A pool for managing temporary files used for indexing in Whoosh.

    This class is responsible for managing temporary files used during the indexing process in Whoosh.
    It provides methods for adding content, postings, field lengths, and dumping runs to temporary files.
    The temporary files are used to store the intermediate data during the indexing process.

    Parameters:
    - lengthfile (str): The path to the length file.
    - limitmb (int): The maximum size limit in megabytes for the temporary files. Default is 32MB.
    - temp_dir (str): The directory where the temporary files will be created. If not provided, a temporary directory will be created.
    - basename (str): The base name for the temporary files. Default is an empty string.
    - **kw: Additional keyword arguments.

    Attributes:
    - lengthfile (str): The path to the length file.
    - limit (int): The maximum size limit in bytes for the temporary files.
    - size (int): The current size of the temporary files in bytes.
    - count (int): The number of postings in the temporary files.
    - postings (list): A list of postings to be written to the temporary files.
    - runs (list): A list of tuples containing the temporary file names and the number of postings in each run.
    - basename (str): The base name for the temporary files.
    - lenspool (LengthSpool): The spool for managing field lengths.
    """

    def __init__(self, lengthfile, limitmb=32, temp_dir=None, basename="", **kw):
        """
        Initialize the TempfilePool.

        Parameters:
        - lengthfile (str): The path to the length file.
        - limitmb (int): The maximum size limit in megabytes for the temporary files. Default is 32MB.
        - temp_dir (str): The directory where the temporary files will be created. If not provided, a temporary directory will be created.
        - basename (str): The base name for the temporary files. Default is an empty string.
        - **kw: Additional keyword arguments.
        """
        # Implementation details...

    def add_content(self, docnum, fieldnum, field, value):
        """
        Add content to the temporary pool.

        This method adds the content of a field in a document to the temporary pool.
        It processes the field's index and adds the postings to the pool.
        If the field is scorable, it also adds the field length.

        Parameters:
        - docnum (int): The document number.
        - fieldnum (int): The field number.
        - field (Field): The field object.
        - value (str): The field value.

        Returns:
        - int: The total term count for the field.
        """
        # Implementation details...

    def add_posting(self, fieldnum, text, docnum, freq, datastring):
        """
        Add a posting to the temporary pool.

        This method adds a posting to the temporary pool.
        It calculates the size of the posting and checks if the size limit has been reached.
        If the limit is reached, it dumps the current postings to a temporary file.

        Parameters:
        - fieldnum (int): The field number.
        - text (str): The text of the posting.
        - docnum (int): The document number.
        - freq (int): The term frequency.
        - datastring (str): The data string associated with the posting.
        """
        # Implementation details...

    def add_field_length(self, docnum, fieldnum, length):
        """
        Add a field length to the temporary pool.

        This method adds the length of a field in a document to the temporary pool.
        It updates the field length totals and maximums.

        Parameters:
        - docnum (int): The document number.
        - fieldnum (int): The field number.
        - length (int): The length of the field.
        """
        # Implementation details...

    def dump_run(self):
        """
        Dump the current postings to a temporary file.

        This method dumps the current postings to a temporary file.
        It sorts the postings, writes them to the file, and updates the runs list.
        It also resets the size and count of the temporary pool.
        """
        # Implementation details...

    def run_filenames(self):
        """
        Get the filenames of the temporary runs.

        This method returns a list of the filenames of the temporary runs.

        Returns:
        - list: A list of filenames.
        """
        # Implementation details...

    def cancel(self):
        """
        Cancel the indexing process.

        This method cancels the indexing process and cleans up the temporary files.
        """
        # Implementation details...

    def cleanup(self):
        """
        Clean up the temporary files.

        This method cleans up the temporary files by removing the temporary directory.
        """
        # Implementation details...

    def _finish_lengths(self, schema, doccount):
        """
        Finish writing the field lengths.

        This method finishes writing the field lengths to the length file.

        Parameters:
        - schema (Schema): The schema object.
        - doccount (int): The total number of documents.
        """
        # Implementation details...

    def finish(self, schema, doccount, termtable, postingwriter):
        """
        Finish the indexing process.

        This method finishes the indexing process by writing the postings to the posting writer.
        It also finishes writing the field lengths and cleans up the temporary files.

        Parameters:
        - schema (Schema): The schema object.
        - doccount (int): The total number of documents.
        - termtable (TermTable): The term table object.
        - postingwriter (PostingWriter): The posting writer object.
        """
        # Implementation details...


# Multiprocessing
class PoolWritingTask(Process):
    """A process that handles writing data to a temporary pool.

    This process is responsible for receiving data units from a posting queue and
    writing them to a temporary pool. The data units can represent content, postings,
    or field lengths. Once all the data units have been processed, the process
    finishes by dumping the temporary pool and sending the results to a result queue.

    Parameters:
    - dir (str): The directory where the temporary pool will be stored.
    - postingqueue (Queue): The queue from which the data units are received.
    - resultqueue (Queue): The queue to which the results will be sent.
    - limitmb (int): The maximum size limit of the temporary pool in megabytes.

    Attributes:
    - dir (str): The directory where the temporary pool will be stored.
    - postingqueue (Queue): The queue from which the data units are received.
    - resultqueue (Queue): The queue to which the results will be sent.
    - limitmb (int): The maximum size limit of the temporary pool in megabytes.

    """

    def __init__(self, dir, postingqueue, resultqueue, limitmb):
        """
        Initialize a PoolProcess object.

        Args:
            dir (str): The directory where the pool process will operate.
            postingqueue (Queue): The queue used for sending posting data to the pool process.
            resultqueue (Queue): The queue used for receiving results from the pool process.
            limitmb (int): The maximum memory limit in megabytes for the pool process.

        Returns:
            None

        Raises:
            None

        Notes:
            This method initializes a PoolProcess object with the given parameters. The PoolProcess is a subclass of Process and represents a separate process that can be used for performing tasks in parallel.

            The `dir` parameter specifies the directory where the pool process will operate. This directory should exist and be writable.

            The `postingqueue` parameter is a Queue object used for sending posting data to the pool process. The pool process will consume data from this queue and perform the necessary operations.

            The `resultqueue` parameter is a Queue object used for receiving results from the pool process. The pool process will put the results of its operations into this queue for the calling process to consume.

            The `limitmb` parameter specifies the maximum memory limit in megabytes for the pool process. If the pool process exceeds this limit, it may be terminated or take appropriate action to free up memory.

            Example usage:
            ```
            posting_queue = Queue()
            result_queue = Queue()
            pool_process = PoolProcess('/path/to/directory', posting_queue, result_queue, 100)
            pool_process.start()
            ```
        """
        Process.__init__(self)
        self.dir = dir
        self.postingqueue = postingqueue
        self.resultqueue = resultqueue
        self.limitmb = limitmb

    def run(self):
        """Starts the process and handles writing data to the temporary pool.

        This method is automatically called when the process starts. It continuously
        retrieves data units from the posting queue and writes them to a temporary
        pool until it receives a termination signal. Once all the data units have
        been processed, the method finishes by dumping the temporary pool and
        sending the results to the result queue.

        """

        pqueue = self.postingqueue
        rqueue = self.resultqueue

        subpool = TempfilePool(
            None, limitmb=self.limitmb, temp_dir=self.dir, basename=self.name
        )

        while True:
            unit = pqueue.get()
            if unit is None:
                break

            code, args = unit
            if code == 0:
                subpool.add_content(*args)
            elif code == 1:
                subpool.add_posting(*args)
            elif code == 2:
                subpool.add_field_length(*args)

        subpool.lenspool.finish()
        subpool.dump_run()
        rqueue.put(
            (
                subpool.runs,
                subpool.fieldlength_totals(),
                subpool.fieldlength_maxes(),
                subpool.lenspool,
            )
        )


class MultiPool(PoolBase):
    """A multi-process pool for efficient indexing.

    This class represents a multi-process pool that is used for efficient indexing in the Whoosh library.
    It inherits from the `PoolBase` class.

    Parameters:
    - lengthfile (str): The path to the length file.
    - procs (int): The number of processes to use. Default is 2.
    - limitmb (int): The maximum memory limit in megabytes. Default is 32.
    - **kw: Additional keyword arguments.

    Attributes:
    - lengthfile (str): The path to the length file.
    - procs (int): The number of processes to use.
    - limitmb (int): The maximum memory limit in megabytes.
    - postingqueue (Queue): The queue for posting tasks.
    - resultsqueue (Queue): The queue for storing results.
    - tasks (list): The list of PoolWritingTask instances.

    Methods:
    - add_content(*args): Adds content to the posting queue.
    - add_posting(*args): Adds a posting to the posting queue.
    - add_field_length(*args): Adds a field length to the posting queue.
    - cancel(): Cancels the pool and terminates all tasks.
    - cleanup(): Cleans up the temporary directory.
    - finish(schema, doccount, termtable, postingwriter): Finishes the indexing process.
    """

    def __init__(self, lengthfile, procs=2, limitmb=32, **kw):
        """
        Initialize a Pool object.

        Parameters:
        - lengthfile (str): The path to the length file.
        - procs (int, optional): The number of worker processes to use. Defaults to 2.
        - limitmb (int, optional): The maximum amount of memory (in megabytes) that each worker process can use. Defaults to 32.
        - **kw: Additional keyword arguments.

        Raises:
        - None.

        Returns:
        - None.
        """
        temp_dir = tempfile.mkdtemp(".whoosh")
        PoolBase.__init__(self, temp_dir)

        self.lengthfile = lengthfile

        self.procs = procs
        self.limitmb = limitmb

        self.postingqueue = Queue()
        self.resultsqueue = Queue()
        self.tasks = [
            PoolWritingTask(
                self._dir, self.postingqueue, self.resultsqueue, self.limitmb
            )
            for _ in range(procs)
        ]
        for task in self.tasks:
            task.start()

    def add_content(self, *args):
        """Adds content to the posting queue.

        Parameters:
        - *args: The content to be added.
        """
        self.postingqueue.put((0, args))

    def add_posting(self, *args):
        """Adds a posting to the posting queue.

        Parameters:
        - *args: The posting to be added.
        """
        self.postingqueue.put((1, args))

    def add_field_length(self, *args):
        """Adds a field length to the posting queue.

        Parameters:
        - *args: The field length to be added.
        """
        self.postingqueue.put((2, args))

    def cancel(self):
        """Cancels the pool and terminates all tasks."""
        for task in self.tasks:
            task.terminate()
        self.cleanup()

    def cleanup(self):
        """Cleans up the temporary directory."""
        shutil.rmtree(self._dir)

    def finish(self, schema, doccount, termtable, postingwriter):
        """Finishes the indexing process.

        This method is called to finish the indexing process. It performs the following steps:
        1. Joins all the tasks.
        2. Retrieves the results from the results queue.
        3. Writes the lengths to the length file.
        4. Merges the runs.
        5. Cleans up the temporary directory.

        Parameters:
        - schema (Schema): The schema object.
        - doccount (int): The total number of documents.
        - termtable (TermTable): The term table object.
        - postingwriter (PostingWriter): The posting writer object.
        """
        _fieldlength_totals = self._fieldlength_totals
        if not self.tasks:
            return

        pqueue = self.postingqueue
        rqueue = self.resultsqueue

        for _ in range(self.procs):
            pqueue.put(None)

        print("Joining...")
        t = time.time()
        for task in self.tasks:
            task.join()
        print("Join:", time.time() - t)

        print("Getting results...")
        t = time.time()
        runs = []
        lenspools = []
        for task in self.tasks:
            taskruns, flentotals, flenmaxes, lenspool = rqueue.get()
            runs.extend(taskruns)
            lenspools.append(lenspool)
            for fieldnum, total in flentotals.items():
                _fieldlength_totals[fieldnum] += total
            for fieldnum, length in flenmaxes.items():
                if length > self._fieldlength_maxes.get(fieldnum, 0):
                    self._fieldlength_maxes[fieldnum] = length
        print("Results:", time.time() - t)

        print("Writing lengths...")
        t = time.time()
        lengthfile = LengthWriter(self.lengthfile, doccount, schema.scorable_fields())
        for lenspool in lenspools:
            lengthfile.add_all(lenspool.readback())
        lengthfile.close()
        print("Lengths:", time.time() - t)

        t = time.time()
        iterator = dividemerge([read_run(runname, count) for runname, count in runs])
        # total = sum(count for runname, count in runs)
        write_postings(schema, termtable, postingwriter, iterator)
        print("Merge:", time.time() - t)

        self.cleanup()
