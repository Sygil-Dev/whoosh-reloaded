# Copyright 2009 Matt Chaput. All rights reserved.
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

"""This module defines writer and reader classes for a fast, immutable
on-disk key-value database format. The current format is based heavily on
D. J. Bernstein's CDB format (http://cr.yp.to/cdb.html).
"""

import os
import struct
import sys
from binascii import crc32
from hashlib import md5  # type: ignore @UnresolvedImport

from whoosh.system import _INT_SIZE, emptybytes
from whoosh.util.numlists import GrowableArray

# Exceptions


class FileFormatError(Exception):
    """
    Exception raised when there is an error with the file format.

    This exception is raised when there is an issue with the format of a file being processed.
    It can be used to handle specific errors related to file formats in the application.

    Attributes:
        message (str): The error message describing the specific file format error.
    """

    def __init__(self, message):
        """
        Initialize a new instance of FileFormatError.

        Args:
            message (str): The error message describing the specific file format error.
        """
        super().__init__(message)


# Hash functions


def cdb_hash(key):
    """
    Implements the CDB hash function.

    This function calculates the hash value of a given key using the CDB hash algorithm.

    Args:
        key (str): The key to be hashed.

    Returns:
        int: The hash value of the key.

    Notes:
        The CDB hash algorithm is a simple and efficient hash function that produces a 32-bit hash value.
        It is commonly used in hash-based data structures like CDB (Constant Database) and similar systems.

    Example:
        >>> cdb_hash("example")
        123456789

    References:
        - CDB Hash Function: https://cr.yp.to/cdb/cdb.txt
    """
    h = 5381
    for c in key:
        h = (h + (h << 5)) & 0xFFFFFFFF ^ ord(c)
    return h


def md5_hash(key):
    """
    Implements the MD5 hash function.

    This function takes a key and returns its hash value using the MD5 algorithm.
    The hash value is a 32-bit integer.

    Args:
        key (bytes or bytearray): The key to be hashed.

    Returns:
        int: The hash value of the key.

    Raises:
        TypeError: If the key is not of type bytes or bytearray.

    Example:
        >>> key = b'my_key'
        >>> hash_value = md5_hash(key)
        >>> print(hash_value)
        1234567890

    Note:
        This function uses the MD5 algorithm to compute the hash value of the key.
        The MD5 algorithm produces a 128-bit hash value, but this function truncates it to a 32-bit integer.
        If the Python version is less than 3.9, the `md5` function from the `hashlib` module is used.
        Otherwise, the `md5` function is called with the `usedforsecurity=False` argument.

    References:
        - Python hashlib module: https://docs.python.org/3/library/hashlib.html
        - MD5 algorithm: https://en.wikipedia.org/wiki/MD5
    """
    if not isinstance(key, (bytes, bytearray)):
        raise TypeError("Key must be of type bytes or bytearray.")

    if sys.version_info < (3, 9):
        return int(md5(key).hexdigest(), 16) & 0xFFFFFFFF
    return int(md5(key, usedforsecurity=False).hexdigest(), 16) & 0xFFFFFFFF


def crc_hash(key):
    """
    Implements the CRC32 hash function.

    This function takes a key as input and returns the hash value of the key using the CRC32 algorithm.

    Args:
        key (bytes or bytearray): The key to be hashed.

    Returns:
        int: The hash value of the key.

    Example:
        >>> key = b"example"
        >>> crc_hash(key)
        123456789

    Note:
        The key should be of type bytes or bytearray. If the key is of any other type, a TypeError will be raised.

    References:
        - CRC32 algorithm: https://en.wikipedia.org/wiki/Cyclic_redundancy_check

    """
    return crc32(key) & 0xFFFFFFFF


_hash_functions = (md5_hash, crc_hash, cdb_hash)

# Structs

# Two uints before the key/value pair giving the length of the key and value
_lengths = struct.Struct("!ii")
# A pointer in a hash table, giving the hash value and the key position
_pointer = struct.Struct("!Iq")
# A pointer in the hash table directory, giving the position and number of slots
_dir_entry = struct.Struct("!qi")

_directory_size = 256 * _dir_entry.size


# Basic hash file


class HashWriter:
    """Implements a fast on-disk key-value store.

    This hash writer uses a two-level hashing scheme, where a key is hashed, and the low eight bits of the hash value
    are used to index into one of 256 hash tables. It is similar to the CDB algorithm but with some differences.

    The HashWriter object writes all data serially and does not seek backwards to overwrite information at the end.
    It supports 64-bit file pointers, allowing for essentially unlimited file length. However, each key and value must
    be less than 2 GB in length.

    Usage:
    1. Create an instance of HashWriter by providing a StructFile object to write to, along with optional parameters
       like the format tag bytes and the hashing algorithm to use.
    2. Use the `add` method to add key/value pairs to the file. Note that keys do not need to be unique, and multiple
       values can be stored under the same key.
    3. Optionally, use the `add_all` method to add a sequence of `(key, value)` pairs.
    4. Call the `close` method to finalize the writing process and return the end position of the file.

    Args:
        dbfile (StructFile): A StructFile object to write to.
        magic (bytes, optional): The format tag bytes to write at the start of the file. Defaults to b"HSH3".
        hashtype (int, optional): An integer indicating which hashing algorithm to use.
            Possible values are 0 (MD5), 1 (CRC32), or 2 (CDB hash). Defaults to 0.

    Attributes:
        dbfile (StructFile): The StructFile object being written to.
        hashtype (int): The hashing algorithm being used.
        hashfn (function): The hash function corresponding to the selected algorithm.
        extras (dict): A dictionary for subclasses to store extra metadata.
        startoffset (int): The starting offset of the file.

    Methods:
        tell() -> int:
            Returns the current position in the file.

        add(key: bytes, value: bytes) -> None:
            Adds a key/value pair to the file.

        add_all(items: Iterable[Tuple[bytes, bytes]]) -> None:
            Adds a sequence of `(key, value)` pairs to the file.

        close() -> int:
            Finalizes the writing process and returns the end position of the file.
    """

    def __init__(self, dbfile, magic=b"HSH3", hashtype=0):
        """
        Initializes a FileTables object.

        :param dbfile: A :class:`~whoosh.filedb.structfile.StructFile` object to write to.
        :type dbfile: :class:`~whoosh.filedb.structfile.StructFile`
        :param magic: The format tag bytes to write at the start of the file. Default is b"HSH3".
        :type magic: bytes, optional
        :param hashtype: An integer indicating which hashing algorithm to use. Possible values are 0 (MD5), 1 (CRC32), or 2 (CDB hash). Default is 0.
        :type hashtype: int, optional
        """

        self.dbfile = dbfile
        self.hashtype = hashtype
        self.hashfn = _hash_functions[self.hashtype]
        self.extras = {}  # A place for subclasses to put extra metadata

        self.startoffset = dbfile.tell()
        dbfile.write(magic)  # Write format tag
        dbfile.write_byte(self.hashtype)  # Write hash type
        dbfile.write_int(0)  # Unused future expansion bits
        dbfile.write_int(0)

        self.buckets = [
            [] for _ in range(256)
        ]  # 256 lists of hashed keys and positions
        self.directory = []  # List to remember the positions of the hash tables

    def tell(self):
        """
        Returns the current position of the file pointer within the database file.

        :return: The current position of the file pointer.
        :rtype: int
        """
        return self.dbfile.tell()

    def add(self, key, value):
        """Adds a key/value pair to the file.

        This method is used to add a key/value pair to the file. The keys do not need to be unique,
        meaning you can store multiple values under the same key. The values are stored in a file
        using the specified key.

        Parameters:
        - key (bytes): The key associated with the value. It must be of type bytes.
        - value (bytes): The value to be stored. It must be of type bytes.

        Returns:
        None

        Raises:
        AssertionError: If the key or value is not of type bytes.

        Usage:
        file_table = FileTable()
        file_table.add(b'key1', b'value1')
        file_table.add(b'key1', b'value2')
        file_table.add(b'key2', b'value3')
        """
        assert isinstance(key, bytes)
        assert isinstance(value, bytes)

        dbfile = self.dbfile
        pos = dbfile.tell()
        dbfile.write(_lengths.pack(len(key), len(value)))
        dbfile.write(key)
        dbfile.write(value)

        # Get hash value for the key
        h = self.hashfn(key)
        # Add hash and on-disk position to appropriate bucket
        self.buckets[h & 255].append((h, pos))

    def add_all(self, items):
        """
        Convenience method to add a sequence of ``(key, value)`` pairs to the file table.

        This method allows you to add multiple key-value pairs to the file table at once.
        It iterates over the given sequence of ``(key, value)`` pairs and calls the
        :meth:`add` method for each pair.

        Parameters:
            items (sequence): A sequence of ``(key, value)`` pairs to be added to the file table.

        Example:
            >>> items = [('key1', 'value1'), ('key2', 'value2'), ('key3', 'value3')]
            >>> file_table.add_all(items)

        Note:
            - The `items` parameter should be an iterable containing ``(key, value)`` pairs.
            - The `key` should be a unique identifier for each value in the file table.
            - The `value` can be any object that needs to be associated with the `key`.
        """
        add = self.add
        for key, value in items:
            add(key, value)

    def _write_hashes(self):
        """
        Writes 256 hash tables containing pointers to the key/value pairs.

        This method is responsible for creating and writing the hash tables to disk.
        Each hash table contains pointers to the key/value pairs stored in the database.

        Parameters:
        - None

        Returns:
        - None

        Usage:
        - Call this method to write the hash tables to disk after populating the buckets.

        Algorithm:
        - For each bucket in the buckets list:
            - Get the start position of the bucket's hash table in the database file.
            - Calculate the number of slots in the hash table.
            - Append the (start position, number of slots) tuple to the directory list.
            - Create an empty hash table with the specified number of slots.
            - For each (hash value, key position) tuple in the bucket:
                - Calculate the slot index for the entry using bit shifting and wrapping.
                - If the slot is already taken, find the next empty slot.
                - Insert the entry into the hash table at the calculated slot index.
            - Write the hash table for the bucket to the database file.

        Note:
        - The hash tables are written in a specific format using the _pointer.pack() method.
        - The database file (dbfile) and the null value (representing an empty slot) are used throughout the method.
        """
        dbfile = self.dbfile
        # Represent and empty slot in the hash table using 0,0 (no key can
        null = (0, 0)

        for entries in self.buckets:
            # Start position of this bucket's hash table
            pos = dbfile.tell()
            # Remember the start position and the number of slots
            numslots = 2 * len(entries)
            self.directory.append((pos, numslots))

            # Create the empty hash table
            hashtable = [null] * numslots
            # For each (hash value, key position) tuple in the bucket
            for hashval, position in entries:
                # Bitshift and wrap to get the slot for this entry
                slot = (hashval >> 8) % numslots
                # If the slot is taken, keep going until we find an empty slot
                while hashtable[slot] != null:
                    slot = (slot + 1) % numslots
                # Insert the entry into the hashtable
                hashtable[slot] = (hashval, position)

            # Write the hash table for this bucket to disk
            for hashval, position in hashtable:
                dbfile.write(_pointer.pack(hashval, position))

    def _write_directory(self):
        """
        Writes a directory of pointers to the 256 hash tables.

        This method is responsible for writing a directory of pointers to the 256 hash tables
        in the database file. Each entry in the directory consists of the position and number
        of slots for a hash table.

        Parameters:
            None

        Returns:
            None

        Raises:
            None

        Usage:
            Call this method to write the directory of pointers to the hash tables in the
            database file.

        Example:
            _write_directory()
        """
        dbfile = self.dbfile
        for position, numslots in self.directory:
            dbfile.write(_dir_entry.pack(position, numslots))

    def _write_extras(self):
        """
        Write the extras dictionary to the database file.

        This method serializes and writes the extras dictionary to the database file.
        The extras dictionary contains additional metadata or information associated
        with the file database.

        Note:
            This method should only be called internally by the filetables module.

        Raises:
            IOError: If there is an error writing the extras dictionary to the file.

        """
        self.dbfile.write_pickle(self.extras)

    def close(self):
        """
        Closes the file database and performs necessary write operations.

        This method is responsible for closing the file database and performing
        necessary write operations before closing. It writes hash tables, the
        directory of pointers to hash tables, extra information, and the length
        of the pickle to the file.

        Returns:
            int: The position of the end of the file.

        Usage:
            Call this method when you are finished using the file database and
            want to close it. It ensures that all necessary write operations are
            performed before closing the file.

        Example:
            file_db = FileDatabase()
            # ... perform operations on the file database ...
            file_db.close()
        """
        dbfile = self.dbfile

        # Write hash tables
        self._write_hashes()
        # Write directory of pointers to hash tables
        self._write_directory()

        expos = dbfile.tell()
        # Write extra information
        self._write_extras()
        # Write length of pickle
        dbfile.write_int(dbfile.tell() - expos)

        endpos = dbfile.tell()
        dbfile.close()
        return endpos


class HashReader:
    """Reader for the fast on-disk key-value files created by
    :class:`HashWriter`.

    This class provides methods to read and retrieve key-value pairs from a
    hash file. It is designed to work with files created by the `HashWriter`
    class.

    Usage:
    ------
    To use the `HashReader` class, you need to provide a file object and
    optionally the length of the file data. The file object should be an
    instance of `whoosh.filedb.structfile.StructFile`.

    Example:
    --------
    # Open a hash file
    dbfile = StructFile("data.hash")
    reader = HashReader(dbfile)

    # Retrieve a value for a given key
    value = reader["key"]

    # Iterate over all key-value pairs
    for key, value in reader:
        print(key, value)

    # Close the reader
    reader.close()

    Parameters:
    -----------
    dbfile : whoosh.filedb.structfile.StructFile
        A file object to read from. This should be an instance of
        `whoosh.filedb.structfile.StructFile`.
    length : int, optional
        The length of the file data. This is necessary since the hashing
        information is written at the end of the file.
    magic : bytes, optional
        The format tag bytes to look for at the start of the file. If the
        file's format tag does not match these bytes, the object raises a
        `FileFormatError` exception.
    startoffset : int, optional
        The starting point of the file data.

    Attributes:
    -----------
    dbfile : whoosh.filedb.structfile.StructFile
        The file object being read from.
    startoffset : int
        The starting point of the file data.
    is_closed : bool
        Indicates whether the reader has been closed.

    Methods:
    --------
    open(cls, storage, name)
        Convenience method to open a hash file given a
        `whoosh.filedb.filestore.Storage` object and a name. This takes care
        of opening the file and passing its length to the initializer.
    file()
        Returns the file object being read from.
    close()
        Closes the reader.
    key_at(pos)
        Returns the key bytes at the given position.
    key_and_range_at(pos)
        Returns a (keybytes, datapos, datalen) tuple for the key at the given
        position.
    __getitem__(key)
        Retrieves the value associated with the given key.
    __iter__()
        Iterates over all key-value pairs.
    __contains__(key)
        Checks if the given key exists in the hash file.
    keys()
        Returns an iterator over all keys.
    values()
        Returns an iterator over all values.
    items()
        Returns an iterator over all key-value pairs.
    get(key, default=None)
        Retrieves the value associated with the given key, or returns the
        default value if the key is not found.
    all(key)
        Returns a generator that yields all values associated with the given
        key.
    ranges_for_key(key)
        Returns a generator that yields (datapos, datalength) tuples
        associated with the given key.
    range_for_key(key)
        Returns the first (datapos, datalength) tuple associated with the
        given key.

    """

    def __init__(self, dbfile, length=None, magic=b"HSH3", startoffset=0):
        """
        Initializes a FileTables object.

        :param dbfile: A :class:`~whoosh.filedb.structfile.StructFile` object to read from.
        :type dbfile: :class:`~whoosh.filedb.structfile.StructFile`
        :param length: The length of the file data. This is necessary since the hashing information is written at the end of the file.
        :type length: int, optional
        :param magic: The format tag bytes to look for at the start of the file. If the file's format tag does not match these bytes, the object raises a :class:`~whoosh.filedb.filetables.FileFormatError` exception.
        :type magic: bytes, optional
        :param startoffset: The starting point of the file data.
        :type startoffset: int, optional

        :raises FileFormatError: If the format tag of the file does not match the specified magic bytes.

        The FileTables object represents a file-based hash table. It reads and interprets the data from the provided `dbfile` object.

        The `dbfile` parameter should be an instance of :class:`~whoosh.filedb.structfile.StructFile`, which is a file-like object that supports reading and seeking.

        The `length` parameter is the length of the file data. If not provided, the object will determine the length by seeking to the end of the file and calculating the difference between the current position and the `startoffset`.

        The `magic` parameter is the format tag bytes to look for at the start of the file. If the file's format tag does not match these bytes, a :class:`~whoosh.filedb.filetables.FileFormatError` exception is raised.

        The `startoffset` parameter is the starting point of the file data. If not provided, it defaults to 0.

        After initialization, the FileTables object provides access to the hash tables and other metadata stored in the file.

        Example usage:

        .. code-block:: python

            from whoosh.filedb.structfile import StructFile
            from whoosh.filedb.filetables import FileTables

            # Open the file in binary mode
            with open("data.db", "rb") as f:
                # Create a StructFile object
                dbfile = StructFile(f)
                # Create a FileTables object
                tables = FileTables(dbfile)

                # Access the hash tables
                for table in tables.tables:
                    position, numslots = table
                    print(f"Table at position {position} with {numslots} slots")

        """
        self.dbfile = dbfile
        self.startoffset = startoffset
        self.is_closed = False

        if length is None:
            dbfile.seek(0, os.SEEK_END)
            length = dbfile.tell() - startoffset

        dbfile.seek(startoffset)
        # Check format tag
        filemagic = dbfile.read(4)
        if filemagic != magic:
            raise FileFormatError(f"Unknown file header {filemagic!r}")
        # Read hash type
        self.hashtype = dbfile.read_byte()
        self.hashfn = _hash_functions[self.hashtype]
        # Skip unused future expansion bits
        dbfile.read_int()
        dbfile.read_int()
        self.startofdata = dbfile.tell()

        exptr = startoffset + length - _INT_SIZE
        # Get the length of extras from the end of the file
        exlen = dbfile.get_int(exptr)
        # Read the extras
        expos = exptr - exlen
        dbfile.seek(expos)
        self._read_extras()

        # Calculate the directory base from the beginning of the extras
        dbfile.seek(expos - _directory_size)
        # Read directory of hash tables
        self.tables = []
        entrysize = _dir_entry.size
        unpackentry = _dir_entry.unpack
        for _ in range(256):
            # position, numslots
            self.tables.append(unpackentry(dbfile.read(entrysize)))
        # The position of the first hash table is the end of the key/value pairs
        self.endofdata = self.tables[0][0]

    @classmethod
    def open(cls, storage, name):
        """Convenience method to open a hash file given a
        :class:`whoosh.filedb.filestore.Storage` object and a name. This takes
        care of opening the file and passing its length to the initializer.

        :param storage: The storage object representing the file store.
        :type storage: whoosh.filedb.filestore.Storage
        :param name: The name of the hash file to open.
        :type name: str
        :return: An instance of the hash file.
        :rtype: whoosh.filedb.filetables.HashFile

        :raises FileNotFoundError: If the specified file does not exist.
        :raises IOError: If there is an error opening the file.

        Usage:
        >>> storage = Storage()
        >>> hash_file = HashFile.open(storage, "example.txt")
        """
        length = storage.file_length(name)
        dbfile = storage.open_file(name)
        return cls(dbfile, length)

    def file(self):
        """
        Returns the database file associated with this instance.

        Returns:
            str: The path to the database file.

        """
        return self.dbfile

    def _read_extras(self):
        """
        Reads the extras from the database file.

        This method reads the extras stored in the database file and assigns them to the `extras` attribute of the
        FileTables object. If an EOFError occurs during the reading process, an empty dictionary is assigned to the
        `extras` attribute.

        Returns:
            None

        Raises:
            None
        """
        try:
            self.extras = self.dbfile.read_pickle()
        except EOFError:
            self.extras = {}

    def close(self):
        """
        Closes the file table.

        This method closes the file table by closing the underlying database file.
        Once closed, the file table cannot be used for any further operations.

        Raises:
            ValueError: If the file table is already closed.

        Usage:
            table = FileTable(...)
            table.close()
        """
        if self.is_closed:
            raise ValueError(f"Tried to close {self} twice")
        self.dbfile.close()
        self.is_closed = True

    def key_at(self, pos):
        """
        Returns the key bytes at the given position.

        Parameters:
            pos (int): The position of the key in the database file.

        Returns:
            bytes: The key bytes at the given position.

        Raises:
            IndexError: If the position is out of range.

        Notes:
            This method retrieves the key bytes from the database file at the specified position.
            The position should be a valid index within the file.
            The returned key bytes can be used for further processing or lookups in the database.

        Example:
            >>> db = FileTables()
            >>> key = db.key_at(10)
        """
        dbfile = self.dbfile
        keylen = dbfile.get_uint(pos)
        return dbfile.get(pos + _lengths.size, keylen)

    def key_and_range_at(self, pos):
        """
        Returns a tuple containing the key, data position, and data length for the key at the given position.

        Parameters:
        - pos (int): The position of the key in the database file.

        Returns:
        - tuple: A tuple containing the following elements:
            - keybytes (bytes): The key as bytes.
            - datapos (int): The position of the data in the database file.
            - datalen (int): The length of the data.

        Raises:
        - None

        Notes:
        - This method assumes that the database file is already open and accessible.
        - The position should be within the valid range of data in the file.
        """
        dbfile = self.dbfile
        lenssize = _lengths.size

        if pos >= self.endofdata:
            return None

        keylen, datalen = _lengths.unpack(dbfile.get(pos, lenssize))
        keybytes = dbfile.get(pos + lenssize, keylen)
        datapos = pos + lenssize + keylen
        return keybytes, datapos, datalen

    def _ranges(self, pos=None, eod=None):
        """
        Yields a series of (keypos, keylength, datapos, datalength) tuples for the key/value pairs in the file.

        Parameters:
            pos (int, optional): The starting position to iterate from. If not provided, it defaults to self.startofdata.
            eod (int, optional): The ending position to iterate until. If not provided, it defaults to self.endofdata.

        Yields:
            tuple: A tuple containing the key position, key length, data position, and data length.

        Usage:
            Use this method to iterate over the key/value pairs in the file. It returns a series of tuples, where each tuple represents a key/value pair in the file. The tuple contains the following information:
            - keypos: The position of the key in the file.
            - keylen: The length of the key.
            - datapos: The position of the data in the file.
            - datalen: The length of the data.

        Example:
            for keypos, keylen, datapos, datalen in _ranges():
                # Process the key/value pair
                ...
        """
        dbfile = self.dbfile
        pos = pos or self.startofdata
        eod = eod or self.endofdata
        lenssize = _lengths.size
        unpacklens = _lengths.unpack

        while pos < eod:
            keylen, datalen = unpacklens(dbfile.get(pos, lenssize))
            keypos = pos + lenssize
            datapos = keypos + keylen
            yield (keypos, keylen, datapos, datalen)
            pos = datapos + datalen

    def __getitem__(self, key):
        """
        Retrieve the value associated with the given key.

        Args:
            key: The key to retrieve the value for.

        Returns:
            The value associated with the given key.

        Raises:
            KeyError: If the key is not found in the table.
        """
        for value in self.all(key):
            return value
        raise KeyError(key)

    def __iter__(self):
        """
        Iterate over the key-value pairs stored in the file table.

        Yields:
            tuple: A tuple containing the key and value of each entry in the file table.

        Raises:
            IOError: If there is an error reading the file table.

        Usage:
            file_table = FileTable()
            for key, value in file_table:
                # Process key-value pair
                ...
        """
        dbfile = self.dbfile
        for keypos, keylen, datapos, datalen in self._ranges():
            key = dbfile.get(keypos, keylen)
            value = dbfile.get(datapos, datalen)
            yield (key, value)

    def __contains__(self, key):
        """
        Check if the given key exists in the file table.

        Parameters:
        - key (str): The key to check for existence in the file table.

        Returns:
        - bool: True if the key exists in the file table, False otherwise.

        Description:
        This method checks if the given key exists in the file table. It iterates over the ranges associated with the key
        and returns True if at least one range is found. Otherwise, it returns False.

        Example:
        >>> file_table = FileTable()
        >>> file_table["key1"] = Range(0, 100)
        >>> file_table["key2"] = Range(200, 300)
        >>> "key1" in file_table
        True
        >>> "key3" in file_table
        False
        """
        for _ in self.ranges_for_key(key):
            return True
        return False

    def keys(self):
        """
        Retrieve the keys from the file table.

        This method iterates over the file table and yields each key stored in it.

        Yields:
            str: The keys stored in the file table.

        """
        dbfile = self.dbfile
        for keypos, keylen, _, _ in self._ranges():
            yield dbfile.get(keypos, keylen)

    def values(self):
        """
        Returns an iterator over the values stored in the file table.

        Yields:
            bytes: The value stored in the file table.

        Raises:
            KeyError: If the file table is empty.

        Notes:
            This method iterates over the ranges of data stored in the file table and retrieves
            the corresponding values using the `dbfile.get()` method. The values are yielded one
            by one, allowing for efficient memory usage when working with large file tables.

        Example:
            >>> table = FileTable()
            >>> table.add(1, b'value1')
            >>> table.add(2, b'value2')
            >>> table.add(3, b'value3')
            >>> for value in table.values():
            ...     print(value)
            b'value1'
            b'value2'
            b'value3'
        """
        dbfile = self.dbfile
        for _, _, datapos, datalen in self._ranges():
            yield dbfile.get(datapos, datalen)

    def items(self):
        """
        Returns an iterator over the key-value pairs stored in the file table.

        Yields:
            tuple: A tuple containing the key and value retrieved from the file table.

        Notes:
            This method iterates over the ranges of the file table and retrieves the key-value pairs
            using the positions and lengths stored in each range. The key and value are obtained by
            calling the `get` method of the `dbfile` object.

        Example:
            >>> file_table = FileTable()
            >>> for key, value in file_table.items():
            ...     print(key, value)
        """
        dbfile = self.dbfile
        for keypos, keylen, datapos, datalen in self._ranges():
            yield (dbfile.get(keypos, keylen), dbfile.get(datapos, datalen))

    def get(self, key, default=None):
        """
        Retrieve the value associated with the given key.

        This method returns the first value found for the given key in the file table.
        If no value is found, it returns the default value provided.

        Parameters:
        - key (str): The key to search for in the file table.
        - default (Any, optional): The default value to return if no value is found. Defaults to None.

        Returns:
        - The value associated with the given key, or the default value if no value is found.
        """
        for value in self.all(key):
            return value
        return default

    def all(self, key):
        """
        Yields a sequence of values associated with the given key.

        Parameters:
        - key (str): The key to retrieve values for.

        Returns:
        - generator: A generator that yields the values associated with the key.

        Raises:
        - KeyError: If the key is not found in the database.

        Example:
        >>> db = FileTables()
        >>> db.all("key1")
        <generator object all at 0x7f9e9a6e3f20>
        >>> list(db.all("key1"))
        ['value1', 'value2', 'value3']
        """
        dbfile = self.dbfile
        for datapos, datalen in self.ranges_for_key(key):
            yield dbfile.get(datapos, datalen)

    def ranges_for_key(self, key):
        """Yields a sequence of ``(datapos, datalength)`` tuples associated
        with the given key.

        Args:
            key (bytes): The key to search for. Should be of type bytes.

        Yields:
            tuple: A tuple containing the data position and data length associated with the key.

        Raises:
            TypeError: If the key is not of type bytes.

        Notes:
            This method is used to retrieve the data position and data length associated with a given key.
            It performs a lookup in the hash table to find the key's slot, and then checks if the key matches
            the one stored in the slot. If a match is found, it yields the data position and data length.

            The method assumes that the hash table and data file have been properly initialized.

        Example:
            >>> db = FileTables()
            >>> key = b'my_key'
            >>> for datapos, datalength in db.ranges_for_key(key):
            ...     print(f"Data position: {datapos}, Data length: {datalength}")
        """

        if not isinstance(key, bytes):
            raise TypeError(f"Key {key!r} should be bytes")
        dbfile = self.dbfile

        # Hash the key
        keyhash = self.hashfn(key)
        # Get the position and number of slots for the hash table in which the
        # key may be found
        tablestart, numslots = self.tables[keyhash & 255]
        # If the hash table is empty, we know the key doesn't exists
        if not numslots:
            return

        ptrsize = _pointer.size
        unpackptr = _pointer.unpack
        lenssize = _lengths.size
        unpacklens = _lengths.unpack

        # Calculate where the key's slot should be
        slotpos = tablestart + (((keyhash >> 8) % numslots) * ptrsize)
        # Read slots looking for our key's hash value
        for _ in range(numslots):
            slothash, itempos = unpackptr(dbfile.get(slotpos, ptrsize))
            # If this slot is empty, we're done
            if not itempos:
                return

            # If the key hash in this slot matches our key's hash, we might have
            # a match, so read the actual key and see if it's our key
            if slothash == keyhash:
                # Read the key and value lengths
                keylen, datalen = unpacklens(dbfile.get(itempos, lenssize))
                # Only bother reading the actual key if the lengths match
                if keylen == len(key):
                    keystart = itempos + lenssize
                    if key == dbfile.get(keystart, keylen):
                        # The keys match, so yield (datapos, datalen)
                        yield (keystart + keylen, datalen)

            slotpos += ptrsize
            # If we reach the end of the hashtable, wrap around
            if slotpos == tablestart + (numslots * ptrsize):
                slotpos = tablestart

    def range_for_key(self, key):
        """
        Returns the range associated with the given key.

        This method retrieves the range associated with the given key from the file table.
        If the key is found, the range is returned. If the key is not found, a KeyError is raised.

        Parameters:
        - key (str): The key to search for in the file table.

        Returns:
        - range (tuple): The range associated with the given key.

        Raises:
        - KeyError: If the key is not found in the file table.

        Example:
        >>> table = FileTable()
        >>> table.range_for_key('key1')
        (0, 100)
        """

        for item in self.ranges_for_key(key):
            return item
        raise KeyError(key)


# Ordered hash file


class OrderedHashWriter(HashWriter):
    """
    Implements an on-disk hash, but requires that keys be added in order.
    An OrderedHashReader can then look up "nearest keys" based on the ordering.

    Parameters:
    - dbfile (file-like object): The file-like object to write the hash data to.

    Usage:
    1. Create an instance of OrderedHashWriter by providing a file-like object.
    2. Use the add() method to add keys and values to the hash in increasing order.
    3. Call the _write_extras() method to write the metadata and index array to the file.

    Example:
    ```
    with open("hash.db", "wb") as dbfile:
        writer = OrderedHashWriter(dbfile)
        writer.add("key1", "value1")
        writer.add("key2", "value2")
        writer._write_extras()
    ```

    Note:
    - Keys must be added in increasing order. If a key is added that is not greater than the previous key, a ValueError will be raised.
    - The index array, which contains the positions of all keys, will be stored as metadata in the file.
    """

    def __init__(self, dbfile):
        """
        Initialize a FileTables object.

        Args:
            dbfile (str): The path to the database file.

        Attributes:
            index (GrowableArray): An array of the positions of all keys.
            lastkey (bytes): The last key added.

        """
        HashWriter.__init__(self, dbfile)
        # Keep an array of the positions of all keys
        self.index = GrowableArray("H")
        # Keep track of the last key added
        self.lastkey = emptybytes

    def add(self, key, value):
        """
        Adds a key-value pair to the hash.

        Parameters:
        - key: The key to add. Must be greater than the previous key.
        - value: The value associated with the key.

        Raises:
        - ValueError: If the key is not greater than the previous key.

        Note:
        - The position of the key in the file will be stored in the index array.
        """
        if key <= self.lastkey:
            raise ValueError(f"Keys must increase: {self.lastkey!r}..{key!r}")
        self.index.append(self.dbfile.tell())
        HashWriter.add(self, key, value)
        self.lastkey = key

    def _write_extras(self):
        """
        Writes the metadata and index array to the file.

        Note:
        - This method should be called after adding all keys and values to the hash.
        """
        dbfile = self.dbfile
        index = self.index

        # Store metadata about the index array
        self.extras["indextype"] = index.typecode
        self.extras["indexlen"] = len(index)
        # Write the extras
        HashWriter._write_extras(self)
        # Write the index array
        index.to_file(dbfile)


class OrderedHashReader(HashReader):
    """A class for reading an ordered hash file and performing operations on it.

    This class extends the `HashReader` class and provides additional methods
    for working with an ordered series of keys in the hash file.

    Methods:
        closest_key(key):
            Returns the closest key equal to or greater than the given key. If
            there is no key in the file equal to or greater than the given key,
            returns None.

        ranges_from(key):
            Yields a series of ``(keypos, keylen, datapos, datalen)`` tuples
            for the ordered series of keys equal or greater than the given key.

        keys_from(key):
            Yields an ordered series of keys equal to or greater than the given
            key.

        items_from(key):
            Yields an ordered series of ``(key, value)`` tuples for keys equal
            to or greater than the given key.

    Attributes:
        indexbase:
            The base position of the index array in the hash file.

        indexlen:
            The length of the index array.

        indexsize:
            The size of each index element in bytes.

    """

    def closest_key(self, key):
        """
        Returns the closest key equal to or greater than the given key. If there is no key in the file
        equal to or greater than the given key, returns None.

        Parameters:
            key (Any): The key to search for.

        Returns:
            Any: The closest key equal to or greater than the given key, or None if no such key exists.
        """
        pos = self.closest_key_pos(key)
        if pos is None:
            return None
        return self.key_at(pos)

    def ranges_from(self, key):
        """Yields a series of ``(keypos, keylen, datapos, datalen)`` tuples
        for the ordered series of keys equal or greater than the given key.

        Parameters:
        - key (bytes): The key to start the range from.

        Returns:
        - Generator: A generator that yields ``(keypos, keylen, datapos, datalen)`` tuples.

        Notes:
        - This method returns a generator that iterates over the ordered series of keys in the file table,
            starting from the given key and including all keys that are equal or greater.
        - Each tuple in the generator represents a range of data associated with a key, where:
            - keypos: The position of the key in the file table.
            - keylen: The length of the key.
            - datapos: The position of the associated data in the file table.
            - datalen: The length of the associated data.

        Example:
        ```
        file_table = FileTable()
        for keypos, keylen, datapos, datalen in file_table.ranges_from(b'my_key'):
                # Process the key and associated data
                ...
        ```
        """

        pos = self.closest_key_pos(key)
        if pos is None:
            return

        yield from self._ranges(pos=pos)

    def keys_from(self, key):
        """Yields an ordered series of keys equal to or greater than the given key.

        Args:
            key: The key to start yielding from.

        Yields:
            The keys equal to or greater than the given key.

        Raises:
            None.

        Example:
            >>> db = FileTables()
            >>> for key in db.keys_from('abc'):
            ...     print(key)
            abc
            abcd
            abcde
        """

        dbfile = self.dbfile
        for keypos, keylen, _, _ in self.ranges_from(key):
            yield dbfile.get(keypos, keylen)

    def items_from(self, key):
        """Yields an ordered series of ``(key, value)`` tuples for keys equal
        to or greater than the given key.

        Parameters:
        - key (bytes): The key to start iterating from.

        Yields:
        - tuple: A ``(key, value)`` tuple for each key equal to or greater than the given key.

        Notes:
        - This method retrieves the ``(key, value)`` pairs from the file database starting from the given key.
        - The keys are ordered in ascending order.
        - The values are retrieved from the file database using the key positions and lengths.

        Example:
        >>> db = FileTables()
        >>> for key, value in db.items_from(b'key1'):
        ...     print(key, value)
        ('key1', 'value1')
        ('key2', 'value2')
        ('key3', 'value3')
        """

        dbfile = self.dbfile
        for keypos, keylen, datapos, datalen in self.ranges_from(key):
            yield (dbfile.get(keypos, keylen), dbfile.get(datapos, datalen))

    def _read_extras(self):
        """
        Reads the extras from the database file and sets up the necessary variables for reading the index array.

        This method is called internally by the FileTables class.

        Parameters:
        - None

        Returns:
        - None

        Raises:
        - Exception: If the index type is unknown.

        Usage:
        - This method should not be called directly. It is called internally by the FileTables class to read the extras
          from the database file and set up the necessary variables for reading the index array.
        """
        dbfile = self.dbfile

        # Read the extras
        HashReader._read_extras(self)

        # Set up for reading the index array
        indextype = self.extras["indextype"]
        self.indexbase = dbfile.tell()
        self.indexlen = self.extras["indexlen"]
        self.indexsize = struct.calcsize(indextype)
        # Set up the function to read values from the index array
        if indextype == "B":
            self._get_pos = dbfile.get_byte
        elif indextype == "H":
            self._get_pos = dbfile.get_ushort
        elif indextype == "i":
            self._get_pos = dbfile.get_int
        elif indextype == "I":
            self._get_pos = dbfile.get_uint
        elif indextype == "q":
            self._get_pos = dbfile.get_long
        else:
            raise Exception(f"Unknown index type {indextype!r}")

    def closest_key_pos(self, key):
        """
        Given a key, return the position of that key OR the next highest key if the given key does not exist.

        Args:
            key (bytes): The key to search for. Should be of type bytes.

        Returns:
            int or None: The position of the key in the index array, or None if the key is not found.

        Raises:
            TypeError: If the key is not of type bytes.

        Notes:
            This method performs a binary search on the positions in the index array to find the closest key.
            It assumes that the index array is sorted in ascending order.

        Example:
            >>> index = FileTables()
            >>> index.closest_key_pos(b'key1')
            0
            >>> index.closest_key_pos(b'key2')
            1
            >>> index.closest_key_pos(b'key3')
            2
            >>> index.closest_key_pos(b'key4')
            2
        """
        if not isinstance(key, bytes):
            raise TypeError(f"Key {key!r} should be bytes")

        indexbase = self.indexbase
        indexsize = self.indexsize
        key_at = self.key_at
        _get_pos = self._get_pos

        # Do a binary search of the positions in the index array
        lo = 0
        hi = self.indexlen
        while lo < hi:
            mid = (lo + hi) // 2
            midkey = key_at(_get_pos(indexbase + mid * indexsize))
            if midkey < key:
                lo = mid + 1
            else:
                hi = mid

        # If we went off the end, return None
        if lo == self.indexlen:
            return None
        # Return the closest key
        return _get_pos(indexbase + lo * indexsize)


# Fielded Ordered hash file


class FieldedOrderedHashWriter(HashWriter):
    """
    Implements an on-disk hash, but writes separate position indexes for each field.

    This class is used to write a hash table to disk, where each field has its own position index.
    It is designed to work with the `HashReader` class to provide efficient retrieval of values
    based on keys.

    Usage:
    1. Create an instance of `FieldedOrderedHashWriter` by passing the `dbfile` parameter, which
       represents the file to write the hash table to.
    2. Call the `start_field` method to indicate the start of a new field. Pass the `fieldname`
       parameter to specify the name of the field.
    3. Call the `add` method to add a key-value pair to the hash table. The keys must be in increasing
       order. If a key is added that is less than or equal to the previous key, a `ValueError` is raised.
    4. Repeat steps 2 and 3 for each field and key-value pair.
    5. Call the `end_field` method to indicate the end of the current field. This will store the
       position index for the field in the `fieldmap` dictionary.
    6. After adding all fields and key-value pairs, the hash table can be accessed using the `HashReader`
       class.

    Attributes:
    - `fieldmap`: A dictionary that maps field names to tuples containing the start position, end position,
      length, and typecode of the position index for each field.
    - `lastkey`: The last key that was added to the hash table.

    Note:
    - This class inherits from the `HashWriter` class, which provides the basic functionality for writing
      a hash table to disk.

    Example:
    ```
    writer = FieldedOrderedHashWriter(dbfile)
    writer.start_field("field1")
    writer.add("key1", "value1")
    writer.add("key2", "value2")
    writer.end_field()
    writer.start_field("field2")
    writer.add("key3", "value3")
    writer.end_field()
    # ...
    ```

    """

    def __init__(self, dbfile):
        """
        Initialize a FileTables object.

        Args:
            dbfile (str): The path to the database file.

        Attributes:
            fieldmap (dict): A dictionary mapping field names to tuples containing
                the start position, index position, length, and type code.
            lastkey (bytes): The last key added to the FileTables object.

        """
        HashWriter.__init__(self, dbfile)
        # Map field names to (startpos, indexpos, length, typecode)
        self.fieldmap = self.extras["fieldmap"] = {}
        # Keep track of the last key added
        self.lastkey = emptybytes

    def start_field(self, fieldname):
        """
        Start a new field in the hash table.

        This method is used to initialize a new field in the hash table. It sets the current position in the database file
        as the starting position for the field and stores the field name. It also initializes an array to keep track of the
        positions of all keys associated with this field.

        Args:
            fieldname (str): The name of the field.

        Returns:
            None

        Example:
            To start a new field named "title", you can call this method as follows:
            >>> start_field("title")
        """
        self.fieldstart = self.dbfile.tell()
        self.fieldname = fieldname
        # Keep an array of the positions of all keys
        self.poses = GrowableArray("H")
        self.lastkey = emptybytes

    def add(self, key, value):
        """
        Add a key-value pair to the hash table.

        Args:
        - `key` (int): The key to add. It should be greater than any previously added key.
        - `value` (Any): The value associated with the key.

        Raises:
        - `ValueError`: If the key is less than or equal to the previous key.

        Returns:
        - None

        Notes:
        - This method appends the position of the value in the database file to the `poses` list.
        - The `HashWriter.add` method is called to actually add the key-value pair to the hash table.
        - The `lastkey` attribute is updated with the newly added key.

        Example usage:
        ```
        table = FileTable()
        table.add(1, "Value 1")
        table.add(2, "Value 2")
        table.add(3, "Value 3")
        ```

        """
        if key <= self.lastkey:
            raise ValueError(f"Keys must increase: {self.lastkey!r}..{key!r}")
        self.poses.append(self.dbfile.tell() - self.fieldstart)
        HashWriter.add(self, key, value)
        self.lastkey = key

    def end_field(self):
        """
        End the current field in the hash table.

        This method stores the position index for the field in the `fieldmap` dictionary.
        The `fieldmap` dictionary is used to keep track of the start and end positions of each field
        in the hash table, as well as the number of positions and the typecode of the positions.

        Usage:
        ------
        Call this method after adding all the positions for a field in the hash table.
        It will update the `fieldmap` dictionary with the relevant information for the field.

        Example:
        --------
        # Create a FileTables object
        filetables = FileTables()

        # Add positions for a field
        filetables.add_position(1)
        filetables.add_position(2)
        filetables.add_position(3)

        # End the field and update the fieldmap
        filetables.end_field()

        Parameters:
        -----------
        None

        Returns:
        --------
        None
        """
        dbfile = self.dbfile
        fieldname = self.fieldname
        poses = self.poses
        self.fieldmap[fieldname] = (
            self.fieldstart,
            dbfile.tell(),
            len(poses),
            poses.typecode,
        )
        poses.to_file(dbfile)


class FieldedOrderedHashReader(HashReader):
    """
    A subclass of HashReader that provides additional functionality for reading fielded ordered hash data.

    This class extends the HashReader class and adds methods for working with fielded ordered hash data.
    It provides methods for iterating over terms, retrieving term data, checking if a term exists,
    finding the closest term, and more.

    Usage:
    1. Create an instance of FieldedOrderedHashReader by passing the necessary arguments to the constructor.
    2. Use the various methods provided by this class to interact with the fielded ordered hash data.

    Example:
    ```
    reader = FieldedOrderedHashReader(...)
    for fieldname, term in reader.iter_terms():
        print(fieldname, term)
    ```

    Args:
        *args: Variable length argument list to be passed to the parent class constructor.
        **kwargs: Arbitrary keyword arguments to be passed to the parent class constructor.

    Attributes:
        fieldmap (dict): A dictionary mapping field names to their corresponding start and end ranges.
        fieldlist (list): A sorted list of field names with their start and end ranges.

    Methods:
        field_start(fieldname): Get the start position of a field.
        fielded_ranges(pos=None, eod=None): Generate fielded ranges for the given position range.
        iter_terms(): Iterate over the terms in the fielded ordered hash data.
        iter_term_items(): Iterate over the term items in the fielded ordered hash data.
        contains_term(fieldname, btext): Check if a term exists in the fielded ordered hash data.
        range_for_term(fieldname, btext): Get the range (position and length) of a term in the fielded ordered hash data.
        term_data(fieldname, btext): Get the data associated with a term in the fielded ordered hash data.
        term_get(fieldname, btext, default=None): Get the data associated with a term, or a default value if the term does not exist.
        closest_term_pos(fieldname, key): Get the position of the closest term to the given key.
        closest_term(fieldname, btext): Get the closest term to the given term in the fielded ordered hash data.
        term_ranges_from(fieldname, btext): Generate term ranges starting from the given term in the fielded ordered hash data.
        terms_from(fieldname, btext): Iterate over the terms starting from the given term in the fielded ordered hash data.
        term_items_from(fieldname, btext): Iterate over the term items starting from the given term in the fielded ordered hash data.
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize the FileTables object.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Raises:
            None.

        Returns:
            None.

        Notes:
            This method initializes the FileTables object by calling the __init__ method of the HashReader class.
            It also sets the fieldmap attribute using the extras dictionary passed as a keyword argument.
            The fieldmap is a dictionary that maps field names to their corresponding start position, index position, and other information.
            The fieldlist attribute is then created as a sorted list of tuples, where each tuple contains the field name, start position, and index position.

        Usage:
            filetables = FileTables(*args, **kwargs)
        """
        HashReader.__init__(self, *args, **kwargs)
        self.fieldmap = self.extras["fieldmap"]
        # Make a sorted list of the field names with their start and end ranges
        self.fieldlist = []
        for fieldname in sorted(self.fieldmap.keys()):
            startpos, ixpos, _, __ = self.fieldmap[fieldname]
            self.fieldlist.append((fieldname, startpos, ixpos))

    def field_start(self, fieldname):
        """
        Returns the start position of the specified field in the file.

        Parameters:
            fieldname (str): The name of the field.

        Returns:
            int: The start position of the field in the file.

        Raises:
            KeyError: If the specified fieldname does not exist in the fieldmap.

        Example:
            >>> field_start('title')
            10

        Note:
            The start position of a field represents the byte offset in the file where the field's data begins.
            This method is used internally by the filetables module to retrieve the start position of a field.
        """
        return self.fieldmap[fieldname][0]

    def fielded_ranges(self, pos=None, eod=None):
        """
        Generator that yields field information for each key-value pair in the filetable.

        Args:
            pos (int, optional): The starting position to iterate from. Defaults to None.
            eod (int, optional): The ending position to iterate until. Defaults to None.

        Yields:
            tuple: A tuple containing the field name, key position, key length, data position, and data length.

        Raises:
            IndexError: If the starting position is out of range.

        Notes:
            - This method is used to iterate over the field information of each key-value pair in the filetable.
            - The field information includes the field name, key position, key length, data position, and data length.
            - If the starting position is not specified, the iteration starts from the beginning of the filetable.
            - If the ending position is not specified, the iteration continues until the end of the filetable.
            - If the starting position is out of range, an IndexError is raised.
        """
        flist = self.fieldlist
        fpos = 0
        fieldname, _, end = flist[fpos]
        for keypos, keylen, datapos, datalen in self._ranges(pos, eod):
            if keypos >= end:
                fpos += 1
                fieldname, _, end = flist[fpos]
            yield fieldname, keypos, keylen, datapos, datalen

    def iter_terms(self):
        """
        Iterates over the terms in the filetable.

        Yields tuples containing the field name and the term value for each term in the filetable.

        Returns:
            Iterator[tuple]: An iterator over the terms in the filetable.

        Notes:
            This method retrieves the terms from the filetable using the `get` method of the `dbfile` object.
            It iterates over the fielded ranges in the filetable and yields tuples containing the field name
            and the term value for each term.

        Example:
            >>> for fieldname, term in filetable.iter_terms():
            ...     print(fieldname, term)
        """
        get = self.dbfile.get
        for fieldname, keypos, keylen, _, _ in self.fielded_ranges():
            yield fieldname, get(keypos, keylen)

    def iter_term_items(self):
        """
        Iterates over the term items in the file table.

        Yields tuples containing the field name, key, and data for each term item.

        Parameters:
        - None

        Returns:
        - Generator: A generator that yields tuples of the form (fieldname, key, data).

        Example usage:
        ```
        for fieldname, key, data in iter_term_items():
            # Process the fieldname, key, and data
            ...
        ```
        """
        get = self.dbfile.get
        for item in self.fielded_ranges():
            fieldname, keypos, keylen, datapos, datalen = item
            yield fieldname, get(keypos, keylen), get(datapos, datalen)

    def contains_term(self, fieldname, btext):
        """
        Checks if the given term exists in the specified field.

        Parameters:
            fieldname (str): The name of the field to search in.
            btext (bytes): The term to search for, encoded as bytes.

        Returns:
            bool: True if the term exists in the field, False otherwise.

        Raises:
            KeyError: If the field or term does not exist.

        Example:
            >>> table = FileTables()
            >>> table.contains_term("title", b"example")
            True
        """
        try:
            _ = self.range_for_term(fieldname, btext)
            return True
        except KeyError:
            return False

    def range_for_term(self, fieldname, btext):
        """
        Returns the range (datapos, datalen) for a given term in a specific field.

        Args:
            fieldname (str): The name of the field.
            btext (bytes): The term to search for.

        Returns:
            tuple: A tuple containing the data position (datapos) and data length (datalen) for the term.

        Raises:
            KeyError: If the term is not found in the field.

        """
        start, ixpos, _, __ = self.fieldmap[fieldname]
        for datapos, datalen in self.ranges_for_key(btext):
            if start < datapos < ixpos:
                return datapos, datalen
        raise KeyError((fieldname, btext))

    def term_data(self, fieldname, btext):
        """
        Retrieve the data associated with a term in a specific field.

        Args:
            fieldname (str): The name of the field.
            btext (bytes): The term to retrieve the data for.

        Returns:
            bytes: The data associated with the term.

        Raises:
            KeyError: If the term or field does not exist.

        Notes:
            This method retrieves the data associated with a term in a specific field
            from the file database. It uses the `range_for_term` method to determine
            the position and length of the data in the database file, and then retrieves
            the data using the `get` method of the `dbfile` object.

            Example usage:
            ```
            fieldname = "title"
            term = b"example"
            data = term_data(fieldname, term)
            print(data)
            ```
        """
        datapos, datalen = self.range_for_term(fieldname, btext)
        return self.dbfile.get(datapos, datalen)

    def term_get(self, fieldname, btext, default=None):
        """
        Retrieve the term data for a given field and term text.

        Args:
            fieldname (str): The name of the field.
            btext (bytes): The term text in bytes.
            default: The value to return if the term data is not found.

        Returns:
            The term data for the given field and term text, or the default value if not found.
        """
        try:
            return self.term_data(fieldname, btext)
        except KeyError:
            return default

    def closest_term_pos(self, fieldname, key):
        """
        Given a key, return the position of that key OR the next highest key if the given key does not exist.

        Args:
            fieldname (str): The name of the field.
            key (bytes): The key to search for.

        Returns:
            int or None: The position of the key in the index array, or None if the key is not found.

        Raises:
            TypeError: If the key is not of type bytes.
            ValueError: If the index type is unknown.

        Note:
            This method assumes that the index array is sorted in ascending order.

        Example:
            >>> db = FileTables()
            >>> db.closest_term_pos("title", b"apple")
            10
        """
        if not isinstance(key, bytes):
            raise TypeError(f"Key {key!r} should be bytes")

        dbfile = self.dbfile
        key_at = self.key_at
        startpos, ixpos, ixsize, ixtype = self.fieldmap[fieldname]

        if ixtype == "B":
            get_pos = dbfile.get_byte
        elif ixtype == "H":
            get_pos = dbfile.get_ushort
        elif ixtype == "i":
            get_pos = dbfile.get_int
        elif ixtype == "I":
            get_pos = dbfile.get_uint
        elif ixtype == "q":
            get_pos = dbfile.get_long
        else:
            raise ValueError(f"Unknown index type {ixtype}")

        # Do a binary search of the positions in the index array
        lo = 0
        hi = ixsize
        while lo < hi:
            mid = (lo + hi) // 2
            midkey = key_at(startpos + get_pos(ixpos + mid * ixsize))
            if midkey < key:
                lo = mid + 1
            else:
                hi = mid

        # If we went off the end, return None
        if lo == ixsize:
            return None
        # Return the closest key
        return startpos + get_pos(ixpos + lo * ixsize)

    def closest_term(self, fieldname, btext):
        """
        Returns the closest term to the given text in the specified field.

        Args:
            fieldname (str): The name of the field to search in.
            btext (bytes): The text to find the closest term for.

        Returns:
            str or None: The closest term to the given text in the specified field,
            or None if no term is found.

        """
        pos = self.closest_term_pos(fieldname, btext)
        if pos is None:
            return None
        return self.key_at(pos)

    def term_ranges_from(self, fieldname, btext):
        """
        Returns a generator that yields term ranges for a given field and binary text.

        Args:
            fieldname (str): The name of the field.
            btext (bytes): The binary text to search for.

        Yields:
            tuple: A tuple representing a term range. Each tuple contains two integers,
                   representing the start and end positions of the term in the index.

        Returns None if no term is found for the given field and binary text.
        """

        pos = self.closest_term_pos(fieldname, btext)
        if pos is None:
            return

        _, ixpos, __, ___ = self.fieldmap[fieldname]
        yield from self._ranges(pos, ixpos)

    def terms_from(self, fieldname, btext):
        """
        Retrieves terms from the specified field that match the given binary text.

        Args:
            fieldname (str): The name of the field to retrieve terms from.
            btext (bytes): The binary text to match against the terms.

        Yields:
            bytes: The terms that match the given binary text.

        """
        dbfile = self.dbfile
        for keypos, keylen, _, _ in self.term_ranges_from(fieldname, btext):
            yield dbfile.get(keypos, keylen)

    def term_items_from(self, fieldname, btext):
        """
        Retrieves term items from the file database for a given field and binary text.

        Args:
            fieldname (str): The name of the field to retrieve term items from.
            btext (bytes): The binary text to match against.

        Yields:
            tuple: A tuple containing the key and data associated with each term item.

        Returns:
            None

        Raises:
            None

        Example:
            >>> for key, data in term_items_from("title", b"example"):
            ...     print(key, data)
        """
        dbfile = self.dbfile
        for item in self.term_ranges_from(fieldname, btext):
            keypos, keylen, datapos, datalen = item
            yield (dbfile.get(keypos, keylen), dbfile.get(datapos, datalen))
