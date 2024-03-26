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

import struct
import sys
from array import array
from binascii import crc32
from collections import defaultdict
from decimal import Decimal
from hashlib import md5  # type: ignore @UnresolvedImport
from pickle import dumps, loads
from struct import Struct

from iniconfig import ParseError

from whoosh.qparser.dateparse import DateParseError

try:
    import zlib
except ImportError:
    zlib = None

from whoosh.automata.fst import GraphReader, GraphWriter
from whoosh.codec import base
from whoosh.filedb.filestore import Storage
from whoosh.matching import LeafMatcher, ListMatcher, ReadTooFar
from whoosh.reading import NoGraphError, TermInfo, TermNotFound
from whoosh.system import (
    _FLOAT_SIZE,
    _INT_SIZE,
    _LONG_SIZE,
    IS_LITTLE,
    emptybytes,
    pack_byte,
    pack_long,
    pack_ushort,
    unpack_long,
    unpack_ushort,
)
from whoosh.util.numeric import (
    NaN,
    byte_to_length,
    from_sortable,
    length_to_byte,
    to_sortable,
)
from whoosh.util.numlists import GrowableArray
from whoosh.util.text import utf8decode, utf8encode
from whoosh.util.times import datetime_to_long, long_to_datetime

# Old hash file implementations

_4GB = 4 * 1024 * 1024 * 1024


def cdb_hash(key):
    """
    Calculate the hash value for a given key using the CDB hash algorithm.

    Args:
        key (str): The key to calculate the hash value for.

    Returns:
        int: The calculated hash value.

    Algorithm:
        The CDB hash algorithm is a simple and efficient hash function.
        It uses the following steps to calculate the hash value:
        1. Initialize the hash value to 5381.
        2. For each character in the key, update the hash value using the formula:
           h = (h + (h << 5)) & 0xFFFFFFFF ^ ord(c)
        3. Return the final hash value.

    Example:
        >>> cdb_hash("hello")
        1934859637
    """
    h = 5381
    for c in key:
        h = (h + (h << 5)) & 0xFFFFFFFF ^ ord(c)
    return h


def md5_hash(key):
    """
    Calculate the MD5 hash of the given key and return the hash value as an integer.

    Parameters:
    key (str): The key to be hashed.

    Returns:
    int: The MD5 hash value of the key as an integer.

    Example:
    >>> md5_hash("hello")
    1234567890

    Note:
    This function uses the MD5 algorithm to calculate the hash value of the key.
    The resulting hash value is converted to an integer and returned.
    """
    return int(md5(key).hexdigest(), 16) & 0xFFFFFFFF


def crc_hash(key):
    """
    Calculates the CRC hash value for the given key.

    Args:
        key (bytes): The key to calculate the CRC hash for.

    Returns:
        int: The CRC hash value.

    """
    return crc32(key) & 0xFFFFFFFF


hash_functions = (hash, cdb_hash, md5_hash, crc_hash)

_header_entry_struct = struct.Struct("!qI")  # Position, number of slots
header_entry_size = _header_entry_struct.size
pack_header_entry = _header_entry_struct.pack
unpack_header_entry = _header_entry_struct.unpack

_lengths_struct = struct.Struct("!II")  # Length of key, length of data
lengths_size = _lengths_struct.size
pack_lengths = _lengths_struct.pack
unpack_lengths = _lengths_struct.unpack

_pointer_struct = struct.Struct("!Iq")  # Hash value, position
pointer_size = _pointer_struct.size
pack_pointer = _pointer_struct.pack
unpack_pointer = _pointer_struct.unpack


# Table classes


class HashWriter:
    """
    A class for writing hash-based data to a file.

    Parameters:
    - dbfile (file-like object): The file-like object to write the hash data to.
    - hashtype (int, optional): The type of hashing function to use. Defaults to 2.

    Attributes:
    - dbfile (file-like object): The file-like object to write the hash data to.
    - hashtype (int): The type of hashing function used.
    - extras (dict): Extra data associated with the hash data.
    - startoffset (int): The starting offset in the file where the hash data is written.
    - header_size (int): The size of the header in bytes.
    - hash_func (function): The hashing function used.
    - hashes (defaultdict): A dictionary of hashed values.

    Methods:
    - add(key, value): Adds a key-value pair to the hash data.
    - add_all(items): Adds multiple key-value pairs to the hash data.
    - _write_hashes(): Writes the hash data to the file.
    - _write_extras(): Writes the extra data to the file.
    - _write_directory(): Writes the directory of hash values to the file.
    - close(): Closes the file.

    """

    def __init__(self, dbfile, hashtype=2):
        """
        Initialize a Whoosh2 codec object.

        Args:
            dbfile (file-like object): The file-like object representing the database file.
            hashtype (int, optional): The type of hashing function to be used. Defaults to 2.

        Attributes:
            dbfile (file-like object): The file-like object representing the database file.
            hashtype (int): The type of hashing function used.
            extras (dict): A dictionary to store additional data.
            startoffset (int): The starting offset in the database file.
            header_size (int): The size of the header in bytes.
            hash_func (function): The hashing function used.
            hashes (defaultdict): A dictionary to store the directory of hashed values.
        """
        self.dbfile = dbfile
        self.hashtype = hashtype
        self.extras = {}

        self.startoffset = dbfile.tell()
        dbfile.write(b"HASH")  # Magic tag
        dbfile.write_byte(self.hashtype)  # Identify hashing function used
        dbfile.write(b"\x00\x00\x00")  # Unused bytes
        dbfile.write_long(0)  # Pointer to end of hashes

        self.header_size = 16 + 256 * header_entry_size
        self.hash_func = hash_functions[self.hashtype]

        # Seek past the first "header_size" bytes of the file... we'll come
        # back here to write the header later
        dbfile.seek(self.header_size)
        # Store the directory of hashed values
        self.hashes = defaultdict(list)

    def add(self, key, value):
        """
        Adds a key-value pair to the hash data.

        Parameters:
        - key (bytes): The key to be hashed.
        - value (bytes): The value associated with the key.

        Returns:
        None

        Raises:
        AssertionError: If the key or value is not of type bytes.

        Notes:
        - This method writes the length of the key and value to the database file, followed by the key and value themselves.
        - The key is hashed using the hash function specified during initialization.
        - The hashed key and the position in the database file where the key-value pair is written are stored in a list for efficient retrieval.

        Usage:
        ```
        db = HashDatabase()
        key = b'my_key'
        value = b'my_value'
        db.add(key, value)
        ```
        """
        assert isinstance(key, bytes)
        assert isinstance(value, bytes)

        dbfile = self.dbfile
        pos = dbfile.tell()
        dbfile.write(pack_lengths(len(key), len(value)))
        dbfile.write(key)
        dbfile.write(value)

        h = self.hash_func(key)
        self.hashes[h & 255].append((h, pos))

    def add_all(self, items):
        """
        Adds multiple key-value pairs to the hash data.

        Parameters:
        - items (iterable): An iterable of (key, value) pairs.

        Usage:
        - To add multiple key-value pairs to the hash data, pass an iterable of (key, value) pairs to the `add_all` method.

        Example:
        >>> data = [('key1', 'value1'), ('key2', 'value2'), ('key3', 'value3')]
        >>> hash_data.add_all(data)

        """
        add = self.add
        for key, value in items:
            add(key, value)

    def _write_hashes(self):
        """
        Writes the hash data to the file.

        This method writes the hash data to the file, which is used for efficient
        lookup of terms in the index. It generates a directory of positions and
        number of slots for each hash value, and then writes the hash table entries
        to the file.

        The hash table entries are stored in a list of tuples, where each tuple
        contains the hash value and the position of the term in the index file.

        Usage:
            _write_hashes()

        Returns:
            None
        """
        dbfile = self.dbfile
        hashes = self.hashes
        directory = self.directory = []

        pos = dbfile.tell()
        for i in range(0, 256):
            entries = hashes[i]
            numslots = 2 * len(entries)
            directory.append((pos, numslots))

            null = (0, 0)
            hashtable = [null] * numslots
            for hashval, position in entries:
                n = (hashval >> 8) % numslots
                while hashtable[n] != null:
                    n = (n + 1) % numslots
                hashtable[n] = (hashval, position)

            write = dbfile.write
            for hashval, position in hashtable:
                write(pack_pointer(hashval, position))
                pos += pointer_size

        dbfile.flush()
        self.extrasoffset = dbfile.tell()

    def _write_extras(self):
        """
        Writes the extra data to the file.

        This method is responsible for writing the extra data to the file.
        It first serializes the extras object using pickle and writes it to the file.
        Then, it seeks back to the start offset + 8 and writes the pointer to the extras.

        Note: The extras object must be serializable using pickle.

        """
        self.dbfile.write_pickle(self.extras)
        # Seek back and write the pointer to the extras
        self.dbfile.flush()
        self.dbfile.seek(self.startoffset + 8)
        self.dbfile.write_long(self.extrasoffset)

    def _write_directory(self):
        """
        Writes the directory of hash values to the file.

        This method is responsible for writing the directory of hash values to the file.
        It seeks back to the header, writes the pointer to the end of the hashes,
        and writes the pointers to the hash tables.

        Note:
            This method assumes that the file has already been opened and positioned
            correctly at the start offset.

        """
        dbfile = self.dbfile
        directory = self.directory

        # Seek back to the header
        dbfile.seek(self.startoffset + 8)
        # Write the pointer to the end of the hashes
        dbfile.write_long(self.extrasoffset)
        # Write the pointers to the hash tables
        for position, numslots in directory:
            dbfile.write(pack_header_entry(position, numslots))

        dbfile.flush()
        assert dbfile.tell() == self.header_size

    def close(self):
        """
        Closes the file.

        This method is responsible for closing the file and performing any necessary cleanup operations.
        It writes the hashes, extras, and directory to the file, and then closes the file object.

        Note:
        - After calling this method, the file object should no longer be used.

        """
        self._write_hashes()
        self._write_extras()
        self._write_directory()
        self.dbfile.close()


class HashReader:
    """
    A class for reading and accessing data from a hash-based file format.

    Args:
        dbfile (file-like object): The file-like object representing the hash-based file.
        startoffset (int, optional): The starting offset in the file. Defaults to 0.

    Raises:
        ValueError: If the file header is unknown.

    Attributes:
        dbfile (file-like object): The file-like object representing the hash-based file.
        startoffset (int): The starting offset in the file.
        is_closed (bool): Indicates whether the HashReader is closed or not.

    """

    def __init__(self, dbfile, startoffset=0):
        """
        Initialize a Whoosh2 object.

        Args:
            dbfile (file-like object): The file-like object representing the Whoosh2 database file.
            startoffset (int, optional): The starting offset in the file. Defaults to 0.
        """
        self.dbfile = dbfile
        self.startoffset = startoffset
        self.is_closed = False

        dbfile.seek(startoffset)
        # Check magic tag
        magic = dbfile.read(4)
        if magic != b"HASH":
            raise ValueError(f"Unknown file header {magic}")

        self.hashtype = dbfile.read_byte()  # Hash function type
        self.hash_func = hash_functions[self.hashtype]

        dbfile.read(3)  # Unused
        self.extrasoffset = dbfile.read_long()  # Pointer to end of hashes

        self.header_size = 16 + 256 * header_entry_size
        assert self.extrasoffset >= self.header_size

        # Read pointers to hash tables
        self.buckets = []
        for _ in range(256):
            he = unpack_header_entry(dbfile.read(header_entry_size))
            self.buckets.append(he)
        self._start_of_hashes = self.buckets[0][0]

        dbfile.seek(self.extrasoffset)
        self._read_extras()

    def _read_extras(self):
        """
        Read the extras section of the hash-based file.

        This method reads the extras section of the hash-based file and stores the
        data in the `extras` attribute of the object. The extras section contains
        additional metadata or auxiliary information associated with the file.

        Raises:
            EOFError: If the end of the file is reached before reading the extras.
        """
        try:
            self.extras = self.dbfile.read_pickle()
        except EOFError:
            self.extras = {}

    def close(self):
        """
        Close the HashReader.

        This method closes the HashReader and releases any resources held by it. Once closed,
        the HashReader cannot be used again.

        Raises:
            ValueError: If the HashReader is already closed.
        """
        if self.is_closed:
            raise ValueError(f"Tried to close {self} twice")
        self.dbfile.close()
        self.is_closed = True

    def read(self, position, length):
        """
        Read data from the hash-based file.

        Args:
            position (int): The position in the file to start reading from.
            length (int): The number of bytes to read.

        Returns:
            bytes: The read data.

        Raises:
            OSError: If there is an error reading the file.

        Notes:
            This method reads data from the hash-based file at the specified position and with the specified length.
            It is used to retrieve data from the file.
        """
        self.dbfile.seek(position)
        return self.dbfile.read(length)

    def _ranges(self, pos=None):
        """
        Generate ranges of key-value pairs in the hash-based file.

        Args:
            pos (int, optional): The starting position in the file. Defaults to None.

        Yields:
            tuple: A tuple containing the key position, key length, data position, and data length.

        Raises:
            ValueError: If the starting position is beyond the end of the file.

        Notes:
            This method is used to iterate over the key-value pairs stored in the hash-based file.
            It generates tuples containing the position and length of the key, as well as the position
            and length of the corresponding data.

            The `pos` parameter allows you to specify a starting position in the file. If `pos` is not
            provided, the method will start from the beginning of the file.

            The method uses the `read` method to read data from the file. The `read` method should be
            implemented by the subclass to read the specified number of bytes from the file at the given
            position.

            The method calculates the key position, key length, data position, and data length based on
            the lengths stored in the file. It then updates the position to point to the next key-value
            pair in the file.

            The method yields each tuple of key-value pair ranges, allowing you to process them one by one.
            The caller can iterate over the yielded tuples using a for loop or any other iterable method.

            If the starting position is beyond the end of the file, a `ValueError` is raised.

        """
        if pos is None:
            pos = self.header_size
        eod = self._start_of_hashes
        read = self.read
        while pos < eod:
            keylen, datalen = unpack_lengths(read(pos, lengths_size))
            keypos = pos + lengths_size
            datapos = pos + lengths_size + keylen
            pos = datapos + datalen
            yield (keypos, keylen, datapos, datalen)

    def __iter__(self):
        """
        Iterate over the key-value pairs in the hash-based file.

        This method returns an iterator that allows iterating over the key-value pairs
        stored in the hash-based file. Each iteration yields a tuple containing the key
        and value.

        Returns:
            iterator: An iterator over the key-value pairs in the hash-based file.

        Example:
            >>> for key, value in hash_file:
            ...     print(key, value)
        """
        return iter(self.items())

    def items(self):
        """
        Iterate over the key-value pairs in the hash-based file.

        Yields:
            tuple: A tuple containing the key and value.

        """
        read = self.read
        for keypos, keylen, datapos, datalen in self._ranges():
            key = read(keypos, keylen)
            value = read(datapos, datalen)
            yield (key, value)

    def keys(self):
        """
        Iterate over the keys in the hash-based file.

        This method returns an iterator that yields the keys stored in the hash-based file.
        The keys are returned as bytes.

        Yields:
            bytes: The key.
        """
        read = self.read
        for keypos, keylen, _, _ in self._ranges():
            yield read(keypos, keylen)

    def values(self):
        """
        Iterate over the values in the hash-based file.

        This method returns a generator that iterates over the values stored in the hash-based file.
        Each value is read from the file using the `read` method.

        Yields:
            bytes: The value.
        """
        read = self.read
        for _, _, datapos, datalen in self._ranges():
            yield read(datapos, datalen)

    def __getitem__(self, key):
        """
        Get the value associated with the given key.

        Args:
            key (bytes): The key to retrieve the value for.

        Returns:
            bytes: The value associated with the key.

        Raises:
            KeyError: If the key is not found.
        """
        for data in self.all(key):
            return data
        raise KeyError(key)

    def get(self, key, default=None):
        """
        Get the value associated with the given key, or a default value if the key is not found.

        Args:
            key (bytes): The key to retrieve the value for.
            default (Any, optional): The default value to return if the key is not found. Defaults to None.

        Returns:
            bytes: The value associated with the key, or the default value if the key is not found.
        """
        for data in self.all(key):
            return data
        return default

    def all(self, key):
        """
        Get all values associated with the given key.

        Args:
            key (bytes): The key to retrieve the values for.

        Yields:
            bytes: The values associated with the key.
        """
        read = self.read
        for datapos, datalen in self.ranges_for_key(key):
            yield read(datapos, datalen)

    def __contains__(self, key):
        """
        Check if the given key is present in the hash-based file.

        Args:
            key (bytes): The key to check.

        Returns:
            bool: True if the key is present, False otherwise.
        """
        for _ in self.ranges_for_key(key):
            return True
        return False

    def _hashtable_info(self, keyhash):
        """
        Get the directory position and number of hash entries for the given key hash.

        Args:
            keyhash (int): The hash value of the key.

        Returns:
            tuple: A tuple containing the directory position and number of hash entries.
        """
        # Return (directory_position, number_of_hash_entries)
        return self.buckets[keyhash & 255]

    def _key_position(self, key):
        """
        Get the position of the given key in the hash-based file.

        Args:
            key (bytes): The key to get the position for.

        Returns:
            int: The position of the key.

        Raises:
            KeyError: If the key is not found.
        """
        keyhash = self.hash_func(key)
        hpos, hslots = self._hashtable_info(keyhash)
        if not hslots:
            raise KeyError(key)
        slotpos = hpos + (((keyhash >> 8) % hslots) * header_entry_size)

        return self.dbfile.get_long(slotpos + _INT_SIZE)

    def _key_at(self, pos):
        """
        Get the key at the given position in the hash-based file.

        Args:
            pos (int): The position of the key.

        Returns:
            bytes: The key.
        """
        keylen = self.dbfile.get_uint(pos)
        return self.read(pos + lengths_size, keylen)

    def ranges_for_key(self, key):
        """
        Get the ranges of data associated with the given key.

        Args:
            key (bytes): The key to retrieve the ranges for.

        Yields:
            tuple: A tuple containing the data position and data length.
        """
        read = self.read
        if not isinstance(key, bytes):
            raise TypeError(f"Key {key} should be bytes")
        keyhash = self.hash_func(key)
        hpos, hslots = self._hashtable_info(keyhash)
        if not hslots:
            return

        slotpos = hpos + (((keyhash >> 8) % hslots) * pointer_size)
        for _ in range(hslots):
            slothash, pos = unpack_pointer(read(slotpos, pointer_size))
            if not pos:
                return

            slotpos += pointer_size
            # If we reach the end of the hashtable, wrap around
            if slotpos == hpos + (hslots * pointer_size):
                slotpos = hpos

            if slothash == keyhash:
                keylen, datalen = unpack_lengths(read(pos, lengths_size))
                if keylen == len(key):
                    if key == read(pos + lengths_size, keylen):
                        yield (pos + lengths_size + keylen, datalen)

    def range_for_key(self, key):
        """
        Get the first range of data associated with the given key.

        Args:
            key (bytes): The key to retrieve the range for.

        Returns:
            tuple: A tuple containing the data position and data length.

        Raises:
            KeyError: If the key is not found.
        """
        for item in self.ranges_for_key(key):
            return item
        raise KeyError(key)


class OrderedHashWriter(HashWriter):
    """
    A class for writing key-value pairs to a hash-based database file with ordered keys.

    Inherits from HashWriter.

    Usage:
    writer = OrderedHashWriter(dbfile)
    writer.add(key, value)
    writer.commit()
    """

    def __init__(self, dbfile):
        """
        Initializes an OrderedHashWriter object.

        Parameters:
        - dbfile (file): The file object representing the hash-based database file.
        """
        HashWriter.__init__(self, dbfile)
        self.index = GrowableArray("H")
        self.lastkey = emptybytes

    def add(self, key, value):
        """
        Adds a key-value pair to the database.

        Parameters:
        - key: The key to be added.
        - value: The value associated with the key.

        Raises:
        - ValueError: If the keys are not in increasing order.
        """
        if key <= self.lastkey:
            raise ValueError(f"Keys must increase: {self.lastkey!r}..{key!r}")
        self.index.append(self.dbfile.tell())
        HashWriter.add(self, key, value)
        self.lastkey = key

    def _write_extras(self):
        """
        Writes additional information about the index to the extras section of the database file.
        """
        dbfile = self.dbfile

        # Save information about the index in the extras
        ndxarray = self.index
        self.extras["indexbase"] = dbfile.tell()
        self.extras["indextype"] = ndxarray.typecode
        self.extras["indexlen"] = len(ndxarray)
        # Write key index
        ndxarray.to_file(dbfile)

        # Call the super method to write the extras
        self.extrasoffset = dbfile.tell()
        HashWriter._write_extras(self)


class OrderedHashReader(HashReader):
    """
    A class for reading ordered hash data from a database file.

    Inherits from HashReader.

    Attributes:
        indexbase (int): The base position of the index in the database file.
        indexlen (int): The length of the index.
        indextype (str): The type of the index.
        _ixsize (int): The size of each index entry in bytes.
        _ixpos (function): A function for reading index values based on the indextype.

    Methods:
        closest_key(key): Returns the closest key to the given key in the hash data.
        items_from(key): Yields key-value pairs starting from the given key.
        keys_from(key): Yields keys starting from the given key.
    """

    def __init__(self, dbfile):
        """
        Initializes an OrderedHashReader object.

        Args:
            dbfile (file): The database file to read from.
        """
        HashReader.__init__(self, dbfile)
        self.indexbase = self.extras["indexbase"]
        self.indexlen = self.extras["indexlen"]

        self.indextype = indextype = self.extras["indextype"]
        self._ixsize = struct.calcsize(indextype)
        if indextype == "B":
            self._ixpos = dbfile.get_byte
        elif indextype == "H":
            self._ixpos = dbfile.get_ushort
        elif indextype == "i":
            self._ixpos = dbfile.get_int
        elif indextype == "I":
            self._ixpos = dbfile.get_uint
        elif indextype == "q":
            self._ixpos = dbfile.get_long
        else:
            raise ValueError(f"Unknown index type {indextype}")

    def _closest_key(self, key):
        """
        Finds the closest key to the given key in the hash data.

        Args:
            key (bytes): The key to search for.

        Returns:
            int or None: The position of the closest key in the hash data, or None if not found.
        """
        key_at = self._key_at
        indexbase = self.indexbase
        ixpos, ixsize = self._ixpos, self._ixsize

        lo = 0
        hi = self.indexlen
        if not isinstance(key, bytes):
            raise TypeError(f"Key {key} should be bytes")
        while lo < hi:
            mid = (lo + hi) // 2
            midkey = key_at(ixpos(indexbase + mid * ixsize))
            if midkey < key:
                lo = mid + 1
            else:
                hi = mid
        # i = max(0, mid - 1)
        if lo == self.indexlen:
            return None
        return ixpos(indexbase + lo * ixsize)

    def closest_key(self, key):
        """
        Returns the closest key to the given key in the hash data.

        Args:
            key (bytes): The key to search for.

        Returns:
            bytes or None: The closest key to the given key, or None if not found.
        """
        pos = self._closest_key(key)
        if pos is None:
            return None
        return self._key_at(pos)

    def _ranges_from(self, key):
        """
        Generates ranges of key-value pairs starting from the given key.

        Args:
            key (bytes): The key to start from.

        Yields:
            tuple: A tuple containing the key position, key length, data position, and data length.
        """
        pos = self._closest_key(key)
        if pos is None:
            return

        yield from self._ranges(pos=pos)

    def items_from(self, key):
        """
        Yields key-value pairs starting from the given key.

        Args:
            key (bytes): The key to start from.

        Yields:
            tuple: A tuple containing the key and value.
        """
        read = self.read
        for keypos, keylen, datapos, datalen in self._ranges_from(key):
            yield (read(keypos, keylen), read(datapos, datalen))

    def keys_from(self, key):
        """
        Yields keys starting from the given key.

        Args:
            key (bytes): The key to start from.

        Yields:
            bytes: The key.
        """
        read = self.read
        for keypos, keylen, _, _ in self._ranges_from(key):
            yield read(keypos, keylen)


# Standard codec top-level object


class W2Codec(base.Codec):
    """
    Codec implementation for the Whoosh 2 index format.

    This codec provides the necessary methods for reading and writing
    various components of the index, such as term index, term postings,
    spelling graph, field lengths, vector index, vector postings, and
    stored fields.

    Args:
        blocklimit (int): The maximum number of terms to store in a block.
        compression (int): The level of compression to apply to the index data.
        loadlengths (bool): Whether to load field lengths during reading.
        inlinelimit (int): The maximum number of terms to store in a field block.

    Attributes:
        TERMS_EXT (str): The file extension for the term index.
        POSTS_EXT (str): The file extension for the term postings.
        DAWG_EXT (str): The file extension for the spelling graph.
        LENGTHS_EXT (str): The file extension for the field lengths.
        VECTOR_EXT (str): The file extension for the vector index.
        VPOSTS_EXT (str): The file extension for the vector postings.
        STORED_EXT (str): The file extension for the stored fields.

    """

    TERMS_EXT = ".trm"  # Term index
    POSTS_EXT = ".pst"  # Term postings
    DAWG_EXT = FST_EXT = ".dag"  # Spelling graph file
    LENGTHS_EXT = ".fln"  # Field lengths file
    VECTOR_EXT = ".vec"  # Vector index
    VPOSTS_EXT = ".vps"  # Vector postings
    STORED_EXT = ".sto"  # Stored fields file

    def __init__(self, blocklimit=128, compression=3, loadlengths=False, inlinelimit=1):
        """
        Initialize the W2Codec.

        Args:
            blocklimit (int): The maximum number of terms to store in a block.
            compression (int): The level of compression to apply to the index data.
            loadlengths (bool): Whether to load field lengths during reading.
            inlinelimit (int): The maximum number of terms to store in a field block.
        """
        self.blocklimit = blocklimit
        self.compression = compression
        self.loadlengths = loadlengths
        self.inlinelimit = inlinelimit

    def per_document_writer(self, storage, segment):
        """
        Create a per-document value writer.

        Args:
            storage: The storage object for the index.
            segment: The segment object for the index.

        Returns:
            W2PerDocWriter: The per-document value writer.
        """
        return W2PerDocWriter(
            storage, segment, blocklimit=self.blocklimit, compression=self.compression
        )

    def field_writer(self, storage, segment):
        """
        Create an inverted index writer.

        Args:
            storage: The storage object for the index.
            segment: The segment object for the index.

        Returns:
            W2FieldWriter: The inverted index writer.
        """
        return W2FieldWriter(
            storage,
            segment,
            blocklimit=self.blocklimit,
            compression=self.compression,
            inlinelimit=self.inlinelimit,
        )

    def terms_reader(self, storage, segment):
        """
        Create a terms reader.

        Args:
            storage: The storage object for the index.
            segment: The segment object for the index.

        Returns:
            W2TermsReader: The terms reader.
        """
        tifile = segment.open_file(storage, self.TERMS_EXT)
        postfile = segment.open_file(storage, self.POSTS_EXT)
        return W2TermsReader(tifile, postfile)

    def per_document_reader(self, storage, segment):
        """
        Create a per-document reader.

        Args:
            storage: The storage object for the index.
            segment: The segment object for the index.

        Returns:
            W2PerDocReader: The per-document reader.
        """
        return W2PerDocReader(storage, segment)

    def graph_reader(self, storage, segment):
        """
        Create a graph reader.

        Args:
            storage: The storage object for the index.
            segment: The segment object for the index.

        Returns:
            GraphReader: The graph reader.

        Raises:
            NoGraphError: If the spelling graph file is not found.
        """
        try:
            dawgfile = segment.open_file(storage, self.DAWG_EXT)
        except ValueError:
            raise NoGraphError
        return GraphReader(dawgfile)

    def new_segment(self, storage, indexname):
        """
        Create a new segment.

        Args:
            storage: The storage object for the index.
            indexname (str): The name of the index.

        Returns:
            W2Segment: The new segment.
        """
        return W2Segment(indexname)


# Per-document value writer


class W2PerDocWriter(base.PerDocumentWriter):
    """A class for writing per-document data in the Whoosh 2 codec.

    Args:
        storage (Storage): The storage object to use for creating files.
        segment (Segment): The segment object representing the current segment.
        blocklimit (int, optional): The maximum number of vector items to store in a block. Defaults to 128.
        compression (int, optional): The compression level to use when writing vector blocks. Defaults to 3.

    Attributes:
        storage (Storage): The storage object used for creating files.
        segment (Segment): The segment object representing the current segment.
        blocklimit (int): The maximum number of vector items to store in a block.
        compression (int): The compression level used when writing vector blocks.
        doccount (int): The total number of documents written.
        is_closed (bool): Indicates whether the writer has been closed.

    Note:
        This class is used internally by the Whoosh 2 codec and should not be instantiated directly.

    """

    def __init__(self, storage, segment, blocklimit=128, compression=3):
        if not isinstance(blocklimit, int):
            raise ValueError("blocklimit must be an integer")
        self.storage = storage
        self.segment = segment
        self.blocklimit = blocklimit
        self.compression = compression
        self.doccount = 0
        self.is_closed = False

        sffile = segment.create_file(storage, W2Codec.STORED_EXT)
        self.stored = StoredFieldWriter(sffile)
        self.storedfields = None

        self.lengths = InMemoryLengths()

        # We'll wait to create the vector files until someone actually tries
        # to add a vector
        self.vindex = self.vpostfile = None

    def _make_vector_files(self):
        """Create the vector index and vector postings files."""
        vifile = self.segment.create_file(self.storage, W2Codec.VECTOR_EXT)
        self.vindex = VectorWriter(vifile)
        self.vpostfile = self.segment.create_file(self.storage, W2Codec.VPOSTS_EXT)

    def start_doc(self, docnum):
        """Start writing a new document.

        Args:
            docnum (int): The document number.

        """
        self.docnum = docnum
        self.storedfields = {}
        self.doccount = max(self.doccount, docnum + 1)

    def add_field(self, fieldname, fieldobj, value, length):
        """Add a field to the current document.

        Args:
            fieldname (str): The name of the field.
            fieldobj (Field): The field object.
            value (object): The field value.
            length (int): The length of the field value.

        """
        if length:
            self.lengths.add(self.docnum, fieldname, length)
        if value is not None:
            self.storedfields[fieldname] = value

    def _new_block(self, vformat):
        """Create a new vector block.

        Args:
            vformat (Format): The vector format.

        Returns:
            W2Block: The new vector block.

        """
        postingsize = vformat.posting_size
        return W2Block(postingsize, stringids=True)

    def add_vector_items(self, fieldname, fieldobj, items):
        """Add vector items to the current document.

        Args:
            fieldname (str): The name of the vector field.
            fieldobj (Field): The vector field object.
            items (list): A list of vector items in the format (text, weight, value_bytes).

        """
        if self.vindex is None:
            self._make_vector_files()

        postfile = self.vpostfile
        blocklimit = self.blocklimit
        block = self._new_block(fieldobj.vector)

        startoffset = postfile.tell()
        postfile.write(block.magic)  # Magic number
        blockcount = 0
        postfile.write_uint(0)  # Placeholder for block count

        countdown = blocklimit
        for text, weight, valuestring in items:
            block.add(text, weight, valuestring)
            countdown -= 1
            if countdown == 0:
                block.to_file(postfile, compression=self.compression)
                block = self._new_block(fieldobj.vector)
                blockcount += 1
                countdown = blocklimit
        # If there are leftover items in the current block, write them out
        if block:
            block.to_file(postfile, compression=self.compression)
            blockcount += 1

        # Seek back to the start of this list of posting blocks and write the
        # number of blocks
        postfile.flush()
        here = postfile.tell()
        postfile.seek(startoffset + 4)
        postfile.write_uint(blockcount)
        postfile.seek(here)

        # Add to the index
        self.vindex.add((self.docnum, fieldname), startoffset)

    def finish_doc(self):
        """Finish writing the current document."""
        self.stored.add(self.storedfields)
        self.storedfields = None

    def close(self):
        """Close the writer."""
        if self.storedfields is not None:
            self.stored.add(self.storedfields)
        self.stored.close()
        flfile = self.segment.create_file(self.storage, W2Codec.LENGTHS_EXT)
        self.lengths.to_file(flfile, self.doccount)
        if self.vindex:
            self.vindex.close()
            self.vpostfile.close()
        self.is_closed = True


# Inverted index writer


class W2FieldWriter(base.FieldWriter):
    """
    The W2FieldWriter class is responsible for writing field data to the index files in the Whoosh search engine.

    Parameters:
    - storage (Storage): The storage object used to store the index files.
    - segment (base.Segment): The segment object representing the current segment being written.
    - blocklimit (int): The maximum number of documents to store in a single block.
    - compression (int): The level of compression to apply to the block data.
    - inlinelimit (int): The maximum number of documents to store inline without creating a separate block.

    Attributes:
    - storage (Storage): The storage object used to store the index files.
    - segment (base.Segment): The segment object representing the current segment being written.
    - fieldname (str): The name of the field being written.
    - text (str): The text of the current term being written.
    - field (Field): The field object being written.
    - format (Format): The format object associated with the field.
    - spelling (bool): Indicates whether the field has spelling enabled.
    - termsindex (TermIndexWriter): The term index writer object.
    - postfile (File): The file object for storing the posting data.
    - dawg (GraphWriter): The DAWG (Directed Acyclic Word Graph) writer object.
    - blocklimit (int): The maximum number of documents to store in a single block.
    - compression (int): The level of compression to apply to the block data.
    - inlinelimit (int): The maximum number of documents to store inline without creating a separate block.
    - block (W2Block): The current block being written.
    - terminfo (FileTermInfo): The term info object for the current term.
    - _infield (bool): Indicates whether the writer is currently inside a field.
    - is_closed (bool): Indicates whether the writer has been closed.

    Methods:
    - _make_dawg_files(): Creates the DAWG (Directed Acyclic Word Graph) files if needed.
    - _new_block(): Creates a new block object.
    - _reset_block(): Resets the current block.
    - _write_block(): Writes the current block to the posting file.
    - _start_blocklist(): Starts a new block list in the posting file.
    - start_field(fieldname, fieldobj): Starts writing a new field.
    - start_term(text): Starts writing a new term.
    - add(docnum, weight, valuestring, length): Adds a document to the current block.
    - add_spell_word(fieldname, text): Adds a spelling word to the DAWG.
    - finish_term(): Finishes writing the current term.
    - finish_field(): Finishes writing the current field.
    - close(): Closes the writer and releases any resources.
    """

    def __init__(self, storage, segment, blocklimit=128, compression=3, inlinelimit=1):
        """
        Initializes a new instance of the W2FieldWriter class.

        Parameters:
        - storage (Storage): The storage object used to store the index files.
        - segment (base.Segment): The segment object representing the current segment being written.
        - blocklimit (int): The maximum number of documents to store in a single block.
        - compression (int): The level of compression to apply to the block data.
        - inlinelimit (int): The maximum number of documents to store inline without creating a separate block.

        Raises:
        - AssertionError: If the input parameters are not of the expected types.
        """
        assert isinstance(storage, Storage)
        assert isinstance(segment, base.Segment)
        assert isinstance(blocklimit, int)
        assert isinstance(compression, int)
        assert isinstance(inlinelimit, int)

        self.storage = storage
        self.segment = segment
        self.fieldname = None
        self.text = None
        self.field = None
        self.format = None
        self.spelling = False

        tifile = segment.create_file(storage, W2Codec.TERMS_EXT)
        self.termsindex = TermIndexWriter(tifile)
        self.postfile = segment.create_file(storage, W2Codec.POSTS_EXT)

        # We'll wait to create the DAWG builder until someone actually adds
        # a spelled field
        self.dawg = None

        self.blocklimit = blocklimit
        self.compression = compression
        self.inlinelimit = inlinelimit
        self.block = None
        self.terminfo = None
        self._infield = False
        self.is_closed = False

    def _make_dawg_files(self):
        """
        Creates the DAWG (Directed Acyclic Word Graph) files if needed.
        """
        dawgfile = self.segment.create_file(self.storage, W2Codec.DAWG_EXT)
        self.dawg = GraphWriter(dawgfile)

    def _new_block(self):
        """
        Creates a new block object.

        Returns:
        - W2Block: The new block object.
        """
        return W2Block(self.format.posting_size)

    def _reset_block(self):
        """
        Resets the current block.
        """
        self.block = self._new_block()

    def _write_block(self):
        """
        Writes the current block to the posting file.
        """
        self.terminfo.add_block(self.block)
        self.block.to_file(self.postfile, compression=self.compression)
        self._reset_block()
        self.blockcount += 1

    def _start_blocklist(self):
        """
        Starts a new block list in the posting file.
        """
        postfile = self.postfile
        self._reset_block()

        # Magic number
        self.startoffset = postfile.tell()
        postfile.write(W2Block.magic)
        # Placeholder for block count
        self.blockcount = 0
        postfile.write_uint(0)

    def start_field(self, fieldname, fieldobj):
        """
        Starts writing a new field.

        Parameters:
        - fieldname (str): The name of the field.
        - fieldobj (Field): The field object.

        Raises:
        - ValueError: If called before finishing the previous field.
        """
        self.fieldname = fieldname
        self.field = fieldobj
        self.format = fieldobj.format
        self.spelling = fieldobj.spelling and not fieldobj.separate_spelling()
        self._dawgfield = False
        if self.spelling or fieldobj.separate_spelling():
            if self.dawg is None:
                self._make_dawg_files()
            self.dawg.start_field(fieldname)
            self._dawgfield = True
        self._infield = True

    def start_term(self, text):
        """
        Starts writing a new term.

        Parameters:
        - text (str): The text of the term.

        Raises:
        - ValueError: If called inside a block.
        """
        if self.block is not None:
            raise ValueError("Called start_term in a block")
        self.text = text
        self.terminfo = FileTermInfo()
        if self.spelling:
            self.dawg.insert(
                text.decode()
            )  # use text.decode() to convert bytes to string. Revert to text.decode("utf-8") if error occurs
        self._start_blocklist()

    def add(self, docnum, weight, valuestring, length):
        """
        Adds a document to the current block.

        Parameters:
        - docnum (int): The document number.
        - weight (float): The weight of the document.
        - valuestring (str): The value string of the document.
        - length (int): The length of the document.

        Raises:
        - ValueError: If the block size exceeds the block limit, the current block is written to the posting file.
        """
        self.block.add(docnum, weight, valuestring, length)
        if len(self.block) > self.blocklimit:
            self._write_block()

    def add_spell_word(self, fieldname, text):
        """
        Adds a spelling word to the DAWG (Directed Acyclic Word Graph).

        Parameters:
        - fieldname (str): The name of the field.
        - text (str): The spelling word.
        """
        if self.dawg is None:
            self._make_dawg_files()
        self.dawg.insert(text)

    def finish_term(self):
        """
        Finishes writing the current term.

        Raises:
        - ValueError: If called when not in a block.
        """
        block = self.block
        if block is None:
            raise ValueError("Called finish_term when not in a block")

        terminfo = self.terminfo
        if self.blockcount < 1 and block and len(block) < self.inlinelimit:
            # Inline the single block
            terminfo.add_block(block)
            vals = None if not block.values else tuple(block.values)
            postings = (tuple(block.ids), tuple(block.weights), vals)
        else:
            if block:
                # Write the current unfinished block to disk
                self._write_block()

            # Seek back to the start of this list of posting blocks and write
            # the number of blocks
            postfile = self.postfile
            postfile.flush()
            here = postfile.tell()
            postfile.seek(self.startoffset + 4)
            postfile.write_uint(self.blockcount)
            postfile.seek(here)

            self.block = None
            postings = self.startoffset

        self.block = None
        terminfo.postings = postings
        self.termsindex.add((self.fieldname, self.text), terminfo)

    def finish_field(self):
        """
        Finishes writing the current field.

        Raises:
        - ValueError: If called before starting a field.
        """
        if not self._infield:
            raise ValueError("Called finish_field before start_field")
        self._infield = False

        if self._dawgfield:
            self.dawg.finish_field()
            self._dawgfield = False

    def close(self):
        """
        Closes the writer and releases any resources.
        """
        self.termsindex.close()
        self.postfile.close()
        if self.dawg is not None:
            self.dawg.close()
        self.is_closed = True


# Matcher


class W2LeafMatcher(LeafMatcher):
    """
    Represents a leaf matcher for the Whoosh 2 codec.

    Args:
        postfile (file-like object): The file-like object containing the posting data.
        startoffset (int): The starting offset of the leaf matcher in the postfile.
        fmt (CodecFormat): The codec format used for encoding and decoding data.
        scorer (Scorer, optional): The scorer used for scoring documents. Defaults to None.
        term (Term, optional): The term associated with the leaf matcher. Defaults to None.
        stringids (bool, optional): Whether the leaf matcher uses string-based document IDs. Defaults to False.
    """

    def __init__(
        self, postfile, startoffset, fmt, scorer=None, term=None, stringids=False
    ):
        self.postfile = postfile
        self.startoffset = startoffset
        self.format = fmt
        self.scorer = scorer
        self._term = term
        self.stringids = stringids

        postfile.seek(startoffset)
        magic = postfile.read(4)
        assert magic == W2Block.magic
        self.blockclass = W2Block

        self.blockcount = postfile.read_uint()
        self.baseoffset = postfile.tell()

        self._active = True
        self.currentblock = -1
        self._next_block()

    def id(self):
        """
        Returns the document ID associated with the current posting.

        Returns:
            int: The document ID.
        """
        return self.block.ids[self.i]

    def is_active(self):
        """
        Checks if the leaf matcher is active.

        Returns:
            bool: True if the leaf matcher is active, False otherwise.
        """
        return self._active

    def weight(self):
        """
        Returns the weight of the current posting.

        Returns:
            float: The weight of the posting.
        """
        weights = self.block.weights
        if not weights:
            weights = self.block.read_weights()
        return weights[self.i]

    def value(self):
        """
        Returns the value of the current posting.

        Returns:
            object: The value of the posting.
        """
        values = self.block.values
        if values is None:
            values = self.block.read_values()
        return values[self.i]

    def all_ids(self):
        """
        Generator that yields all document IDs in the leaf matcher.

        Yields:
            int: The document ID.
        """
        nextoffset = self.baseoffset
        for _ in range(self.blockcount):
            block = self._read_block(nextoffset)
            nextoffset = block.nextoffset
            ids = block.read_ids()
            yield from ids

    def next(self):
        """
        Moves to the next posting in the leaf matcher.

        Returns:
            bool: True if there is a next posting, False otherwise.
        """
        if self.i == self.block.count - 1:
            self._next_block()
            return True
        else:
            self.i += 1
            return False

    def skip_to(self, id):
        """
        Skips to the posting with the specified document ID.

        Args:
            id (int): The document ID to skip to.

        Raises:
            ReadTooFar: If the leaf matcher has been read beyond the target ID.
        """
        if not self.is_active():
            raise ReadTooFar

        i = self.i
        # If we're already in the block with the target ID, do nothing
        if id <= self.block.ids[i]:
            return

        # Skip to the block that would contain the target ID
        if id > self.block.maxid:
            self._skip_to_block(lambda: id > self.block.maxid)
        if not self.is_active():
            return

        # Iterate through the IDs in the block until we find or pass the target
        ids = self.block.ids
        i = self.i
        while ids[i] < id:
            i += 1
            if i == len(ids):
                self._active = False
                return
        self.i = i

    def skip_to_quality(self, minquality):
        """
        Skips to the posting with a quality greater than or equal to the specified minimum quality.

        Args:
            minquality (float): The minimum quality.

        Returns:
            int: The number of blocks skipped.

        Note:
            The quality of a posting is determined by the block quality function.
        """
        bq = self.block_quality
        if bq() > minquality:
            return 0
        return self._skip_to_block(lambda: bq() <= minquality)

    def block_min_length(self):
        """
        Returns the minimum length of postings in the current block.

        Returns:
            int: The minimum length.
        """
        return self.block.min_length()

    def block_max_length(self):
        """
        Returns the maximum length of postings in the current block.

        Returns:
            int: The maximum length.
        """
        return self.block.max_length()

    def block_max_weight(self):
        """
        Returns the maximum weight of postings in the current block.

        Returns:
            float: The maximum weight.
        """
        return self.block.max_weight()

    def block_max_wol(self):
        """
        Returns the maximum weight of lengths of postings in the current block.

        Returns:
            float: The maximum weight of lengths.
        """
        return self.block.max_wol()

    def _read_block(self, offset):
        pf = self.postfile
        pf.seek(offset)
        return self.blockclass.from_file(
            pf, self.format.posting_size, stringids=self.stringids
        )

    def _consume_block(self):
        self.block.read_ids()
        self.block.read_weights()
        self.i = 0

    def _next_block(self, consume=True):
        if self.currentblock >= self.blockcount:
            raise ValueError("No next block")

        self.currentblock += 1
        if self.currentblock == self.blockcount:
            self._active = False
            return

        if self.currentblock == 0:
            pos = self.baseoffset
        else:
            pos = self.block.nextoffset

        self.block = self._read_block(pos)
        if consume:
            self._consume_block()

    def _skip_to_block(self, targetfn):
        skipped = 0
        while self._active and targetfn():
            self._next_block(consume=False)
            skipped += 1

        if self._active:
            self._consume_block()

        return skipped


# Tables

# Writers


class TermIndexWriter(HashWriter):
    """
    A class for writing term index data to a database file.

    Inherits from HashWriter.

    Attributes:
        index (list): A list of positions in the database file where each term is stored.
        fieldcounter (int): Counter for assigning field numbers.
        fieldmap (dict): Mapping of field names to field numbers.

    Methods:
        keycoder(term): Encodes a term into a key for storage in the database file.
        valuecoder(terminfo): Encodes a TermInfo object into a string for storage in the database file.
        add(key, value): Adds a term and its associated value to the database file.
        _write_extras(): Writes additional data (index and fieldmap) to the database file.
    """

    def __init__(self, dbfile):
        """
        Initializes a TermIndexWriter object.

        Args:
            dbfile (file): The database file to write the term index data to.
        """
        HashWriter.__init__(self, dbfile)
        self.index = []
        self.fieldcounter = 0
        self.fieldmap = {}

    def keycoder(self, term):
        """
        Encodes a term into a key for storage in the database file.

        Args:
            term (tuple): A tuple containing the field name and the term text.

        Returns:
            bytes: The encoded key.
        """
        fieldmap = self.fieldmap
        fieldname, text = term

        if fieldname in fieldmap:
            fieldnum = fieldmap[fieldname]
        else:
            fieldnum = self.fieldcounter
            fieldmap[fieldname] = fieldnum
            self.fieldcounter += 1

        key = pack_ushort(fieldnum) + text
        return key

    def valuecoder(self, terminfo):
        """
        Encodes a TermInfo object into a string for storage in the database file.

        Args:
            terminfo (TermInfo): The TermInfo object to encode.

        Returns:
            str: The encoded string.
        """
        return terminfo.to_string()

    def add(self, key, value):
        """
        Adds a term and its associated value to the database file.

        Args:
            key (bytes): The encoded key representing the term.
            value (str): The encoded value representing the term information.
        """
        pos = self.dbfile.tell()
        self.index.append(pos)
        HashWriter.add(self, self.keycoder(key), self.valuecoder(value))

    def _write_extras(self):
        """
        Writes additional data (index and fieldmap) to the database file.
        """
        dbfile = self.dbfile
        dbfile.write_uint(len(self.index))
        for n in self.index:
            dbfile.write_long(n)
        dbfile.write_pickle(self.fieldmap)


class VectorWriter(TermIndexWriter):
    """A class for writing vector data to the index.

    This class is responsible for encoding and writing vector data to the index.
    It provides methods for encoding keys and values.

    Attributes:
        fieldmap (dict): A dictionary mapping field names to field numbers.
        fieldcounter (int): A counter for assigning field numbers.

    """

    def keycoder(self, key):
        """Encode the key (docnum, fieldname) into a binary representation.

        Args:
            key (tuple): A tuple containing the document number and field name.

        Returns:
            bytes: The binary representation of the key.

        """
        fieldmap = self.fieldmap
        docnum, fieldname = key

        if fieldname in fieldmap:
            fieldnum = fieldmap[fieldname]
        else:
            fieldnum = self.fieldcounter
            fieldmap[fieldname] = fieldnum
            self.fieldcounter += 1

        return _vectorkey_struct.pack(docnum, fieldnum)

    def valuecoder(self, offset):
        """Encode the offset into a binary representation.

        Args:
            offset (int): The offset value.

        Returns:
            bytes: The binary representation of the offset.

        """
        return pack_long(offset)


# Readers


class PostingIndexBase(HashReader):
    """
    Base class for a posting index.

    This class provides methods for reading and manipulating a posting index.

    Args:
        dbfile (file): The file object representing the database file.
        postfile (file): The file object representing the posting file.

    Attributes:
        postfile (file): The file object representing the posting file.
        length (int): The length of the posting index.
        indexbase (int): The base position of the posting index in the database file.
        fieldmap (dict): A mapping of field names to field numbers.
        names (list): A list of field names in the order of their field numbers.
    """

    def __init__(self, dbfile, postfile):
        HashReader.__init__(self, dbfile)
        self.postfile = postfile

    def _read_extras(self):
        """
        Read the extra information from the database file.

        This method reads the length, index base, field map, and field names from the database file.
        """
        dbfile = self.dbfile

        self.length = dbfile.read_uint()
        self.indexbase = dbfile.tell()

        dbfile.seek(self.indexbase + self.length * _LONG_SIZE)
        self.fieldmap = dbfile.read_pickle()
        self.names = [None] * len(self.fieldmap)
        for name, num in self.fieldmap.items():
            self.names[num] = name

    def _closest_key(self, key):
        """
        Find the closest key in the posting index.

        Args:
            key (bytes): The key to search for.

        Returns:
            int: The position of the closest key in the posting index.
        """
        dbfile = self.dbfile
        key_at = self._key_at
        indexbase = self.indexbase
        lo = 0
        hi = self.length
        if not isinstance(key, bytes):
            raise TypeError(f"Key {key!r} should be bytes")
        while lo < hi:
            mid = (lo + hi) // 2
            midkey = key_at(dbfile.get_long(indexbase + mid * _LONG_SIZE))
            if midkey < key:
                lo = mid + 1
            else:
                hi = mid
        if lo == self.length:
            return None
        return dbfile.get_long(indexbase + lo * _LONG_SIZE)

    def closest_key(self, key):
        """
        Find the closest key in the posting index.

        Args:
            key (bytes): The key to search for.

        Returns:
            bytes: The closest key in the posting index.
        """
        pos = self._closest_key(key)
        if pos is None:
            return None
        return self._key_at(pos)

    def _ranges_from(self, key):
        """
        Generate ranges of key-value pairs starting from the given key.

        Args:
            key (bytes): The key to start from.

        Yields:
            tuple: A tuple containing the key position, key length, data position, and data length.
        """
        pos = self._closest_key(key)
        if pos is None:
            return

        yield from self._ranges(pos=pos)

    def __getitem__(self, key):
        """
        Get the value associated with the given key.

        Args:
            key: The key to retrieve the value for.

        Returns:
            object: The value associated with the key.

        Raises:
            KeyError: If the key is not found in the posting index.
        """
        k = self.keycoder(key)
        return self.valuedecoder(HashReader.__getitem__(self, k))

    def __contains__(self, key):
        """
        Check if the given key is present in the posting index.

        Args:
            key: The key to check.

        Returns:
            bool: True if the key is present, False otherwise.
        """
        try:
            codedkey = self.keycoder(key)
        except KeyError:
            return False
        return HashReader.__contains__(self, codedkey)

    def range_for_key(self, key):
        """
        Get the range of key-value pairs for the given key.

        Args:
            key: The key to get the range for.

        Returns:
            tuple: A tuple containing the start position and end position of the range.
        """
        return HashReader.range_for_key(self, self.keycoder(key))

    def get(self, key, default=None):
        """
        Get the value associated with the given key.

        Args:
            key: The key to retrieve the value for.
            default: The default value to return if the key is not found.

        Returns:
            object: The value associated with the key, or the default value if the key is not found.
        """
        k = self.keycoder(key)
        return self.valuedecoder(HashReader.get(self, k, default))

    def keys(self):
        """
        Generate the keys in the posting index.

        Yields:
            object: The keys in the posting index.
        """
        kd = self.keydecoder
        for k in HashReader.keys(self):
            yield kd(k)

    def items(self):
        """
        Generate the key-value pairs in the posting index.

        Yields:
            tuple: A tuple containing the key and value.
        """
        kd = self.keydecoder
        vd = self.valuedecoder
        for key, value in HashReader.items(self):
            yield (kd(key), vd(value))

    def terms_from(self, fieldname, prefix):
        """
        Generate the terms in the posting index starting from the given field name and prefix.

        Args:
            fieldname: The field name to start from.
            prefix: The prefix to match.

        Yields:
            object: The terms in the posting index.
        """
        return self.keys_from((fieldname, prefix))

    def keys_from(self, key):
        """
        Generate the keys in the posting index starting from the given key.

        Args:
            key: The key to start from.

        Yields:
            object: The keys in the posting index.
        """
        key = self.keycoder(key)
        kd = self.keydecoder
        read = self.read
        for keypos, keylen, _, _ in self._ranges_from(key):
            yield kd(read(keypos, keylen))

    def items_from(self, fieldname, prefix):
        """
        Generate the key-value pairs in the posting index starting from the given field name and prefix.

        Args:
            fieldname: The field name to start from.
            prefix: The prefix to match.

        Yields:
            tuple: A tuple containing the key and value.
        """
        read = self.read
        key = self.keycoder((fieldname, prefix))
        kd = self.keydecoder
        vd = self.valuedecoder
        for keypos, keylen, datapos, datalen in self._ranges_from(key):
            yield (kd(read(keypos, keylen)), vd(read(datapos, datalen)))

    def values(self):
        """
        Generate the values in the posting index.

        Yields:
            object: The values in the posting index.
        """
        vd = self.valuedecoder
        for v in HashReader.values(self):
            yield vd(v)

    def close(self):
        """
        Close the posting index.

        This method closes the posting index and the associated files.
        """
        HashReader.close(self)
        self.postfile.close()


class W2TermsReader(PostingIndexBase):
    """
    A class that implements the TermsReader interface for the Whoosh2 codec.

    This class provides methods for reading terms, retrieving term information,
    creating matchers for a given term, encoding and decoding keys, and decoding
    values.

    Note: This class does not filter out deleted documents. A higher-level class
    is expected to wrap the matcher to eliminate deleted documents.

    Args:
        PostingIndexBase: The base class for the terms reader.

    Attributes:
        postfile (PostingsFile): The postings file associated with the terms reader.
        fieldmap (dict): A dictionary mapping field names to field numbers.
        names (list): A list of field names.
        dbfile (DatabaseFile): The database file associated with the terms reader.

    Methods:
        terms(): Returns the list of terms in the index.
        term_info(fieldname, text): Returns the term information for a given field and text.
        matcher(fieldname, text, format_, scorer=None): Returns a matcher for a given field and text.
        keycoder(key): Encodes a key.
        keydecoder(v): Decodes a key.
        valuedecoder(v): Decodes a value.
        frequency(fieldname, btext): Returns the frequency of a term in a given field.
        doc_frequency(fieldname, btext): Returns the document frequency of a term in a given field.
    """

    def terms(self):
        """
        Returns the list of terms in the index.

        Returns:
            list: A list of terms in the index.
        """
        return self.keys()

    def term_info(self, fieldname, text):
        """
        Returns the term information for a given field and text.

        Args:
            fieldname (str): The name of the field.
            text (str): The text of the term.

        Returns:
            TermInfo: The term information for the given field and text.

        Raises:
            TermNotFound: If the term is not found in the index.
        """
        return self[fieldname, text]

    def matcher(self, fieldname, text, format_, scorer=None):
        """
        Returns a matcher for a given field and text.

        Args:
            fieldname (str): The name of the field.
            text (str): The text of the term.
            format_ (str): The format of the matcher.
            scorer (Scorer, optional): The scorer to use for scoring documents. Defaults to None.

        Returns:
            Matcher: A matcher for the given field and text.

        Raises:
            TermNotFound: If the term is not found in the index.
        """
        pf = self.postfile

        term = (fieldname, text)
        try:
            terminfo = self[term]
        except KeyError:
            raise TermNotFound(f"No term {fieldname}:{text!r}")

        p = terminfo.postings
        if isinstance(p, int):
            # terminfo.postings is an offset into the posting file
            pr = W2LeafMatcher(pf, p, format_, scorer=scorer, term=term)
        else:
            # terminfo.postings is an inlined tuple of (ids, weights, values)
            docids, weights, values = p
            pr = ListMatcher(docids, weights, values, format_, scorer=scorer, term=term)
        return pr

    def keycoder(self, key):
        """
        Encodes a key.

        Args:
            key (tuple): The key to encode.

        Returns:
            bytes: The encoded key.
        """
        fieldname, tbytes = key
        fnum = self.fieldmap.get(fieldname, 65535)
        return pack_ushort(fnum) + tbytes

    def keydecoder(self, v):
        """
        Decodes a key.

        Args:
            v (bytes): The key to decode.

        Returns:
            tuple: The decoded key.
        """
        assert isinstance(v, bytes)
        return (self.names[unpack_ushort(v[:2])[0]], v[2:])

    def valuedecoder(self, v):
        """
        Decodes a value.

        Args:
            v (bytes): The value to decode.

        Returns:
            FileTermInfo: The decoded value.
        """
        assert isinstance(v, bytes)
        return FileTermInfo.from_string(v)

    def frequency(self, fieldname, btext):
        """
        Returns the frequency of a term in a given field.

        Args:
            fieldname (str): The name of the field.
            btext (bytes): The encoded text of the term.

        Returns:
            int: The frequency of the term in the given field.
        """
        assert isinstance(btext, bytes)
        datapos = self.range_for_key((fieldname, btext))[0]
        return FileTermInfo.read_weight(self.dbfile, datapos)

    def doc_frequency(self, fieldname, btext):
        """
        Returns the document frequency of a term in a given field.

        Args:
            fieldname (str): The name of the field.
            btext (bytes): The encoded text of the term.

        Returns:
            int: The document frequency of the term in the given field.
        """
        assert isinstance(btext, bytes)
        datapos = self.range_for_key((fieldname, btext))[0]
        return FileTermInfo.read_doc_freq(self.dbfile, datapos)


# docnum, fieldnum
_vectorkey_struct = Struct("!IH")


class W2VectorReader(PostingIndexBase):
    """
    Implements the VectorReader interface for the Whoosh2 codec.

    This class provides methods for reading vector data from the index.

    Attributes:
        postfile (file): The file object representing the posting file.
        fieldmap (dict): A mapping of field names to field numbers.
        names (list): A list of field names.

    """

    def matcher(self, docnum, fieldname, format_):
        """
        Returns a matcher for the given document number, field name, and format.

        Args:
            docnum (int): The document number.
            fieldname (str): The field name.
            format_ (str): The format of the vector data.

        Returns:
            W2LeafMatcher: A matcher object for the given parameters.

        """
        pf = self.postfile
        offset = self[(docnum, fieldname)]
        pr = W2LeafMatcher(pf, offset, format_, stringids=True)
        return pr

    def keycoder(self, key):
        """
        Encodes the key into a binary representation.

        Args:
            key (tuple): The key to encode, consisting of a document number and a field name.

        Returns:
            bytes: The binary representation of the key.

        """
        return _vectorkey_struct.pack(key[0], self.fieldmap[key[1]])

    def keydecoder(self, v):
        """
        Decodes the binary representation of a key.

        Args:
            v (bytes): The binary representation of the key.

        Returns:
            tuple: The decoded key, consisting of a document number and a field name.

        """
        docnum, fieldnum = _vectorkey_struct.unpack(v)
        return (docnum, self.names[fieldnum])

    def valuedecoder(self, v):
        """
        Decodes the binary representation of a value.

        Args:
            v (bytes): The binary representation of the value.

        Returns:
            int: The decoded value.

        """
        return unpack_long(v)[0]


class W2PerDocReader(base.PerDocumentReader):
    """Reader for per-document data in a Whoosh 2 index segment.

    This class provides methods for accessing per-document data such as field lengths,
    stored fields, and vectors in a Whoosh 2 index segment.

    Parameters:
    - storage (Storage): The storage object for the index.
    - segment (Segment): The segment object representing the index segment.

    Attributes:
    - _storage (Storage): The storage object for the index.
    - _segment (Segment): The segment object representing the index segment.
    - _doccount (int): The total number of documents in the segment.
    - _lengths (InMemoryLengths): The object for accessing field lengths.
    - _stored (StoredFieldReader): The object for accessing stored fields.
    - _vectors (W2VectorReader): The object for accessing vectors.

    Methods:
    - supports_columns(): Check if the reader supports column storage.
    - close(): Close the reader and release any resources.
    - doc_count(): Get the number of documents in the segment.
    - doc_count_all(): Get the total number of documents in the segment.
    - has_deletions(): Check if the segment has deleted documents.
    - is_deleted(docnum): Check if a document is deleted.
    - deleted_docs(): Get the list of deleted document numbers.
    - doc_field_length(docnum, fieldname, default=0): Get the length of a field in a document.
    - field_length(fieldname): Get the total length of a field in all documents.
    - min_field_length(fieldname): Get the minimum length of a field in all documents.
    - max_field_length(fieldname): Get the maximum length of a field in all documents.
    - has_vector(docnum, fieldname): Check if a document has a vector for a field.
    - vector(docnum, fieldname, format_): Get the vector for a field in a document.
    - stored_fields(docnum): Get the stored fields for a document.
    """

    def __init__(self, storage, segment):
        self._storage = storage
        self._segment = segment
        self._doccount = segment.doc_count_all()

        flfile = segment.open_file(storage, W2Codec.LENGTHS_EXT)
        self._lengths = InMemoryLengths.from_file(flfile, self._doccount)

        sffile = segment.open_file(storage, W2Codec.STORED_EXT)
        self._stored = StoredFieldReader(sffile)

        self._vectors = None  # Lazy load

    def supports_columns(self):
        """Check if the reader supports column storage.

        Returns:
        - bool: True if the reader supports column storage, False otherwise.
        """
        return False

    def close(self):
        """Close the reader and release any resources."""
        self._lengths.close()
        if self._vectors:
            self._vectors.close()
        self._stored.close()

    def doc_count(self):
        """Get the number of documents in the segment.

        Returns:
        - int: The number of documents in the segment.
        """
        return self._segment.doc_count()

    def doc_count_all(self):
        """Get the total number of documents in the segment.

        Returns:
        - int: The total number of documents in the segment.
        """
        return self._doccount

    def has_deletions(self):
        """Check if the segment has deleted documents.

        Returns:
        - bool: True if the segment has deleted documents, False otherwise.
        """
        return self._segment.has_deletions()

    def is_deleted(self, docnum):
        """Check if a document is deleted.

        Parameters:
        - docnum (int): The document number.

        Returns:
        - bool: True if the document is deleted, False otherwise.
        """
        return self._segment.is_deleted(docnum)

    def deleted_docs(self):
        """Get the list of deleted document numbers.

        Returns:
        - list[int]: The list of deleted document numbers.
        """
        return self._segment.deleted_docs()

    def doc_field_length(self, docnum, fieldname, default=0):
        """Get the length of a field in a document.

        Parameters:
        - docnum (int): The document number.
        - fieldname (str): The field name.
        - default (int, optional): The default length to return if the field is not found. Defaults to 0.

        Returns:
        - int: The length of the field in the document, or the default length if the field is not found.
        """
        return self._lengths.doc_field_length(docnum, fieldname, default)

    def field_length(self, fieldname):
        """Get the total length of a field in all documents.

        Parameters:
        - fieldname (str): The field name.

        Returns:
        - int: The total length of the field in all documents.
        """
        return self._lengths.field_length(fieldname)

    def min_field_length(self, fieldname):
        """Get the minimum length of a field in all documents.

        Parameters:
        - fieldname (str): The field name.

        Returns:
        - int: The minimum length of the field in all documents.
        """
        return self._lengths.min_field_length(fieldname)

    def max_field_length(self, fieldname):
        """Get the maximum length of a field in all documents.

        Parameters:
        - fieldname (str): The field name.

        Returns:
        - int: The maximum length of the field in all documents.
        """
        return self._lengths.max_field_length(fieldname)

    def _prep_vectors(self):
        vifile = self._segment.open_file(self._storage, W2Codec.VECTOR_EXT)
        vpostfile = self._segment.open_file(self._storage, W2Codec.VPOSTS_EXT)
        self._vectors = W2VectorReader(vifile, vpostfile)

    def has_vector(self, docnum, fieldname):
        """Check if a document has a vector for a field.

        Parameters:
        - docnum (int): The document number.
        - fieldname (str): The field name.

        Returns:
        - bool: True if the document has a vector for the field, False otherwise.
        """
        if self._vectors is None:
            try:
                self._prep_vectors()
            except (NameError, OSError):
                return False
        return (docnum, fieldname) in self._vectors

    def vector(self, docnum, fieldname, format_):
        """Get the vector for a field in a document.

        Parameters:
        - docnum (int): The document number.
        - fieldname (str): The field name.
        - format_ (str): The format of the vector.

        Returns:
        - VectorMatcher: The vector matcher object.
        """
        if self._vectors is None:
            self._prep_vectors()
        return self._vectors.matcher(docnum, fieldname, format_)

    def stored_fields(self, docnum):
        """Get the stored fields for a document.

        Parameters:
        - docnum (int): The document number.

        Returns:
        - dict: The stored fields for the document.
        """
        return self._stored[docnum]


# Single-byte field lengths implementations


class ByteLengthsBase:
    """
    Base class for storing byte lengths of fields in a document.

    This class provides methods to read and store byte lengths of fields in a document.
    It also provides methods to retrieve the total number of documents, the length of a specific field,
    and the minimum and maximum lengths of a field.

    Attributes:
        magic (bytes): The magic number used to identify the file format.
    """

    magic = b"~LN1"

    def __init__(self):
        """
        Initializes a new instance of the ByteLengthsBase class.
        """
        self.starts = {}
        self.totals = {}
        self.minlens = {}
        self.maxlens = {}

    def _read_header(self, dbfile, doccount):
        """
        Reads the header information from the database file.

        Args:
            dbfile (file): The file object representing the database file.
            doccount (int): The number of documents saved in the database.

        Raises:
            AssertionError: If the magic number or version number is not as expected.
        """
        first = dbfile.read(4)  # Magic
        assert first == self.magic
        version = dbfile.read_int()  # Version number
        assert version == 1

        self._count = dbfile.read_uint()  # Number of documents saved

        fieldcount = dbfile.read_ushort()  # Number of fields
        # Read per-field info
        for i in range(fieldcount):
            fieldname = dbfile.read_string().decode("utf-8")
            self.totals[fieldname] = dbfile.read_long()
            self.minlens[fieldname] = byte_to_length(dbfile.read_byte())
            self.maxlens[fieldname] = byte_to_length(dbfile.read_byte())
            self.starts[fieldname] = i * doccount

        # Add header length to per-field offsets
        eoh = dbfile.tell()  # End of header
        for fieldname in self.starts:
            self.starts[fieldname] += eoh

    def doc_count_all(self):
        """
        Returns the total number of documents saved in the database.

        Returns:
            int: The total number of documents.
        """
        return self._count

    def field_length(self, fieldname):
        """
        Returns the total length of a specific field in the database.

        Args:
            fieldname (str): The name of the field.

        Returns:
            int: The total length of the field.

        Raises:
            KeyError: If the field name is not found in the database.
        """
        return self.totals.get(fieldname, 0)

    def min_field_length(self, fieldname):
        """
        Returns the minimum length of a specific field in the database.

        Args:
            fieldname (str): The name of the field.

        Returns:
            int: The minimum length of the field.

        Raises:
            KeyError: If the field name is not found in the database.
        """
        return self.minlens.get(fieldname, 0)

    def max_field_length(self, fieldname):
        """
        Returns the maximum length of a specific field in the database.

        Args:
            fieldname (str): The name of the field.

        Returns:
            int: The maximum length of the field.

        Raises:
            KeyError: If the field name is not found in the database.
        """
        return self.maxlens.get(fieldname, 0)


class InMemoryLengths(ByteLengthsBase):
    def __init__(self):
        """
        Initialize the Whoosh2 codec.

        This method initializes the Whoosh2 codec by setting up the necessary data structures.
        It inherits from the ByteLengthsBase class and initializes the totals and lengths dictionaries.
        The totals dictionary keeps track of the total number of occurrences of each term in the index,
        while the lengths dictionary stores the length of each term in bytes.
        The _count variable is used to keep track of the number of terms.

        Usage:
        codec = Whoosh2()
        """

        ByteLengthsBase.__init__(self)
        self.totals = defaultdict(int)
        self.lengths = {}
        self._count = 0

    def close(self):
        """
        Closes the codec.

        This method is called to release any resources held by the codec. It should be called when the codec is no longer needed.

        """
        pass

    # IO

    def to_file(self, dbfile, doccount):
        """
        Write the index data to a file.

        Args:
            dbfile (file): The file object to write the index data to.
            doccount (int): The number of documents in the index.

        Raises:
            IOError: If there is an error writing to the file.

        Notes:
            This method writes the index data to a file in a specific format.
            It writes the magic number, format version number, number of documents,
            and number of fields to the file. Then, it writes per-field information,
            including field name, field length, minimum field length, and maximum field length.
            Finally, it writes the byte arrays for each field.

        Example:
            >>> with open("index.db", "wb") as dbfile:
            ...     codec.to_file(dbfile, 1000)
        """
        self._pad_arrays(doccount)
        fieldnames = list(self.lengths.keys())

        dbfile.write(self.magic)
        dbfile.write_int(1)  # Format version number
        dbfile.write_uint(doccount)  # Number of documents
        dbfile.write_ushort(len(self.lengths))  # Number of fields

        # Write per-field info
        for fieldname in fieldnames:
            dbfile.write_string(fieldname.encode("utf-8"))  # Fieldname
            dbfile.write_long(self.field_length(fieldname))
            dbfile.write_byte(length_to_byte(self.min_field_length(fieldname)))
            dbfile.write_byte(length_to_byte(self.max_field_length(fieldname)))

        # Write byte arrays
        for fieldname in fieldnames:
            dbfile.write_array(self.lengths[fieldname])
        dbfile.close()

    @classmethod
    def from_file(cls, dbfile, doccount=None):
        """
        Load a Whoosh2 object from a file.

        Args:
            cls (class): The class of the object to be loaded.
            dbfile (file): The file object to read from.
            doccount (int, optional): The number of documents in the object. Defaults to None.

        Returns:
            obj: The loaded Whoosh2 object.

        Raises:
            None.

        """
        obj = cls()
        obj._read_header(dbfile, doccount)
        for fieldname, start in obj.starts.items():
            obj.lengths[fieldname] = dbfile.get_array(start, "B", obj._count)
        dbfile.close()
        return obj

    # Get

    def doc_field_length(self, docnum, fieldname, default=0):
        """
        Returns the length of a field in a document.

        Args:
            docnum (int): The document number.
            fieldname (str): The name of the field.
            default (int, optional): The default length to return if the field is not found. Defaults to 0.

        Returns:
            int: The length of the field in the document, or the default length if the field is not found.

        Raises:
            None

        Example:
            >>> codec = WhooshCodec()
            >>> codec.doc_field_length(0, "title")
            10
        """
        try:
            arry = self.lengths[fieldname]
        except KeyError:
            return default
        if docnum >= len(arry):
            return default
        return byte_to_length(arry[docnum])

    # Min/max cache setup -- not meant to be called while adding

    def _minmax(self, fieldname, op, cache):
        """
        Returns the minimum or maximum value for a given field, based on the provided operation.

        Args:
            fieldname (str): The name of the field.
            op (function): The operation to be performed on the field's lengths.
            cache (dict): A dictionary used to cache previously computed results.

        Returns:
            int: The minimum or maximum value for the field.

        """
        if fieldname in cache:
            return cache[fieldname]
        else:
            ls = self.lengths[fieldname]
            if ls:
                result = byte_to_length(op(ls))
            else:
                result = 0
            cache[fieldname] = result
            return result

    def min_field_length(self, fieldname):
        """
        Returns the minimum length allowed for a field.

        Parameters:
        - fieldname (str): The name of the field.

        Returns:
        - int: The minimum length allowed for the field.

        """
        return self._minmax(fieldname, min, self.minlens)

    def max_field_length(self, fieldname):
        """
        Returns the maximum field length for a given field.

        Parameters:
        - fieldname (str): The name of the field.

        Returns:
        - int: The maximum field length.

        """
        return self._minmax(fieldname, max, self.maxlens)

    # Add

    def _create_field(self, fieldname, docnum):
        """
        Create a new field for the given document number.

        Args:
            fieldname (str): The name of the field.
            docnum (int): The document number.

        Returns:
            None

        Raises:
            None

        Notes:
            This method is used to create a new field for a document in the index.
            It updates the lengths dictionary with the field's length information.
            The _count attribute is also updated to reflect the maximum document number.

        """
        dc = max(self._count, docnum + 1)
        self.lengths[fieldname] = array("B", (0 for _ in range(dc)))
        self._count = dc

    def _pad_arrays(self, doccount):
        """
        Pad out arrays to full length.

        This method is used to ensure that the arrays storing the lengths of fields are
        of the same length as the number of documents in the index. If the arrays are
        shorter than the desired length, they are padded with zeros.

        Parameters:
        - doccount (int): The desired length of the arrays.

        Returns:
        None
        """
        for fieldname in self.lengths.keys():
            arry = self.lengths[fieldname]
            if len(arry) < doccount:
                for _ in range(doccount - len(arry)):
                    arry.append(0)
        self._count = doccount

    def add(self, docnum, fieldname, length):
        """
        Add the length of a field for a specific document.

        Args:
            docnum (int): The document number.
            fieldname (str): The name of the field.
            length (int): The length of the field.

        Returns:
            None

        Raises:
            None

        Notes:
            This method updates the lengths and totals dictionaries to keep track of the field lengths
            for each document. If the field does not exist in the lengths dictionary, it will be created.
            The length is converted to a byte value using the length_to_byte function. The byte value is
            then stored in the lengths dictionary for the specified document and field. The totals
            dictionary is also updated to keep track of the total length of each field.

        """
        lengths = self.lengths
        if length:
            if fieldname not in lengths:
                self._create_field(fieldname, docnum)

            arry = self.lengths[fieldname]
            count = docnum + 1
            if len(arry) < count:
                for _ in range(count - len(arry)):
                    arry.append(0)
            if count > self._count:
                self._count = count
            byte = length_to_byte(length)
            arry[docnum] = byte
            self.totals[fieldname] += length

    def add_other(self, other):
        """
        Adds the lengths and totals from another instance of the Whoosh2 class to the current instance.

        Parameters:
        - other (Whoosh2): Another instance of the Whoosh2 class.

        Returns:
        None
        """

        lengths = self.lengths
        totals = self.totals
        doccount = self._count

        # Add missing length arrays
        for fname in other.lengths:
            if fname not in lengths:
                lengths[fname] = array("B")
        self._pad_arrays(doccount)

        # Extend length arrays with values from other instance
        for fname in other.lengths:
            lengths[fname].extend(other.lengths[fname])
        self._count = doccount + other._count
        self._pad_arrays(self._count)

        # Add totals from other instance
        for fname in other.totals:
            totals[fname] += other.totals[fname]


class OnDiskLengths(ByteLengthsBase):
    """
    A class that represents the on-disk lengths of fields in a Whoosh index.

    This class is responsible for reading and retrieving the lengths of fields
    stored on disk. It inherits from the ByteLengthsBase class.

    Parameters:
    - dbfile (file-like object): The file-like object representing the on-disk
        storage of the field lengths.
    - doccount (int, optional): The total number of documents in the index. If
        not provided, it will be determined by reading the header of the dbfile.

    Methods:
    - doc_field_length(docnum, fieldname, default=0): Retrieves the length of a
        field in a specific document. If the field is not found, it returns the
        default value.
    - close(): Closes the dbfile.

    Example usage:
    ```
    dbfile = open("lengths.db", "rb")
    lengths = OnDiskLengths(dbfile)
    length = lengths.doc_field_length(10, "title")
    lengths.close()
    ```
    """

    def __init__(self, dbfile, doccount=None):
        """
        Initialize a Whoosh2 object.

        Args:
            dbfile (str): The path to the Whoosh2 database file.
            doccount (int, optional): The number of documents in the database. Defaults to None.

        Raises:
            SomeException: An exception that may be raised under certain conditions.

        Returns:
            None
        """
        ByteLengthsBase.__init__(self)
        self.dbfile = dbfile
        self._read_header(dbfile, doccount)

    def doc_field_length(self, docnum, fieldname, default=0):
        """
        Retrieves the length of a field in a specific document.

        Parameters:
        - docnum (int): The document number.
        - fieldname (str): The name of the field.
        - default (int, optional): The default value to return if the field is
            not found. Default is 0.

        Returns:
        - int: The length of the field in the specified document, or the default
            value if the field is not found.
        """
        try:
            start = self.starts[fieldname]
        except KeyError:
            return default
        return byte_to_length(self.dbfile.get_byte(start + docnum))

    def close(self):
        """
        Closes the dbfile.

        This method closes the dbfile associated with the codec. It should be called when you are done using the codec to free up system resources.

        Usage:
        codec.close()

        """
        self.dbfile.close()


# Stored fields

_stored_pointer_struct = Struct("!qI")  # offset, length
stored_pointer_size = _stored_pointer_struct.size
pack_stored_pointer = _stored_pointer_struct.pack
unpack_stored_pointer = _stored_pointer_struct.unpack


class StoredFieldWriter:
    """
    Class for writing stored fields to a database file.

    Args:
        dbfile (file): The file object to write the stored fields to.

    Attributes:
        dbfile (file): The file object to write the stored fields to.
        length (int): The number of stored fields written.
        directory (list): A list of pointers to the stored fields in the file.
        names (list): A list of field names.
        name_map (dict): A mapping of field names to their index in the `names` list.
    """

    def __init__(self, dbfile):
        """
        Initialize a Whoosh2 object.

        Args:
            dbfile (file): The file object representing the database file.

        Attributes:
            dbfile (file): The file object representing the database file.
            length (int): The length of the database.
            directory (list): A list of directory entries.
            names (list): A list of names.
            name_map (dict): A dictionary mapping names to their corresponding indices.
        """
        self.dbfile = dbfile
        self.length = 0
        self.directory = []

        self.dbfile.write_long(0)
        self.dbfile.write_uint(0)

        self.names = []
        self.name_map = {}

    def add(self, vdict):
        """
        Adds a dictionary of field values to the stored fields.

        Args:
            vdict (dict): A dictionary of field names and their corresponding values.
        """
        f = self.dbfile
        names = self.names
        name_map = self.name_map

        vlist = [None] * len(names)
        for k, v in vdict.items():
            if k in name_map:
                vlist[name_map[k]] = v
            else:
                name_map[k] = len(names)
                names.append(k)
                vlist.append(v)

        vstring = dumps(tuple(vlist), -1)[2:-1]
        self.length += 1
        self.directory.append(pack_stored_pointer(f.tell(), len(vstring)))
        f.write(vstring)

    def add_reader(self, sfreader):
        """
        Adds stored fields from a reader object.

        Args:
            sfreader (object): An object that provides an iterator over dictionaries of field values.
        """
        add = self.add
        for vdict in sfreader:
            add(vdict)

    def close(self):
        """
        Closes the stored field writer and flushes the changes to the file.
        """
        f = self.dbfile
        dirpos = f.tell()
        f.write_pickle(self.names)
        for pair in self.directory:
            f.write(pair)
        f.flush()
        f.seek(0)
        f.write_long(dirpos)
        f.write_uint(self.length)
        f.close()


class StoredFieldReader:
    """
    Reads stored fields from a database file.

    Args:
        dbfile (file-like object): The database file to read from.

    Attributes:
        dbfile (file-like object): The database file being read.
        length (int): The number of stored fields in the database.
        basepos (int): The base position in the database file.
        names (list): The list of field names.
        directory_offset (int): The offset of the directory in the database file.

    Methods:
        close(): Closes the database file.
        __iter__(): Iterates over the stored fields and yields a dictionary of field names and values.
        __getitem__(num): Retrieves the stored field at the specified index.

    """

    def __init__(self, dbfile):
        """
        Initialize a Whoosh2 object.

        Args:
            dbfile (file-like object): The file-like object representing the Whoosh2 database file.

        Raises:
            ValueError: If the database file is not valid.

        Notes:
            This method reads the metadata from the database file and initializes the Whoosh2 object.

        """
        self.dbfile = dbfile

        dbfile.seek(0)
        dirpos = dbfile.read_long()
        self.length = dbfile.read_uint()
        self.basepos = dbfile.tell()

        dbfile.seek(dirpos)

        nameobj = dbfile.read_pickle()
        if isinstance(nameobj, dict):
            # Previous versions stored the list of names as a map of names to
            # positions... it seemed to make sense at the time...
            self.names = [None] * len(nameobj)
            for name, pos in nameobj.items():
                self.names[pos] = name
        else:
            self.names = nameobj
        self.directory_offset = dbfile.tell()

    def close(self):
        """
        Closes the database file.

        This method closes the database file associated with the current instance of the class.
        After calling this method, any further operations on the database file will raise an exception.

        Usage:
            codec = WhooshCodec()
            codec.close()

        Raises:
            Any exceptions raised by the underlying file object's close() method.
        """
        self.dbfile.close()

    def __iter__(self):
        """
        Iterates over the stored fields and yields a dictionary of field names and values.
        """
        dbfile = self.dbfile
        names = self.names
        lengths = array("I")

        dbfile.seek(self.directory_offset)
        for _ in range(self.length):
            dbfile.seek(_LONG_SIZE, 1)
            lengths.append(dbfile.read_uint())

        dbfile.seek(self.basepos)
        for length in lengths:
            vlist = loads(dbfile.read(length) + b".")
            vdict = {
                names[i]: vlist[i] for i in range(len(vlist)) if vlist[i] is not None
            }
            yield vdict

    def __getitem__(self, num):
        """
        Retrieves the stored field at the specified index.

        Args:
            num (int): The index of the stored field to retrieve.

        Returns:
            dict: A dictionary of field names and values.

        Raises:
            IndexError: If the specified index is out of range.
            ValueError: If there is an error reading the stored field.

        """
        if num > self.length - 1:
            raise IndexError(f"Tried to get document {num}, file has {self.length}")

        dbfile = self.dbfile
        start = self.directory_offset + num * stored_pointer_size
        dbfile.seek(start)
        ptr = dbfile.read(stored_pointer_size)
        if len(ptr) != stored_pointer_size:
            raise ValueError(
                f"Error reading {dbfile} @{start} {len(ptr)} < {stored_pointer_size}"
            )
        position, length = unpack_stored_pointer(ptr)
        dbfile.seek(position)
        vlist = loads(dbfile.read(length) + b".")

        names = self.names
        # Recreate a dictionary by putting the field names and values back
        # together by position. We can't just use dict(zip(...)) because we
        # want to filter out the None values.
        vdict = {names[i]: vlist[i] for i in range(len(vlist)) if vlist[i] is not None}
        return vdict


# Segment object


class W2Segment(base.Segment):
    def __init__(self, indexname, doccount=0, segid=None, deleted=None):
        """
        Represents a segment in the Whoosh index.

        :param indexname: The name of the index.
        :type indexname: str
        :param doccount: The maximum document number in the segment.
        :type doccount: int
        :param segid: The segment ID. If not provided, a random ID will be generated.
        :type segid: str, optional
        :param deleted: A set of deleted document numbers, or None if no deleted documents exist in this segment.
        :type deleted: set, optional
        """
        assert isinstance(indexname, str)
        self.indexname = indexname
        assert isinstance(doccount, int)
        self.doccount = doccount
        self.segid = self._random_id() if segid is None else segid
        self.deleted = deleted
        self.compound = False

    def codec(self, **kwargs):
        """
        Returns the codec associated with this segment.

        :param kwargs: Additional keyword arguments to pass to the codec constructor.
        :return: The codec associated with this segment.
        :rtype: W2Codec
        """
        return W2Codec(**kwargs)

    def set_doc_count(self, dc):
        """
        Sets the document count for this segment.

        :param dc: The document count.
        :type dc: int
        """
        self.doccount = dc

    def doc_count_all(self):
        """
        Returns the total count of all documents in this segment.

        :return: The total count of all documents.
        :rtype: int
        """
        return self.doccount

    def doc_count(self):
        """
        Returns the count of non-deleted documents in this segment.

        :return: The count of non-deleted documents.
        :rtype: int
        """
        return self.doccount - self.deleted_count()

    def has_deletions(self):
        """
        Checks if this segment has any deleted documents.

        :return: True if there are deleted documents, False otherwise.
        :rtype: bool
        """
        return self.deleted is not None and bool(self.deleted)

    def deleted_count(self):
        """
        Returns the count of deleted documents in this segment.

        :return: The count of deleted documents.
        :rtype: int
        """
        if self.deleted is None:
            return 0
        return len(self.deleted)

    def delete_document(self, docnum, delete=True):
        """
        Marks a document as deleted or undeleted.

        :param docnum: The document number.
        :type docnum: int
        :param delete: True to mark the document as deleted, False to mark it as undeleted.
        :type delete: bool, optional
        """
        if delete:
            if self.deleted is None:
                self.deleted = set()
            self.deleted.add(docnum)
        elif self.deleted is not None and docnum in self.deleted:
            self.deleted.remove(docnum)

    def is_deleted(self, docnum):
        """
        Checks if a document is marked as deleted.

        :param docnum: The document number.
        :type docnum: int
        :return: True if the document is marked as deleted, False otherwise.
        :rtype: bool
        """
        if self.deleted is None:
            return False
        return docnum in self.deleted

    def deleted_docs(self):
        """
        Returns an iterator over the deleted document numbers in this segment.

        :return: An iterator over the deleted document numbers.
        :rtype: iterator
        """
        if self.deleted is None:
            return ()
        else:
            return iter(self.deleted)


# Posting blocks


class W2Block:
    """
    Represents a block of data in the Whoosh index file format.

    Attributes:
        magic (bytes): The magic number identifying the block format.
        infokeys (tuple): The keys for the block information.

    Args:
        postingsize (int): The size of the posting data.
        stringids (bool, optional): Whether the block uses string IDs. Defaults to False.
    """

    magic = b"Blk3"

    infokeys = (
        "count",
        "maxid",
        "maxweight",
        "minlength",
        "maxlength",
        "idcode",
        "compression",
        "idslen",
        "weightslen",
    )

    def __init__(self, postingsize, stringids=False):
        """
        Initializes a new instance of the W2Block class.

        Args:
            postingsize (int): The size of the posting data.
            stringids (bool, optional): Whether the block uses string IDs. Defaults to False.
        """
        self.postingsize = postingsize
        self.stringids = stringids
        self.ids = [] if stringids else array("I")
        self.weights = array("f")
        self.values = None

        self.minlength = None
        self.maxlength = 0
        self.maxweight = 0

    def __len__(self):
        """
        Returns the number of IDs in the block.

        Returns:
            int: The number of IDs in the block.
        """
        return len(self.ids)

    def __nonzero__(self):
        """
        Returns whether the block has any IDs.

        Returns:
            bool: True if the block has IDs, False otherwise.
        """
        return bool(self.ids)

    def min_id(self):
        """
        Returns the minimum ID in the block.

        Returns:
            int: The minimum ID in the block.

        Raises:
            IndexError: If the block has no IDs.
        """
        if self.ids:
            return self.ids[0]
        else:
            raise IndexError

    def max_id(self):
        """
        Returns the maximum ID in the block.

        Returns:
            int: The maximum ID in the block.

        Raises:
            IndexError: If the block has no IDs.
        """
        if self.ids:
            return self.ids[-1]
        else:
            raise IndexError

    def min_length(self):
        """
        Returns the minimum length of the values in the block.

        Returns:
            int: The minimum length of the values in the block.
        """
        return self.minlength

    def max_length(self):
        """
        Returns the maximum length of the values in the block.

        Returns:
            int: The maximum length of the values in the block.
        """
        return self.maxlength

    def max_weight(self):
        """
        Returns the maximum weight in the block.

        Returns:
            float: The maximum weight in the block.
        """
        return self.maxweight

    def add(self, id_, weight, valuestring, length=None):
        """
        Adds an ID, weight, and value to the block.

        Args:
            id_ (int): The ID to add.
            weight (float): The weight to add.
            valuestring (str): The value string to add.
            length (int, optional): The length of the value. Defaults to None.
        """
        self.ids.append(id_)
        self.weights.append(weight)
        if weight > self.maxweight:
            self.maxweight = weight
        if valuestring:
            if self.values is None:
                self.values = []
            self.values.append(valuestring)
        if length:
            if self.minlength is None or length < self.minlength:
                self.minlength = length
            if length > self.maxlength:
                self.maxlength = length

    def to_file(self, postfile, compression=3):
        """
        Writes the block data to a file.

        Args:
            postfile (file): The file to write the block data to.
            compression (int, optional): The compression level. Defaults to 3.
        """
        ids = self.ids
        idcode, idstring = minimize_ids(ids, self.stringids, compression)
        wtstring = minimize_weights(self.weights, compression)
        vstring = minimize_values(self.postingsize, self.values, compression)

        info = (
            len(ids),
            ids[-1],
            self.maxweight,
            length_to_byte(self.minlength),
            length_to_byte(self.maxlength),
            idcode,
            compression,
            len(idstring),
            len(wtstring),
        )
        infostring = dumps(info, -1)

        # Offset to next block
        postfile.write_uint(
            len(infostring) + len(idstring) + len(wtstring) + len(vstring)
        )
        # Block contents
        postfile.write(infostring)
        postfile.write(idstring)
        postfile.write(wtstring)
        postfile.write(vstring)

    @classmethod
    def from_file(cls, postfile, postingsize, stringids=False):
        """
        Reads a block from a file.

        Args:
            postfile (file): The file to read the block from.
            postingsize (int): The size of the posting data.
            stringids (bool, optional): Whether the block uses string IDs. Defaults to False.

        Returns:
            W2Block: The read block.
        """
        block = cls(postingsize, stringids=stringids)
        block.postfile = postfile

        delta = postfile.read_uint()
        block.nextoffset = postfile.tell() + delta
        info = postfile.read_pickle()
        block.dataoffset = postfile.tell()

        for key, value in zip(cls.infokeys, info):
            if key in ("minlength", "maxlength"):
                value = byte_to_length(value)
            setattr(block, key, value)

        return block

    def read_ids(self):
        """
        Reads the IDs from the block.

        Returns:
            list: The read IDs.
        """
        offset = self.dataoffset
        self.postfile.seek(offset)
        idstring = self.postfile.read(self.idslen)
        ids = deminimize_ids(self.idcode, self.count, idstring, self.compression)
        self.ids = ids
        return ids

    def read_weights(self):
        """
        Reads the weights from the block.

        Returns:
            list: The read weights.
        """
        if self.weightslen == 0:
            weights = [1.0] * self.count
        else:
            offset = self.dataoffset + self.idslen
            self.postfile.seek(offset)
            wtstring = self.postfile.read(self.weightslen)
            weights = deminimize_weights(self.count, wtstring, self.compression)
        self.weights = weights
        return weights

    def read_values(self):
        """
        Reads the values from the block.

        Returns:
            list: The read values.
        """
        postingsize = self.postingsize
        if postingsize == 0:
            values = [None] * self.count
        else:
            offset = self.dataoffset + self.idslen + self.weightslen
            self.postfile.seek(offset)
            vstring = self.postfile.read(self.nextoffset - offset)
            values = deminimize_values(
                postingsize, self.count, vstring, self.compression
            )
        self.values = values
        return values


# File TermInfo

NO_ID = 0xFFFFFFFF


class FileTermInfo(TermInfo):
    """
    Represents term information stored in a file-based index.

    Attributes:
        postings: The postings associated with the term.
    """

    struct = Struct("!fIBBffII")

    def __init__(self, *args, **kwargs):
        """
        Initializes a new instance of the FileTermInfo class.

        Args:
            *args: Variable length arguments.
            **kwargs: Keyword arguments.

        Keyword Args:
            postings: The postings associated with the term.
        """
        self.postings = None
        if "postings" in kwargs:
            self.postings = kwargs["postings"]
            del kwargs["postings"]
        TermInfo.__init__(self, *args, **kwargs)

    def add_block(self, block):
        """
        Adds a block of postings to the term information.

        Args:
            block: The block of postings to add.
        """
        self._weight += sum(block.weights)
        self._df += len(block)

        ml = block.min_length()
        if self._minlength is None:
            self._minlength = ml
        else:
            self._minlength = min(self._minlength, ml)

        self._maxlength = max(self._maxlength, block.max_length())
        self._maxweight = max(self._maxweight, block.max_weight())
        if self._minid is None:
            self._minid = block.ids[0]
        self._maxid = block.ids[-1]

    def to_string(self):
        """
        Converts the term information to a string representation.

        Returns:
            The string representation of the term information.
        """
        # Encode the lengths as 0-255 values
        ml = 0 if self._minlength is None else length_to_byte(self._minlength)
        xl = length_to_byte(self._maxlength)
        # Convert None values to the out-of-band NO_ID constant so they can be
        # stored as unsigned ints
        mid = NO_ID if self._minid is None else self._minid
        xid = NO_ID if self._maxid is None else self._maxid

        # Pack the term info into bytes
        st = self.struct.pack(
            self._weight, self._df, ml, xl, self._maxweight, 0, mid, xid
        )

        if isinstance(self.postings, tuple):
            # Postings are inlined - dump them using the pickle protocol
            isinlined = 1
            st += dumps(self.postings, -1)[2:-1]
        else:
            # Append postings pointer as long to end of term info bytes
            isinlined = 0
            # It's possible for a term info to not have a pointer to postings
            # on disk, in which case postings will be None. Convert a None
            # value to -1 so it can be stored as a long.
            p = -1 if self.postings is None else self.postings
            st += pack_long(p)

        # Prepend byte indicating whether the postings are inlined to the term
        # info bytes
        return pack_byte(isinlined) + st

    @classmethod
    def from_string(cls, s):
        """
        Creates a new FileTermInfo instance from a string representation.

        Args:
            s: The string representation of the term information.

        Returns:
            A new FileTermInfo instance.
        """
        assert isinstance(s, bytes)

        if isinstance(s, str):
            hbyte = ord(s[0])  # Python 2.x - str
        else:
            hbyte = s[0]  # Python 3 - bytes

        if hbyte < 2:
            st = cls.struct
            # Weight, Doc freq, min len, max len, max w, unused, min ID, max ID
            w, df, ml, xl, xw, _, mid, xid = st.unpack(s[1 : st.size + 1])
            mid = None if mid == NO_ID else mid
            xid = None if xid == NO_ID else xid
            # Postings
            pstr = s[st.size + 1 :]
            if hbyte == 0:
                p = unpack_long(pstr)[0]
            else:
                p = loads(pstr + b".")
        else:
            # Old format was encoded as a variable length pickled tuple
            v = loads(s + b".")
            if len(v) == 1:
                w = df = 1
                p = v[0]
            elif len(v) == 2:
                w = df = v[1]
                p = v[0]
            else:
                w, p, df = v
            # Fake values for stats which weren't stored before
            ml = 1
            xl = 255
            xw = 999999999
            mid = -1
            xid = -1

        ml = byte_to_length(ml)
        xl = byte_to_length(xl)
        obj = cls(w, df, ml, xl, xw, mid, xid)
        obj.postings = p
        return obj

    @classmethod
    def read_weight(cls, dbfile, datapos):
        """
        Reads the weight from the database file.

        Args:
            dbfile: The database file.
            datapos: The position of the weight in the file.

        Returns:
            The weight.
        """
        return dbfile.get_float(datapos + 1)

    @classmethod
    def read_doc_freq(cls, dbfile, datapos):
        """
        Reads the document frequency from the database file.

        Args:
            dbfile: The database file.
            datapos: The position of the document frequency in the file.

        Returns:
            The document frequency.
        """
        return dbfile.get_uint(datapos + 1 + _FLOAT_SIZE)

    @classmethod
    def read_min_and_max_length(cls, dbfile, datapos):
        """
        Reads the minimum and maximum length from the database file.

        Args:
            dbfile: The database file.
            datapos: The position of the lengths in the file.

        Returns:
            A tuple containing the minimum and maximum length.
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
            dbfile: The database file.
            datapos: The position of the maximum weight in the file.

        Returns:
            The maximum weight.
        """
        weightspos = datapos + 1 + _FLOAT_SIZE + _INT_SIZE + 2
        return dbfile.get_float(weightspos)


# Utility functions


def minimize_ids(arry, stringids, compression=0):
    """
    Minimizes the given array of IDs for efficient storage and retrieval.

    Args:
        arry (array): The array of IDs to be minimized.
        stringids (bool): Indicates whether the IDs are string-based or not.
        compression (int, optional): The compression level to apply to the minimized IDs. Defaults to 0.

    Returns:
        tuple: A tuple containing the typecode of the minimized IDs and the minimized IDs as a string.

    Raises:
        None

    Notes:
        - If the IDs are string-based, they will be serialized using the `pickle.dumps` function.
        - If the IDs are not string-based, they will be converted to the appropriate typecode based on their maximum value.
        - If the typecode of the array needs to be changed, a new array will be created with the updated typecode.
        - If the system is big-endian, the byte order of the array will be swapped.
        - If compression is enabled, the minimized IDs will be compressed using the zlib library.
    """
    amax = arry[-1]

    if stringids:
        typecode = ""
        string = dumps(arry)
    else:
        typecode = arry.typecode
        if amax <= 255:
            typecode = "B"
        elif amax <= 65535:
            typecode = "H"

        if typecode != arry.typecode:
            arry = array(typecode, iter(arry))
        if not IS_LITTLE:
            arry.byteswap()
        string = arry.tobytes()
    if compression:
        string = zlib.compress(string, compression)
    return (typecode, string)


def deminimize_ids(typecode, count, string, compression=0):
    """
    Deserialize and decompress a string representation of an array of integers.

    Args:
        typecode (str): The typecode of the array.
        count (int): The number of elements in the array.
        string (bytes): The serialized and optionally compressed string representation of the array.
        compression (int, optional): The compression level used for the string. Defaults to 0.

    Returns:
        array: The deserialized and decompressed array of integers.

    Raises:
        TypeError: If the typecode is not a valid array typecode.
    """
    if compression:
        string = zlib.decompress(string)
    if typecode == "":
        return loads(string)
    else:
        arry = array(typecode)
        arry.frombytes(string)
        if not IS_LITTLE:
            arry.byteswap()
        return arry


def minimize_weights(weights, compression=0):
    """
    Minimizes the weights array by converting it to a compressed string representation.

    Args:
        weights (array-like): The weights array to be minimized.
        compression (int, optional): The compression level to be applied. Defaults to 0.

    Returns:
        str: The minimized string representation of the weights array.

    Raises:
        None

    Examples:
        >>> weights = [1.0, 1.0, 1.0]
        >>> minimize_weights(weights)
        b''

        >>> weights = [0.5, 0.75, 1.0]
        >>> minimize_weights(weights, compression=6)
        b'x\x9c\xcbH\xcd\xc9\xc9\x07\x00\x06\xcb\x01'

    Note:
        - If all weights in the array are equal to 1.0, an empty string is returned.
        - The weights array is expected to be a one-dimensional array-like object.
        - The compression level should be an integer between 0 and 9, where 0 means no compression and 9 means maximum compression.
    """
    if all(w == 1.0 for w in weights):
        string = b""
    else:
        if not IS_LITTLE:
            weights.byteswap()
        string = weights.tobytes()
    if string and compression:
        string = zlib.compress(string, compression)
    return string


def deminimize_weights(count, string, compression=0):
    """
    Convert a serialized string representation of weights into an array of floats.

    Args:
        count (int): The number of weights to be converted.
        string (bytes): The serialized string representation of weights.
        compression (int, optional): The compression level used for the serialized string. Defaults to 0.

    Returns:
        array.array: An array of floats representing the weights.

    Raises:
        None

    Examples:
        >>> weights = deminimize_weights(3, b'\x00\x00\x80\x3f\x00\x00\x00\x40\x00\x00\x40\x40')
        >>> print(weights)
        array('f', [1.0, 2.0, 3.0])
    """
    if not string:
        return array("f", (1.0 for _ in range(count)))
    if compression:
        string = zlib.decompress(string)
    arry = array("f")
    arry.frombytes(string)
    if not IS_LITTLE:
        arry.byteswap()
    return arry


def minimize_values(postingsize, values, compression=0):
    """
    Minimizes the values by compressing them and returning the compressed string.

    Args:
        postingsize (int): The size of the posting.
        values (list): The list of values to be minimized.
        compression (int, optional): The compression level. Defaults to 0.

    Returns:
        str: The compressed string.

    Raises:
        None

    Examples:
        >>> minimize_values(10, ['value1', 'value2', 'value3'], 6)
        'compressed_string'
    """
    if postingsize < 0:
        string = dumps(values, -1)[2:]
    elif postingsize == 0:
        string = b""
    else:
        string = b"".join(values)
    if string and compression:
        string = zlib.compress(string, compression)
    return string


def deminimize_values(postingsize, count, string, compression=0):
    """
    Deminimizes a string into a list of values.

    Args:
        postingsize (int): The size of each value in the string.
        count (int): The number of values to extract from the string.
        string (bytes): The string to deminimize.
        compression (int, optional): The compression level of the string. Defaults to 0.

    Returns:
        list: The deminimized list of values.

    Raises:
        None

    Examples:
        >>> string = b'\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f'
        >>> deminimize_values(2, 8, string)
        [b'\x00\x01', b'\x02\x03', b'\x04\x05', b'\x06\x07', b'\x08\t', b'\n\x0b', b'\x0c\r', b'\x0e\x0f']
    """
    if compression:
        string = zlib.decompress(string)

    if postingsize < 0:
        return loads(string)
    elif postingsize == 0:
        return [None] * count
    else:
        return [string[i : i + postingsize] for i in range(0, len(string), postingsize)]


# Legacy field types

from whoosh.fields import NUMERIC


class old_numeric(NUMERIC):
    """
    A field type for storing numeric values in the index.

    This field type supports storing integers, floats, and decimals.
    The values can be sorted and searched using numeric range queries.

    Parameters:
    - type (type): The Python type of the numeric values to be stored.
    - stored (bool): Whether the field should be stored in the index.
    - unique (bool): Whether the field values should be unique.
    - field_boost (float): The boost factor for the field.
    - decimal_places (int): The number of decimal places to store for decimal values.
    - shift_step (int): The number of bits to shift the values during sorting.
    - signed (bool): Whether the values should be treated as signed or unsigned.

    Raises:
    - TypeError: If the specified type is not supported by the field.

    """

    NUMERIC_DEFAULTS = {
        "b": 2**7 - 1,
        "B": 2**8 - 1,
        "h": 2**15 - 1,
        "H": 2**16 - 1,
        "i": 2**31 - 1,
        "I": 2**32 - 1,
        "q": 2**63 - 1,
        "Q": 2**64 - 1,
        "f": NaN,
        "d": NaN,
    }

    def __init__(
        self,
        type=int,
        stored=False,
        unique=False,
        field_boost=1.0,
        decimal_places=0,
        shift_step=4,
        signed=True,
    ):
        """
        Initialize the old_numeric field.

        Args:
        - type (type): The Python type of the numeric values to be stored.
        - stored (bool): Whether the field should be stored in the index.
        - unique (bool): Whether the field values should be unique.
        - field_boost (float): The boost factor for the field.
        - decimal_places (int): The number of decimal places to store for decimal values.
        - shift_step (int): The number of bits to shift the values during sorting.
        - signed (bool): Whether the values should be treated as signed or unsigned.

        Raises:
        - TypeError: If the specified type is not supported by the field.

        """
        from whoosh import analysis, formats

        self.type = type
        if self.type is int:
            # This will catch the Python 3 int type
            self._to_text = self._long_to_text
            self._from_text = self._text_to_long
            self.sortable_typecode = "q" if signed else "Q"
        elif self.type is int:
            self._to_text = self._int_to_text
            self._from_text = self._text_to_int
            self.sortable_typecode = "i" if signed else "I"
        elif self.type is float:
            self._to_text = self._float_to_text
            self._from_text = self._text_to_float
            self.sortable_typecode = "f"
        elif self.type is Decimal:
            raise TypeError(
                "To store Decimal instances, set type to int or "
                "float and use the decimal_places argument"
            )
        else:
            raise TypeError(f"{self.__class__} field type can't store {self.type!r}")

        self.stored = stored
        self.unique = unique
        self.decimal_places = decimal_places
        self.shift_step = shift_step
        self.signed = signed

        self.analyzer = analysis.id_analyzer()
        self.format = formats.Existence(field_boost=field_boost)

    def __setstate__(self, d):
        """
        Set the state of the field.

        Args:
        - d (dict): The state dictionary.

        """
        self.__dict__.update(d)
        self.numtype = d["type"]
        self.bits = 64

    def prepare_number(self, x):
        """
        Prepare a numeric value for storage in the index.

        Args:
        - x: The numeric value to prepare.

        Returns:
        - The prepared numeric value.

        """
        if x is None or x == emptybytes:
            return x
        if self.decimal_places:
            x = Decimal(x)
            x *= 10**self.decimal_places
        x = self.type(x)
        return x

    def unprepare_number(self, x):
        """
        Convert a prepared numeric value back to its original form.

        Args:
        - x: The prepared numeric value.

        Returns:
        - The original numeric value.

        """
        dc = self.decimal_places
        if dc:
            s = str(x)
            x = Decimal(s[:-dc] + "." + s[-dc:])
        return x

    def to_bytes(self, x, shift=0):
        """
        Convert a numeric value to bytes.

        Args:
        - x: The numeric value to convert.
        - shift (int): The number of bits to shift the value.

        Returns:
        - The bytes representation of the numeric value.

        """
        if isinstance(x, bytes):
            return x
        return utf8encode(self.to_text(x, shift))[0]

    def from_bytes(self, bs):
        """
        Convert bytes to a numeric value.

        Args:
        - bs (bytes): The bytes to convert.

        Returns:
        - The numeric value.

        """
        return self.from_text(utf8decode(bs)[0])

    def sortable_to_bytes(self, x, shift=0):
        """
        Convert a numeric value to sortable bytes.

        Args:
        - x: The numeric value to convert.
        - shift (int): The number of bits to shift the value.

        Returns:
        - The sortable bytes representation of the numeric value.

        """
        if shift:
            x >>= shift
        return pack_byte(shift) + self._to_text()

    def to_text(self, x, shift=0):
        """
        Convert a numeric value to text.

        Args:
        - x: The numeric value to convert.
        - shift (int): The number of bits to shift the value.

        Returns:
        - The text representation of the numeric value.

        """
        x = self.prepare_number(x)
        x = self._to_text(x, shift=shift, signed=self.signed)
        return x

    def from_text(self, t):
        """
        Convert text to a numeric value.

        Args:
        - t (str): The text to convert.

        Returns:
        - The numeric value.

        """
        x = self._from_text(t, signed=self.signed)
        return self.unprepare_number(x)

    def process_text(self, text, **kwargs):
        """
        Process the text value of the field.

        Args:
        - text (str): The text value to process.

        Returns:
        - A tuple containing the processed text value.

        """
        return (self.to_text(text),)

    def self_parsing(self):
        """
        Check if the field is self-parsing.

        Returns:
        - True if the field is self-parsing, False otherwise.

        """
        return True

    def parse_query(self, fieldname, qstring, boost=1.0):
        """
        Parse a query string for the field.

        Args:
        - fieldname (str): The name of the field.
        - qstring (str): The query string to parse.
        - boost (float): The boost factor for the query.

        Returns:
        - A query object representing the parsed query.

        """
        from whoosh import query

        if qstring == "*":
            return query.Every(fieldname, boost=boost)

        try:
            text = self.to_text(qstring)
        except ValueError:
            e = sys.exc_info()[1]
            return query.error_query(e)

        return query.Term(fieldname, text, boost=boost)

    def parse_range(self, fieldname, start, end, startexcl, endexcl, boost=1.0):
        """
        Parse a range query for the field.

        Args:
        - fieldname (str): The name of the field.
        - start: The start value of the range.
        - end: The end value of the range.
        - startexcl (bool): Whether the start value is exclusive.
        - endexcl (bool): Whether the end value is exclusive.
        - boost (float): The boost factor for the query.

        Returns:
        - A query object representing the parsed range query.

        """
        from whoosh import query
        from whoosh.qparser.common import QueryParserError

        try:
            if start is not None:
                start = self.from_text(self.to_text(start))
            if end is not None:
                end = self.from_text(self.to_text(end))
        except ValueError:
            e = sys.exc_info()[1]
            raise QueryParserError(e)

        return query.NumericRange(
            fieldname, start, end, startexcl, endexcl, boost=boost
        )

    def sortable_terms(self, ixreader, fieldname):
        """
        Generate sortable terms for the field.

        Args:
        - ixreader: The index reader object.
        - fieldname (str): The name of the field.

        Yields:
        - Sortable terms for the field.

        """
        for btext in ixreader.lexicon(fieldname):
            if btext[0:1] != "\x00":
                # Only yield the full-precision values
                break
            yield btext


class old_datetime(old_numeric):
    """
    A field type for storing and indexing datetime values.

    This field type stores datetime values as long integers internally, using the `datetime_to_long` function
    to convert datetime objects to long integers, and the `long_to_datetime` function to convert long integers
    back to datetime objects.

    Parameters:
    - stored (bool): Whether the field should be stored in the index. Default is False.
    - unique (bool): Whether the field should be unique in the index. Default is False.

    Example usage:
    ```
    from whoosh.codec.whoosh2 import old_datetime

    # Create an instance of old_datetime field type
    my_datetime_field = old_datetime(stored=True, unique=True)
    ```

    """

    def __init__(self, stored=False, unique=False):
        old_numeric.__init__(self, type=int, stored=stored, unique=unique, shift_step=8)

    def to_text(self, x, shift=0):
        """
        Convert a datetime value to a string representation.

        Parameters:
        - x: The datetime value to convert.
        - shift (int): The number of bits to shift the value by. Default is 0.

        Returns:
        - str: The string representation of the datetime value.

        Raises:
        - ValueError: If the datetime value cannot be converted to a string.

        """

        from datetime import datetime

        from whoosh.util.times import floor

        try:
            if isinstance(x, str):
                # For indexing, support same strings as for query parsing
                x = self._parse_datestring(x)
                x = floor(x)  # this makes most sense (unspecified = lowest)
            if isinstance(x, datetime):
                x = datetime_to_long(x)
            elif not isinstance(x, int):
                raise TypeError()
        except ValueError:
            raise ValueError(f"DATETIME.to_text can't convert from {x!r}")

        x = old_numeric.to_text(self, x, shift=shift)
        return x

    def from_text(self, x):
        """
        Convert a string representation to a datetime value.

        Parameters:
        - x (str): The string representation of the datetime value.

        Returns:
        - datetime.datetime: The datetime value.

        """

        x = old_numeric.from_text(self, x)
        return long_to_datetime(x)

    def _parse_datestring(self, qstring):
        """
        Parse a simple datetime representation.

        This method parses a very simple datetime representation of the form YYYY[MM[DD[hh[mm[ss[uuuuuu]]]]]].

        Parameters:
        - qstring (str): The datetime string to parse.

        Returns:
        - whoosh.util.times.adatetime: The parsed datetime value.

        Raises:
        - Exception: If the datetime string is not parseable.

        """

        from whoosh.util.times import adatetime, fix, is_void

        qstring = qstring.replace(" ", "").replace("-", "").replace(".", "")
        year = month = day = hour = minute = second = microsecond = None
        if len(qstring) >= 4:
            year = int(qstring[:4])
        if len(qstring) >= 6:
            month = int(qstring[4:6])
        if len(qstring) >= 8:
            day = int(qstring[6:8])
        if len(qstring) >= 10:
            hour = int(qstring[8:10])
        if len(qstring) >= 12:
            minute = int(qstring[10:12])
        if len(qstring) >= 14:
            second = int(qstring[12:14])
        if len(qstring) == 20:
            microsecond = int(qstring[14:])

        at = fix(adatetime(year, month, day, hour, minute, second, microsecond))
        if is_void(at):
            raise DateParseError(f"{qstring} is not a parseable date")
        return at

    def parse_query(self, fieldname, qstring, boost=1.0):
        """
        Parse a query string into a query object.

        Parameters:
        - fieldname (str): The name of the field to parse the query for.
        - qstring (str): The query string to parse.
        - boost (float): The boost factor for the query. Default is 1.0.

        Returns:
        - whoosh.query.Query: The parsed query object.

        """

        from whoosh import query
        from whoosh.util.times import is_ambiguous

        try:
            at = self._parse_datestring(qstring)
        except:
            e = sys.exc_info()[1]
            return query.error_query(e)

        if is_ambiguous(at):
            startnum = datetime_to_long(at.floor())
            endnum = datetime_to_long(at.ceil())
            return query.NumericRange(fieldname, startnum, endnum)
        else:
            return query.Term(fieldname, self.to_text(at), boost=boost)

    def parse_range(self, fieldname, start, end, startexcl, endexcl, boost=1.0):
        """
        Parse a range query into a query object.

        Parameters:
        - fieldname (str): The name of the field to parse the range query for.
        - start (str): The start value of the range query.
        - end (str): The end value of the range query.
        - startexcl (bool): Whether the start value is exclusive. Default is False.
        - endexcl (bool): Whether the end value is exclusive. Default is False.
        - boost (float): The boost factor for the query. Default is 1.0.

        Returns:
        - whoosh.query.Query: The parsed range query object.

        """

        from whoosh import query

        if start is None and end is None:
            return query.Every(fieldname, boost=boost)

        if start is not None:
            startdt = self._parse_datestring(start).floor()
            start = datetime_to_long(startdt)

        if end is not None:
            enddt = self._parse_datestring(end).ceil()
            end = datetime_to_long(enddt)

        return query.NumericRange(fieldname, start, end, boost=boost)


# Functions for converting numbers to and from text


def int_to_text(x, shift=0, signed=True):
    """
    Convert an integer to a sortable text representation.

    Args:
        x (int): The integer to be converted.
        shift (int, optional): The number of bits to shift the integer before conversion. Defaults to 0.
        signed (bool, optional): Whether the integer is signed or not. Defaults to True.

    Returns:
        str: The sortable text representation of the integer.
    """
    x = to_sortable(int, 32, signed, x)
    return sortable_int_to_text(x, shift)


def text_to_int(text, signed=True):
    """
    Convert a text string to an integer representation.

    Args:
        text (str): The text string to convert.
        signed (bool, optional): Whether the resulting integer should be signed or unsigned.
            Defaults to True.

    Returns:
        int: The integer representation of the text string.

    """
    x = text_to_sortable_int(text)
    x = from_sortable(int, 32, signed, x)
    return x


def long_to_text(x, shift=0, signed=True):
    """
    Convert a long integer to a text representation.

    Args:
        x (int): The long integer to be converted.
        shift (int, optional): The number of bits to shift the integer before conversion. Defaults to 0.
        signed (bool, optional): Whether the integer is signed or not. Defaults to True.

    Returns:
        str: The text representation of the long integer.

    """
    x = to_sortable(int, 64, signed, x)
    return sortable_long_to_text(x, shift)


def text_to_long(text, signed=True):
    """
    Converts a text string to a long integer.

    Args:
        text (str): The text string to convert.
        signed (bool, optional): Whether the resulting long integer should be signed.
            Defaults to True.

    Returns:
        int: The converted long integer.

    Raises:
        None

    Examples:
        >>> text_to_long("12345")
        12345
        >>> text_to_long("-54321")
        -54321
    """
    x = text_to_sortable_long(text)
    x = from_sortable(int, 64, signed, x)
    return x


def float_to_text(x, shift=0, signed=True):
    """
    Convert a floating-point number to a sortable text representation.

    Args:
        x (float): The floating-point number to be converted.
        shift (int, optional): The number of bits to shift the sortable representation. Defaults to 0.
        signed (bool, optional): Whether the sortable representation should support negative numbers. Defaults to True.

    Returns:
        str: The sortable text representation of the floating-point number.
    """
    x = to_sortable(float, 32, signed, x)
    return sortable_long_to_text(x, shift)


def text_to_float(text, signed=True):
    """
    Converts a text representation of a float to a float value.

    Args:
        text (str): The text representation of the float.
        signed (bool, optional): Whether the float is signed or not. Defaults to True.

    Returns:
        float: The float value represented by the text.

    Raises:
        ValueError: If the text cannot be converted to a float.

    Examples:
        >>> text_to_float("3.14")
        3.14
        >>> text_to_float("-2.5", signed=True)
        -2.5
    """
    x = text_to_sortable_long(text)
    x = from_sortable(float, 32, signed, x)
    return x


# Functions for converting sortable representations to and from text.

from whoosh.support.base85 import from_base85, to_base85


def sortable_int_to_text(x, shift=0):
    """
    Convert a sortable integer to a text representation.

    Args:
        x (int): The integer to be converted.
        shift (int, optional): The number of bits to shift the integer before conversion. Defaults to 0.

    Returns:
        str: The text representation of the sortable integer.

    Notes:
        This function converts a sortable integer to a text representation by shifting the integer (if specified) and encoding it using base85 encoding.

    Example:
        >>> sortable_int_to_text(12345)
        '0gV'
    """
    if shift:
        x >>= shift
    text = chr(shift) + to_base85(x, False)
    return text


def sortable_long_to_text(x, shift=0):
    """
    Convert a sortable long integer to a text representation.

    Args:
        x (int): The long integer to be converted.
        shift (int, optional): The number of bits to shift the integer before conversion. Defaults to 0.

    Returns:
        str: The text representation of the sortable long integer.

    Notes:
        This function converts a long integer to a text representation using base85 encoding.
        The resulting text representation is prefixed with a character representing the shift value.

    Example:
        >>> sortable_long_to_text(1234567890, 4)
        'E@9jqo'
    """
    if shift:
        x >>= shift
    text = chr(shift) + to_base85(x, True)
    return text


def text_to_sortable_int(text):
    """
    Converts a text representation of a sortable integer to an actual integer.

    Args:
        text (str): The text representation of the sortable integer.

    Returns:
        int: The converted integer.

    Raises:
        ValueError: If the text representation is invalid.

    Example:
        >>> text_to_sortable_int('x12345678')
        305419896
    """
    return from_base85(text[1:])


def text_to_sortable_long(text):
    """
    Converts a text string to a sortable long value.

    Parameters:
    text (str): The text string to convert.

    Returns:
    int: The converted sortable long value.

    Raises:
    ValueError: If the input text is not a valid sortable long value.

    Example:
    >>> text_to_sortable_long('0x123456789abcdef')
    81985529216486895
    """
    return from_base85(text[1:])
