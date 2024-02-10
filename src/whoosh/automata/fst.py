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

"""
This module implements an FST/FSA writer and reader. An FST (Finite State
Transducer) stores a directed acyclic graph with values associated with the
leaves. Common elements of the values are pushed inside the tree. An FST that
does not store values is a regular FSA.

The format of the leaf values is pluggable using subclasses of the Values
class.

Whoosh uses these structures to store a directed acyclic word graph (DAWG) for
use in (at least) spell checking.
"""


import copy
import sys
from array import array
from hashlib import sha1  # type: ignore @UnresolvedImport
from io import BytesIO

from whoosh.filedb.structfile import StructFile
from whoosh.system import (
    _INT_SIZE,
    emptybytes,
    pack_byte,
    pack_int,
    pack_long,
    pack_uint,
)
from whoosh.util.text import utf8decode, utf8encode
from whoosh.util.varints import varint


def b(s):
    """
    Encodes the input string using the Latin-1 encoding.

    Args:
        s (str): The string to be encoded.

    Returns:
        bytes: The encoded string.

    Raises:
        UnicodeEncodeError: If the input string cannot be encoded using the Latin-1 encoding.

    Example:
        >>> b("hello")
        b'hello'
    """
    return s.encode("latin-1")


def u(s):
    """
    Convert the input string to Unicode if it is a byte string.

    Parameters:
    s (str or bytes): The input string to be converted.

    Returns:
    str: The converted Unicode string.

    Raises:
    None.

    Examples:
    >>> u(b'hello')
    'hello'
    >>> u('world')
    'world'
    """

    return s.decode("ascii") if isinstance(s, bytes) else s


class FileVersionError(Exception):
    """
    Exception raised when there is a mismatch between the version of a file and the expected version.

    This exception is typically raised when a file is being read or processed and its version does not match the expected version.
    It can be used to handle version-related errors in file handling operations.

    Attributes:
        message (str): Explanation of the error.
    """

    def __init__(self, message):
        """
        Initialize a new instance of FileVersionError.

        Args:
            message (str): Explanation of the error.
        """
        self.message = message
        super().__init__(message)


class InactiveCursor(Exception):
    """
    Exception raised when attempting to use an inactive cursor.

    An inactive cursor is a cursor that has been closed or is no longer valid.
    This exception is raised to indicate that an operation cannot be performed
    because the cursor is inactive.

    Attributes:
        message -- explanation of the error
    """

    pass


ARC_LAST = 1
ARC_ACCEPT = 2
ARC_STOP = 4
ARC_HAS_VAL = 8
ARC_HAS_ACCEPT_VAL = 16
MULTIBYTE_LABEL = 32


# FST Value types


class Values:
    """Base for classes that describe how to encode and decode FST values.

    This class provides a set of methods that define the behavior of FST values.
    Subclasses should implement these methods to handle specific types of values.

    Attributes:
        None

    Methods:
        is_valid(v): Returns True if v is a valid object that can be stored by this class.
        common(v1, v2): Returns the "common" part of the two values.
        add(prefix, v): Adds the given prefix to the given value.
        subtract(v, prefix): Subtracts the "common" part (the prefix) from the given value.
        write(dbfile, v): Writes value v to a file.
        read(dbfile): Reads a value from the given file.
        skip(dbfile): Skips over a value in the given file.
        to_bytes(v): Returns a str (Python 2.x) or bytes (Python 3) representation of the given value.
        merge(v1, v2): Merges two values.

    """

    @staticmethod
    def is_valid(v):
        """Returns True if v is a valid object that can be stored by this class.

        Args:
            v: The value to check.

        Returns:
            bool: True if v is a valid object, False otherwise.

        Raises:
            NotImplementedError: This method should be implemented by subclasses.

        """

        raise NotImplementedError

    @staticmethod
    def common(v1, v2):
        """Returns the "common" part of the two values.

        The definition of "common" depends on the specific subclass implementation.
        For example, a string implementation would return the common shared prefix,
        while an int implementation would return the minimum of the two numbers.

        If there is no common part, this method should return None.

        Args:
            v1: The first value.
            v2: The second value.

        Returns:
            object: The common part of the two values, or None if there is no common part.

        Raises:
            NotImplementedError: This method should be implemented by subclasses.

        """

        raise NotImplementedError

    @staticmethod
    def add(prefix, v):
        """Adds the given prefix to the given value.

        The prefix is the result of a call to the `common()` method.

        Args:
            prefix: The prefix to add.
            v: The value to add the prefix to.

        Returns:
            object: The value with the prefix added.

        Raises:
            NotImplementedError: This method should be implemented by subclasses.

        """

        raise NotImplementedError

    @staticmethod
    def subtract(v, prefix):
        """Subtracts the "common" part (the prefix) from the given value.

        Args:
            v: The value to subtract the prefix from.
            prefix: The prefix to subtract.

        Returns:
            object: The value with the prefix subtracted.

        Raises:
            NotImplementedError: This method should be implemented by subclasses.

        """

        raise NotImplementedError

    @staticmethod
    def write(dbfile, v):
        """Writes value v to a file.

        Args:
            dbfile: The file to write the value to.
            v: The value to write.

        Returns:
            None

        Raises:
            NotImplementedError: This method should be implemented by subclasses.

        """

        raise NotImplementedError

    @staticmethod
    def read(dbfile):
        """Reads a value from the given file.

        Args:
            dbfile: The file to read the value from.

        Returns:
            object: The value read from the file.

        Raises:
            NotImplementedError: This method should be implemented by subclasses.

        """

        raise NotImplementedError

    @classmethod
    def skip(cls, dbfile):
        """Skips over a value in the given file.

        This method is a convenience method that calls the `read()` method.

        Args:
            dbfile: The file to skip the value in.

        Returns:
            None

        """

        cls.read(dbfile)

    @staticmethod
    def to_bytes(v):
        """Returns a str (Python 2.x) or bytes (Python 3) representation of the given value.

        This method is used for calculating node digests. The representation should be
        unique but fast to calculate, and does not have to be parseable.

        Args:
            v: The value to convert.

        Returns:
            str or bytes: The representation of the value.

        Raises:
            NotImplementedError: This method should be implemented by subclasses.

        """

        raise NotImplementedError

    @staticmethod
    def merge(v1, v2):
        """Merges two values.

        The definition of "merge" depends on the specific subclass implementation.

        Args:
            v1: The first value.
            v2: The second value.

        Returns:
            object: The merged value.

        Raises:
            NotImplementedError: This method should be implemented by subclasses.

        """

        raise NotImplementedError


class IntValues(Values):
    """Stores integer values in an FST.

    This class provides methods for working with integer values in a Finite State Transducer (FST).
    It defines operations such as validation, common value calculation, addition, subtraction, and serialization.

    """

    @staticmethod
    def is_valid(v):
        """Check if a value is a valid integer for the FST.

        Args:
            v (int): The value to check.

        Returns:
            bool: True if the value is a valid integer, False otherwise.

        """
        return isinstance(v, int) and v >= 0

    @staticmethod
    def common(v1, v2):
        """Calculate the common value between two integers.

        Args:
            v1 (int): The first integer value.
            v2 (int): The second integer value.

        Returns:
            int or None: The common value if it exists, None otherwise.

        """
        if v1 is None or v2 is None:
            return None
        if v1 == v2:
            return v1
        return min(v1, v2)

    @staticmethod
    def add(base, v):
        """Add an integer value to a base value.

        Args:
            base (int or None): The base value.
            v (int or None): The value to add.

        Returns:
            int or None: The result of the addition.

        """
        if base is None:
            return v
        if v is None:
            return base
        return base + v

    @staticmethod
    def subtract(v, base):
        """Subtract a base value from an integer value.

        Args:
            v (int or None): The integer value.
            base (int or None): The base value.

        Returns:
            int or None: The result of the subtraction.

        """
        if v is None:
            return None
        if base is None:
            return v
        return v - base

    @staticmethod
    def write(dbfile, v):
        """Write an integer value to a database file.

        Args:
            dbfile (file): The database file to write to.
            v (int): The integer value to write.

        """
        dbfile.write_uint(v)

    @staticmethod
    def read(dbfile):
        """Read an integer value from a database file.

        Args:
            dbfile (file): The database file to read from.

        Returns:
            int: The read integer value.

        """
        return dbfile.read_uint()

    @staticmethod
    def skip(dbfile):
        """Skip a fixed number of bytes in a database file.

        Args:
            dbfile (file): The database file to skip bytes in.

        """
        dbfile.seek(_INT_SIZE, 1)

    @staticmethod
    def to_bytes(v):
        """Convert an integer value to bytes.

        Args:
            v (int): The integer value to convert.

        Returns:
            bytes: The byte representation of the integer value.

        """
        return pack_int(v)


class SequenceValues(Values):
    """Abstract base class for value types that store sequences."""

    @staticmethod
    def is_valid(self, v):
        """
        Check if a value is a valid sequence.

        Parameters:
        - v (object): The value to check.

        Returns:
        - bool: True if the value is a list or tuple, False otherwise.
        """
        return isinstance(self, (list, tuple))

    @staticmethod
    def common(v1, v2):
        """
        Find the common prefix between two sequences.

        Parameters:
        - v1 (list or tuple): The first sequence.
        - v2 (list or tuple): The second sequence.

        Returns:
        - list or tuple or None: The common prefix between v1 and v2, or None if there is no common prefix.
        """
        if v1 is None or v2 is None:
            return None

        i = 0
        while i < len(v1) and i < len(v2):
            if v1[i] != v2[i]:
                break
            i += 1

        if i == 0:
            return None
        if i == len(v1):
            return v1
        if i == len(v2):
            return v2
        return v1[:i]

    @staticmethod
    def add(prefix, v):
        """
        Concatenate a prefix and a sequence.

        Parameters:
        - prefix (list or tuple): The prefix sequence.
        - v (list or tuple): The sequence to concatenate.

        Returns:
        - list or tuple: The concatenation of prefix and v.
        """
        if prefix is None:
            return v
        if v is None:
            return prefix
        return prefix + v

    @staticmethod
    def subtract(v, prefix):
        """
        Remove a prefix from a sequence.

        Parameters:
        - v (list or tuple): The sequence.
        - prefix (list or tuple): The prefix to remove.

        Returns:
        - list or tuple or None: The sequence with the prefix removed, or None if the prefix is not valid.
        """
        if prefix is None:
            return v
        if v is None:
            return None
        if len(v) == len(prefix):
            return None
        if len(v) < len(prefix) or len(prefix) == 0:
            raise ValueError((v, prefix))
        return v[len(prefix) :]

    @staticmethod
    def write(dbfile, v):
        """
        Write a sequence to a database file.

        Parameters:
        - dbfile (file): The database file to write to.
        - v (list or tuple): The sequence to write.
        """
        dbfile.write_pickle(v)

    @staticmethod
    def read(dbfile):
        """
        Read a sequence from a database file.

        Parameters:
        - dbfile (file): The database file to read from.

        Returns:
        - list or tuple: The sequence read from the database file.
        """
        return dbfile.read_pickle()


class BytesValues(SequenceValues):
    """Stores bytes objects (str in Python 2.x) in an FST.

    This class is used to store bytes objects in a Finite State Transducer (FST).
    It provides methods for writing, reading, and skipping bytes objects in a database file.

    Attributes:
        None

    Methods:
        is_valid: Checks if a given value is a valid bytes object.
        write: Writes a bytes object to a database file.
        read: Reads a bytes object from a database file.
        skip: Skips a bytes object in a database file.
        to_bytes: Converts a value to bytes.

    """

    @staticmethod
    def is_valid(v):
        """Checks if a given value is a valid bytes object.

        Args:
            v (bytes): The value to check.

        Returns:
            bool: True if the value is a bytes object, False otherwise.

        """
        return isinstance(v, bytes)

    @staticmethod
    def write(dbfile, v):
        """Writes a bytes object to a database file.

        Args:
            dbfile (file): The database file to write to.
            v (bytes): The bytes object to write.

        Returns:
            None

        """
        dbfile.write_int(len(v))
        dbfile.write(v)

    @staticmethod
    def read(dbfile):
        """Reads a bytes object from a database file.

        Args:
            dbfile (file): The database file to read from.

        Returns:
            bytes: The read bytes object.

        """
        length = dbfile.read_int()
        return dbfile.read(length)

    @staticmethod
    def skip(dbfile):
        """Skips a bytes object in a database file.

        Args:
            dbfile (file): The database file to skip from.

        Returns:
            None

        """
        length = dbfile.read_int()
        dbfile.seek(length, 1)

    @staticmethod
    def to_bytes(v):
        """Converts a value to bytes.

        Args:
            v: The value to convert.

        Returns:
            bytes: The converted bytes object.

        """
        return v


class ArrayValues(SequenceValues):
    """Stores array.array objects in an FST.

    This class is used to store array.array objects in a finite state transducer (FST).
    It provides methods for writing, reading, and skipping array.array objects in a database file.

    Args:
        typecode (str): The typecode of the array.array objects to be stored.

    Attributes:
        typecode (str): The typecode of the array.array objects.
        itemsize (int): The size of each item in the array.array objects.

    """

    def __init__(self, typecode):
        """
        Initialize a new FST object.

        Args:
            typecode (str): The typecode of the array used to store the FST.

        Attributes:
            typecode (str): The typecode of the array used to store the FST.
            itemsize (int): The size of each item in the array.

        Note:
            The FST (Finite State Transducer) is a data structure used for efficient string matching and lookup operations.
            The typecode specifies the type of elements stored in the FST array, such as 'i' for integers or 'f' for floats.
            The itemsize is calculated based on the typecode and represents the size (in bytes) of each element in the array.
        """
        self.typecode = typecode
        self.itemsize = array(self.typecode).itemsize

    def is_valid(self, v):
        """
        Check if a value is a valid array.array object.

        Args:
            v (Any): The value to be checked.

        Returns:
            bool: True if the value is a valid array.array object, False otherwise.

        Raises:
            None

        Examples:
            >>> a = array.array('i', [1, 2, 3])
            >>> is_valid(a)
            True

            >>> b = [1, 2, 3]
            >>> is_valid(b)
            False

        This method checks if the given value is a valid array.array object. It returns True if the value is a valid array.array object with the same typecode as the current instance, and False otherwise.
        """
        return isinstance(v, array) and v.typecode == self.typecode

    @staticmethod
    def write(dbfile, v):
        """Write an array.array object to a database file.

        Args:
            dbfile (file): The file object representing the database file.
            v (array.array): The array.array object to be written.

        Raises:
            TypeError: If `dbfile` is not a file object.
            TypeError: If `v` is not an array.array object.

        Notes:
            - The `dbfile` should be opened in binary mode.
            - The `v` array.array object should contain elements of a single type.

        Example:
            >>> import array
            >>> v = array.array('i', [1, 2, 3, 4, 5])
            >>> with open('data.db', 'wb') as dbfile:
            ...     write(dbfile, v)
        """
        dbfile.write(b(v.typecode))
        dbfile.write_int(len(v))
        dbfile.write_array(v)

    def read(self, dbfile):
        """Read an array.array object from a database file.

        Args:
            dbfile (file): The file object representing the database file.

        Returns:
            array.array: The read array.array object.

        Raises:
            ValueError: If the file object is not valid or the data cannot be read.

        Notes:
            This method reads an array.array object from a database file. The file object
            should be opened in binary mode. The method reads the typecode of the array,
            the length of the array, and then reads the array data from the file. The
            method returns the read array.array object.

        Example:
            >>> with open('data.db', 'rb') as file:
            ...     fst = FST()
            ...     array_obj = fst.read(file)
            ...     print(array_obj)
        """
        typecode = u(dbfile.read(1))
        length = dbfile.read_int()
        return dbfile.read_array(typecode, length)

    def skip(self, dbfile):
        """
        Skip an array.array object in a database file.

        This method is used to skip over an array.array object in a database file.
        It reads the length of the array from the file, and then seeks forward in the file
        by multiplying the length with the item size.

        Args:
            dbfile (file): The file object representing the database file.

        Raises:
            ValueError: If the length read from the file is negative.

        Example:
            Suppose you have a database file containing an array.array object.
            You can use this method to skip over the array.array object in the file.

            >>> with open('database.db', 'rb') as dbfile:
            ...     skip_array(dbfile)

        """
        length = dbfile.read_int()
        if length < 0:
            raise ValueError(f"Invalid length: {length}")

        dbfile.seek(length * self.itemsize, 1)

    @staticmethod
    def to_bytes(v):
        """Convert an array.array object to bytes.

        Args:
            v (array.array): The array.array object to be converted.

        Returns:
            bytes: The converted bytes.

        Raises:
            TypeError: If the input is not an array.array object.

        Example:
            >>> import array
            >>> a = array.array('B', [1, 2, 3])
            >>> to_bytes(a)
            b'\x01\x02\x03'
        """

        return v.tobytes()


class IntListValues(SequenceValues):
    """Stores lists of positive, increasing integers (that is, lists of
    integers where each number is >= 0 and each number is greater than or equal
    to the number that precedes it) in an FST.

    This class provides methods to write and read lists of integers to/from a database file.

    Usage:
        To write a list of integers to a database file:
            IntListValues.write(dbfile, v)

        To read a list of integers from a database file:
            result = IntListValues.read(dbfile)

        To convert a list of integers to bytes:
            bytes_data = IntListValues.to_bytes(v)
    """

    @staticmethod
    def is_valid(v):
        """Check if a given value is a valid list of positive, increasing integers.

        This function checks if the given value is a list or tuple of positive, increasing integers.
        It returns True if the value is valid, and False otherwise.

        Args:
            v (list or tuple): The value to check.

        Returns:
            bool: True if the value is a valid list of positive, increasing integers, False otherwise.
        """
        if isinstance(v, (list, tuple)):
            if len(v) < 2:
                return True
            for i in range(1, len(v)):
                if not isinstance(v[i], int) or v[i] < v[i - 1]:
                    return False
            return True
        return False

    @staticmethod
    def write(dbfile, v):
        """Write a list of positive, increasing integers to a database file.

        Args:
            dbfile: The database file to write to.
            v (list or tuple): The list of positive, increasing integers to write.
        """
        base = 0
        dbfile.write_varint(len(v))
        for x in v:
            delta = x - base
            assert delta >= 0
            dbfile.write_varint(delta)
            base = x

    @staticmethod
    def read(dbfile):
        """Read a list of positive, increasing integers from a database file.

        Args:
            dbfile: The database file to read from.

        Returns:
            list: The list of positive, increasing integers read from the database file.
        """
        length = dbfile.read_varint()
        result = []
        if length > 0:
            base = 0
            for _ in range(length):
                base += dbfile.read_varint()
                result.append(base)
        return result

    @staticmethod
    def to_bytes(v):
        """Convert a list of positive, increasing integers to bytes.

        Args:
            v (list or tuple): The list of positive, increasing integers to convert.

        Returns:
            bytes: The bytes representation of the list of positive, increasing integers.
        """
        return b(repr(v))


# Node-like interface wrappers


class Node:
    """A slow but easier-to-use wrapper for FSA/DAWGs. Translates the low-level
    arc-based interface of GraphReader into Node objects with methods to follow
    edges.
    """

    def __init__(self, owner, address, accept=False):
        """
        Initialize a Node object.

        Args:
            owner (GraphReader): The owner of the node.
            address (int): The address of the node.
            accept (bool, optional): Whether the node is an accept state. Defaults to False.
        """
        self.owner = owner
        self.address = address
        self._edges = None
        self.accept = accept

    def __iter__(self):
        """
        Iterate over the keys of the outgoing edges.

        Returns:
            Iterator: An iterator over the keys of the outgoing edges.
        """
        if not self._edges:
            self._load()
        return self._edges.keys()

    def __contains__(self, key):
        """
        Check if the node has an outgoing edge with the given key.

        Args:
            key: The key of the outgoing edge.

        Returns:
            bool: True if the node has an outgoing edge with the given key, False otherwise.
        """
        if self._edges is None:
            self._load()
        return key in self._edges

    def _load(self):
        """
        Load the outgoing edges of the node.
        """
        owner = self.owner
        if self.address is None:
            d = {}
        else:
            d = {
                arc.label: Node(owner, arc.target, arc.accept)
                for arc in self.owner.iter_arcs(self.address)
            }
        self._edges = d

    def keys(self):
        """
        Get the keys of the outgoing edges.

        Returns:
            list: A list of the keys of the outgoing edges.
        """
        if self._edges is None:
            self._load()
        return self._edges.keys()

    def all_edges(self):
        """
        Get all the outgoing edges.

        Returns:
            dict: A dictionary containing all the outgoing edges.
        """
        if self._edges is None:
            self._load()
        return self._edges

    def edge(self, key):
        """
        Get the node reached by following the outgoing edge with the given key.

        Args:
            key: The key of the outgoing edge.

        Returns:
            Node: The node reached by following the outgoing edge with the given key.
        """
        if self._edges is None:
            self._load()
        return self._edges[key]

    def flatten(self, sofar=emptybytes):
        """
        Flatten the node and yield all the strings that can be formed by concatenating
        the keys of the outgoing edges.

        Args:
            sofar (bytes, optional): The prefix string formed so far. Defaults to emptybytes.

        Yields:
            bytes: The strings that can be formed by concatenating the keys of the outgoing edges.
        """
        if self.accept:
            yield sofar
        for key in sorted(self):
            node = self.edge(key)
            yield from node.flatten(sofar + key)

    def flatten_strings(self):
        """
        Flatten the node and yield all the strings that can be formed by concatenating
        the keys of the outgoing edges.

        Yields:
            str: The strings that can be formed by concatenating the keys of the outgoing edges.
        """
        return (utf8decode(k)[0] for k in self.flatten())


class ComboNode(Node):
    """Base class for nodes that blend the nodes of two different graphs.

    This class serves as a base for nodes that combine the nodes of two different graphs.
    Subclasses of ComboNode should implement the `edge()` method and may override the `accept` property.

    Attributes:
        a (Node): The first node to be blended.
        b (Node): The second node to be blended.
    """

    def __init__(self, a, b):
        """Initialize a new ComboNode.

        Args:
            a (Node): The first node to be blended.
            b (Node): The second node to be blended.
        """
        self.a = a
        self.b = b

    def __repr__(self):
        """Return a string representation of the ComboNode.

        Returns:
            str: A string representation of the ComboNode.
        """
        return f"<{self.__class__.__name__} {self.a!r} {self.b!r}>"

    def __contains__(self, key):
        """Check if a key is present in the ComboNode.

        Args:
            key: The key to check.

        Returns:
            bool: True if the key is present in either `a` or `b`, False otherwise.
        """
        return key in self.a or key in self.b

    def __iter__(self):
        """Iterate over the keys in the ComboNode.

        Returns:
            iter: An iterator over the keys in the ComboNode.
        """
        return iter(set(self.a) | set(self.b))

    @property
    def accept(self):
        """Check if the ComboNode is an accept node.

        Returns:
            bool: True if either `a` or `b` is an accept node, False otherwise.
        """
        return self.a.accept or self.b.accept


class UnionNode(ComboNode):
    """Makes two graphs appear to be the union of the two graphs."""

    def __init__(self, a, b):
        """
        Initialize a UnionNode with two graphs.

        Args:
            a (Graph): The first graph.
            b (Graph): The second graph.
        """
        self.a = a
        self.b = b

    def edge(self, key):
        """
        Get the edge for the given key.

        If the key is present in both graphs, returns a UnionNode with the edges from both graphs.
        If the key is only present in the first graph, returns the edge from the first graph.
        If the key is only present in the second graph, returns the edge from the second graph.

        Args:
            key: The key to get the edge for.

        Returns:
            UnionNode or Edge: The edge for the given key.
        """
        a = self.a
        b = self.b
        if key in a and key in b:
            return UnionNode(a.edge(key), b.edge(key))
        elif key in a:
            return a.edge(key)
        else:
            return b.edge(key)


class IntersectionNode(ComboNode):
    """Makes two graphs appear to be the intersection of the two graphs.

    This class represents a node in the intersection graph, which is created by taking the intersection of two graphs.
    The intersection graph appears as if it contains only the common elements between the two original graphs.

    Attributes:
        a (ComboNode): The first graph to be intersected.
        b (ComboNode): The second graph to be intersected.
    """

    def edge(self, key):
        """Returns the next node in the intersection graph for the given key.

        Args:
            key: The key representing the edge to traverse.

        Returns:
            IntersectionNode: The next node in the intersection graph for the given key.

        Raises:
            KeyError: If the key is not present in both graphs.
        """
        a = self.a
        b = self.b
        if key in a and key in b:
            return IntersectionNode(a.edge(key), b.edge(key))


# Cursor


class BaseCursor:
    """Base class for a cursor-type object for navigating an FST/word graph,
    represented by a :class:`GraphReader` object.

    The cursor "rests" on arcs in the FSA/FST graph, rather than nodes.

    Methods:
        - is_active(): Returns True if this cursor is still active.
        - label(): Returns the label bytes of the current arc.
        - prefix(): Returns a sequence of the label bytes for the path from the root to the current arc.
        - prefix_bytes(): Returns the label bytes for the path from the root to the current arc as a single joined bytes object.
        - prefix_string(): Returns the labels of the path from the root to the current arc as a decoded unicode string.
        - peek_key(): Returns a sequence of label bytes representing the next closest key in the graph.
        - peek_key_bytes(): Returns the next closest key in the graph as a single bytes object.
        - peek_key_string(): Returns the next closest key in the graph as a decoded unicode string.
        - stopped(): Returns True if the current arc leads to a stop state.
        - value(): Returns the value at the current arc, if reading an FST.
        - accept(): Returns True if the current arc leads to an accept state.
        - at_last_arc(): Returns True if the current arc is the last outgoing arc from the previous node.
        - next_arc(): Moves to the next outgoing arc from the previous node.
        - follow(): Follows the current arc.
        - switch_to(label): Switches to the sibling arc with the given label bytes.
        - skip_to(key): Moves the cursor to the path represented by the given key bytes.
        - flatten(): Yields the keys in the graph, starting at the current position.
        - flatten_v(): Yields (key, value) tuples in an FST, starting at the current position.
        - flatten_strings(): Yields the keys in the graph as decoded unicode strings, starting at the current position.
        - find_path(path): Follows the labels in the given path, starting at the current position.
    """

    def is_active(self):
        """Returns True if this cursor is still active, that is it has not
        read past the last arc in the graph.
        """
        raise NotImplementedError

    def label(self):
        """Returns the label bytes of the current arc."""
        raise NotImplementedError

    def prefix(self):
        """Returns a sequence of the label bytes for the path from the root
        to the current arc.
        """
        raise NotImplementedError

    def prefix_bytes(self):
        """Returns the label bytes for the path from the root to the current
        arc as a single joined bytes object.
        """
        return emptybytes.join(self.prefix())

    def prefix_string(self):
        """Returns the labels of the path from the root to the current arc as
        a decoded unicode string.
        """
        return utf8decode(self.prefix_bytes())[0]

    def peek_key(self):
        """Returns a sequence of label bytes representing the next closest
        key in the graph.
        """
        yield from self.prefix()
        c = self.copy()
        while not c.stopped():
            c.follow()
            yield c.label()

    def peek_key_bytes(self):
        """Returns the next closest key in the graph as a single bytes object."""
        return emptybytes.join(self.peek_key())

    def peek_key_string(self):
        """Returns the next closest key in the graph as a decoded unicode
        string.
        """
        return utf8decode(self.peek_key_bytes())[0]

    def stopped(self):
        """Returns True if the current arc leads to a stop state."""
        raise NotImplementedError

    def value(self):
        """Returns the value at the current arc, if reading an FST."""
        raise NotImplementedError

    def accept(self):
        """Returns True if the current arc leads to an accept state (the end
        of a valid key).
        """
        raise NotImplementedError

    def at_last_arc(self):
        """Returns True if the current arc is the last outgoing arc from the
        previous node.
        """
        raise NotImplementedError

    def next_arc(self):
        """Moves to the next outgoing arc from the previous node."""
        raise NotImplementedError

    def follow(self):
        """Follows the current arc."""
        raise NotImplementedError

    def switch_to(self, label):
        """Switch to the sibling arc with the given label bytes."""
        _label = self.label
        _at_last_arc = self.at_last_arc
        _next_arc = self.next_arc

        while True:
            thislabel = _label()
            if thislabel == label:
                return True
            if thislabel > label or _at_last_arc():
                return False
            _next_arc()

    def skip_to(self, key):
        """Moves the cursor to the path represented by the given key bytes."""
        _accept = self.accept
        _prefix = self.prefix
        _next_arc = self.next_arc

        keylist = list(key)
        while True:
            if _accept():
                thiskey = list(_prefix())
                if keylist == thiskey:
                    return True
                elif keylist > thiskey:
                    return False
            _next_arc()

    def flatten(self):
        """Yields the keys in the graph, starting at the current position."""
        _is_active = self.is_active
        _accept = self.accept
        _stopped = self.stopped
        _follow = self.follow
        _next_arc = self.next_arc
        _prefix_bytes = self.prefix_bytes

        if not _is_active():
            raise InactiveCursor
        while _is_active():
            if _accept():
                yield _prefix_bytes()
            if not _stopped():
                _follow()
                continue
            _next_arc()

    def flatten_v(self):
        """Yields (key, value) tuples in an FST, starting at the current
        position.
        """
        for key in self.flatten():
            yield key, self.value()

    def flatten_strings(self):
        """Yields the keys in the graph as decoded unicode strings, starting at the current position."""
        return (utf8decode(k)[0] for k in self.flatten())

    def find_path(self, path):
        """Follows the labels in the given path, starting at the current
        position.
        """
        path = to_labels(path)
        _switch_to = self.switch_to
        _follow = self.follow
        _stopped = self.stopped

        first = True
        for i, label in enumerate(path):
            if not first:
                _follow()
            if not _switch_to(label) or (_stopped() and i < len(path) - 1):
                return False
            first = False
        return True


class Cursor(BaseCursor):
    def __init__(self, graph, root=None, stack=None):
        """
        Initializes a Cursor object.

        Args:
            graph (Graph): The graph to navigate.
            root (int, optional): The root node of the graph. Defaults to None.
            stack (list, optional): The stack of arcs. Defaults to None.
        """
        self.graph = graph
        self.vtype = graph.vtype
        self.root = root if root is not None else graph.default_root()
        if stack:
            self.stack = stack
        else:
            self.reset()

    def is_active(self):
        """
        Checks if the cursor is active.

        Returns:
            bool: True if the cursor is active, False otherwise.
        """
        return bool(self.stack)

    def stopped(self):
        """
        Checks if the cursor has stopped.

        Returns:
            bool: True if the cursor has stopped, False otherwise.
        """
        return self._current_attr("target") is None

    def accept(self):
        """
        Checks if the cursor is in an accepting state.

        Returns:
            bool: True if the cursor is in an accepting state, False otherwise.
        """
        return self._current_attr("accept")

    def at_last_arc(self):
        """
        Checks if the cursor is at the last arc.

        Returns:
            bool: True if the cursor is at the last arc, False otherwise.
        """
        return self._current_attr("lastarc")

    def label(self):
        """
        Returns the label of the current arc.

        Returns:
            object: The label of the current arc.
        """
        return self._current_attr("label")

    def reset(self):
        """
        Resets the cursor to its initial state.
        """
        self.stack = []
        self.sums = [None]
        self._push(self.graph.arc_at(self.root))

    def copy(self):
        """
        Creates a copy of the cursor.

        Returns:
            Cursor: A copy of the cursor.
        """
        return self.__class__(self.graph, self.root, copy.deepcopy(self.stack))

    def prefix(self):
        """
        Returns the prefix labels of the current stack.

        Yields:
            object: The prefix labels of the current stack.
        """
        stack = self.stack
        if not stack:
            raise InactiveCursor
        return (arc.label for arc in stack)

    def peek_key(self):
        """
        Returns an iterator over the labels of the current stack.

        Yields:
            object: The labels of the current stack.
        """
        if not self.stack:
            raise InactiveCursor

        yield from self.prefix()
        arc = copy.copy(self.stack[-1])
        graph = self.graph
        while not arc.accept and arc.target is not None:
            graph.arc_at(arc.target, arc)
            yield arc.label

    def value(self):
        """
        Returns the value associated with the current stack.

        Returns:
            object: The value associated with the current stack.
        """
        stack = self.stack
        if not stack:
            raise InactiveCursor
        vtype = self.vtype
        if not vtype:
            raise ValueError("No value type")

        v = self.sums[-1]
        current = stack[-1]
        if current.value:
            v = vtype.add(v, current.value)
        if current.accept and current.acceptval is not None:
            v = vtype.add(v, current.acceptval)
        return v

    def next_arc(self):
        """
        Moves the cursor to the next arc.

        Returns:
            Arc: The next arc.
        """
        stack = self.stack
        if not stack:
            raise InactiveCursor

        while stack and stack[-1].lastarc:
            self.pop()
        if stack:
            current = stack[-1]
            self.graph.arc_at(current.endpos, current)
            return current

    def follow(self):
        """
        Follows the target arc.

        Returns:
            Cursor: The updated cursor.
        """
        address = self._current_attr("target")
        if address is None:
            raise Exception("Can't follow a stop arc")
        self._push(self.graph.arc_at(address))
        return self

    def skip_to(self, key):
        """
        Skips to the specified key.

        Args:
            key (list): The key to skip to.
        """
        key = to_labels(key)
        stack = self.stack
        if not stack:
            raise InactiveCursor

        _follow = self.follow
        _next_arc = self.next_arc

        i = self._pop_to_prefix(key)
        while stack and i < len(key):
            curlabel = stack[-1].label
            keylabel = key[i]
            if curlabel == keylabel:
                _follow()
                i += 1
            elif curlabel > keylabel:
                return
            else:
                _next_arc()

    def switch_to(self, label):
        """
        Switches to the specified label.

        Args:
            label (object): The label to switch to.

        Returns:
            bool: True if the switch was successful, False otherwise.
        """
        stack = self.stack
        if not stack:
            raise InactiveCursor

        current = stack[-1]
        if label == current.label:
            return True
        else:
            arc = self.graph.find_arc(current.endpos, label, current)
            return arc

    def _push(self, arc):
        if self.vtype and self.stack:
            sums = self.sums
            sums.append(self.vtype.add(sums[-1], self.stack[-1].value))
        self.stack.append(arc)

    def pop(self):
        """
        Pops the top arc from the stack.
        """
        self.stack.pop()
        if self.vtype:
            self.sums.pop()

    def _pop_to_prefix(self, key):
        stack = self.stack
        if not stack:
            raise InactiveCursor

        i = 0
        maxpre = min(len(stack), len(key))
        while i < maxpre and key[i] == stack[i].label:
            i += 1
        if stack[i].label > key[i]:
            self.current = None
            return
        while len(stack) > i + 1:
            self.pop()
        self.next_arc()
        return i


class UncompiledNode:
    """
    Represents an "in-memory" node used by the GraphWriter before it is written to disk.
    """

    compiled = False

    def __init__(self, owner):
        """
        Initializes a new instance of the UncompiledNode class.

        Parameters:
        - owner: The owner of the node.

        Returns:
        None
        """
        self.owner = owner
        self._digest = None
        self.clear()

    def clear(self):
        """
        Clears the node by resetting its arcs, value, accept flag, and input count.

        Parameters:
        None

        Returns:
        None
        """
        self.arcs = []
        self.value = None
        self.accept = False
        self.inputcount = 0

    def __repr__(self):
        """
        Returns a string representation of the node.

        Parameters:
        None

        Returns:
        str: The string representation of the node.
        """
        return f"<{[(a.label, a.value) for a in self.arcs]!r}>"

    def digest(self):
        """
        Calculates and returns the digest of the node.

        Parameters:
        None

        Returns:
        bytes: The digest of the node.
        """
        if self._digest is None:
            d = sha1()
            vtype = self.owner.vtype
            for arc in self.arcs:
                d.update(arc.label)
                if arc.target:
                    d.update(pack_long(arc.target))
                else:
                    d.update(b"z")
                if arc.value:
                    d.update(vtype.to_bytes(arc.value))
                if arc.accept:
                    d.update(b"T")
            self._digest = d.digest()
        return self._digest

    def edges(self):
        """
        Returns the arcs of the node.

        Parameters:
        None

        Returns:
        list: The arcs of the node.
        """
        return self.arcs

    def last_value(self, label):
        """
        Returns the value of the last arc with the specified label.

        Parameters:
        - label: The label of the arc.

        Returns:
        object: The value of the last arc with the specified label.
        """
        assert self.arcs[-1].label == label
        return self.arcs[-1].value

    def add_arc(self, label, target):
        """
        Adds a new arc to the node with the specified label and target.

        Parameters:
        - label: The label of the arc.
        - target: The target of the arc.

        Returns:
        None
        """
        self.arcs.append(Arc(label, target))

    def replace_last(self, label, target, accept, acceptval=None):
        """
        Replaces the last arc with the specified label, target, accept flag, and accept value.

        Parameters:
        - label: The label of the arc.
        - target: The target of the arc.
        - accept: The accept flag of the arc.
        - acceptval: The accept value of the arc.

        Returns:
        None
        """
        arc = self.arcs[-1]
        assert arc.label == label, f"{arc.label!r} != {label!r}"
        arc.target = target
        arc.accept = accept
        arc.acceptval = acceptval

    def delete_last(self, label, target):
        """
        Deletes the last arc with the specified label and target.

        Parameters:
        - label: The label of the arc.
        - target: The target of the arc.

        Returns:
        None
        """
        arc = self.arcs.pop()
        assert arc.label == label
        assert arc.target == target

    def set_last_value(self, label, value):
        """
        Sets the value of the last arc with the specified label.

        Parameters:
        - label: The label of the arc.
        - value: The value to set.

        Returns:
        None
        """
        arc = self.arcs[-1]
        assert arc.label == label, f"{arc.label!r}->{label!r}"
        arc.value = value

    def prepend_value(self, prefix):
        """
        Prepends the specified prefix to the values of all arcs and the node's value.

        Parameters:
        - prefix: The prefix to prepend.

        Returns:
        None
        """
        add = self.owner.vtype.add
        for arc in self.arcs:
            arc.value = add(prefix, arc.value)
        if self.accept:
            self.value = add(prefix, self.value)


class Arc:
    """
    Represents a directed arc between two nodes in an FSA/FST graph.

    Attributes:
        label (bytes): The label bytes for this arc. For a word graph, this will be a character.
        target (int): The address of the node at the endpoint of this arc.
        value: The inner FST value at the endpoint of this arc.
        accept (bool): Whether the endpoint of this arc is an accept state (e.g. the end of a valid word).
        acceptval: If the endpoint of this arc is an accept state, the final FST value for that accepted state.
        lastarc: True if this is the last outgoing arc from the previous node.
        endpos: The end position of the arc.

    Methods:
        __init__: Initializes a new instance of the Arc class.
        __repr__: Returns a string representation of the Arc object.
        __eq__: Compares two Arc objects for equality.
        copy: Creates a copy of the Arc object.

    """

    __slots__ = ("label", "target", "accept", "value", "lastarc", "acceptval", "endpos")

    def __init__(
        self,
        label=None,
        target=None,
        value=None,
        accept=False,
        acceptval=None,
        lastarc=None,
        endpos=None,
    ):
        """
        Initializes a new instance of the Arc class.

        Args:
            label (bytes, optional): The label bytes for this arc. For a word graph, this will be a character.
            target (int, optional): The address of the node at the endpoint of this arc.
            value (optional): The inner FST value at the endpoint of this arc.
            accept (bool, optional): Whether the endpoint of this arc is an accept state (e.g. the end of a valid word).
            acceptval (optional): If the endpoint of this arc is an accept state, the final FST value for that accepted state.
            lastarc (optional): True if this is the last outgoing arc from the previous node.
            endpos (optional): The end position of the arc.
        """

        self.label = label
        self.target = target
        self.value = value
        self.accept = accept
        self.acceptval = acceptval
        self.lastarc = lastarc
        self.endpos = endpos

    def __repr__(self):
        """
        Returns a string representation of the Arc object.

        Returns:
            str: A string representation of the Arc object.
        """
        return "<{!r}-{} {}{}>".format(
            self.label,
            self.target,
            "." if self.accept else "",
            f" {self.value!r}" if self.value else "",
        )

    def __eq__(self, other):
        """
        Compares two Arc objects for equality.

        Args:
            other (Arc): The other Arc object to compare.

        Returns:
            bool: True if the two Arc objects are equal, False otherwise.
        """
        if (
            isinstance(other, self.__class__)
            and self.accept == other.accept
            and self.lastarc == other.lastarc
            and self.target == other.target
            and self.value == other.value
            and self.label == other.label
        ):
            return True
        return False

    def copy(self):
        """
        Creates a copy of the Arc object.

        Returns:
            Arc: A copy of the Arc object.
        """
        # This is faster than using the copy module
        return Arc(
            label=self.label,
            target=self.target,
            value=self.value,
            accept=self.accept,
            acceptval=self.acceptval,
            lastarc=self.lastarc,
            endpos=self.endpos,
        )


# Graph writer


class GraphWriter:
    """Writes an FSA/FST graph to disk.

    The GraphWriter class is used to write an FSA/FST graph to disk. It provides
    methods for inserting keys into the graph, starting and finishing fields,
    and closing the graph.

    Usage:
    >>> gw = GraphWriter(my_file)
    >>> gw.insert("alfa")
    >>> gw.insert("bravo")
    >>> gw.insert("charlie")
    >>> gw.close()

    The graph writer can write separate graphs for multiple fields. Use
    ``start_field(name)`` and ``finish_field()`` to separate fields.

    Usage:
    >>> gw = GraphWriter(my_file)
    >>> gw.start_field("content")
    >>> gw.insert("alfalfa")
    >>> gw.insert("apple")
    >>> gw.finish_field()
    >>> gw.start_field("title")
    >>> gw.insert("artichoke")
    >>> gw.finish_field()
    >>> gw.close()

    Attributes:
        version (int): The version number of the graph writer.

    Args:
        dbfile (file): The file to write the graph to.
        vtype (class, optional): A class to use for storing values. Defaults to None.
        merge (function, optional): A function that merges two values. Defaults to None.

    Raises:
        ValueError: If the field name is equivalent to False.
        Exception: If finish_field() is called before start_field().

    """

    version = 1

    def __init__(self, dbfile, vtype=None, merge=None):
        """
        Initializes a new instance of the GraphWriter class.

        Args:
            dbfile (file): The file to write the graph to.
            vtype (class, optional): A class to use for storing values. Defaults to None.
            merge (function, optional): A function that merges two values. Defaults to None.
        """

        self.dbfile = dbfile
        self.vtype = vtype
        self.merge = merge
        self.fieldroots = {}
        self.arc_count = 0
        self.node_count = 0
        self.fixed_count = 0

        dbfile.write(b"GRPH")
        dbfile.write_int(self.version)
        dbfile.write_uint(0)

        self._infield = False

    def start_field(self, fieldname):
        """
        Starts a new graph for the given field.

        Args:
            fieldname (str): The name of the field.

        Raises:
            ValueError: If the field name is equivalent to False.
            Exception: If start_field() is called while already in a field.
        """

        if not fieldname:
            raise ValueError("Field name cannot be equivalent to False")
        if self._infield:
            self.finish_field()
        self.fieldname = fieldname
        self.seen = {}
        self.nodes = [UncompiledNode(self)]
        self.lastkey = ""
        self._inserted = False
        self._infield = True

    def finish_field(self):
        """
        Finishes the graph for the current field.

        Raises:
            Exception: If finish_field() is called before start_field().
        """

        if not self._infield:
            raise Exception("Called finish_field before start_field")
        self._infield = False
        if self._inserted:
            self.fieldroots[self.fieldname] = self._finish()
        self.fieldname = None

    def close(self):
        """
        Finishes the current graph and closes the underlying file.
        """

        if self.fieldname is not None:
            self.finish_field()
        dbfile = self.dbfile
        here = dbfile.tell()
        dbfile.write_pickle(self.fieldroots)
        dbfile.flush()
        dbfile.seek(4 + _INT_SIZE)  # Seek past magic and version number
        dbfile.write_uint(here)
        dbfile.close()

    def insert(self, key, value=None):
        """
        Inserts the given key into the graph.

        Args:
            key (bytes, str): The key to insert into the graph.
            value (object, optional): The value to encode in the graph along with the key. Defaults to None.

        Raises:
            Exception: If insert() is called before starting a field.
            KeyError: If the key is null or out of order.
            ValueError: If the value is not valid for the value type.
        """

        if not self._infield:
            raise Exception(f"Inserted {key!r} before starting a field")
        self._inserted = True
        key = to_labels(key)  # Python 3 sucks

        vtype = self.vtype
        lastkey = self.lastkey
        nodes = self.nodes
        if len(key) < 1:
            raise KeyError(f"Can't store a null key {key!r}")
        if lastkey and lastkey > key:
            raise KeyError(f"Keys out of order {lastkey!r}..{key!r}")

        # Find the common prefix shared by this key and the previous one
        prefixlen = 0
        for i in range(min(len(lastkey), len(key))):
            if lastkey[i] != key[i]:
                break
            prefixlen += 1
        # Compile the nodes after the prefix, since they're not shared
        self._freeze_tail(prefixlen + 1)

        # Create new nodes for the parts of this key after the shared prefix
        for char in key[prefixlen:]:
            node = UncompiledNode(self)
            # Create an arc to this node on the previous node
            nodes[-1].add_arc(char, node)
            nodes.append(node)
        # Mark the last node as an accept state
        lastnode = nodes[-1]
        lastnode.accept = True

        if vtype:
            if value is not None and not vtype.is_valid(value):
                raise ValueError(f"{value!r} is not valid for {vtype}")

            # Push value commonalities through the tree
            common = None
            for i in range(1, prefixlen + 1):
                node = nodes[i]
                parent = nodes[i - 1]
                lastvalue = parent.last_value(key[i - 1])
                if lastvalue is not None:
                    common = vtype.common(value, lastvalue)
                    suffix = vtype.subtract(lastvalue, common)
                    parent.set_last_value(key[i - 1], common)
                    node.prepend_value(suffix)
                else:
                    common = suffix = None
                value = vtype.subtract(value, common)

            if key == lastkey:
                # If this key is a duplicate, merge its value with the value of
                # the previous (same) key
                lastnode.value = self.merge(lastnode.value, value)
            else:
                nodes[prefixlen].set_last_value(key[prefixlen], value)
        elif value:
            raise Exception(f"Value {value!r} but no value type")

        self.lastkey = key

    def _freeze_tail(self, prefixlen):
        nodes = self.nodes
        lastkey = self.lastkey
        downto = max(1, prefixlen)

        while len(nodes) > downto:
            node = nodes.pop()
            parent = nodes[-1]
            inlabel = lastkey[len(nodes) - 1]

            self._compile_targets(node)
            accept = node.accept or len(node.arcs) == 0
            address = self._compile_node(node)
            parent.replace_last(inlabel, address, accept, node.value)

    def _finish(self):
        nodes = self.nodes
        root = nodes[0]
        # Minimize nodes in the last word's suffix
        self._freeze_tail(0)
        # Compile remaining targets
        self._compile_targets(root)
        return self._compile_node(root)

    def _compile_targets(self, node):
        for arc in node.arcs:
            if isinstance(arc.target, UncompiledNode):
                n = arc.target
                if len(n.arcs) == 0:
                    arc.accept = n.accept = True
                arc.target = self._compile_node(n)

    def _compile_node(self, uncnode):
        seen = self.seen

        if len(uncnode.arcs) == 0:
            # Leaf node
            address = self._write_node(uncnode)
        else:
            d = uncnode.digest()
            address = seen.get(d)
            if address is None:
                address = self._write_node(uncnode)
                seen[d] = address
        return address

    def _write_node(self, uncnode):
        vtype = self.vtype
        dbfile = self.dbfile
        arcs = uncnode.arcs
        numarcs = len(arcs)

        if not numarcs:
            if uncnode.accept:
                return None
            else:
                # What does it mean for an arc to stop but not be accepted?
                raise Exception
        self.node_count += 1

        buf = StructFile(BytesIO())
        nodestart = dbfile.tell()
        # self.count += 1
        # self.arccount += numarcs

        fixedsize = -1
        arcstart = buf.tell()
        for i, arc in enumerate(arcs):
            self.arc_count += 1
            target = arc.target
            label = arc.label

            flags = 0
            if len(label) > 1:
                flags += MULTIBYTE_LABEL
            if i == numarcs - 1:
                flags += ARC_LAST
            if arc.accept:
                flags += ARC_ACCEPT
            if target is None:
                flags += ARC_STOP
            if arc.value is not None:
                flags += ARC_HAS_VAL
            if arc.acceptval is not None:
                flags += ARC_HAS_ACCEPT_VAL

            buf.write(pack_byte(flags))
            if len(label) > 1:
                buf.write(varint(len(label)))
            buf.write(label)
            if target is not None:
                buf.write(pack_uint(target))
            if arc.value is not None:
                vtype.write(buf, arc.value)
            if arc.acceptval is not None:
                vtype.write(buf, arc.acceptval)

            here = buf.tell()
            thissize = here - arcstart
            arcstart = here
            if fixedsize == -1:
                fixedsize = thissize
            elif fixedsize > 0 and thissize != fixedsize:
                fixedsize = 0

        if fixedsize > 0:
            # Write a fake arc containing the fixed size and number of arcs
            dbfile.write_byte(255)  # FIXED_SIZE
            dbfile.write_int(fixedsize)
            dbfile.write_int(numarcs)
            self.fixed_count += 1
        dbfile.write(buf.file.getvalue())

        return nodestart


# Graph reader


class BaseGraphReader:
    """Base class for reading graph data structures."""

    def cursor(self, rootname=None):
        """
        Returns a cursor object for traversing the graph.

        Args:
            rootname (str, optional): The name of the root node. Defaults to None.

        Returns:
            Cursor: A cursor object.

        """
        return Cursor(self, self.root(rootname))

    def has_root(self, rootname):
        """
        Checks if the graph has a root node with the given name.

        Args:
            rootname (str): The name of the root node.

        Returns:
            bool: True if the root node exists, False otherwise.

        Raises:
            NotImplementedError: This method must be implemented by subclasses.

        """
        raise NotImplementedError

    def root(self, rootname=None):
        """
        Returns the root node of the graph.

        Args:
            rootname (str, optional): The name of the root node. Defaults to None.

        Returns:
            Node: The root node.

        Raises:
            NotImplementedError: This method must be implemented by subclasses.

        """
        raise NotImplementedError

    # Low level methods

    def arc_at(self, address, arc):
        """
        Retrieves the arc at the given address.

        Args:
            address (int): The address of the arc.
            arc (Arc): An arc object to store the retrieved arc.

        Raises:
            NotImplementedError: This method must be implemented by subclasses.

        """
        raise NotImplementedError

    def iter_arcs(self, address, arc=None):
        """
        Iterates over the arcs starting from the given address.

        Args:
            address (int): The starting address.
            arc (Arc, optional): An arc object to store each iterated arc. Defaults to None.

        Yields:
            Arc: The iterated arcs.

        Raises:
            NotImplementedError: This method must be implemented by subclasses.

        """
        raise NotImplementedError

    def find_arc(self, address, label, arc=None):
        """
        Finds the arc with the given label starting from the given address.

        Args:
            address (int): The starting address.
            label (str): The label of the arc to find.
            arc (Arc, optional): An arc object to store the found arc. Defaults to None.

        Returns:
            Arc: The found arc, or None if not found.

        Raises:
            NotImplementedError: This method must be implemented by subclasses.

        """
        arc = arc or Arc()
        for arc in self.iter_arcs(address, arc):
            thislabel = arc.label
            if thislabel == label:
                return arc
            elif thislabel > label:
                return None

    # Convenience methods

    def list_arcs(self, address):
        """
        Returns a list of arcs starting from the given address.

        Args:
            address (int): The starting address.

        Returns:
            list: A list of arcs.

        """
        return [arc.copy() for arc in self.iter_arcs(address)]

    def arc_dict(self, address):
        """
        Returns a dictionary of arcs starting from the given address.

        Args:
            address (int): The starting address.

        Returns:
            dict: A dictionary of arcs, where the keys are the arc labels.

        """
        return {arc.label: arc.copy() for arc in self.iter_arcs(address)}

    def find_path(self, path, arc=None, address=None):
        """
        Finds a path in the graph based on a sequence of labels.

        Args:
            path (list): A list of labels representing the path.
            arc (Arc, optional): An arc object to store the found arc. Defaults to None.
            address (int, optional): The starting address. Defaults to None.

        Returns:
            Arc: The arc at the end of the path, or None if the path is not found.

        """
        path = to_labels(path)

        if arc:
            address = address if address is not None else arc.target
        else:
            arc = Arc()

        if address is None:
            address = self._root

        find_arc = self.find_arc
        for label in path:
            if address is None:
                return None
            if not find_arc(address, label, arc):
                return None
            address = arc.target
        return arc


class GraphReader(BaseGraphReader):
    """
    A class for reading graph data from a database file.

    Args:
        dbfile (file-like object): The database file to read from.
        rootname (str, optional): The name of the root node. If not provided and there is only one root, it will be used automatically. Defaults to None.
        vtype (object, optional): The type of values associated with the arcs. Defaults to None.
        filebase (int, optional): The base offset in the file where the graph data starts. Defaults to 0.

    Attributes:
        dbfile (file-like object): The database file being read.
        vtype (object): The type of values associated with the arcs.
        filebase (int): The base offset in the file where the graph data starts.
        version (int): The version of the graph data.
        roots (dict): A dictionary of root nodes in the graph.
        _root (object): The current root node.

    Raises:
        FileVersionError: If the database file has an invalid version.

    """

    def __init__(self, dbfile, rootname=None, vtype=None, filebase=0):
        self.dbfile = dbfile
        self.vtype = vtype
        self.filebase = filebase

        dbfile.seek(filebase)
        magic = dbfile.read(4)
        if magic != b"GRPH":
            raise FileVersionError
        self.version = dbfile.read_int()
        dbfile.seek(dbfile.read_uint())
        self.roots = dbfile.read_pickle()

        self._root = None
        if rootname is None and len(self.roots) == 1:
            # If there's only one root, just use it. Have to wrap a list around
            # the keys() method here because of Python 3.
            rootname = list(self.roots.keys())[0]
        if rootname is not None:
            self._root = self.root(rootname)

    def close(self):
        """
        Close the database file.

        """
        self.dbfile.close()

    def has_root(self, rootname):
        """
        Check if a root node with the given name exists in the graph.

        Args:
            rootname (str): The name of the root node.

        Returns:
            bool: True if the root node exists, False otherwise.

        """
        return rootname in self.roots

    def root(self, rootname=None):
        """
        Get the root node of the graph.

        Args:
            rootname (str, optional): The name of the root node. If not provided, returns the current root node.

        Returns:
            object: The root node.

        """
        if rootname is None:
            return self._root
        else:
            return self.roots[rootname]

    def default_root(self):
        """
        Get the default root node of the graph.

        Returns:
            object: The default root node.

        """
        return self._root

    def arc_at(self, address, arc=None):
        """
        Get the arc at the specified address in the graph.

        Args:
            address (int): The address of the arc.
            arc (Arc, optional): An instance of the Arc class to store the arc data. If not provided, a new Arc instance will be created.

        Returns:
            Arc: The arc at the specified address.

        """
        arc = arc or Arc()
        self.dbfile.seek(address)
        return self._read_arc(arc)

    def iter_arcs(self, address, arc=None):
        """
        Iterate over the arcs starting from the specified address in the graph.

        Args:
            address (int): The address of the first arc.
            arc (Arc, optional): An instance of the Arc class to store the arc data. If not provided, a new Arc instance will be created.

        Yields:
            Arc: The arcs in the graph.

        """
        arc = arc or Arc()
        _read_arc = self._read_arc

        self.dbfile.seek(address)
        while True:
            _read_arc(arc)
            yield arc
            if arc.lastarc:
                break

    def find_arc(self, address, label, arc=None):
        """
        Find the arc with the specified label starting from the specified address in the graph.

        Args:
            address (int): The address of the first arc.
            label (bytes): The label of the arc.
            arc (Arc, optional): An instance of the Arc class to store the arc data. If not provided, a new Arc instance will be created.

        Returns:
            Arc: The arc with the specified label, or None if not found.

        """
        # Overrides the default scanning implementation

        arc = arc or Arc()
        dbfile = self.dbfile
        dbfile.seek(address)

        # If records are fixed size, we can do a binary search
        finfo = self._read_fixed_info()
        if finfo:
            size, count = finfo
            address = dbfile.tell()
            if count > 2:
                return self._binary_search(address, size, count, label, arc)

        # If records aren't fixed size, fall back to the parent's linear
        # search method
        return BaseGraphReader.find_arc(self, address, label, arc)

    def _read_arc(self, toarc=None):
        """
        Read an arc from the database file.

        Args:
            toarc (Arc, optional): An instance of the Arc class to store the arc data. If not provided, a new Arc instance will be created.

        Returns:
            Arc: The arc read from the database file.

        """
        toarc = toarc or Arc()
        dbfile = self.dbfile
        flags = dbfile.read_byte()
        if flags == 255:
            # This is a fake arc containing fixed size information; skip it
            # and read the next arc
            dbfile.seek(_INT_SIZE * 2, 1)
            flags = dbfile.read_byte()
        toarc.label = self._read_label(flags)
        return self._read_arc_data(flags, toarc)

    def _read_label(self, flags):
        """
        Read the label of an arc from the database file.

        Args:
            flags (int): The flags indicating the label type.

        Returns:
            bytes: The label of the arc.

        """
        dbfile = self.dbfile
        if flags & MULTIBYTE_LABEL:
            length = dbfile.read_varint()
        else:
            length = 1
        label = dbfile.read(length)
        return label

    def _read_fixed_info(self):
        """
        Read the fixed size information from the database file.

        Returns:
            tuple: A tuple containing the size and count of the fixed size records, or None if not applicable.

        """
        dbfile = self.dbfile

        flags = dbfile.read_byte()
        if flags == 255:
            size = dbfile.read_int()
            count = dbfile.read_int()
            return (size, count)
        else:
            return None

    def _read_arc_data(self, flags, arc):
        """
        Read the data of an arc from the database file.

        Args:
            flags (int): The flags indicating the arc properties.
            arc (Arc): An instance of the Arc class to store the arc data.

        Returns:
            Arc: The arc with the data read from the database file.

        """
        dbfile = self.dbfile
        accept = arc.accept = bool(flags & ARC_ACCEPT)
        arc.lastarc = flags & ARC_LAST
        if flags & ARC_STOP:
            arc.target = None
        else:
            arc.target = dbfile.read_uint()
        if flags & ARC_HAS_VAL:
            arc.value = self.vtype.read(dbfile)
        else:
            arc.value = None
        if accept and flags & ARC_HAS_ACCEPT_VAL:
            arc.acceptval = self.vtype.read(dbfile)
        arc.endpos = dbfile.tell()
        return arc

    def _binary_search(self, address, size, count, label, arc):
        """
        Perform a binary search to find the arc with the specified label.

        Args:
            address (int): The address of the first arc.
            size (int): The size of each arc record.
            count (int): The number of arcs.
            label (bytes): The label of the arc to find.
            arc (Arc): An instance of the Arc class to store the arc data.

        Returns:
            Arc: The arc with the specified label, or None if not found.

        """
        dbfile = self.dbfile
        _read_label = self._read_label

        lo = 0
        hi = count
        while lo < hi:
            mid = (lo + hi) // 2
            midaddr = address + mid * size
            dbfile.seek(midaddr)
            flags = dbfile.read_byte()
            midlabel = self._read_label(flags)
            if midlabel == label:
                arc.label = midlabel
                return self._read_arc_data(flags, arc)
            elif midlabel < label:
                lo = mid + 1
            else:
                hi = mid
        if lo == count:
            return None


def to_labels(key):
    """
    Takes a string and returns a list of bytestrings, suitable for use as
    a key or path in an FSA/FST graph.

    Args:
        key (str or bytes or list or tuple): The input string.

    Returns:
        tuple: A tuple of bytestrings representing the input string.

    Raises:
        TypeError: If the input contains a non-bytestring.

    Example:
        >>> to_labels('hello')
        (b'h', b'e', b'l', b'l', b'o')
    """

    # Convert to tuples of bytestrings (must be tuples so they can be hashed)
    keytype = type(key)

    # I hate the Python 3 bytes object so friggin much
    if keytype is tuple or keytype is list:
        if not all(isinstance(e, bytes) for e in key):
            raise TypeError(f"{key!r} contains a non-bytestring")
        if keytype is list:
            key = tuple(key)
    elif isinstance(key, bytes):
        key = tuple(key[i : i + 1] for i in range(len(key)))
    elif isinstance(key, str):
        key = tuple(utf8encode(key[i : i + 1])[0] for i in range(len(key)))
    else:
        raise TypeError(f"Don't know how to convert {key!r}")
    return key


# Within edit distance function


def within(graph, text, k=1, prefix=0, address=None):
    """
    Yields a series of keys in the given graph within ``k`` edit distance of
    ``text``. If ``prefix`` is greater than 0, all keys must match the first
    ``prefix`` characters of ``text``.

    Args:
        graph (Graph): The graph to search within.
        text (str): The text to search for.
        k (int, optional): The maximum edit distance allowed. Defaults to 1.
        prefix (int, optional): The number of characters that must match at the beginning of the keys. Defaults to 0.
        address (int, optional): The starting address in the graph. Defaults to None.

    Yields:
        str: A key within the specified edit distance of the text.

    """
    text = to_labels(text)
    if address is None:
        address = graph._root

    sofar = emptybytes
    accept = False
    if prefix:
        prefixchars = text[:prefix]
        arc = graph.find_path(prefixchars, address=address)
        if arc is None:
            return
        sofar = emptybytes.join(prefixchars)
        address = arc.target
        accept = arc.accept

    stack = [(address, k, prefix, sofar, accept)]
    seen = set()
    while stack:
        state = stack.pop()
        # Have we already tried this state?
        if state in seen:
            continue
        seen.add(state)

        address, k, i, sofar, accept = state
        # If we're at the end of the text (or deleting enough chars would get
        # us to the end and still within K), and we're in the accept state,
        # yield the current result
        if (len(text) - i <= k) and accept:
            yield utf8decode(sofar)[0]

        # If we're in the stop state, give up
        if address is None:
            continue

        # Exact match
        if i < len(text):
            arc = graph.find_arc(address, text[i])
            if arc:
                stack.append((arc.target, k, i + 1, sofar + text[i], arc.accept))
        # If K is already 0, can't do any more edits
        if k < 1:
            continue
        k -= 1

        arcs = graph.arc_dict(address)
        # Insertions
        stack.extend(
            (arc.target, k, i, sofar + char, arc.accept) for char, arc in arcs.items()
        )

        # Deletion, replacement, and transpo only work before the end
        if i >= len(text):
            continue
        char = text[i]

        # Deletion
        stack.append((address, k, i + 1, sofar, False))
        # Replacement
        for char2, arc in arcs.items():
            if char2 != char:
                stack.append((arc.target, k, i + 1, sofar + char2, arc.accept))
        # Transposition
        if i < len(text) - 1:
            char2 = text[i + 1]
            if char != char2 and char2 in arcs:
                # Find arc from next char to this char
                target = arcs[char2].target
                if target:
                    arc = graph.find_arc(target, char)
                    if arc:
                        stack.append(
                            (arc.target, k, i + 2, sofar + char2 + char, arc.accept)
                        )


# Utility functions


def dump_graph(graph, address=None, tab=0, out=None):
    """
    Dump the graph structure starting from the given address.

    Args:
        graph (Graph): The graph object.
        address (int, optional): The address to start dumping from. If not provided, the root address of the graph will be used.
        tab (int, optional): The number of tabs to indent the output. Defaults to 0.
        out (file-like object, optional): The output stream to write the dumped graph. Defaults to sys.stdout.

    Returns:
        None

    """
    if address is None:
        address = graph._root
    if out is None:
        out = sys.stdout

    here = "%06d" % address
    for i, arc in enumerate(graph.list_arcs(address)):
        if i == 0:
            out.write(here)
        else:
            out.write(" " * 6)
        out.write("  " * tab)
        out.write(f"{arc.label!r} {arc.target!r} {arc.accept} {arc.value!r}\n")
        if arc.target is not None:
            dump_graph(graph, arc.target, tab + 1, out=out)
