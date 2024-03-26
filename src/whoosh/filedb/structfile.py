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

from array import array
from copy import copy
from io import BytesIO
from pickle import dump, load
from struct import calcsize

from whoosh.system import (
    _FLOAT_SIZE,
    _INT_SIZE,
    _LONG_SIZE,
    _SHORT_SIZE,
    IS_LITTLE,
    pack_byte,
    pack_float,
    pack_int,
    pack_long,
    pack_sbyte,
    pack_uint,
    pack_uint_le,
    pack_ulong,
    pack_ushort,
    pack_ushort_le,
    unpack_byte,
    unpack_float,
    unpack_int,
    unpack_long,
    unpack_sbyte,
    unpack_uint,
    unpack_uint_le,
    unpack_ulong,
    unpack_ushort,
    unpack_ushort_le,
)
from whoosh.util.varints import decode_signed_varint, read_varint, signed_varint, varint

_SIZEMAP = {typecode: calcsize(typecode) for typecode in "bBiIhHqQf"}
_ORDERMAP = {"little": "<", "big": ">"}

_types = (("sbyte", "b"), ("ushort", "H"), ("int", "i"), ("long", "q"), ("float", "f"))


# Main function


class StructFile:
    """A "structured file" object that wraps a given file object and provides
    additional methods for writing and reading structured data.

    This class provides a convenient way to work with structured data in a file.
    It wraps a file object and adds methods for reading and writing various data
    types, such as strings, integers, floats, and arrays.

    Usage:
    ------
    To use the StructFile class, create an instance by passing a file object to
    the constructor:

    >>> with open('data.bin', 'wb') as file:
    ...     sf = StructFile(file)

    You can then use the various methods provided by StructFile to read and write
    data:

    >>> sf.write_string('Hello, World!')
    >>> sf.write_int(42)
    >>> sf.write_float(3.14)

    To read data from the file, use the corresponding read methods:

    >>> string = sf.read_string()
    >>> integer = sf.read_int()
    >>> float_num = sf.read_float()

    Methods:
    --------
    The StructFile class provides the following methods:

    - read: Read a specified number of bytes from the file.
    - write: Write data to the file.
    - read_string: Read a string from the file.
    - write_string: Write a string to the file.
    - read_int: Read an integer from the file.
    - write_int: Write an integer to the file.
    - read_float: Read a float from the file.
    - write_float: Write a float to the file.
    - read_array: Read an array from the file.
    - write_array: Write an array to the file.
    - seek: Move the file pointer to a specified position.
    - tell: Get the current position of the file pointer.
    - flush: Flush the buffer of the wrapped file.
    - close: Close the wrapped file.

    Note:
    -----
    The StructFile class is designed to work with binary files. It provides
    methods for reading and writing various data types in their binary
    representation. Make sure to open the file in binary mode when using
    StructFile.

    """

    def __init__(self, fileobj, name=None, onclose=None):
        """
        Initialize a StructFile object.

        Args:
            fileobj (file-like object): The file-like object to be wrapped by the StructFile.
            name (str, optional): The name of the file. Defaults to None.
            onclose (callable, optional): A callable object to be called when the StructFile is closed. Defaults to None.

        Attributes:
            file (file-like object): The wrapped file-like object.
            _name (str): The name of the file.
            onclose (callable): A callable object to be called when the StructFile is closed.
            is_closed (bool): Indicates whether the StructFile is closed or not.
            is_real (bool): Indicates whether the wrapped file-like object has a fileno() method.
            fileno (method): The fileno() method of the wrapped file-like object.

        Note:
            The StructFile is a wrapper around a file-like object that provides additional functionality.
            It keeps track of the file's name, whether it is closed, and whether it is a real file object.
            The fileno() method is only available if the wrapped file-like object has a fileno() method.

        Usage:
            # Create a StructFile object
            fileobj = open("example.txt", "r")
            struct_file = StructFile(fileobj, "example.txt", onclose=my_callback)

            # Access the wrapped file object
            file = struct_file.file

            # Check if the StructFile is closed
            is_closed = struct_file.is_closed

            # Check if the wrapped file object is a real file object
            is_real = struct_file.is_real

            # Call the onclose callback when the StructFile is closed
            struct_file.onclose = my_callback

            # Get the fileno of the wrapped file object
            fileno = struct_file.fileno()
        """
        self.file = fileobj
        self._name = name
        self.onclose = onclose
        self.is_closed = False

        self.is_real = hasattr(fileobj, "fileno")
        if self.is_real:
            self.fileno = fileobj.fileno

    def __repr__(self):
        """
        Return a string representation of the StructFile object.

        The returned string includes the class name and the name of the file.

        Returns:
            str: A string representation of the StructFile object.

        Example:
            >>> file = StructFile("example.txt")
            >>> repr(file)
            'StructFile("example.txt")'
        """
        return f"{self.__class__.__name__}({self._name!r})"

    def __str__(self):
        """
        Returns a string representation of the StructFile object.

        The string representation is the name of the file associated with the StructFile object.

        Returns:
            str: The name of the file associated with the StructFile object.

        Example:
            >>> file = StructFile("example.txt")
            >>> str(file)
            'example.txt'
        """
        return self._name

    def __enter__(self):
        """
        Enter method for the StructFile context manager.

        This method is automatically called when using the `with` statement to open a StructFile.
        It returns the StructFile object itself, allowing it to be used within the `with` block.

        Returns:
            StructFile: The StructFile object itself.

        Example:
            with StructFile("data.bin", "rb") as file:
                # Perform operations on the file
                data = file.read(1024)
                # ...
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Closes the file when exiting a context manager.

        Args:
            exc_type (type): The type of the exception raised, if any.
            exc_val (Exception): The exception raised, if any.
            exc_tb (traceback): The traceback object associated with the exception, if any.

        Returns:
            None

        Raises:
            Any exception raised during the closing process.

        This method is automatically called when exiting a `with` statement. It ensures that the file is properly closed,
        regardless of whether an exception occurred or not. It should not be called directly.
        """
        self.close()

    def __iter__(self):
        """
        Returns an iterator over the lines of the file.

        This method allows the `StructFile` object to be used in a `for` loop or
        with other iterable constructs. It returns an iterator that yields each
        line of the file.

        Returns:
            An iterator over the lines of the file.

        Example:
            >>> with StructFile('data.txt') as file:
            ...     for line in file:
            ...         print(line)
        """
        return iter(self.file)

    def raw_file(self):
        """
        Returns the raw file object associated with this StructFile.

        This method returns the underlying file object that is used by the StructFile
        instance. It can be used to perform low-level file operations directly on the file.

        Returns:
            file: The raw file object associated with this StructFile.

        Example:
            # Open a StructFile
            sf = StructFile("data.bin", "rb")

            # Get the raw file object
            f = sf.raw_file()

            # Perform low-level file operations
            f.seek(0)
            data = f.read(1024)

        Note:
            Modifying the raw file object directly may lead to unexpected behavior and
            should be done with caution. It is recommended to use the methods provided by
            the StructFile class for reading and writing data to the file.
        """
        return self.file

    def read(self, *args, **kwargs):
        """
        Read data from the file.

        This method reads data from the file and returns it. It delegates the actual reading
        operation to the underlying file object.

        Parameters:
        *args: Variable length argument list to be passed to the underlying file object's read method.
        **kwargs: Arbitrary keyword arguments to be passed to the underlying file object's read method.

        Returns:
        The data read from the file.

        Example usage:
        file = StructFile("example.txt")
        data = file.read(10)  # Read 10 bytes from the file
        """

        return self.file.read(*args, **kwargs)

    def readline(self, *args, **kwargs):
        """
        Read and return a line from the file.

        This method reads a line from the file and returns it as a string. It delegates the actual reading to the underlying file object.

        Parameters:
        *args: Variable length argument list to be passed to the underlying file object's readline method.
        **kwargs: Arbitrary keyword arguments to be passed to the underlying file object's readline method.

        Returns:
        str: The line read from the file.

        Raises:
        Any exceptions raised by the underlying file object's readline method.

        Example:
        >>> file = StructFile("example.txt")
        >>> line = file.readline()
        >>> print(line)
        "This is an example line."

        Note:
        This method assumes that the file has been opened in text mode.
        """
        return self.file.readline(*args, **kwargs)

    def write(self, *args, **kwargs):
        """
        Writes the specified data to the file.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            int: The number of bytes written to the file.

        Raises:
            OSError: If an error occurs while writing to the file.

        Example:
            To write a string to the file:

            >>> file.write("Hello, World!")

        Note:
            This method delegates the write operation to the underlying file object.
        """
        return self.file.write(*args, **kwargs)

    def tell(self, *args, **kwargs):
        """
        Return the current file position.

        This method returns the current file position in bytes. It delegates the call to the underlying file object's `tell()` method.

        :param args: Optional positional arguments to be passed to the `tell()` method of the underlying file object.
        :param kwargs: Optional keyword arguments to be passed to the `tell()` method of the underlying file object.
        :return: The current file position in bytes.
        """
        return self.file.tell(*args, **kwargs)

    def seek(self, *args, **kwargs):
        """
        Change the file position to the given offset.

        This method is a wrapper around the `seek` method of the underlying file object.
        It allows you to change the current position within the file.

        Parameters:
            *args: Variable length argument list to be passed to the `seek` method.
            **kwargs: Arbitrary keyword arguments to be passed to the `seek` method.

        Returns:
            The new absolute position within the file.

        Raises:
            OSError: If an error occurs while seeking the file.

        Example:
            To seek to the beginning of the file:
            ```
            file.seek(0)
            ```

            To seek to a specific offset from the current position:
            ```
            file.seek(10, 1)
            ```

            To seek to a specific offset from the end of the file:
            ```
            file.seek(-10, 2)
            ```
        """
        return self.file.seek(*args, **kwargs)

    def truncate(self, *args, **kwargs):
        """
        Truncates the file to the specified size.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            int: The new size of the file after truncation.

        Raises:
            OSError: If an error occurs while truncating the file.

        Note:
            This method is a wrapper around the `truncate` method of the underlying file object.

        Example:
            # Truncate the file to 100 bytes
            size = file.truncate(100)
        """
        return self.file.truncate(*args, **kwargs)

    def flush(self):
        """
        Flushes the buffer of the wrapped file. This is a no-op if the
        wrapped file does not have a flush method.

        This method ensures that any buffered data in the file is written to the underlying storage.
        It is recommended to call this method after performing write operations to ensure data integrity.

        Usage:
            file = StructFile(...)
            # Perform write operations
            file.flush()

        Note:
            If the wrapped file does not have a flush method, this method does nothing.

        """
        if hasattr(self.file, "flush"):
            self.file.flush()

    def close(self):
        """
        Closes the wrapped file.

        This method closes the file object that is being wrapped by the StructFile.
        It is important to close the file after using it to free up system resources
        and ensure data integrity.

        Raises:
            ValueError: If the file is already closed.

        Usage:
            To close the StructFile object, simply call the close() method:

            file = StructFile(...)
            file.close()
        """
        if self.is_closed:
            raise ValueError("This file is already closed")
        if self.onclose:
            self.onclose(self)
        if hasattr(self.file, "close"):
            self.file.close()
        self.is_closed = True

    def subset(self, offset, length, name=None):
        """
        Returns a subset of the current StructFile object.

        Args:
            offset (int): The starting offset of the subset, in bytes.
            length (int): The length of the subset, in bytes.
            name (str, optional): The name of the subset. If not provided, the name of the current StructFile object is used.

        Returns:
            StructFile: A new StructFile object representing the subset.

        Raises:
            None.

        Example:
            # Create a StructFile object
            sf = StructFile(file, name="example.txt")

            # Get a subset of the StructFile object
            subset = sf.subset(10, 20, name="subset.txt")
        """
        from whoosh.filedb.compound import SubFile

        name = name or self._name
        return StructFile(SubFile(self.file, offset, length), name=name)

    def write_string(self, s):
        """
        Writes a string to the wrapped file.

        This method writes the length of the string first, so you can read the string back
        without having to know how long it was.

        :param s: The string to be written.
        :type s: str
        """
        self.write_varint(len(s))
        self.write(s)

    def write_string2(self, s):
        """
        Writes a string to the file.

        Args:
            s (str): The string to be written.

        Raises:
            TypeError: If the input is not a string.

        Notes:
            This method writes the length of the string as an unsigned short (2 bytes) followed by the string itself.
            The length of the string is encoded using the `pack_ushort` function.

        Example:
            >>> file = StructFile()
            >>> file.write_string2("Hello, World!")
        """
        self.write(pack_ushort(len(s)) + s)

    def write_string4(self, s):
        """
        Writes a string to the file using a custom 4-byte length prefix.

        Args:
            s (str): The string to be written.

        Raises:
            TypeError: If the input is not a string.

        Notes:
            This method writes the length of the string as a 4-byte integer
            followed by the string itself. The length prefix allows for efficient
            reading of the string later on.

        Example:
            >>> file.write_string4("Hello, World!")
        """
        self.write(pack_int(len(s)) + s)

    def read_string(self):
        """
        Reads a string from the wrapped file.

        This method reads a string from the file by first reading the length of the string
        using the `read_varint` method, and then reading the actual string using the `read` method.

        Returns:
            str: The string read from the file.

        Raises:
            IOError: If there is an error reading from the file.
        """
        return self.read(self.read_varint())

    def read_string2(self):
        """
        Reads a string from the file.

        This method reads a string from the file by first reading the length of the string as an unsigned short,
        and then reading the actual string data from the file.

        Returns:
            str: The string read from the file.

        Raises:
            IOError: If there is an error reading from the file.

        Usage:
            string = read_string2()
        """
        l = self.read_ushort()
        return self.read(l)

    def read_string4(self):
        """
        Reads a string from the file.

        This method reads a string from the file by first reading the length of the string
        as an integer using the `read_int()` method, and then reading the actual string
        using the `read()` method.

        Returns:
            str: The string read from the file.

        """
        l = self.read_int()
        return self.read(l)

    def get_string2(self, pos):
        """
        Retrieves a string from the file at the given position.

        Args:
            pos (int): The position in the file where the string starts.

        Returns:
            tuple: A tuple containing the string and the position of the next byte after the string.

        Raises:
            IndexError: If the position is out of range.

        Notes:
            This method reads the length of the string from the file, and then reads the string itself.
            The length of the string is stored as an unsigned short (2 bytes) at the given position.
            The string is read from the file starting at `pos + 2` and its length is determined by the value read from the file.
            The returned tuple contains the string and the position of the next byte after the string.

        Example:
            >>> file = StructFile(...)
            >>> string, next_pos = file.get_string2(10)
        """
        l = self.get_ushort(pos)
        base = pos + _SHORT_SIZE
        return self.get(base, l), base + l

    def get_string4(self, pos):
        """
        Retrieves a string from the file at the given position.

        Args:
            pos (int): The position in the file where the string starts.

        Returns:
            tuple: A tuple containing the string and the position of the next byte after the string.

        Raises:
            ValueError: If the position is invalid or the string length is negative.

        Notes:
            This method reads the length of the string from the file at the given position,
            then reads the string itself from the file. It returns the string and the position
            of the next byte after the string.

            The string is read from the file using the `get` method, which reads a specified
            number of bytes from the file starting at a given position.

            Example usage:
            ```
            string, next_pos = structfile.get_string4(10)
            ```

        """
        l = self.get_int(pos)
        base = pos + _INT_SIZE
        return self.get(base, l), base + l

    def skip_string(self):
        """
        Skips a string in the file.

        This method reads the length of the string from the file using the `read_varint` method,
        and then seeks forward in the file by that length.

        Note:
        - This method assumes that the file pointer is positioned at the start of the string.
        - The `read_varint` method is responsible for reading the variable-length integer that
            represents the length of the string.

        Returns:
        None

        Raises:
        IOError: If there is an error reading or seeking in the file.
        """
        l = self.read_varint()
        self.seek(l, 1)

    def write_varint(self, i):
        """
        Writes a variable-length unsigned integer to the wrapped file.

        Parameters:
            i (int): The integer value to be written.

        Returns:
            None

        Raises:
            TypeError: If the input value is not an integer.
            ValueError: If the input value is negative.

        Notes:
            This method writes a variable-length unsigned integer to the file. The integer value is encoded using a
            variable-length encoding scheme, where smaller values require fewer bytes to represent. The encoded value
            is written to the file using the `write` method of the wrapped file object.

        Example:
            To write the integer value 42 to the file, you can use the following code:

            >>> file = StructFile(...)
            >>> file.write_varint(42)
        """
        self.write(varint(i))

    def write_svarint(self, i):
        """
        Writes a variable-length signed integer to the wrapped file.

        Parameters:
            i (int): The signed integer to be written.

        Returns:
            None

        Raises:
            IOError: If an error occurs while writing to the file.

        Notes:
            This method writes a variable-length signed integer to the file. The integer is encoded using a
            variable-length encoding scheme, where the most significant bit of each byte indicates whether
            there are more bytes to follow. This allows for efficient storage of integers that can have a
            wide range of values.

            The method uses the `signed_varint` function to encode the integer before writing it to the file.

        Example:
            To write a signed integer to a file:

            ```
            file = StructFile("data.bin")
            file.write_svarint(-42)
            file.close()
            ```
        """
        self.write(signed_varint(i))

    def read_varint(self):
        """Reads a variable-length encoded unsigned integer from the wrapped
        file.

        This method reads a variable-length encoded unsigned integer from the
        file object that is wrapped by this StructFile instance. The integer
        is encoded using a variable-length encoding scheme, where the number
        of bytes used to represent the integer depends on its value.

        Returns:
            int: The decoded unsigned integer.

        Raises:
            IOError: If there is an error reading from the file.

        Example:
            >>> with open('data.bin', 'rb') as f:
            ...     sf = StructFile(f)
            ...     value = sf.read_varint()
            ...     print(value)
            42

        Note:
            This method assumes that the file object is positioned at the
            start of the encoded integer. After reading the integer, the file
            object's position will be advanced by the number of bytes read.

        """
        return read_varint(self.read)

    def read_svarint(self):
        """Reads a variable-length encoded signed integer from the wrapped file.

        This method reads a variable-length encoded signed integer from the wrapped file.
        It uses the `read_varint` function to read the variable-length encoded integer,
        and then decodes it as a signed integer using the `decode_signed_varint` function.

        Returns:
            int: The decoded signed integer.

        Raises:
            IOError: If there is an error reading from the file.

        Example:
            >>> file = StructFile("data.bin")
            >>> value = file.read_svarint()
        """
        return decode_signed_varint(read_varint(self.read))

    def write_tagint(self, i):
        """
        Writes a sometimes-compressed unsigned integer to the wrapped file.

        The write_tagint method is used to write an unsigned integer to the file. It uses a
        sometimes-compressed format for faster writing. The method supports numbers from 0 to
        2^32-1.

        Parameters:
        - i (int): The unsigned integer to be written to the file.

        Notes:
        - Numbers from 0 to 253 are stored in one byte.
        - Byte 254 indicates that an unsigned 16-bit integer follows.
        - Byte 255 indicates that an unsigned 32-bit integer follows.

        Example usage:
        ```
        file = StructFile()
        file.write_tagint(42)
        ```

        """
        if i <= 253:
            self.write(chr(i))
        elif i <= 65535:
            self.write("\xFE" + pack_ushort(i))
        else:
            self.write("\xFF" + pack_uint(i))

    def read_tagint(self):
        """Reads a sometimes-compressed unsigned integer from the wrapped file.

        This method reads an unsigned integer from the file. The integer can be
        stored in two different formats: a compressed format and a faster but
        less compressed format.

        The compressed format uses a single byte to represent the integer. If
        the first byte read from the file is 254, the integer is stored in the
        compressed format and can be retrieved using the `read_ushort()` method.
        If the first byte is 255, the integer is stored in the compressed format
        and can be retrieved using the `read_uint()` method. Otherwise, the first
        byte represents the integer itself.

        Returns:
            int: The unsigned integer read from the file.

        Example:
            Suppose we have a file with the following bytes: [253, 42]. Calling
            `read_tagint()` on this file will return 253, as the first byte
            represents the integer itself.

        Note:
            This method assumes that the file is opened in binary mode.

        """
        tb = ord(self.read(1))
        if tb == 254:
            return self.read_ushort()
        elif tb == 255:
            return self.read_uint()
        else:
            return tb

    def write_byte(self, n):
        """Writes a single byte to the wrapped file.

        This method writes a single byte to the file object that is wrapped by the StructFile instance.
        It is a shortcut for calling `file.write(chr(n))`.

        Parameters:
        - n (int): The byte value to be written to the file. Must be an integer between 0 and 255.

        Raises:
        - TypeError: If the provided value `n` is not an integer.
        - ValueError: If the provided value `n` is not within the valid range of 0 to 255.

        Example:
        ```
        with open("data.bin", "wb") as file:
            struct_file = StructFile(file)
            struct_file.write_byte(65)  # Writes the ASCII value for 'A' to the file
        ```

        Note:
        This method assumes that the file object is opened in binary mode ('b').

        """
        self.write(pack_byte(n))

    def read_byte(self):
        """
        Reads a single byte from the file and returns its integer value.

        Returns:
            int: The integer value of the byte read from the file.

        Raises:
            IOError: If an error occurs while reading from the file.
        """
        return ord(self.read(1))

    def write_pickle(self, obj, protocol=-1):
        """
        Writes a pickled representation of obj to the wrapped file.

        Parameters:
            obj (object): The object to be pickled and written to the file.
            protocol (int, optional): The pickling protocol to use. Default is -1.

        Raises:
            pickle.PicklingError: If an error occurs during pickling.

        Notes:
            This method uses the `pickle.dump()` function to write a pickled representation
            of the given object to the file. The pickling protocol determines the format
            in which the object is serialized. The default protocol (-1) uses the highest
            available protocol supported by the Python interpreter.

        Example:
            # Create a StructFile object
            file = StructFile("data.bin")

            # Write a list object to the file using pickle
            data = [1, 2, 3, 4, 5]
            file.write_pickle(data)

        """
        dump(obj, self.file, protocol)

    def read_pickle(self):
        """
        Reads a pickled object from the wrapped file.

        Returns:
            object: The pickled object read from the file.

        Raises:
            EOFError: If the end of the file is reached before a pickled object is found.
            pickle.UnpicklingError: If there is an error while unpickling the object.
        """
        return load(self.file)

    def write_sbyte(self, n):
        """
        Writes a signed byte to the file.

        Args:
            n (int): The signed byte value to write.

        Raises:
            IOError: If an error occurs while writing to the file.

        Notes:
            - The signed byte value should be within the range of -128 to 127.
            - The file should be opened in binary mode before calling this method.

        Example:
            To write a signed byte value of -42 to the file:

            >>> file.write_sbyte(-42)
        """
        self.write(pack_sbyte(n))

    def write_int(self, n):
        """
        Writes an integer to the file.

        Parameters:
        - n (int): The integer to be written.

        Returns:
        None

        Raises:
        - TypeError: If the input is not an integer.

        Notes:
        - This method writes the integer to the file using the pack_int function.
        - The pack_int function converts the integer into a binary representation.
        - The binary representation is then written to the file.
        - If the input is not an integer, a TypeError is raised.
        """
        self.write(pack_int(n))

    def write_uint(self, n):
        """
        Writes an unsigned integer to the file.

        Parameters:
            n (int): The unsigned integer to write.

        Returns:
            None

        Raises:
            IOError: If an error occurs while writing to the file.

        Notes:
            This method writes the unsigned integer `n` to the file. The integer is encoded using the `pack_uint` function.

        Example:
            file.write_uint(42)
        """
        self.write(pack_uint(n))

    def write_uint_le(self, n):
        """
        Writes an unsigned integer in little-endian format to the file.

        Parameters:
        - n (int): The unsigned integer to write.

        Returns:
        None

        Raises:
        - TypeError: If the input is not an integer.
        - ValueError: If the input is a negative integer.

        Example:
        >>> file.write_uint_le(42)
        """
        self.write(pack_uint_le(n))

    def write_ushort(self, n):
        """
        Writes an unsigned short integer (2 bytes) to the file.

        Parameters:
        - n (int): The unsigned short integer to be written.

        Returns:
        None

        Raises:
        - IOError: If an error occurs while writing to the file.

        Usage:
        file.write_ushort(42)
        """
        self.write(pack_ushort(n))

    def write_ushort_le(self, n):
        """
        Writes an unsigned short integer (2 bytes) in little-endian byte order to the file.

        Parameters:
        - n (int): The unsigned short integer to be written.

        Returns:
        None

        Raises:
        - IOError: If an error occurs while writing to the file.

        Usage:
        file.write_ushort_le(65535)
        """
        self.write(pack_ushort_le(n))

    def write_long(self, n):
        """
        Writes a long integer to the file.

        Parameters:
        - n (int): The long integer to be written.

        Returns:
        None

        Raises:
        - IOError: If an error occurs while writing to the file.

        Notes:
        - This method writes the long integer to the file using the pack_long function.
        - The pack_long function converts the long integer into a binary representation.
        - The binary representation is then written to the file.
        - If an error occurs while writing to the file, an IOError is raised.
        """
        self.write(pack_long(n))

    def write_ulong(self, n):
        """
        Writes an unsigned long integer to the file.

        Parameters:
            n (int): The unsigned long integer to write.

        Returns:
            None

        Raises:
            IOError: If an error occurs while writing to the file.

        Notes:
            This method writes an unsigned long integer to the file using the pack_ulong function.
            The pack_ulong function converts the integer into a byte string representation according to the platform's byte order.
            The resulting byte string is then written to the file.

        Example:
            To write the unsigned long integer 123456789 to the file:

            >>> file.write_ulong(123456789)
        """
        self.write(pack_ulong(n))

    def write_float(self, n):
        """
        Writes a floating-point number to the file.

        Args:
            n (float): The floating-point number to write.

        Raises:
            IOError: If an error occurs while writing to the file.

        Notes:
            This method uses the `pack_float` function to convert the floating-point number
            into a binary representation before writing it to the file.

        Example:
            >>> file = StructFile("data.bin", "wb")
            >>> file.write_float(3.14)
        """
        self.write(pack_float(n))

    def write_array(self, arry):
        """
        Write an array to the file.

        This method writes the given array to the file. If the system is little-endian,
        the array is first byte-swapped before writing. If the file is a real file,
        the array is written using the `tofile()` method. Otherwise, the array is
        converted to bytes and written using the `write()` method.

        Parameters:
        - arry (array): The array to be written to the file.

        Returns:
        None

        Raises:
        None
        """
        if IS_LITTLE:
            arry = copy(arry)
            arry.byteswap()
        if self.is_real:
            arry.tofile(self.file)
        else:
            self.write(arry.tobytes())

    def read_sbyte(self):
        """
        Reads a signed byte from the file.

        Returns:
            int: The signed byte value read from the file.

        Raises:
            IOError: If an error occurs while reading from the file.

        Notes:
            This method reads a single byte from the file and interprets it as a signed value.
            The byte is unpacked using the `unpack_sbyte` function, which returns a tuple.
            The first element of the tuple is the signed byte value, which is then returned.

        Example:
            >>> file = StructFile("data.bin")
            >>> byte = file.read_sbyte()
            >>> print(byte)
            -42
        """
        return unpack_sbyte(self.read(1))[0]

    def read_int(self):
        """
        Reads an integer value from the file.

        Returns:
            int: The integer value read from the file.

        Raises:
            IOError: If there is an error reading from the file.

        """
        return unpack_int(self.read(_INT_SIZE))[0]

    def read_uint(self):
        """
        Reads an unsigned integer from the file.

        Returns:
            int: The unsigned integer read from the file.

        Raises:
            IOError: If there is an error reading from the file.
        """
        return unpack_uint(self.read(_INT_SIZE))[0]

    def read_uint_le(self):
        """
        Reads an unsigned integer from the file using little-endian byte order.

        Returns:
            int: The unsigned integer read from the file.

        Raises:
            IOError: If an error occurs while reading from the file.

        Notes:
            This method reads an unsigned integer from the file using little-endian byte order.
            It assumes that the file is opened in binary mode.

        Example:
            >>> file = StructFile("data.bin")
            >>> value = file.read_uint_le()
        """
        return unpack_uint_le(self.read(_INT_SIZE))[0]

    def read_ushort(self):
        """
        Reads an unsigned short (2 bytes) from the file.

        Returns:
            int: The unsigned short value read from the file.

        Raises:
            IOError: If there is an error reading from the file.
        """
        return unpack_ushort(self.read(_SHORT_SIZE))[0]

    def read_ushort_le(self):
        """
        Reads an unsigned short (2 bytes) from the file in little-endian byte order.

        Returns:
            int: The unsigned short value read from the file.

        Raises:
            IOError: If there is an error reading from the file.

        Example:
            >>> file = StructFile("data.bin")
            >>> value = file.read_ushort_le()
        """
        return unpack_ushort_le(self.read(_SHORT_SIZE))[0]

    def read_long(self):
        """
        Reads a long integer from the file.

        Returns:
            int: The long integer read from the file.

        Raises:
            IOError: If an error occurs while reading from the file.

        Notes:
            This method reads a long integer from the file using the `read` method of the file object.
            The long integer is unpacked from the binary data using the `unpack_long` function.
            The `unpack_long` function returns a tuple, and the first element of the tuple is returned as the result.

        Example:
            >>> file = StructFile("data.bin")
            >>> value = file.read_long()
        """
        return unpack_long(self.read(_LONG_SIZE))[0]

    def read_ulong(self):
        """
        Reads an unsigned long integer from the file.

        Returns:
            int: The unsigned long integer read from the file.

        Raises:
            IOError: If an error occurs while reading from the file.

        Notes:
            This method reads a fixed-size unsigned long integer from the file. The size of the
            unsigned long integer is determined by the `_LONG_SIZE` constant.

        Example:
            >>> file = StructFile("data.bin")
            >>> value = file.read_ulong()
        """
        return unpack_ulong(self.read(_LONG_SIZE))[0]

    def read_float(self):
        """
        Reads a single floating-point number from the file.

        Returns:
            float: The floating-point number read from the file.

        Raises:
            IOError: If an error occurs while reading from the file.
        """
        return unpack_float(self.read(_FLOAT_SIZE))[0]

    def read_array(self, typecode, length):
        """
        Read an array of elements from the file.

        Args:
            typecode (str): The typecode of the array elements.
            length (int): The number of elements to read.

        Returns:
            array: The array of elements read from the file.

        Raises:
            IOError: If there is an error reading from the file.

        Notes:
            - If the file is in "real" mode, the array is read using the `fromfile` method of the array object.
            - If the file is not in "real" mode, the array is read using the `read` method of the file object and then converted to an array using the `frombytes` method of the array object.
            - If the system is little-endian, the byte order of the array is swapped using the `byteswap` method of the array object.

        Example:
            # Create a StructFile object
            file = StructFile("data.bin")

            # Read an array of integers from the file
            arr = file.read_array('i', 10)
        """
        a = array(typecode)
        if self.is_real:
            a.fromfile(self.file, length)
        else:
            a.frombytes(self.read(length * _SIZEMAP[typecode]))
        if IS_LITTLE:
            a.byteswap()
        return a

    def get(self, position, length):
        """
        Reads a specified number of bytes from the file starting at the given position.

        Args:
            position (int): The position in the file to start reading from.
            length (int): The number of bytes to read from the file.

        Returns:
            bytes: The bytes read from the file.

        Raises:
            OSError: If an error occurs while reading from the file.

        Example:
            >>> file = StructFile("data.bin")
            >>> data = file.get(10, 20)
        """
        self.seek(position)
        return self.read(length)

    def get_byte(self, position):
        """
        Retrieves a single byte from the file at the specified position.

        Parameters:
            position (int): The position in the file from which to retrieve the byte.

        Returns:
            int: The byte value at the specified position.

        Raises:
            IndexError: If the position is out of range.

        Example:
            # Create a StructFile object
            file = StructFile("data.bin")

            # Get the byte at position 10
            byte = file.get_byte(10)
        """
        return unpack_byte(self.get(position, 1))[0]

    def get_sbyte(self, position):
        """
        Retrieves a signed byte (8-bit integer) from the file at the specified position.

        Parameters:
        - position (int): The position in the file from which to read the signed byte.

        Returns:
        - int: The signed byte value read from the file.

        Raises:
        - IndexError: If the position is out of range.

        Example:
        ```
        file = StructFile("data.bin")
        byte = file.get_sbyte(10)
        print(byte)  # Output: -42
        ```
        """
        return unpack_sbyte(self.get(position, 1))[0]

    def get_int(self, position):
        """
        Retrieves an integer value from the file at the specified position.

        Parameters:
            position (int): The position in the file from which to retrieve the integer value.

        Returns:
            int: The integer value retrieved from the file.

        Raises:
            IndexError: If the position is out of range.

        """
        return unpack_int(self.get(position, _INT_SIZE))[0]

    def get_uint(self, position):
        """
        Retrieves an unsigned integer from the file at the given position.

        Parameters:
        - position (int): The position in the file from which to read the unsigned integer.

        Returns:
        - int: The unsigned integer value read from the file.

        Raises:
        - IndexError: If the position is out of range.
        """
        return unpack_uint(self.get(position, _INT_SIZE))[0]

    def get_ushort(self, position):
        """
        Retrieves an unsigned short integer (2 bytes) from the file at the specified position.

        Parameters:
        - position (int): The position in the file from which to read the unsigned short integer.

        Returns:
        - ushort (int): The unsigned short integer value read from the file.

        Raises:
        - IndexError: If the position is out of range.

        Example:
        ```
        file = StructFile("data.bin")
        ushort_value = file.get_ushort(10)
        ```
        """
        return unpack_ushort(self.get(position, _SHORT_SIZE))[0]

    def get_long(self, position):
        """
        Retrieves a long integer value from the file at the given position.

        Parameters:
            position (int): The position in the file from which to read the long integer.

        Returns:
            int: The long integer value read from the file.

        Raises:
            ValueError: If the position is out of bounds or if the file is not open.

        Notes:
            - This method reads a long integer value from the file at the specified position.
            - The file must be open before calling this method.
            - The position must be a valid position within the file.
        """
        return unpack_long(self.get(position, _LONG_SIZE))[0]

    def get_ulong(self, position):
        """
        Retrieves an unsigned long integer from the file at the specified position.

        Parameters:
            position (int): The position in the file from which to read the unsigned long integer.

        Returns:
            int: The unsigned long integer value read from the file.

        Raises:
            IndexError: If the position is out of range.

        Notes:
            - The unsigned long integer is read from the file using the `get` method.
            - The `unpack_ulong` function is used to convert the byte string to an unsigned long integer.
            - Only the first value of the unpacked result is returned.

        Example:
            # Create a StructFile object
            file = StructFile("data.bin")

            # Read an unsigned long integer from the file at position 100
            value = file.get_ulong(100)
        """
        return unpack_ulong(self.get(position, _LONG_SIZE))[0]

    def get_float(self, position):
        """
        Retrieves a float value from the file at the specified position.

        Parameters:
            position (int): The position in the file where the float value is located.

        Returns:
            float: The float value retrieved from the file.

        Raises:
            IndexError: If the position is out of range.

        """
        return unpack_float(self.get(position, _FLOAT_SIZE))[0]

    def get_array(self, position, typecode, length):
        """
        Reads an array of elements from the file starting at the given position.

        Args:
            position (int): The position in the file to start reading from.
            typecode (str): The typecode of the elements in the array.
            length (int): The number of elements to read.

        Returns:
            list: A list containing the elements read from the file.

        Raises:
            OSError: If there is an error reading the file.

        Example:
            To read an array of 10 integers starting from position 100 in the file:

            >>> file = StructFile("data.bin")
            >>> array = file.get_array(100, 'i', 10)
        """
        self.seek(position)
        return self.read_array(typecode, length)


class BufferFile(StructFile):
    """
    A class representing a file stored in memory as a buffer.

    This class provides methods to manipulate and retrieve data from the buffer.

    Attributes:
        _buf (bytes): The buffer containing the file data.
        _name (str): The name of the file.
        file (BytesIO): A BytesIO object representing the file.
        onclose (callable): A callback function to be called when the file is closed.
        is_real (bool): Indicates whether the file is a real file or a buffer.
        is_closed (bool): Indicates whether the file is closed.

    Methods:
        __init__(self, buf, name=None, onclose=None): Initializes a BufferFile object.
        subset(self, position, length, name=None): Creates a new BufferFile object representing a subset of the current file.
        get(self, position, length): Retrieves a portion of the file data.
        get_array(self, position, typecode, length): Retrieves an array of data from the file.
    """

    def __init__(self, buf, name=None, onclose=None):
        """
        Initializes a BufferFile object.

        Args:
            buf (bytes): The buffer containing the file data.
            name (str, optional): The name of the file. Defaults to None.
            onclose (callable, optional): A callback function to be called when the file is closed. Defaults to None.
        """
        self._buf = buf
        self._name = name
        self.file = BytesIO(buf)
        self.onclose = onclose

        self.is_real = False
        self.is_closed = False

    def subset(self, position, length, name=None):
        """
        Creates a new BufferFile object that represents a subset of the current file.

        Args:
            position (int): The starting position of the subset.
            length (int): The length of the subset.
            name (str, optional): The name of the new file. Defaults to None.

        Returns:
            BufferFile: A new BufferFile object representing the subset of the current file.
        """
        name = name or self._name
        return BufferFile(self.get(position, length), name=name)

    def get(self, position, length):
        """
        Retrieves a portion of the file data.

        Args:
            position (int): The starting position of the data.
            length (int): The length of the data to retrieve.

        Returns:
            bytes: The requested portion of the file data.
        """
        return bytes(self._buf[position : position + length])

    def get_array(self, position, typecode, length):
        """
        Retrieves an array of data from the file.

        Args:
            position (int): The starting position of the array.
            typecode (str): The typecode of the array elements.
            length (int): The length of the array.

        Returns:
            array: An array of data retrieved from the file.
        """
        a = array(typecode)
        a.frombytes(self.get(position, length * _SIZEMAP[typecode]))
        if IS_LITTLE:
            a.byteswap()
        return a


class ChecksumFile(StructFile):
    """
    A file-like object that calculates a checksum of the data read or written.

    This class inherits from StructFile and provides additional functionality to calculate a checksum
    using the CRC32 algorithm from the zlib module. The checksum is updated as data is read or written.

    Note: This class does not support seeking.

    Usage:
    - Create an instance of ChecksumFile by passing the file path or file object to the constructor.
    - Read or write data using the file-like methods provided by ChecksumFile.
    - Call the checksum() method to get the calculated checksum.

    Example:
    ```
    with ChecksumFile("data.txt", "rb") as file:
        data = file.read(1024)
        print(file.checksum())
    ```

    Attributes:
    - _check: The current checksum value.
    - _crc32: The CRC32 function from the zlib module.

    Methods:
    - __iter__(): Returns an iterator over the lines of the file.
    - seek(): Raises a ValueError as seeking is not supported.
    - read(): Reads data from the file and updates the checksum.
    - write(): Writes data to the file and updates the checksum.
    - checksum(): Returns the calculated checksum.

    """

    def __init__(self, *args, **kwargs):
        StructFile.__init__(self, *args, **kwargs)
        self._check = 0
        self._crc32 = __import__("zlib").crc32

    def __iter__(self):
        for line in self.file:
            self._check = self._crc32(line, self._check)
            yield line

    def seek(self, *args):
        raise ValueError("Cannot seek on a ChecksumFile")

    def read(self, *args, **kwargs):
        """
        Read data from the file and update the checksum.

        Args:
        - *args: Variable length argument list to pass to the underlying file's read() method.
        - **kwargs: Arbitrary keyword arguments to pass to the underlying file's read() method.

        Returns:
        - b: The read data.

        """
        b = self.file.read(*args, **kwargs)
        self._check = self._crc32(b, self._check)
        return b

    def write(self, b):
        """
        Write data to the file and update the checksum.

        Args:
        - b: The data to write.

        """
        self._check = self._crc32(b, self._check)
        self.file.write(b)

    def checksum(self):
        """
        Get the calculated checksum.

        Returns:
        - The calculated checksum as an unsigned 32-bit integer.

        """
        return self._check & 0xFFFFFFFF
