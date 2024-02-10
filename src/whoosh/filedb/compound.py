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

import errno
import os
import sys
from io import BytesIO
from shutil import copyfileobj
from threading import Lock

try:
    import mmap
except ImportError:
    mmap = None

from whoosh.filedb.filestore import FileStorage, StorageError
from whoosh.filedb.structfile import BufferFile, StructFile
from whoosh.system import emptybytes
from whoosh.util import random_name


def memoryview_(source, offset=None, length=None):
    mv = memoryview(source)
    if offset or length:
        return mv[offset : offset + length]
    else:
        return mv


class CompoundStorage(FileStorage):
    """
    CompoundStorage is a class that represents a compound file storage for Whoosh indexes.
    It provides methods to read and write files within the compound file.

    Parameters:
    - dbfile (file-like object): The file-like object representing the compound file.
    - use_mmap (bool, optional): Whether to use memory-mapped file for faster access. Defaults to True.
    - basepos (int, optional): The base position in the file. Defaults to 0.

    Attributes:
    - readonly (bool): Whether the compound file is read-only.
    - is_closed (bool): Whether the compound file is closed.
    - _file (file-like object): The file-like object representing the compound file.
    - _diroffset (int): The offset of the directory within the compound file.
    - _dirlength (int): The length of the directory within the compound file.
    - _dir (dict): The directory mapping file names to their offset and length within the compound file.
    - _options (dict): Additional options associated with the compound file.
    - _locks (dict): A dictionary of locks for file-level synchronization.
    - _source (mmap.mmap or None): The memory-mapped object representing the compound file, if mmap is used.

    Methods:
    - __init__(self, dbfile, use_mmap=True, basepos=0): Initializes a CompoundStorage object.
    - __repr__(self): Returns a string representation of the CompoundStorage object.
    - close(self): Closes the compound file.
    - range(self, name): Returns the offset and length of a file within the compound file.
    - open_file(self, name, *args, **kwargs): Opens a file within the compound file.
    - list(self): Returns a list of file names within the compound file.
    - file_exists(self, name): Checks if a file exists within the compound file.
    - file_length(self, name): Returns the length of a file within the compound file.
    - file_modified(self, name): Returns the modification time of a file within the compound file.
    - lock(self, name): Returns a lock object for file-level synchronization.
    - assemble(dbfile, store, names, **options): Assembles a compound file from multiple files.
    - write_dir(dbfile, basepos, directory, options=None): Writes the directory and options to the compound file.
    """

    readonly = True

    def __init__(self, dbfile, use_mmap=True, basepos=0):
        """
        Initializes a CompoundStorage object.

        Parameters:
        - dbfile (file-like object): The file-like object representing the compound file.
        - use_mmap (bool, optional): Whether to use memory-mapped file for faster access. Defaults to True.
        - basepos (int, optional): The base position in the file. Defaults to 0.
        """
        self._file = dbfile
        self.is_closed = False

        # Seek to the end to get total file size (to check if mmap is OK)
        dbfile.seek(0, os.SEEK_END)
        filesize = self._file.tell()
        dbfile.seek(basepos)

        self._diroffset = self._file.read_long()
        self._dirlength = self._file.read_int()
        self._file.seek(self._diroffset)
        self._dir = self._file.read_pickle()
        self._options = self._file.read_pickle()
        self._locks = {}
        self._source = None

        use_mmap = (
            use_mmap
            and hasattr(self._file, "fileno")  # check file is a real file
            and filesize < sys.maxsize  # check fit on 32-bit Python
        )
        if mmap and use_mmap:
            # Try to open the entire segment as a memory-mapped object
            try:
                fileno = self._file.fileno()
                self._source = mmap.mmap(fileno, 0, access=mmap.ACCESS_READ)
            except OSError:
                e = sys.exc_info()[1]
                # If we got an error because there wasn't enough memory to
                # open the map, ignore it and fall through, we'll just use the
                # (slower) "sub-file" implementation
                if e.errno == errno.ENOMEM:
                    pass
                else:
                    raise
            else:
                # If that worked, we can close the file handle we were given
                self._file.close()
                self._file = None

    def __repr__(self):
        """
        Returns a string representation of the CompoundStorage object.
        """
        return f"<{self.__class__.__name__} ({self._name})>"

    def close(self):
        """
        Closes the compound file.
        """
        if self.is_closed:
            raise RuntimeError(
                "Already closed"
            )  # Replaced generic Exception with RuntimeError
        self.is_closed = True

        if self._source:
            try:
                self._source.close()
            except BufferError:
                del self._source
        if self._file:
            self._file.close()

    def range(self, name):
        """
        Returns the offset and length of a file within the compound file.

        Parameters:
        - name (str): The name of the file.

        Returns:
        - offset (int): The offset of the file within the compound file.
        - length (int): The length of the file.
        """
        try:
            fileinfo = self._dir[name]
        except KeyError:
            raise NameError(f"Unknown file {name!r}")
        return fileinfo["offset"], fileinfo["length"]

    def open_file(self, name, *args, **kwargs):
        """
        Opens a file within the compound file.

        Parameters:
        - name (str): The name of the file.
        - *args: Additional positional arguments.
        - **kwargs: Additional keyword arguments.

        Returns:
        - f (file-like object): The file-like object representing the opened file.
        """
        if self.is_closed:
            raise StorageError("Storage was closed")

        offset, length = self.range(name)
        if self._source:
            # Create a memoryview/buffer from the mmap
            buf = memoryview_(self._source, offset, length)
            f = BufferFile(buf, name=name)
        elif hasattr(self._file, "subset"):
            f = self._file.subset(offset, length, name=name)
        else:
            f = StructFile(SubFile(self._file, offset, length), name=name)
        return f

    def list(self):
        """
        Returns a list of file names within the compound file.
        """
        return list(self._dir.keys())

    def file_exists(self, name):
        """
        Checks if a file exists within the compound file.

        Parameters:
        - name (str): The name of the file.

        Returns:
        - exists (bool): True if the file exists, False otherwise.
        """
        return name in self._dir

    def file_length(self, name):
        """
        Returns the length of a file within the compound file.

        Parameters:
        - name (str): The name of the file.

        Returns:
        - length (int): The length of the file.
        """
        info = self._dir[name]
        return info["length"]

    def file_modified(self, name):
        """
        Returns the modification time of a file within the compound file.

        Parameters:
        - name (str): The name of the file.

        Returns:
        - modified (float): The modification time of the file.
        """
        info = self._dir[name]
        return info["modified"]

    def lock(self, name):
        """
        Returns a lock object for file-level synchronization.

        Parameters:
        - name (str): The name of the file.

        Returns:
        - lock (Lock): The lock object.
        """
        if name not in self._locks:
            self._locks[name] = Lock()
        return self._locks[name]

    @staticmethod
    def assemble(dbfile, store, names, **options):
        """
        Assembles a compound file from multiple files.

        Parameters:
        - dbfile (file-like object): The file-like object representing the compound file.
        - store (FileStorage): The file storage object containing the files to be assembled.
        - names (list): The list of file names to be assembled.
        - **options: Additional options to be associated with the compound file.
        """
        assert names, names

        directory = {}
        basepos = dbfile.tell()
        dbfile.write_long(0)  # Directory position
        dbfile.write_int(0)  # Directory length

        # Copy the files into the compound file
        for name in names:
            if name.endswith(".toc") or name.endswith(".seg"):
                raise ValueError(name)

        for name in names:
            offset = dbfile.tell()
            length = store.file_length(name)
            modified = store.file_modified(name)
            directory[name] = {"offset": offset, "length": length, "modified": modified}
            f = store.open_file(name)
            copyfileobj(f, dbfile)
            f.close()

        CompoundStorage.write_dir(dbfile, basepos, directory, options)

    @staticmethod
    def write_dir(dbfile, basepos, directory, options=None):
        """
        Writes the directory and options to the compound file.

        Parameters:
        - dbfile (file-like object): The file-like object representing the compound file.
        - basepos (int): The base position in the file.
        - directory (dict): The directory mapping file names to their offset and length within the compound file.
        - options (dict, optional): Additional options to be associated with the compound file. Defaults to None.
        """
        options = options or {}

        dirpos = dbfile.tell()  # Remember the start of the directory
        dbfile.write_pickle(directory)  # Write the directory
        dbfile.write_pickle(options)
        endpos = dbfile.tell()  # Remember the end of the directory
        dbfile.flush()
        dbfile.seek(basepos)  # Seek back to the start
        dbfile.write_long(dirpos)  # Directory position
        dbfile.write_int(endpos - dirpos)  # Directory length

        dbfile.close()


class SubFile:
    """
    Represents a subset of a parent file.

    This class provides methods to read and manipulate a subset of a parent file.
    It keeps track of the subset's position, length, and name.

    Attributes:
        _file (file-like object): The parent file.
        _offset (int): The offset of the subset within the parent file.
        _length (int): The length of the subset.
        _end (int): The end position of the subset.
        _pos (int): The current position within the subset.
        name (str): The name of the subset.
        closed (bool): Indicates whether the subset is closed.

    Methods:
        close(): Closes the subset.
        subset(position, length, name=None): Creates a new subset from the current subset.
        read(size=None): Reads data from the subset.
        readline(): Reads a line from the subset.
        seek(where, whence=0): Moves the current position within the subset.
        tell(): Returns the current position within the subset.
    """

    def __init__(self, parentfile, offset, length, name=None):
        """
        Initialize a CompoundFile object.

        Args:
            parentfile (file-like object): The parent file object that represents the compound file.
            offset (int): The offset within the parent file where the compound file starts.
            length (int): The length of the compound file in bytes.
            name (str, optional): The name of the compound file. Defaults to None.

        Attributes:
            _file (file-like object): The parent file object that represents the compound file.
            _offset (int): The offset within the parent file where the compound file starts.
            _length (int): The length of the compound file in bytes.
            _end (int): The end position of the compound file within the parent file.
            _pos (int): The current position within the compound file.
            name (str): The name of the compound file.
            closed (bool): Indicates whether the compound file is closed.

        Raises:
            None.

        Returns:
            None.
        """
        self._file = parentfile
        self._offset = offset
        self._length = length
        self._end = offset + length
        self._pos = 0

        self.name = name
        self.closed = False

    def close(self):
        """
        Closes the subset.

        This method sets the `closed` attribute to True, indicating that the subset is closed.
        """
        self.closed = True

    def subset(self, position, length, name=None):
        """
        Creates a new subset from the current subset.

        Args:
            position (int): The position of the new subset within the current subset.
            length (int): The length of the new subset.
            name (str, optional): The name of the new subset. Defaults to None.

        Returns:
            SubFile: The new subset.

        Raises:
            AssertionError: If the position or length is out of bounds.
        """
        start = self._offset + position
        end = start + length
        name = name or self.name
        assert self._offset >= start >= self._end
        assert self._offset >= end >= self._end
        return SubFile(self._file, self._offset + position, length, name=name)

    def read(self, size=None):
        """
        Reads data from the subset.

        Args:
            size (int, optional): The number of bytes to read. If None, reads until the end of the subset.
                Defaults to None.

        Returns:
            bytes: The read data.

        Raises:
            ValueError: If the size is negative.
        """
        if size is None:
            size = self._length - self._pos
        else:
            size = min(size, self._length - self._pos)
        if size < 0:
            size = 0

        if size > 0:
            self._file.seek(self._offset + self._pos)
            self._pos += size
            return self._file.read(size)
        else:
            return emptybytes

    def readline(self):
        """
        Reads a line from the subset.

        Returns:
            bytes: The read line.

        Raises:
            ValueError: If the line length exceeds the remaining subset length.
        """
        maxsize = self._length - self._pos
        self._file.seek(self._offset + self._pos)
        data = self._file.readline()
        if len(data) > maxsize:
            data = data[:maxsize]
        self._pos += len(data)
        return data

    def seek(self, where, whence=0):
        """
        Moves the current position within the subset.

        Args:
            where (int): The new position.
            whence (int, optional): The reference position for the new position.
                0 for absolute, 1 for relative to the current position, 2 for relative to the end.
                Defaults to 0.

        Raises:
            ValueError: If the `whence` value is invalid.
        """
        if whence == 0:  # Absolute
            pos = where
        elif whence == 1:  # Relative
            pos = self._pos + where
        elif whence == 2:  # From end
            pos = self._length - where
        else:
            raise ValueError

        self._pos = pos

    def tell(self):
        """
        Returns the current position within the subset.

        Returns:
            int: The current position.
        """
        return self._pos


class CompoundWriter:
    """
    A class for writing compound files in Whoosh.

    CompoundWriter is responsible for creating compound files, which are files that contain multiple smaller files
    combined into a single file. This class provides methods to create and manage substreams within the compound file,
    and to save the compound file either as a single file or as separate files.

    Args:
        tempstorage (object): The temporary storage object used to create the compound file.
        buffersize (int, optional): The size of the buffer used for writing data to the compound file. Defaults to
            32 * 1024 bytes.

    Attributes:
        _tempstorage (object): The temporary storage object used to create the compound file.
        _tempname (str): The name of the temporary file used for storing the compound file data.
        _temp (file-like object): The temporary file object used for writing the compound file data.
        _buffersize (int): The size of the buffer used for writing data to the compound file.
        _streams (dict): A dictionary that maps substream names to their corresponding SubStream objects.

    """

    def __init__(self, tempstorage, buffersize=32 * 1024):
        """
        Initialize a CompoundStorage object.

        Args:
            tempstorage (object): The temporary storage object used to create the compound file.
            buffersize (int, optional): The buffer size in bytes for reading and writing data. Defaults to 32 * 1024.

        Raises:
            AssertionError: If the buffersize is not an integer.

        Notes:
            - The CompoundStorage object is responsible for managing a compound file, which is a file that contains multiple
              smaller files combined into a single file.
            - The tempstorage object should implement the `create_file` method to create a temporary file.
            - The buffersize determines the size of the buffer used for reading and writing data to the compound file.

        Example:
            tempstorage = TempStorage()
            compound = CompoundStorage(tempstorage, buffersize=64 * 1024)
        """
        assert isinstance(buffersize, int)
        self._tempstorage = tempstorage
        self._tempname = f"{random_name()}.ctmp"
        self._temp = tempstorage.create_file(self._tempname, mode="w+b")
        self._buffersize = buffersize
        self._streams = {}

    def create_file(self, name):
        """
        Creates a new file with the given name in the compound file.

        Parameters:
        - name (str): The name of the file to be created.

        Returns:
        - StructFile: A StructFile object representing the newly created file.

        Description:
        This method creates a new file with the given name in the compound file.
        It internally creates a SubStream object with a temporary file and a buffer size.
        The SubStream object is then stored in the _streams dictionary with the given name as the key.
        Finally, a StructFile object is returned, which wraps the SubStream object.

        Example usage:
        compound_file = CompoundFile()
        file = compound_file.create_file("example.txt")
        file.write("Hello, World!")
        file.close()
        """
        ss = self.SubStream(self._temp, self._buffersize)
        self._streams[name] = ss
        return StructFile(ss)

    def _readback(self):
        """
        Reads back the contents of the compound file.

        This method reads back the contents of the compound file, yielding each substream's name and a generator that
        yields the data blocks of the substream. The data blocks are read from either the substream or a temporary file,
        depending on whether the substream is closed or not.

        Returns:
            generator: A generator that yields tuples containing the name of the substream and a generator that yields
            the data blocks of the substream.

        Example:
            compound_file = CompoundFile()
            for name, gen in compound_file._readback():
                print(f"Substream: {name}")
                for data_block in gen():
                    process_data_block(data_block)
        """
        temp = self._temp
        for name, substream in self._streams.items():
            substream.close()

            def gen():
                for f, offset, length in substream.blocks:
                    if f is None:
                        f = temp
                    f.seek(offset)
                    yield f.read(length)

            yield (name, gen)
        temp.close()
        self._tempstorage.delete_file(self._tempname)

    def save_as_compound(self, dbfile):
        """
        Save the current index as a compound file.

        This method writes the index data to a single file in a compound format.
        The compound file contains multiple sub-files, each representing a segment
        of the index. The directory structure of the compound file is stored at the
        beginning of the file.

        Parameters:
            dbfile (file-like object): The file-like object to write the compound file to.

        Returns:
            None

        Raises:
            IOError: If there is an error writing the compound file.

        Usage:
            To save the index as a compound file, pass a file-like object to this method.
            The file-like object should be opened in binary mode for writing. After calling
            this method, the compound file will be written to the provided file-like object.
        """
        basepos = dbfile.tell()
        dbfile.write_long(0)  # Directory offset
        dbfile.write_int(0)  # Directory length

        directory = {}
        for name, blocks in self._readback():
            filestart = dbfile.tell()
            for block in blocks():
                dbfile.write(block)
            directory[name] = {"offset": filestart, "length": dbfile.tell() - filestart}

        CompoundStorage.write_dir(dbfile, basepos, directory)

    def save_as_files(self, storage, name_fn):
        """
        Save the compound file as separate files in the given storage.

        Args:
            storage (Storage): The storage object where the files will be saved.
            name_fn (callable): A function that takes a name and returns the filename.

        Returns:
            None

        Raises:
            Any exceptions raised by the storage object.

        Notes:
            This method saves the compound file as separate files in the given storage.
            Each file is created using the provided name_fn function, which takes a name
            and returns the filename. The compound file is read back and written to the
            separate files block by block.

        Example:
            storage = MyStorage()
            name_fn = lambda name: name + ".txt"
            compound_file.save_as_files(storage, name_fn)
        """
        for name, blocks in self._readback():
            f = storage.create_file(name_fn(name))
            for block in blocks():
                f.write(block)
            f.close()

    class SubStream:
        """A class representing a substream for writing data to a file.

        This class is used internally by the `CompoundFileWriter` class to write data to a file in blocks.
        It provides methods for writing data to the substream and keeping track of the offsets and lengths of the blocks.

        Attributes:
            _dbfile (file): The file object representing the main database file.
            _buffersize (int): The maximum size of the buffer before writing to the main file.
            _buffer (BytesIO): The buffer used to store the data before writing.
            blocks (list): A list of tuples representing the blocks written to the main file. Each tuple contains:
                - A BytesIO object if the block is in the buffer, or None if the block is in the main file.
                - The offset of the block in the main file.
                - The length of the block.

        Methods:
            tell(): Returns the current position in the substream.
            write(inbytes): Writes the given bytes to the substream.
            close(): Closes the substream and writes any remaining data to the main file.

        Usage:
            # Create a SubStream object
            substream = SubStream(dbfile, buffersize)

            # Write data to the substream
            substream.write(inbytes)

            # Get the current position in the substream
            position = substream.tell()

            # Close the substream
            substream.close()
        """

        def __init__(self, dbfile, buffersize):
            """
            Initialize a CompoundFile object.

            Args:
                dbfile (str): The path to the compound file.
                buffersize (int): The size of the buffer used for reading and writing.

            Attributes:
                _dbfile (str): The path to the compound file.
                _buffersize (int): The size of the buffer used for reading and writing.
                _buffer (BytesIO): The buffer used for temporary storage.
                blocks (list): The list of blocks in the compound file.

            """
            self._dbfile = dbfile
            self._buffersize = buffersize
            self._buffer = BytesIO()
            self.blocks = []

        def tell(self):
            """Returns the current position in the substream.

            Returns:
                int: The current position in the substream.
            """
            return sum(b[2] for b in self.blocks) + self._buffer.tell()

        def write(self, inbytes):
            """Writes the given bytes to the substream.

            If the length of the buffer exceeds the specified buffer size, the buffer is written to the main file
            and a new block is created.

            Args:
                inbytes (bytes): The bytes to write to the substream.
            """
            bio = self._buffer
            buflen = bio.tell()
            length = buflen + len(inbytes)
            if length >= self._buffersize:
                offset = self._dbfile.tell()
                self._dbfile.write(bio.getvalue()[:buflen])
                self._dbfile.write(inbytes)

                self.blocks.append((None, offset, length))
                self._buffer.seek(0)
            else:
                bio.write(inbytes)

        def close(self):
            """Closes the substream and writes any remaining data to the main file."""
            bio = self._buffer
            length = bio.tell()
            if length:
                self.blocks.append((bio, 0, length))
