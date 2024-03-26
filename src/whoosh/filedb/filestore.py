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


import errno
import os
import sys
import tempfile
from io import BytesIO
from threading import Lock

from whoosh.filedb.structfile import BufferFile, StructFile
from whoosh.index import _DEF_INDEX_NAME, EmptyIndexError
from whoosh.util import random_name
from whoosh.util.filelock import FileLock


def memoryview_(source, offset=None, length=None):
    """
    Create a memoryview object from the given source object.

    Parameters:
    - source: The source object to create the memoryview from.
    - offset (optional): The starting offset within the source object. If not provided, the memoryview will start from the beginning.
    - length (optional): The length of the memoryview. If not provided, the memoryview will extend to the end of the source object.

    Returns:
    - mv: The memoryview object created from the source object.

    Usage:
    - Create a memoryview from a bytes object:
        mv = memoryview_(b'Hello, World!')

    - Create a memoryview from a bytearray object with a specified offset and length:
        ba = bytearray(b'Hello, World!')
        mv = memoryview_(ba, offset=7, length=5)
    """
    mv = memoryview(source)
    if offset or length:
        return mv[offset : offset + length]
    else:
        return mv


# Exceptions


class StorageError(Exception):
    """
    Exception raised for errors related to storage operations.

    This exception is raised when there is an error performing operations
    related to storage, such as reading or writing files.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message
        super().__init__(message)


class ReadOnlyError(StorageError):
    """
    Exception raised when attempting to modify a read-only storage.

    This exception is raised when attempting to modify a storage that has been opened in read-only mode.
    It is a subclass of `StorageError` and can be caught separately from other storage-related exceptions.

    Usage:
    ------
    When using a storage object, if an attempt is made to modify the storage while it is in read-only mode,
    a `ReadOnlyError` will be raised. To handle this exception, you can use a try-except block like this:

    try:
        # Attempt to modify the storage
        storage.modify()
    except ReadOnlyError:
        # Handle the read-only error
        print("The storage is read-only and cannot be modified.")

    """

    def __init__(self, message):
        self.message = message
        super().__init__(message)


# Base class
class Storage:
    """Abstract base class for storage objects.

    A storage object is a virtual flat filesystem, allowing the creation and
    retrieval of file-like objects
    (:class:`~whoosh.filedb.structfile.StructFile` objects). The default
    implementation (:class:`FileStorage`) uses actual files in a directory.

    All access to files in Whoosh goes through this object. This allows more
    different forms of storage (for example, in RAM, in a database, in a single
    file) to be used transparently.

    For example, to create a :class:`FileStorage` object::

        # Create a storage object
        st = FileStorage("indexdir")
        # Create the directory if it doesn't already exist
        st.create()

    The :meth:`Storage.create` method makes it slightly easier to swap storage
    implementations. The `create()` method handles set-up of the storage
    object. For example, `FileStorage.create()` creates the directory. A
    database implementation might create tables. This is designed to let you
    avoid putting implementation-specific setup code in your application.

    Attributes:
        readonly (bool): Indicates if the storage object is read-only.
        supports_mmap (bool): Indicates if the storage object supports memory-mapped files.

    Methods:
        create(): Creates any required implementation-specific resources.
        destroy(*args, **kwargs): Removes any implementation-specific resources related to this storage object.
        create_index(schema, indexname=_DEF_INDEX_NAME, indexclass=None): Creates a new index in this storage.
        open_index(indexname=_DEF_INDEX_NAME, schema=None, indexclass=None): Opens an existing index in this storage.
        index_exists(indexname=None): Returns True if a non-empty index exists in this storage.
        create_file(name): Creates a file with the given name in this storage.
        open_file(name, *args, **kwargs): Opens a file with the given name in this storage.
        list(): Returns a list of file names in this storage.
        file_exists(name): Returns True if the given file exists in this storage.
        file_modified(name): Returns the last-modified time of the given file in this storage.
        file_length(name): Returns the size (in bytes) of the given file in this storage.
        delete_file(name): Removes the given file from this storage.
        rename_file(frm, to, safe=False): Renames a file in this storage.
        lock(name): Returns a named lock object.
        close(): Closes any resources opened by this storage object.
        optimize(): Optimizes the storage object.
        temp_storage(name=None): Creates a new storage object for temporary files.

    """

    readonly = False
    supports_mmap = False

    def __iter__(self):
        """
        Returns an iterator over the files in the filestore.

        This method returns an iterator that allows iterating over the files
        stored in the filestore. It internally calls the `list()` method to
        retrieve the list of files.

        Returns:
            iterator: An iterator over the files in the filestore.

        Example:
            filestore = FileStore()
            for file in filestore:
                print(file)
        """
        return iter(self.list())

    def __enter__(self):
        """
        Creates a new instance of the FileStore object and returns it.

        This method is used in conjunction with the 'with' statement to provide a context manager for the FileStore object.
        It ensures that the FileStore is properly created before entering the context and returns the created instance.

        Returns:
            FileStore: The created instance of the FileStore object.

        Example:
            with FileStore() as fs:
                # Perform operations using the FileStore object
        """
        self.create()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Closes the filestore.

        This method is automatically called when exiting a context manager block.
        It ensures that the filestore is properly closed, regardless of any exceptions that may have occurred.

        :param exc_type: The type of the exception (if any) that caused the context to be exited.
        :param exc_val: The exception instance (if any) that caused the context to be exited.
        :param exc_tb: The traceback object (if any) that caused the context to be exited.
        """
        self.close()

    def create(self):
        """
        Creates any required implementation-specific resources.

        This method is used to create the necessary resources for a storage implementation. For example, a filesystem-based implementation might create a directory, while a database implementation might create tables.

        Usage:
        ------
        1. Import the necessary modules:
            from whoosh.filedb.filestore import FileStorage

        2. Create a storage object:
            st = FileStorage("indexdir")

        3. Call the create() method to create the required resources:
            st.create()

        Returns:
        --------
        A Storage instance representing the created resources.

        Example:
        --------
        st = FileStorage("indexdir").create()

        Notes:
        ------
        - Storage implementations should be written in such a way that calling create() multiple times on the same storage does not cause any issues.
        - The create() method returns the Storage instance itself, allowing method chaining.

        :return: A Storage instance representing the created resources.
        """
        return self

    def destroy(self, *args, **kwargs):
        """
        Removes any implementation-specific resources related to this storage
        object. For example, a filesystem-based implementation might delete a
        directory, and a database implementation might drop tables.

        :param args: Implementation-specific arguments.
        :param kwargs: Implementation-specific keyword arguments.
        :return: None

        This method should be called when you want to permanently remove all
        resources associated with this storage object. It is implementation-specific,
        so the behavior may vary depending on the storage implementation being used.

        Example usage:
        >>> store = FileStore()
        >>> store.destroy()
        """
        pass

    def create_index(self, schema, indexname=_DEF_INDEX_NAME, indexclass=None):
        """Creates a new index in this storage.

        >>> from whoosh import fields
        >>> from whoosh.filedb.filestore import FileStorage
        >>> schema = fields.Schema(content=fields.TEXT)
        >>> # Create the storage directory
        >>> st = FileStorage.create("indexdir")
        >>> # Create an index in the storage
        >>> ix = st.create_index(schema)

        :param schema: the :class:`whoosh.fields.Schema` object to use for the
            new index.
        :param indexname: the name of the index within the storage object. You
            can use this option to store multiple indexes in the same storage.
        :param indexclass: an optional custom ``Index`` sub-class to use to
            create the index files. The default is
            :class:`whoosh.index.FileIndex`. This method will call the
            ``create`` class method on the given class to create the index.
        :return: a :class:`whoosh.index.Index` instance.
        """

        if self.readonly:
            raise ReadOnlyError
        if indexclass is None:
            import whoosh.index

            indexclass = whoosh.index.FileIndex
        return indexclass.create(self, schema, indexname)

    def open_index(self, indexname=_DEF_INDEX_NAME, schema=None, indexclass=None):
        """Opens an existing index (created using :meth:`create_index`) in this
        storage.

        >>> from whoosh.filedb.filestore import FileStorage
        >>> st = FileStorage("indexdir")
        >>> # Open an index in the storage
        >>> ix = st.open_index()

        :param indexname: the name of the index within the storage object. You
            can use this option to store multiple indexes in the same storage.
        :param schema: if you pass in a :class:`whoosh.fields.Schema` object
            using this argument, it will override the schema that was stored
            with the index.
        :param indexclass: an optional custom ``Index`` sub-class to use to
            open the index files. The default is
            :class:`whoosh.index.FileIndex`. This method will instantiate the
            class with this storage object.
        :return: a :class:`whoosh.index.Index` instance.
        """

        if indexclass is None:
            import whoosh.index

            indexclass = whoosh.index.FileIndex
        return indexclass(self, schema=schema, indexname=indexname)

    def index_exists(self, indexname=None):
        """
        Returns True if a non-empty index exists in this storage.

        :param indexname: (str, optional) The name of the index within the storage object.
                          You can use this option to store multiple indexes in the same storage.
        :return: (bool) True if a non-empty index exists, False otherwise.
        """

        if indexname is None:
            indexname = _DEF_INDEX_NAME
        try:
            ix = self.open_index(indexname)
            gen = ix.latest_generation()
            ix.close()
            return gen > -1
        except EmptyIndexError:
            pass
        return False

    def create_file(self, name):
        """
        Creates a file with the given name in this storage.

        :param name: The name for the new file.
        :type name: str
        :return: A :class:`whoosh.filedb.structfile.StructFile` instance.
        :rtype: whoosh.filedb.structfile.StructFile
        :raises NotImplementedError: If the method is not implemented by the subclass.

        This method creates a new file with the specified name in the storage. It returns
        an instance of the `StructFile` class, which provides methods for reading and writing
        data to the file.

        Example usage:
        >>> storage = FileStorage("/path/to/storage")
        >>> file = storage.create_file("example.txt")
        >>> file.write("Hello, World!")
        >>> file.close()
        """
        raise NotImplementedError

    def open_file(self, name, *args, **kwargs):
        """
        Opens a file with the given name in this storage.

        :param name: The name of the file to be opened.
        :type name: str
        :param args: Additional positional arguments to be passed to the file opening mechanism.
        :param kwargs: Additional keyword arguments to be passed to the file opening mechanism.
        :return: A :class:`whoosh.filedb.structfile.StructFile` instance representing the opened file.
        :rtype: whoosh.filedb.structfile.StructFile
        :raises NotImplementedError: If the method is not implemented by a subclass.

        This method is used to open a file within the storage. It returns a :class:`whoosh.filedb.structfile.StructFile`
        instance that provides file-like operations for reading and writing data.

        Example usage:

        >>> storage = FileStorage('/path/to/storage')
        >>> file = storage.open_file('example.txt', mode='r')
        >>> content = file.read()
        >>> file.close()

        Note that the specific behavior of the `open_file` method may vary depending on the implementation of the storage.
        Subclasses of `FileStorage` should override this method to provide the appropriate file opening mechanism.

        """
        raise NotImplementedError

    def list(self):
        """Returns a list of file names in this storage.

        This method returns a list of file names present in the storage. The storage represents a file system or a similar
        file storage mechanism.

        :return: A list of strings representing the file names in the storage.
        :rtype: list[str]

        :raises NotImplementedError: If the method is not implemented by a subclass.
        """
        raise NotImplementedError

    def file_exists(self, name):
        """
        Check if the given file exists in this storage.

        :param name: The name of the file to check.
        :type name: str
        :return: True if the file exists, False otherwise.
        :rtype: bool
        :raises NotImplementedError: This method is not implemented in the base class.
        """

        raise NotImplementedError

    def file_modified(self, name):
        """Returns the last-modified time of the given file in this storage (as
        a "ctime" UNIX timestamp).

        :param name: The name of the file to check.
        :type name: str
        :return: The "ctime" number representing the last-modified time of the file.
        :rtype: float
        :raises NotImplementedError: This method is not implemented in the base class and should be overridden in subclasses.

        This method returns the last-modified time of the specified file in the storage.
        The last-modified time is returned as a "ctime" UNIX timestamp, which represents the number of seconds
        since the epoch (January 1, 1970).

        Example usage:
        >>> storage = FileStorage()
        >>> last_modified = storage.file_modified("example.txt")
        >>> print(last_modified)
        1629876543.0
        """

        raise NotImplementedError

    def file_length(self, name):
        """Returns the size (in bytes) of the given file in this storage.

        :param name: The name of the file to check.
        :type name: str
        :return: The size of the file in bytes.
        :rtype: int
        :raises NotImplementedError: If the method is not implemented by a subclass.

        This method returns the size of the file with the given name in the storage.
        It is used to determine the size of a file stored in the file storage.

        Example usage:
        >>> storage = FileStorage()
        >>> file_size = storage.file_length("example.txt")
        >>> print(file_size)
        1024
        """

        raise NotImplementedError

    def delete_file(self, name):
        """
        Removes the given file from this storage.

        :param name: The name of the file to delete.
        :type name: str
        :raises NotImplementedError: This method is not implemented in the base class.
        """

        raise NotImplementedError

    def rename_file(self, frm, to, safe=False):
        """
        Renames a file in this storage.

        :param frm: The current name of the file.
        :type frm: str
        :param to: The new name for the file.
        :type to: str
        :param safe: If True, raise an exception if a file with the new name already exists.
        :type safe: bool
        :raises NotImplementedError: This method is not implemented in the base class.

        This method renames a file in the storage. It takes the current name of the file
        (`frm`) and the new name for the file (`to`). By default, if a file with the new
        name already exists, it will overwrite the existing file. However, if the `safe`
        parameter is set to True, an exception will be raised if a file with the new name
        already exists.

        Example usage:
        >>> storage = FileStorage()
        >>> storage.rename_file("old_file.txt", "new_file.txt")
        """
        raise NotImplementedError

    def lock(self, name):
        """
        Return a named lock object (implementing ``.acquire()`` and ``.release()`` methods).

        Different storage implementations may use different lock types with different guarantees.
        For example, the RamStorage object uses Python thread locks, while the FileStorage object
        uses filesystem-based locks that are valid across different processes.

        :param name: A name for the lock. This can be any string that uniquely identifies the lock.
        :type name: str
        :return: A lock-like object that provides the ``acquire()`` and ``release()`` methods.
        :rtype: object

        :raises NotImplementedError: This method is meant to be overridden by subclasses.

        Lock objects are used to synchronize access to shared resources, ensuring that only one
        thread or process can access the resource at a time. The ``acquire()`` method is used to
        acquire the lock, and the ``release()`` method is used to release the lock.

        Example usage:

        >>> store = FileStorage()
        >>> lock = store.lock("my_lock")
        >>> lock.acquire()
        >>> try:
        ...     # Perform operations on the shared resource
        ...     pass
        ... finally:
        ...     lock.release()

        Note that the lock object returned by this method may have additional methods or properties
        specific to the storage implementation being used. It is recommended to consult the
        documentation of the specific storage implementation for more details.
        """
        raise NotImplementedError

    def close(self):
        """Closes any resources opened by this storage object.

        This method is used to release any resources held by the storage object, such as locks or file handles.
        It should be called when you are done using the storage object to prevent resource leaks.

        Note:
            For some storage implementations, this method may be a no-op and not perform any actions.
            However, it is still good practice to call this method to ensure proper cleanup.

        Usage:
            storage = FileStorage()
            # Perform operations using the storage object
            storage.close()

        """
        pass

    def optimize(self):
        """Optimizes the storage object.

        This method is used to optimize the storage object. The specific
        implementation of optimization may vary depending on the storage
        backend being used. For example, a database implementation might
        run a garbage collection procedure on the underlying database.

        This method does not take any arguments and does not return any
        values. It performs the optimization operation in-place on the
        storage object.

        Usage:
            store = FileStore()
            store.optimize()

        Note:
            The behavior of this method may be different for different
            storage backends. It is recommended to consult the documentation
            of the specific storage backend for more information on how
            optimization is performed.

        Raises:
            NotImplementedError: If the storage backend does not support
                optimization.
        """
        pass

    def temp_storage(self, name=None):
        """
        Creates a new storage object for temporary files.

        This method creates a new storage object that can be used to store temporary files. The storage object can be accessed using the returned value and can be manipulated using its methods.

        :param name: Optional. A name for the new storage. This parameter may be required or optional depending on the storage implementation.
        :type name: str or None
        :return: A new storage object for temporary files.
        :rtype: Storage
        :raises NotImplementedError: This method is not implemented in the current class and should be overridden by subclasses.

        Example usage:
        >>> storage = temp_storage()
        >>> # Use the storage object to perform operations on temporary files
        >>> storage.destroy()  # Clean up the temporary storage when finished
        """

        raise NotImplementedError


class OverlayStorage(Storage):
    """Overlays two storage objects. Reads are processed from the first if it
    has the named file, otherwise the second. Writes always go to the second.

    This class provides a way to overlay two storage objects, where the first storage
    is used for reading files and the second storage is used for writing files. It is
    designed to be used as a storage backend for the Whoosh search engine library.

    Usage:
    1. Create an instance of OverlayStorage by passing two storage objects as arguments.
    2. Use the create_index() method to create an index in the second storage.
    3. Use the open_index() method to open an index in the first storage.
    4. Use the create_file() method to create a file in the second storage.
    5. Use the open_file() method to open a file for reading. If the file exists in the
       first storage, it will be read from there, otherwise it will be read from the second
       storage.
    6. Use the list() method to get a list of all files in both storages.
    7. Use the file_exists() method to check if a file exists in either storage.
    8. Use the file_modified() method to get the modification time of a file. If the file
       exists in the first storage, its modification time will be returned, otherwise the
       modification time of the file in the second storage will be returned.
    9. Use the file_length() method to get the length of a file. If the file exists in the
       first storage, its length will be returned, otherwise the length of the file in the
       second storage will be returned.
    10. Use the delete_file() method to delete a file from the second storage.
    11. Use the lock() method to acquire a lock on a file in the second storage.
    12. Use the close() method to close both storages.
    13. Use the optimize() method to optimize both storages.
    14. Use the temp_storage() method to get a temporary storage object from the second storage.

    Note: The rename_file() method is not implemented and will raise a NotImplementedError if called.
    """

    def __init__(self, a, b):
        """
        Initialize a new instance of the Storage class.

        Args:
            a: The value for parameter a.
            b: The value for parameter b.
        """
        self.a = a
        self.b = b

    def create_index(self, *args, **kwargs):
        """
        Create an index in the filestore.

        This method creates an index in the filestore using the provided arguments and keyword arguments.
        It delegates the actual index creation to the `create_index` method of the underlying `b` object.

        Parameters:
        *args: Variable length argument list.
            Positional arguments to be passed to the `create_index` method of the underlying `b` object.
        **kwargs: Arbitrary keyword arguments.
            Keyword arguments to be passed to the `create_index` method of the underlying `b` object.

        Returns:
        None

        Raises:
        Any exceptions raised by the `create_index` method of the underlying `b` object.

        Usage:
        filestore = FileStore()
        filestore.create_index("my_index", schema=my_schema)
        """
        self.b.create_index(*args, **kwargs)

    def open_index(self, *args, **kwargs):
        """
        Opens an index using the specified arguments and returns the opened index.

        Parameters:
        *args: Variable length argument list.
        **kwargs: Arbitrary keyword arguments.

        Returns:
        The opened index.

        Raises:
        Any exceptions raised by the underlying implementation.
        """
        self.a.open_index(*args, **kwargs)

    def create_file(self, *args, **kwargs):
        """
        Create a new file in the filestore.

        This method delegates the creation of the file to the underlying
        filestore backend.

        Parameters:
        *args: Variable length argument list.
        **kwargs: Arbitrary keyword arguments.

        Returns:
        The created file object.

        Raises:
        Any exceptions raised by the underlying filestore backend.
        """
        return self.b.create_file(*args, **kwargs)

    def open_file(self, name, *args, **kwargs):
        """
        Opens a file with the given name.

        If the file exists in the first file store (self.a), it is opened using the
        `open_file` method of the first file store. Otherwise, if the file exists in
        the second file store (self.b), it is opened using the `open_file` method of
        the second file store.

        Parameters:
            name (str): The name of the file to open.
            *args: Additional positional arguments to pass to the `open_file` method.
            **kwargs: Additional keyword arguments to pass to the `open_file` method.

        Returns:
            file-like object: The opened file.

        Raises:
            FileNotFoundError: If the file does not exist in either file store.

        Usage:
            To open a file, call the `open_file` method with the name of the file as the
            first argument. Additional arguments and keyword arguments can be passed to
            customize the file opening behavior.

            Example:
                file = open_file("example.txt", mode="r")
        """
        if self.a.file_exists(name):
            return self.a.open_file(name, *args, **kwargs)
        else:
            return self.b.open_file(name, *args, **kwargs)

    def list(self):
        """
        Returns a list of all the files in the filestore.

        This method combines the file lists from two filestores, `a` and `b`,
        and removes any duplicates. The resulting list contains all the unique
        files from both filestores.

        Returns:
            list: A list of file names in the filestore.

        Example:
            >>> filestore = FileStore()
            >>> filestore.list()
            ['file1.txt', 'file2.txt', 'file3.txt']
        """
        return list(set(self.a.list()) | set(self.b.list()))

    def file_exists(self, name):
        """
        Check if a file exists in the filestore.

        Parameters:
        - name (str): The name of the file to check.

        Returns:
        - bool: True if the file exists, False otherwise.

        This method checks if a file exists in the filestore by delegating the check to
        both the `a` and `b` filestores. It returns True if the file exists in either of
        the filestores, and False otherwise.
        """
        return self.a.file_exists(name) or self.b.file_exists(name)

    def file_modified(self, name):
        """
        Returns the modified timestamp of a file.

        This method checks if the file exists in the primary file store (self.a).
        If the file exists, it retrieves the modified timestamp from the primary file store.
        If the file does not exist in the primary file store, it retrieves the modified timestamp from the secondary file store (self.b).

        Parameters:
        - name (str): The name of the file.

        Returns:
        - int: The modified timestamp of the file.

        """
        if self.a.file_exists(name):
            return self.a.file_modified(name)
        else:
            return self.b.file_modified(name)

    def file_length(self, name):
        """
        Returns the length of a file with the given name.

        If the file exists in the primary filestore (self.a), the length of the file is returned.
        If the file does not exist in the primary filestore, the length of the file is returned from the secondary filestore (self.b).

        Parameters:
        - name (str): The name of the file.

        Returns:
        - int: The length of the file.

        Example:
        >>> store = FileStore()
        >>> store.file_length("example.txt")
        1024
        """
        if self.a.file_exists(name):
            return self.a.file_length(name)
        else:
            return self.b.file_length(name)

    def delete_file(self, name):
        """
        Deletes a file from the filestore.

        Args:
            name (str): The name of the file to delete.

        Returns:
            bool: True if the file was successfully deleted, False otherwise.

        Raises:
            FileNotFound: If the specified file does not exist in the filestore.

        Example:
            >>> filestore = FileStore()
            >>> filestore.delete_file("example.txt")
            True
        """
        return self.b.delete_file(name)

    def rename_file(self, *args, **kwargs):
        """
        Renames a file in the file store.

        This method is used to rename a file in the file store. It takes the necessary arguments
        to identify the file to be renamed and the new name to assign to it.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Raises:
            NotImplementedError: This method is not implemented in the base class and should be
                overridden in the derived classes.

        """
        raise NotImplementedError

    def lock(self, name):
        """
        Acquires a lock on the specified file.

        Args:
            name (str): The name of the file to lock.

        Returns:
            bool: True if the lock was successfully acquired, False otherwise.

        Raises:
            LockError: If an error occurs while acquiring the lock.

        Notes:
            This method delegates the locking operation to the underlying file store.
            It is used to prevent concurrent access to the same file by multiple processes.

        Example:
            >>> filestore = FileStore()
            >>> filestore.lock("example.txt")
            True
        """
        return self.b.lock(name)

    def close(self):
        """
        Closes the filestore by closing the underlying file handles.

        This method should be called when you are finished using the filestore.
        It closes the file handles for both the primary and secondary files.

        Note:
            After calling this method, any further operations on the filestore
            will raise an exception.

        Example:
            >>> store = FileStore()
            >>> # Perform operations on the filestore
            >>> store.close()

        """
        self.a.close()
        self.b.close()

    def optimize(self):
        """
        Optimize the filestore by optimizing both the 'a' and 'b' components.

        This method performs optimization on the filestore by calling the `optimize` method
        on both the 'a' and 'b' components. Optimization improves the performance of the
        filestore by reorganizing the data and reducing fragmentation.

        Note:
            Optimization may take some time to complete, depending on the size of the filestore.

        Usage:
            filestore = FileStore()
            filestore.optimize()

        """
        self.a.optimize()
        self.b.optimize()

    def temp_storage(self, name=None):
        """
        Returns a temporary storage object.

        This method returns a temporary storage object that can be used to store temporary data.
        The `name` parameter is optional and can be used to specify a name for the temporary storage.

        Parameters:
            name (str, optional): The name of the temporary storage. Defaults to None.

        Returns:
            TempStorage: A temporary storage object.

        Example:
            >>> store = filestore.temp_storage(name="my_temp_storage")
            >>> store.add("data.txt", "Hello, World!")
            >>> store.commit()
        """
        return self.b.temp_storage(name=name)


class FileStorage(Storage):
    """Storage object that stores the index as files in a directory on disk.

    Prior to version 3, the initializer would raise an IOError if the directory
    did not exist. As of version 3, the object does not check if the
    directory exists at initialization. This change is to support using the
    :meth:`FileStorage.create` method.

    Args:
        path (str): A path to a directory.
        supports_mmap (bool, optional): If True (the default), use the ``mmap`` module to
            open memory mapped files. You can open the storage object with
            ``supports_mmap=False`` to force Whoosh to open files normally
            instead of with ``mmap``.
        readonly (bool, optional): If ``True``, the object will raise an exception if you
            attempt to create or rename a file.
        debug (bool, optional): If ``True``, enables debug mode.

    Attributes:
        folder (str): The path to the directory where the index files are stored.
        supports_mmap (bool): If True, the storage object uses memory mapped files.
        readonly (bool): If True, the storage object is read-only.
        _debug (bool): If True, debug mode is enabled.
        locks (dict): A dictionary of file locks.

    Raises:
        IOError: If the given path is not a directory.
        OSError: If an error occurs while creating or removing the directory.

    """

    supports_mmap = True

    def __init__(self, path, supports_mmap=True, readonly=False, debug=False):
        """
        Initializes a FileStorage object.

        Args:
            path (str): A path to a directory.
            supports_mmap (bool, optional): If True (the default), use the ``mmap`` module to
                open memory mapped files. You can open the storage object with
                ``supports_mmap=False`` to force Whoosh to open files normally
                instead of with ``mmap``.
            readonly (bool, optional): If ``True``, the object will raise an exception if you
                attempt to create or rename a file.
            debug (bool, optional): If ``True``, enables debug mode.
        """

        self.folder = path
        self.supports_mmap = supports_mmap
        self.readonly = readonly
        self._debug = debug
        self.locks = {}

    def __repr__(self):
        return f"{self.__class__.__name__}({self.folder!r})"

    def create(self):
        """Creates this storage object's directory path using ``os.makedirs`` if
        it doesn't already exist.

        >>> from whoosh.filedb.filestore import FileStorage
        >>> st = FileStorage("indexdir")
        >>> st.create()

        This method returns ``self``, you can say::

            st = FileStorage("indexdir").create()

        Note that you can simply create handle the creation of the directory
        yourself and open the storage object using the initializer::

            dirname = "indexdir"
            os.mkdir(dirname)
            st = FileStorage(dirname)

        However, using the ``create()`` method allows you to potentially swap in
        other storage implementations more easily.

        :return: a :class:`Storage` instance.
        """

        dirpath = os.path.abspath(self.folder)
        # If the given directory does not already exist, try to create it
        try:
            os.makedirs(dirpath)
        except OSError:
            # This is necessary for compatibility between Py2 and Py3
            e = sys.exc_info()[1]
            # If we get an error because the path already exists, ignore it
            if e.errno != errno.EEXIST:
                raise

        # Raise an exception if the given path is not a directory
        if not os.path.isdir(dirpath):
            e = IOError(f"{dirpath!r} is not a directory")
            e.errno = errno.ENOTDIR
            raise e

        return self

    def destroy(self):
        """
        Removes any files in this storage object and then removes the storage object's directory.
        What happens if any of the files or the directory are in use depends on the underlying platform.

        Raises:
            OSError: If an error occurs while removing the directory.

        Example:
            storage = FileStorage('/path/to/storage')
            storage.destroy()
        """
        # Remove all files
        self.clean()
        try:
            # Try to remove the directory
            os.rmdir(self.folder)
        except OSError:
            e = sys.exc_info()[1]
            if e.errno == errno.ENOENT:
                pass
            else:
                raise e

    def create_file(self, name, excl=False, mode="wb", **kwargs):
        """
        Creates a file with the given name in this storage.

        :param name: The name for the new file.
        :type name: str
        :param excl: If True, try to open the file in "exclusive" mode. Defaults to False.
        :type excl: bool
        :param mode: The mode flags with which to open the file. Defaults to "wb".
        :type mode: str
        :param kwargs: Additional keyword arguments to be passed to the :class:`whoosh.filedb.structfile.StructFile` constructor.
        :return: A :class:`whoosh.filedb.structfile.StructFile` instance representing the created file.
        :rtype: whoosh.filedb.structfile.StructFile
        :raises ReadOnlyError: If the storage is in read-only mode.
        """
        if self.readonly:
            raise ReadOnlyError

        path = self._fpath(name)
        if excl:
            flags = os.O_CREAT | os.O_EXCL | os.O_RDWR
            if hasattr(os, "O_BINARY"):
                flags |= os.O_BINARY
            fd = os.open(path, flags)
            fileobj = os.fdopen(fd, mode)
        else:
            fileobj = open(path, mode)

        f = StructFile(fileobj, name=name, **kwargs)
        return f

    def open_file(self, name, **kwargs):
        """
        Opens an existing file in this storage.

        :param name: The name of the file to open.
        :type name: str
        :param kwargs: Additional keyword arguments passed to the StructFile initializer.
        :type kwargs: dict
        :return: An instance of `whoosh.filedb.structfile.StructFile`.
        :rtype: whoosh.filedb.structfile.StructFile
        :raises FileNotFoundError: If the specified file does not exist.
        :raises IOError: If there is an error opening the file.

        This method opens an existing file in the storage and returns an instance of `whoosh.filedb.structfile.StructFile`.
        The `StructFile` class provides a file-like interface for reading and writing data to the file.

        Example usage:
        >>> storage = FileStorage("/path/to/storage")
        >>> file = storage.open_file("example.txt", mode="rb")
        >>> data = file.read()
        >>> file.close()

        Note that the `name` parameter should be a valid file name within the storage.
        Additional keyword arguments are passed through to the `StructFile` initializer,
        allowing customization of the file opening behavior (e.g., specifying the file mode).

        It is important to close the file after use to release system resources.
        The `StructFile` instance returned by this method provides a `close()` method for this purpose.
        """
        f = StructFile(open(self._fpath(name), "rb"), name=name, **kwargs)
        return f

    def _fpath(self, fname):
        """
        Returns the absolute file path for the given filename within the filestore.

        Args:
            fname (str): The name of the file.

        Returns:
            str: The absolute file path.

        Raises:
            None

        Example:
            >>> store = FileStore('/path/to/folder')
            >>> store._fpath('data.txt')
            '/path/to/folder/data.txt'
        """
        return os.path.abspath(os.path.join(self.folder, fname))

    def clean(self, ignore=False):
        """
        Remove all files in the filestore.

        Args:
            ignore (bool, optional): If True, any OSError raised during file removal will be ignored.
                If False (default), an OSError will be raised if any file removal fails.

        Raises:
            ReadOnlyError: If the filestore is in read-only mode.
            OSError: If an error occurs while removing a file and ignore is set to False.

        Note:
            This method is used to clean the filestore by removing all files within it.
            It is important to note that this operation cannot be undone.

        Example:
            >>> filestore = FileStore('/path/to/folder')
            >>> filestore.clean(ignore=True)
        """
        if self.readonly:
            raise ReadOnlyError

        path = self.folder
        files = self.list()
        for fname in files:
            try:
                os.remove(os.path.join(path, fname))
            except OSError:
                if not ignore:
                    raise

    def list(self):
        """
        Returns a list of files in the specified folder.

        This method lists all the files in the folder specified during the initialization
        of the FileStore object.

        Returns:
            list: A list of file names in the folder.

        Raises:
            OSError: If an error occurs while accessing the folder.

        Example:
            >>> fs = FileStore('/path/to/folder')
            >>> files = fs.list()
            >>> print(files)
            ['file1.txt', 'file2.txt', 'file3.txt']
        """
        try:
            files = os.listdir(self.folder)
        except OSError:
            files = []

        return files

    def file_exists(self, name):
        """
        Check if a file exists in the filestore.

        Args:
            name (str): The name of the file to check.

        Returns:
            bool: True if the file exists, False otherwise.
        """
        return os.path.exists(self._fpath(name))

    def file_modified(self, name):
        """
        Returns the modification time of the file with the given name.

        Parameters:
        - name (str): The name of the file.

        Returns:
        - float: The modification time of the file in seconds since the epoch.

        Raises:
        - FileNotFoundError: If the file does not exist.

        This method retrieves the modification time of the file specified by the given name.
        It uses the os.path.getmtime() function to get the modification time in seconds since the epoch.
        If the file does not exist, a FileNotFoundError is raised.

        Example usage:
        >>> store = FileStore()
        >>> modified_time = store.file_modified("example.txt")
        >>> print(modified_time)
        1629876543.0
        """
        return os.path.getmtime(self._fpath(name))

    def file_length(self, name):
        """
        Returns the length of a file in bytes.

        Args:
            name (str): The name of the file.

        Returns:
            int: The length of the file in bytes.

        Raises:
            FileNotFoundError: If the file does not exist.

        """
        return os.path.getsize(self._fpath(name))

    def delete_file(self, name):
        """
        Delete a file from the filestore.

        Args:
            name (str): The name of the file to delete.

        Raises:
            ReadOnlyError: If the filestore is in read-only mode.

        """
        if self.readonly:
            raise ReadOnlyError

        os.remove(self._fpath(name))

    def rename_file(self, oldname, newname, safe=False):
        """
        Renames a file in the filestore.

        Args:
            oldname (str): The name of the file to be renamed.
            newname (str): The new name for the file.
            safe (bool, optional): If True, raises a NameError if the new name already exists.
                                   If False, the existing file with the new name will be overwritten.

        Raises:
            ReadOnlyError: If the filestore is in read-only mode.
            NameError: If the new name already exists and safe is set to True.

        """
        if self.readonly:
            raise ReadOnlyError

        if os.path.exists(self._fpath(newname)):
            if safe:
                raise NameError(f"File {newname!r} exists")
            else:
                os.remove(self._fpath(newname))
        os.rename(self._fpath(oldname), self._fpath(newname))

    def lock(self, name):
        """
        Acquires a lock for the specified file.

        Args:
            name (str): The name of the file to lock.

        Returns:
            FileLock: A lock object that can be used to manage the file lock.

        Raises:
            OSError: If an error occurs while acquiring the lock.

        Notes:
            This method is used to acquire a lock for a specific file in the filestore.
            The lock prevents other processes from modifying the file while it is locked.
            It is important to release the lock using the `release` method when it is no longer needed.
        """
        return FileLock(self._fpath(name))

    def temp_storage(self, name=None):
        """
        Creates a temporary storage file for the filestore.

        Args:
            name (str, optional): The name of the temporary storage file. If not provided, a random name will be generated.

        Returns:
            FileStorage: The temporary storage file.

        Raises:
            OSError: If there is an error creating the temporary storage file.

        Example:
            >>> filestore = FileStore()
            >>> temp_storage = filestore.temp_storage()
        """
        name = name or f"{random_name()}.tmp"
        path = os.path.join(self.folder, name)
        tempstore = FileStorage(path)
        return tempstore.create()


class RamStorage(Storage):
    """Storage object that keeps the index in memory.

    This class provides an implementation of the `Storage` interface that stores the index in memory.
    It is suitable for small indexes or for testing purposes.

    Attributes:
        files (dict): A dictionary that stores the file content in memory.
        locks (dict): A dictionary that stores locks for file access.
        folder (str): The folder path associated with the storage.

    Note:
        - This implementation does not support memory-mapped files (`supports_mmap` is set to False).
        - The `files` dictionary stores the file content as key-value pairs, where the key is the file name and the value is the file content.
        - The `locks` dictionary stores locks for file access, where the key is the file name and the value is the lock object.
        - The `folder` attribute is not used in this implementation.

    """

    supports_mmap = False

    def __init__(self):
        """
        Initialize a FileStore object.

        This class represents a file store that manages a collection of files and their locks.
        It provides methods for adding, retrieving, and managing files within the store.

        Attributes:
        - files (dict): A dictionary that maps file names to their corresponding file objects.
        - locks (dict): A dictionary that maps file names to their corresponding lock objects.
        - folder (str): The folder path where the files are stored.

        Usage:
        - Create a new FileStore object by calling the constructor.
        - Use the `add_file` method to add a file to the store.
        - Use the `get_file` method to retrieve a file from the store.
        - Use the `lock_file` and `unlock_file` methods to manage file locks.
        """
        self.files = {}
        self.locks = {}
        self.folder = ""

    def destroy(self):
        """
        Deletes all files and locks associated with the file store.

        This method permanently deletes all files and locks associated with the file store.
        After calling this method, the file store will be empty and all resources will be released.

        Note:
            - Use this method with caution as it irreversibly deletes all files and locks.
            - Make sure to close any open indexes before calling this method.

        Raises:
            - OSError: If there is an error while deleting the files or locks.

        """
        del self.files
        del self.locks

    def list(self):
        """
        Return a list of all the files stored in the filestore.

        Returns:
            list: A list of file names.
        """
        return list(self.files.keys())

    def clean(self):
        """
        Removes all files from the filestore.

        This method clears the internal dictionary of files, effectively removing all files from the filestore.
        After calling this method, the filestore will be empty.

        Usage:
            ram_storage = RamStorage()
            ram_storage.clean()

        """
        self.files = {}

    def total_size(self):
        """
        Returns the total size of all files in the filestore.

        This method calculates the total size of all files in the filestore by summing the file lengths
        of all files returned by the `list()` method.

        Returns:
            int: The total size of all files in the filestore.

        Example:
            >>> filestore = RamStorage()
            >>> filestore.total_size()
            1024
        """
        return sum(self.file_length(f) for f in self.list())

    def file_exists(self, name):
        """
        Check if a file with the given name exists in the filestore.

        Parameters:
        - name (str): The name of the file to check.

        Returns:
        - bool: True if the file exists, False otherwise.
        """
        return name in self.files

    def file_length(self, name):
        """
        Returns the length of a file in the filestore.

        Args:
            name (str): The name of the file.

        Returns:
            int: The length of the file in bytes.

        Raises:
            NameError: If the file with the given name does not exist in the filestore.
        """
        if name not in self.files:
            raise NameError(name)
        return len(self.files[name])

    def file_modified(self, name):
        """
        Returns the modification time of the file with the given name.

        Parameters:
        - name (str): The name of the file.

        Returns:
        - int: The modification time of the file in seconds since the epoch.

        Note:
        This method always returns -1, indicating that the modification time is unknown.
        """
        return -1

    def delete_file(self, name):
        """
        Delete a file from the filestore.

        Args:
            name (str): The name of the file to delete.

        Raises:
            NameError: If the specified file does not exist in the filestore.

        Returns:
            None
        """
        if name not in self.files:
            raise NameError(name)
        del self.files[name]

    def rename_file(self, name, newname, safe=False):
        """
        Renames a file in the filestore.

        Args:
            name (str): The name of the file to be renamed.
            newname (str): The new name for the file.
            safe (bool, optional): If True, checks if the new name already exists in the filestore before renaming.
                Raises an error if the new name already exists. Defaults to False.

        Raises:
            NameError: If the file with the given name does not exist in the filestore.
            NameError: If the new name already exists in the filestore and safe is True.

        Returns:
            None

        """
        if name not in self.files:
            raise NameError(name)
        if safe and newname in self.files:
            raise NameError(f"File {newname!r} exists")

        content = self.files[name]
        del self.files[name]
        self.files[newname] = content

    def create_file(self, name, **kwargs):
        """
        Create a file in the filestore.

        This method creates a file in the filestore and returns a StructFile object
        that can be used to read from and write to the file.

        Parameters:
        - name (str): The name of the file to create.

        Returns:
        - StructFile: A StructFile object representing the created file.

        Example usage:
        >>> filestore = FileStore()
        >>> file = filestore.create_file("example.txt")
        >>> file.write("Hello, World!")
        >>> file.close()

        Note:
        - The created file is stored in the `files` dictionary of the FileStore object.
        - The file content is stored as a byte string in the `file` attribute of the StructFile object.
        - The `onclose_fn` function is called when the StructFile object is closed, and it updates the `files` dictionary with the file content.

        """

        def onclose_fn(sfile):
            self.files[name] = sfile.file.getvalue()

        f = StructFile(BytesIO(), name=name, onclose=onclose_fn)
        return f

    def open_file(self, name, **kwargs):
        """
        Opens a file from the filestore.

        Args:
            name (str): The name of the file to open.

        Returns:
            BufferFile: The opened file as a BufferFile object.

        Raises:
            NameError: If the specified file does not exist in the filestore.
        """
        if name not in self.files:
            raise NameError(name)
        buf = memoryview_(self.files[name])
        return BufferFile(buf, name=name, **kwargs)

    def lock(self, name):
        """
        Acquires a lock for the given name.

        If a lock for the given name does not exist, a new lock is created and stored in the `locks` dictionary.
        Subsequent calls to `lock` with the same name will return the same lock object.

        Parameters:
        - name (str): The name of the lock.

        Returns:
        - Lock: The lock object associated with the given name.

        Example:
        >>> store = RamStorage()
        >>> lock1 = store.lock("my_lock")
        >>> lock2 = store.lock("my_lock")
        >>> lock1 is lock2
        True
        """
        if name not in self.locks:
            self.locks[name] = Lock()
        return self.locks[name]

    def temp_storage(self, name=None):
        """
        Creates a temporary storage for the file.

        Args:
            name (str, optional): The name of the temporary file. If not provided, a random name will be generated.

        Returns:
            FileStorage: The temporary storage object.

        Raises:
            OSError: If there is an error creating the temporary file.

        Example:
            >>> store = temp_storage("my_temp_file")
            >>> store.write("Hello, World!")
            >>> store.close()
        """
        tdir = tempfile.gettempdir()
        name = name or f"{random_name()}.tmp"
        path = os.path.join(tdir, name)
        tempstore = FileStorage(path)
        return tempstore.create()


def copy_storage(sourcestore, deststore):
    """Copies the files from the source storage object to the destination
    storage object using ``shutil.copyfileobj``.

    Parameters:
    - sourcestore (object): The source storage object from which files will be copied.
    - deststore (object): The destination storage object to which files will be copied.

    Returns:
    - None

    Raises:
    - None

    Example usage:
    ```
    sourcestore = FileStore(...)
    deststore = FileStore(...)
    copy_storage(sourcestore, deststore)
    ```

    This function iterates over the files in the source storage object and copies each file
    to the destination storage object using the `shutil.copyfileobj` function. It is useful
    for copying files between different storage objects, such as local file systems or cloud
    storage systems.

    Note: Both the source and destination storage objects must implement the following methods:
    - `list()`: Returns a list of file names in the storage object.
    - `open_file(name)`: Opens the file with the given name in the storage object and returns
      a file-like object.
    - `create_file(name)`: Creates a new file with the given name in the storage object and
      returns a file-like object for writing.

    Example storage object implementation:
    ```
    class FileStore:
        def list(self):
            # implementation

        def open_file(self, name):
            # implementation

        def create_file(self, name):
            # implementation
    ```
    """
    from shutil import copyfileobj

    for name in sourcestore.list():
        with sourcestore.open_file(name) as source:
            with deststore.create_file(name) as dest:
                copyfileobj(source, dest)


def copy_to_ram(storage):
    """Copies the given FileStorage object into a new RamStorage object.

    This function creates a new RamStorage object and copies all the files and directories
    from the provided FileStorage object into it. The RamStorage object is an in-memory
    storage implementation that allows fast access to the files.

    :param storage: The FileStorage object to be copied.
    :type storage: :class:`FileStorage`

    :return: The newly created RamStorage object containing the copied files.
    :rtype: :class:`RamStorage`
    """

    ram = RamStorage()
    copy_storage(storage, ram)
    return ram
