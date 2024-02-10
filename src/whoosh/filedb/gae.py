"""
This module contains EXPERIMENTAL support for storing a Whoosh index's files in
the Google App Engine blobstore. This will use a lot of RAM since all files are
loaded into RAM, but it potentially useful as a workaround for the lack of file
storage in Google App Engine.

Use at your own risk, but please report any problems to me so I can fix them.

To create a new index::

    from whoosh.filedb.gae import DatastoreStorage

    ix = DatastoreStorage().create_index(schema)

To open an existing index::

    ix = DatastoreStorage().open_index()

This module provides the following classes:

- `DatastoreFile`: A file-like object that is backed by a BytesIO() object whose contents
  is loaded from a BlobProperty in the app engine datastore.

- `MemcacheLock`: A lock object that uses the Google App Engine memcache service for synchronization.

- `DatastoreStorage`: An implementation of `whoosh.store.Storage` that stores files in
  the app engine datastore as blob properties.

Usage:
1. Creating an index:
    storage = DatastoreStorage()
    schema = Schema(...)
    index = storage.create_index(schema)

2. Opening an existing index:
    storage = DatastoreStorage()
    index = storage.open_index()

3. Listing all files in the storage:
    storage = DatastoreStorage()
    files = storage.list()

4. Deleting a file:
    storage = DatastoreStorage()
    storage.delete_file(filename)

5. Renaming a file:
    storage = DatastoreStorage()
    storage.rename_file(old_filename, new_filename)

6. Creating a new file:
    storage = DatastoreStorage()
    file = storage.create_file(filename)

7. Opening an existing file:
    storage = DatastoreStorage()
    file = storage.open_file(filename)

Note: This class assumes that the necessary dependencies and configurations
for using the app engine datastore are already set up.
"""

import time
from io import BytesIO

from google.appengine.api import memcache  # type: ignore @UnresolvedImport
from google.appengine.ext import db  # type: ignore @UnresolvedImport

from whoosh.filedb.filestore import ReadOnlyError, Storage
from whoosh.filedb.structfile import StructFile
from whoosh.index import _DEF_INDEX_NAME, TOC, FileIndex


class DatastoreFile(db.Model):
    """A file-like object that is backed by a BytesIO() object whose contents
    is loaded from a BlobProperty in the app engine datastore.

    Attributes:
        value (db.BlobProperty): The contents of the file stored as a BlobProperty.
        mtime (db.IntegerProperty): The modification time of the file in seconds since the epoch.

    Methods:
        __init__: Initializes a new instance of the DatastoreFile class.
        loadfile: Loads a DatastoreFile object from the datastore or memcache.
        close: Closes the file, updates the value and mtime properties, and stores the changes in the datastore.
        tell: Returns the current position in the file.
        write: Writes the specified data to the file.
        read: Reads the specified number of bytes from the file.
        seek: Changes the current position in the file.
        readline: Reads a line from the file.
        getvalue: Returns the contents of the file as a string.

    Usage:
        # Create a new DatastoreFile object
        file = DatastoreFile()

        # Load a DatastoreFile object from the datastore or memcache
        file = DatastoreFile.loadfile("filename")

        # Read from the file
        data = file.read(100)

        # Write to the file
        file.write("Hello, World!")

        # Close the file and store the changes in the datastore
        file.close()
    """

    value = db.BlobProperty()
    mtime = db.IntegerProperty(default=0)

    def __init__(self, *args, **kwargs):
        """
        Initialize a GAEStorage object.

        This method initializes the GAEStorage object by calling the parent class's
        __init__ method and setting up the necessary attributes.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Attributes:
            data (BytesIO): A BytesIO object to store the data.

        Returns:
            None
        """
        super().__init__(*args, **kwargs)
        self.data = BytesIO()

    @classmethod
    def loadfile(cls, name):
        """
        Load a file from the datastore or memcache.

        This method retrieves a file from the datastore or memcache based on the given name.
        If the file is not found in memcache, it is fetched from the datastore and stored in memcache for future use.

        Parameters:
        - cls: The class representing the file entity.
        - name: The name of the file to load.

        Returns:
        - file: The loaded file object.

        Usage:
        file = loadfile(FileEntity, "example.txt")
        """

        value = memcache.get(name, namespace="DatastoreFile")
        if value is None:
            file = cls.get_by_key_name(name)
            memcache.set(name, file.value, namespace="DatastoreFile")
        else:
            file = cls(value=value)
        file.data = BytesIO(file.value)
        return file

    def close(self):
        """
        Closes the file and updates the value in the datastore.

        This method is responsible for closing the file and updating the value in the datastore
        if the value has changed. It also updates the modification time and stores the value in
        the memcache for faster access.

        Returns:
            None

        Raises:
            None
        """
        oldvalue = self.value
        self.value = self.getvalue()
        if oldvalue != self.value:
            self.mtime = int(time.time())
            self.put()
            memcache.set(self.key().id_or_name(), self.value, namespace="DatastoreFile")

    def tell(self):
        """
        Returns the current position of the file pointer.

        This method returns the current position of the file pointer within the file.
        It is equivalent to calling the `tell()` method on the underlying file object.

        Returns:
            int: The current position of the file pointer.

        Example:
            >>> file = GAEFile(...)
            >>> file.tell()
            42
        """
        return self.data.tell()

    def write(self, data):
        """
        Writes the given data to the file.

        Args:
            data (bytes): The data to be written to the file.

        Returns:
            int: The number of bytes written.

        Raises:
            IOError: If an error occurs while writing to the file.

        Example:
            >>> file = File()
            >>> data = b"Hello, World!"
            >>> file.write(data)
            13
        """
        return self.data.write(data)

    def read(self, length):
        """
        Read the specified number of bytes from the data.

        Args:
            length (int): The number of bytes to read.

        Returns:
            bytes: The bytes read from the data.

        Raises:
            IOError: If an error occurs while reading the data.

        Example:
            To read 10 bytes from the data, you can use the following code:

            >>> data = GAEFileData()
            >>> data.read(10)
        """
        return self.data.read(length)

    def seek(self, *args):
        """
        Seeks to a specified position in the file.

        Args:
            *args: Variable-length argument list. The arguments are passed to the underlying `seek` method.

        Returns:
            int: The new position in the file.

        Raises:
            OSError: If an error occurs while seeking the file.

        Example:
            To seek to the beginning of the file, use `seek(0)`.

        """
        return self.data.seek(*args)

    def readline(self):
        """
        Read and return the next line from the data file.

        Returns:
            str: The next line from the data file.

        Raises:
            None

        Notes:
            This method reads and returns the next line from the data file associated with the current instance of the `GAEFile` class.

        Example:
            >>> file = GAEFile()
            >>> line = file.readline()
        """
        return self.data.readline()

    def getvalue(self):
        """
        Returns the value stored in the data attribute.

        This method retrieves the value stored in the data attribute of the current object.
        It returns the value as a string.

        Returns:
            str: The value stored in the data attribute.

        Example:
            >>> obj = ClassName()
            >>> obj.getvalue()
            'some value'
        """
        return self.data.getvalue()


class MemcacheLock:
    """
    A lock implementation using Google App Engine's memcache.

    This class provides a simple lock mechanism using memcache to synchronize access to a resource.
    It allows acquiring and releasing locks, with an optional blocking behavior.

    Usage:
    lock = MemcacheLock("my_lock_name")
    lock.acquire()  # Acquire the lock
    # Critical section
    lock.release()  # Release the lock

    If blocking is set to True, the acquire method will block until the lock is acquired.
    If the lock is already acquired by another process, the acquire method will retry every 0.1 seconds until it succeeds.

    Note: This lock implementation assumes that the memcache service is available and properly configured.

    Args:
        name (str): The name of the lock.

    Attributes:
        name (str): The name of the lock.

    """

    def __init__(self, name):
        self.name = name

    def acquire(self, blocking=False):
        """
        Acquire the lock.

        Args:
            blocking (bool, optional): If True, the method will block until the lock is acquired.
                                       If False (default), the method will return immediately.

        Returns:
            bool: True if the lock is acquired, False otherwise.

        """
        val = memcache.add(self.name, "L", 360, namespace="whooshlocks")

        if blocking and not val:
            # Simulate blocking by retrying the acquire over and over
            import time

            while not val:
                time.sleep(0.1)
                val = memcache.add(self.name, "", 360, namespace="whooshlocks")

        return val

    def release(self):
        """
        Release the lock.

        """
        memcache.delete(self.name, namespace="whooshlocks")


class DatastoreStorage(Storage):
    """An implementation of :class:`whoosh.store.Storage` that stores files in
    the app engine datastore as blob properties.

    This class provides methods to create, open, list, clean, and manipulate files
    stored in the app engine datastore. It is designed to be used as a storage
    backend for the Whoosh search engine library.

    Usage:
    1. Creating an index:
        storage = DatastoreStorage()
        schema = Schema(...)
        index = storage.create_index(schema)

    2. Opening an existing index:
        storage = DatastoreStorage()
        index = storage.open_index()

    3. Listing all files in the storage:
        storage = DatastoreStorage()
        files = storage.list()

    4. Deleting a file:
        storage = DatastoreStorage()
        storage.delete_file(filename)

    5. Renaming a file:
        storage = DatastoreStorage()
        storage.rename_file(old_filename, new_filename)

    6. Creating a new file:
        storage = DatastoreStorage()
        file = storage.create_file(filename)

    7. Opening an existing file:
        storage = DatastoreStorage()
        file = storage.open_file(filename)

    Note: This class assumes that the necessary dependencies and configurations
    for using the app engine datastore are already set up.

    """

    def create_index(self, schema, indexname=_DEF_INDEX_NAME):
        """Create a new index with the given schema.

        Args:
            schema (Schema): The schema for the index.
            indexname (str, optional): The name of the index. Defaults to _DEF_INDEX_NAME.

        Returns:
            FileIndex: The created index.

        Raises:
            ReadOnlyError: If the storage is in read-only mode.

        """
        if self.readonly:
            raise ReadOnlyError

        TOC.create(self, schema, indexname)
        return FileIndex(self, schema, indexname)

    def open_index(self, indexname=_DEF_INDEX_NAME, schema=None):
        """Open an existing index.

        Args:
            indexname (str, optional): The name of the index. Defaults to _DEF_INDEX_NAME.
            schema (Schema, optional): The schema for the index. Defaults to None.

        Returns:
            FileIndex: The opened index.

        """
        return FileIndex(self, schema=schema, indexname=indexname)

    def list(self):
        """List all files in the storage.

        Returns:
            list: A list of file names.

        """
        query = DatastoreFile.all()
        return [file.key().id_or_name() for file in query]

    def clean(self):
        """Clean up the storage.

        This method does nothing in the case of the app engine datastore storage.

        """
        pass

    def total_size(self):
        """Get the total size of the storage.

        Returns:
            int: The total size in bytes.

        """
        return sum(self.file_length(f) for f in self.list())

    def file_exists(self, name):
        """Check if a file exists in the storage.

        Args:
            name (str): The name of the file.

        Returns:
            bool: True if the file exists, False otherwise.

        """
        return DatastoreFile.get_by_key_name(name) is not None

    def file_modified(self, name):
        """Get the modification time of a file.

        Args:
            name (str): The name of the file.

        Returns:
            datetime: The modification time of the file.

        """
        return DatastoreFile.get_by_key_name(name).mtime

    def file_length(self, name):
        """Get the length of a file.

        Args:
            name (str): The name of the file.

        Returns:
            int: The length of the file in bytes.

        """
        return len(DatastoreFile.get_by_key_name(name).value)

    def delete_file(self, name):
        """Delete a file from the storage.

        Args:
            name (str): The name of the file.

        Returns:
            bool: True if the file was successfully deleted, False otherwise.

        """
        memcache.delete(name, namespace="DatastoreFile")
        return DatastoreFile.get_by_key_name(name).delete()

    def rename_file(self, name, newname, safe=False):
        """Rename a file in the storage.

        Args:
            name (str): The current name of the file.
            newname (str): The new name for the file.
            safe (bool, optional): Whether to perform a safe rename. Defaults to False.

        """
        file = DatastoreFile.get_by_key_name(name)
        newfile = DatastoreFile(key_name=newname)
        newfile.value = file.value
        newfile.mtime = file.mtime
        newfile.put()
        file.delete()

    def create_file(self, name, **kwargs):
        """Create a new file in the storage.

        Args:
            name (str): The name of the file.
            **kwargs: Additional keyword arguments.

        Returns:
            StructFile: The created file.

        """
        f = StructFile(
            DatastoreFile(key_name=name),
            name=name,
            onclose=lambda sfile: sfile.file.close(),
        )
        return f

    def open_file(self, name, *args, **kwargs):
        """Open an existing file in the storage.

        Args:
            name (str): The name of the file.
            *args: Additional positional arguments.
            **kwargs: Additional keyword arguments.

        Returns:
            StructFile: The opened file.

        """
        return StructFile(DatastoreFile.loadfile(name))

    def lock(self, name):
        """Lock a file in the storage.

        Args:
            name (str): The name of the file.

        Returns:
            MemcacheLock: The lock object.

        """
        return MemcacheLock(name)

    def temp_storage(self, name=None):
        """Create a temporary storage.

        Args:
            name (str, optional): The name of the temporary storage. Defaults to None.

        Returns:
            DatastoreStorage: The temporary storage.

        """
        tempstore = DatastoreStorage()
        return tempstore.create()
