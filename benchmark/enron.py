import os.path
import tarfile
from email import message_from_string
from marshal import dump, load
from urllib.request import urlretrieve
from zlib import compress, decompress

try:
    import xappy  # type: ignore
except ImportError:
    pass

from whoosh import analysis, fields
from whoosh.support.bench import Bench, Spec
from whoosh.util import now


class Enron(Spec):
    """
    The Enron class provides functionality for downloading, caching, and processing the Enron email archive.
    """

    name = "enron"

    enron_archive_url = "http://www.cs.cmu.edu/~enron/enron_mail_082109.tar.gz"
    enron_archive_filename = "enron_mail_082109.tar.gz"
    cache_filename = "enron_cache.pickle"

    header_to_field = {
        "Date": "date",
        "From": "frm",
        "To": "to",
        "Subject": "subject",
        "Cc": "cc",
        "Bcc": "bcc",
    }

    main_field = "body"
    headline_field = "subject"

    field_order = ("subject", "date", "from", "to", "cc", "bcc", "body")

    cachefile = None

    def download_archive(self, archive):
        """
        Downloads the Enron email archive from the specified URL and saves it to the given file path.

        Args:
            archive (str): The file path to save the downloaded archive.

        Raises:
            FileNotFoundError: If the archive file does not exist.
        """
        print(f"Downloading Enron email archive to {archive}...")
        t = now()
        urlretrieve(self.enron_archive_url, archive)
        print(f"Downloaded in {now() - t} seconds")

    @staticmethod
    def get_texts(archive):
        """
        Generator function that yields the text content of each email in the given archive.

        Args:
            archive (str): The file path of the archive.

        Yields:
            str: The text content of each email.
        """
        archive = tarfile.open(archive, "r:gz")
        while True:
            entry = next(archive)
            archive.members = []
            if entry is None:
                break
            f = archive.extractfile(entry)
            if f is not None:
                text = f.read()
                yield text

    @staticmethod
    def get_messages(archive, headers=True):
        """
        Generator function that yields the parsed messages from the given email archive.

        Args:
            archive (str): The file path of the archive.
            headers (bool, optional): Whether to include message headers. Defaults to True.

        Yields:
            dict: The dictionary representation of each message.
        """
        header_to_field = Enron.header_to_field
        for text in Enron.get_texts(archive):
            message = message_from_string(text)
            body = message.as_string().decode("latin_1")
            blank = body.find("\n\n")
            if blank > -1:
                body = body[blank + 2 :]
            d = {"body": body}
            if headers:
                for k in message.keys():
                    fn = header_to_field.get(k)
                    if not fn:
                        continue
                    v = message.get(k).strip()
                    if v:
                        d[fn] = v.decode("latin_1")
            yield d

    def cache_messages(self, archive, cache):
        """
        Caches the messages from the given email archive into a pickle file.

        Args:
            archive (str): The file path of the archive.
            cache (str): The file path to save the cached messages.

        Raises:
            FileNotFoundError: If the archive file does not exist.
        """
        print(f"Caching messages in {cache}...")

        if not os.path.exists(archive):
            raise FileNotFoundError(f"Archive file {archive} does not exist")

        t = now()
        f = open(cache, "wb")
        c = 0
        for d in self.get_messages(archive):
            c += 1
            dump(d, f)
            if not c % 1000:
                print(c)
        f.close()
        print(f"Cached messages in {now() - t} seconds")

    def setup(self):
        """
        Sets up the Enron email archive by downloading it if necessary and caching the messages.
        """
        archive = os.path.abspath(
            os.path.join(self.options.dir, self.enron_archive_filename)
        )
        cache = os.path.abspath(os.path.join(self.options.dir, self.cache_filename))

        if not os.path.exists(archive):
            self.download_archive(archive)
        else:
            print("Archive is OK")

        if not os.path.exists(cache):
            self.cache_messages(archive, cache)
        else:
            print("Cache is OK")

    def documents(self):
        """
        Generator function that yields the cached messages from the pickle file.

        Yields:
            dict: The dictionary representation of each message.

        Raises:
            FileNotFoundError: If the message cache does not exist.
        """
        if not os.path.exists(self.cache_filename):
            raise FileNotFoundError("Message cache does not exist, use --setup")

        f = open(self.cache_filename, "rb")
        try:
            while True:
                self.filepos = f.tell()
                d = load(f)
                yield d
        except EOFError:
            pass
        f.close()

    def whoosh_schema(self):
        """
        Returns the Whoosh schema for indexing the Enron email archive.

        Returns:
            whoosh.fields.Schema: The schema for indexing the emails.
        """
        ana = analysis.stemming_analyzer(maxsize=40, cachesize=None)
        storebody = self.options.storebody
        schema = fields.Schema(
            body=fields.TEXT(analyzer=ana, stored=storebody),
            filepos=fields.STORED,
            date=fields.ID(stored=True),
            frm=fields.ID(stored=True),
            to=fields.IDLIST(stored=True),
            subject=fields.TEXT(stored=True),
            cc=fields.IDLIST,
            bcc=fields.IDLIST,
        )
        return schema

    def xappy_indexer_connection(self, path):
        """
        Creates and returns an Xapian indexer connection for indexing the Enron email archive.

        Args:
            path (str): The path to the Xapian index.

        Returns:
            xappy.IndexerConnection: The Xapian indexer connection.
        """
        conn = xappy.IndexerConnection(path)
        conn.add_field_action("body", xappy.FieldActions.INDEX_FREETEXT, language="en")
        if self.options.storebody:
            conn.add_field_action("body", xappy.FieldActions.STORE_CONTENT)
        conn.add_field_action("date", xappy.FieldActions.INDEX_EXACT)
        conn.add_field_action("date", xappy.FieldActions.STORE_CONTENT)
        conn.add_field_action("frm", xappy.FieldActions.INDEX_EXACT)
        conn.add_field_action("frm", xappy.FieldActions.STORE_CONTENT)
        conn.add_field_action("to", xappy.FieldActions.INDEX_EXACT)
        conn.add_field_action("to", xappy.FieldActions.STORE_CONTENT)
        conn.add_field_action(
            "subject", xappy.FieldActions.INDEX_FREETEXT, language="en"
        )
        conn.add_field_action("subject", xappy.FieldActions.STORE_CONTENT)
        conn.add_field_action("cc", xappy.FieldActions.INDEX_EXACT)
        conn.add_field_action("bcc", xappy.FieldActions.INDEX_EXACT)
        return conn

    def zcatalog_setup(self, cat):
        """
        Sets up the ZCatalog indexes for indexing the Enron email archive.

        Args:
            cat (zcatalog.catalog.Catalog): The ZCatalog catalog.
        """
        from zcatalog import indexes  # type: ignore

        for name in ("date", "frm"):
            cat[name] = indexes.FieldIndex(field_name=name)
        for name in ("to", "subject", "cc", "bcc", "body"):
            cat[name] = indexes.TextIndex(field_name=name)

    def process_document_whoosh(self, d):
        """
        Processes a document for indexing with Whoosh.

        Args:
            d (dict): The document to process.
        """
        d["filepos"] = self.filepos
        if self.options.storebody:
            mf = self.main_field
            d[f"_stored_{mf}"] = compress(d[mf], 9)

    def process_result_whoosh(self, d):
        """
        Processes a search result from Whoosh.

        Args:
            d (dict): The search result.

        Returns:
            dict: The processed search result.
        """
        mf = self.main_field
        if mf in d:
            d.fields()[mf] = decompress(d[mf])
        else:
            if not self.cachefile:
                self.cachefile = open(self.cache_filename, "rb")
            filepos = d["filepos"]
            self.cachefile.seek(filepos)
            dd = load(self.cachefile)
            d.fields()[mf] = dd[mf]
        return d

    def process_document_xapian(self, d):
        """
        Processes a document for indexing with Xapian.

        Args:
            d (dict): The document to process.
        """
        d[self.main_field] = " ".join([d.get(name, "") for name in self.field_order])


if __name__ == "__main__":
    Bench().run(Enron)
