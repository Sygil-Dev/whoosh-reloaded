# Copyright 2010 Matt Chaput. All rights reserved.
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


import os.path
from optparse import OptionParser
from shutil import rmtree

from whoosh import index, qparser, query, scoring
from whoosh.util import find_object, now

try:
    import xappy  # type: ignore
except ImportError:
    pass
try:
    import xapian  # type: ignore
except ImportError:
    pass
try:
    import pysolr  # type: ignore
except ImportError:
    pass

try:
    from persistent import Persistent  # type: ignore

    class ZDoc(Persistent):
        def __init__(self, d):
            self.__dict__.update(d)

except ImportError:
    pass


class Module:
    def __init__(self, bench, options, args):
        """
        Initializes a Module object.

        Args:
            bench (object): The benchmark object.
            options (object): The options object.
            args (object): The arguments object.
        """
        self.bench = bench
        self.options = options
        self.args = args

    def __repr__(self):
        """
        Returns a string representation of the Module object.
        """
        return self.__class__.__name__

    def indexer(self, **kwargs):
        """
        Indexes the data using the specified keyword arguments.

        Args:
            **kwargs: Additional keyword arguments for configuring the indexing process.

        Returns:
            None
        """
        pass

    def index_document(self, d):
        """
        Indexes a document.

        Args:
            d (object): The document object.

        Raises:
            NotImplementedError: If the method is not implemented in the subclass.
        """
        raise NotImplementedError

    def finish(self, **kwargs):
        """
        Finishes the benchmark and performs any necessary cleanup.

        Args:
            **kwargs: Additional keyword arguments.

        Returns:
            None
        """
        pass

    def _process_result(self, d):
        """
        Processes the result.

        Args:
            d (object): The result object.

        Returns:
            The processed result.
        """
        attrname = f"process_result_{self.options.lib}"
        if hasattr(self.bench.spec, attrname):
            method = getattr(self.bench.spec, attrname)
            self._process_result = method
            return method(d)
        else:
            self._process_result = lambda x: x
            return d

    def searcher(self):
        """
        Returns a searcher object.
        """
        pass

    def query(self):
        """
        Executes a query.

        Raises:
            NotImplementedError: If the method is not implemented in the subclass.
        """
        raise NotImplementedError

    def find(self, q):
        """
        Finds a query.

        Args:
            q (object): The query object.

        Raises:
            NotImplementedError: If the method is not implemented in the subclass.
        """
        raise NotImplementedError

    def findterms(self, terms):
        """
        Finds terms.

        Args:
            terms (object): The terms object.

        Raises:
            NotImplementedError: If the method is not implemented in the subclass.
        """
        raise NotImplementedError

    def results(self, r):
        """
        Generates processed results.

        Args:
            r (object): The results object.

        Yields:
            The processed results.
        """
        for hit in r:
            yield self._process_result(hit)


class Spec:
    """
    The Spec class represents a benchmark specification.

    Attributes:
        headline_field (str): The name of the field containing the headline.
        main_field (str): The name of the main field.
        options (object): The benchmark options.
        args (list): The benchmark arguments.

    Methods:
        __init__(self, options, args): Initializes a new instance of the Spec class.
        documents(self): Abstract method to be implemented by subclasses.
        setup(self): Performs the setup for the benchmark.
        print_results(self, ls): Prints the benchmark results.

    Usage:
        spec = Spec(options, args)
        spec.setup()
        spec.print_results(ls)
    """

    headline_field = "title"
    main_field = "body"

    def __init__(self, options, args):
        """
        Initializes a new instance of the Spec class.

        Args:
            options (object): The benchmark options.
            args (list): The benchmark arguments.
        """
        self.options = options
        self.args = args

    def documents(self):
        """
        Abstract method to be implemented by subclasses.
        """
        raise NotImplementedError

    def setup(self):
        """
        Performs the setup for the benchmark.
        """
        pass

    def print_results(self, ls):
        """
        Prints the benchmark results.

        Args:
            ls (list): The list of benchmark results.
        """
        showbody = self.options.showbody
        snippets = self.options.snippets
        limit = self.options.limit
        for i, hit in enumerate(ls):
            if i >= limit:
                break

            print("%d. %s" % (i + 1, hit.get(self.headline_field)))
            if snippets:
                print(self.show_snippet(hit))
            if showbody:
                print(hit.get(self.main_field))


class WhooshModule(Module):
    """
    A module for interacting with the Whoosh search engine.

    This module provides methods for indexing documents, searching the index, and retrieving search results.

    Attributes:
        writer: An instance of the Whoosh IndexWriter used for adding documents to the index.
        srch: An instance of the Whoosh IndexSearcher used for searching the index.
        parser: An instance of the Whoosh QueryParser used for parsing search queries.

    Methods:
        indexer(create=True): Initializes the Whoosh index and sets up the IndexWriter.
        index_document(d): Indexes a document in the Whoosh index.
        finish(merge=True, optimize=False): Commits changes to the index.
        searcher(): Initializes the IndexSearcher and QueryParser.
        query(): Parses the search query string and returns a Query object.
        find(q): Executes a search query and returns the search results.
        findterms(terms): Executes multiple search queries for each term and returns the search results.

    Usage:
        module = WhooshModule()
        module.indexer()
        module.index_document(document)
        module.finish()
        module.searcher()
        query = module.query()
        results = module.find(query)
    """

    def indexer(self, create=True):
        """
        Creates or opens an index using the specified schema and options.

        Args:
            create (bool, optional): If True, creates a new index if it doesn't exist.
                                    If False, opens an existing index.
                                    Defaults to True.

        Returns:
            IndexWriter: An instance of IndexWriter for the created or opened index.
        """
        schema = self.bench.spec.whoosh_schema()
        path = os.path.join(self.options.dir, f"{self.options.indexname}_whoosh")

        if not os.path.exists(path):
            os.mkdir(path)
        if create:
            ix = index.create_in(path, schema)
        else:
            ix = index.open_dir(path)

        poolclass = None
        if self.options.pool:
            poolclass = find_object(self.options.pool)

        self.writer = ix.writer(
            limitmb=int(self.options.limitmb),
            poolclass=poolclass,
            dir=self.options.tempdir,
            procs=int(self.options.procs),
            batchsize=int(self.options.batch),
            multisegment=self.options.xms,
        )
        self._procdoc = None
        if hasattr(self.bench.spec, "process_document_whoosh"):
            self._procdoc = self.bench.spec.process_document_whoosh

    def index_document(self, d):
        """
        Indexes a document in the Whoosh index.

        Args:
            d (dict): The document to be indexed. The keys represent the field names and the values represent the field values.

        Returns:
            None
        """
        _procdoc = self._procdoc
        if _procdoc:
            _procdoc(d)
        self.writer.add_document(**d)

    def finish(self, merge=True, optimize=False):
        """
        Commits the changes made to the index.

        Args:
            merge (bool, optional): Specifies whether to perform a merge operation before committing.
                Defaults to True.
            optimize (bool, optional): Specifies whether to optimize the index after committing.
                Defaults to False.

        Returns:
            None

        Raises:
            Any exceptions raised by the underlying writer.commit() method.

        Notes:
            - This method should be called after making changes to the index to ensure that the changes
              are persisted.
            - By default, a merge operation is performed before committing. This helps in optimizing
              the index by merging smaller segments into larger ones.
            - If the `optimize` parameter is set to True, the index will be further optimized after
              committing. This can improve search performance but may take longer to complete.

        Usage:
            bench = Bench()
            # ... perform index modifications ...
            bench.finish(merge=True, optimize=False)
        """
        self.writer.commit(merge=merge, optimize=optimize)

    def searcher(self):
        """
        Creates and returns a searcher object for performing searches on the index.

        Returns:
            Searcher: A searcher object that can be used to perform searches on the index.

        Raises:
            OSError: If there is an error while opening the index directory.
        """
        path = os.path.join(self.options.dir, f"{self.options.indexname}_whoosh")
        ix = index.open_dir(path)
        self.srch = ix.searcher(weighting=scoring.PL2())
        self.parser = qparser.QueryParser(self.bench.spec.main_field, schema=ix.schema)

    def query(self):
        """
        Parses the query string and returns a parsed query object.

        Args:
            None

        Returns:
            A parsed query object.

        Raises:
            None

        Example:
            bench = Bench()
            bench.query()  # Returns a parsed query object
        """
        qstring = " ".join(self.args).decode("utf-8")
        return self.parser.parse(qstring)

    def find(self, q):
        """
        Executes a search query and returns the results.

        Args:
            q (str): The search query string.

        Returns:
            list: A list of search results.

        """
        return self.srch.search(
            q, limit=int(self.options.limit), optimize=self.options.optimize
        )

    def findterms(self, terms):
        """
        Searches for the given terms in the specified field and returns the search results.

        Args:
            terms (list): A list of terms to search for.

        Yields:
            whoosh.searching.Results: The search results for each term.

        Returns:
            None

        Raises:
            None

        Example:
            bench = Bench()
            terms = ["term1", "term2", "term3"]
            for result in bench.findterms(terms):
                print(result)
        """
        limit = int(self.options.limit)
        s = self.srch
        q = query.Term(self.bench.spec.main_field, None)
        for term in terms:
            q.text = term
            yield s.search(q, limit=limit)


class XappyModule(Module):
    """
    A module for indexing and searching documents using Xappy.

    This module provides methods for indexing documents, performing searches,
    and retrieving search results using the Xappy library.

    Usage:
    1. Create an instance of XappyModule.
    2. Call the `indexer` method to obtain a connection to the Xappy index.
    3. Use the `index_document` method to add documents to the index.
    4. Call the `finish` method to flush any pending changes to the index.
    5. Call the `searcher` method to obtain a connection for searching the index.
    6. Use the `query` method to create a query object for searching.
    7. Call the `find` method to perform a search and retrieve the results.
    8. Use the `results` method to iterate over the search results.

    Note: Before using this module, make sure to install the Xappy library.

    Attributes:
        options (object): An object containing configuration options.
        bench (object): An object representing the benchmarking tool.

    Methods:
        indexer(**kwargs): Returns a connection to the Xappy index.
        index_document(conn=None, d=None): Indexes a document in the Xappy index.
        finish(conn): Flushes any pending changes to the Xappy index.
        searcher(): Returns a connection for searching the Xappy index.
        query(conn=None): Creates a query object for searching the Xappy index.
        find(conn=None, q=None): Performs a search and retrieves the results.
        findterms(conn=None, terms=None): Performs searches for multiple terms.
        results(r): Iterates over the search results.

    """

    def indexer(self, **kwargs):
        """
        Creates and returns a connection to the Xappy index.

        Args:
            **kwargs: Additional keyword arguments to be passed to the Xappy connection.

        Returns:
            Xappy connection: A connection to the Xappy index.

        Raises:
            None.

        Example usage:
            conn = indexer()
            # Use the connection to perform operations on the Xappy index
        """
        path = os.path.join(self.options.dir, f"{self.options.indexname}_xappy")
        conn = self.bench.spec.xappy_connection(path)
        return conn

    def index_document(self, conn=None, d=None):
        """
        Indexes a document in the Xappy index.

        Args:
            conn (Xappy connection, optional): The connection to the Xappy index. If not provided, a new connection will be created.
            d (dict): The document to be indexed.

        Returns:
            None.

        Raises:
            None.
        """
        if hasattr(self.bench, "process_document_xappy"):
            self.bench.process_document_xappy(d)
        doc = xappy.UnprocessedDocument()
        for key, values in d:
            if not isinstance(values, list):
                values = [values]
            for value in values:
                doc.fields.append(xappy.Field(key, value))
        conn.add(doc)

    def finish(self, conn):
        """
        Flushes any pending changes to the Xappy index.

        Args:
            conn (Xappy connection): The connection to the Xappy index.

        Returns:
            None.

        Raises:
            None.
        """
        conn.flush()

    def searcher(self):
        """
        Returns a connection for searching the Xappy index.

        Args:
            None.

        Returns:
            Xappy connection: A connection for searching the Xappy index.

        Raises:
            None.
        """
        path = os.path.join(self.options.dir, f"{self.options.indexname}_xappy")
        return xappy.SearchConnection(path)

    def query(self, conn=None):
        """
        Creates a query object for searching the Xappy index.

        Args:
            conn (Xappy connection, optional): The connection to the Xappy index. If not provided, a new connection will be created.

        Returns:
            Xappy query: A query object for searching the Xappy index.

        Raises:
            None.
        """
        return conn.query_parse(" ".join(self.args))

    def find(self, conn=None, q=None):
        """
        Performs a search and retrieves the results.

        Args:
            conn (Xappy connection, optional): The connection to the Xappy index. If not provided, a new connection will be created.
            q (Xappy query): The query object for searching the Xappy index.

        Returns:
            Xappy results: The search results.

        Raises:
            None.
        """
        return conn.search(q, 0, int(self.options.limit))

    def findterms(self, conn=None, terms=None):
        """
        Performs searches for multiple terms.

        Args:
            conn (Xappy connection, optional): The connection to the Xappy index. If not provided, a new connection will be created.
            terms (list): The list of terms to search for.

        Returns:
            generator: A generator that yields the search results for each term.

        Raises:
            None.
        """
        limit = int(self.options.limit)
        for term in terms:
            q = conn.query_field(self.bench.spec.main_field, term)
            yield conn.search(q, 0, limit)

    def results(self, r):
        """
        Iterates over the search results.

        Args:
            r (Xappy results): The search results.

        Returns:
            generator: A generator that yields each search result.

        Raises:
            None.
        """
        hf = self.bench.spec.headline_field
        mf = self.bench.spec.main_field
        for hit in r:
            yield self._process_result({hf: hit.data[hf], mf: hit.data[mf]})


class XapianModule(Module):
    """
    XapianModule is a module that provides indexing and searching capabilities using Xapian.

    Args:
        Module (class): The base class for all modules.

    Attributes:
        database (xapian.WritableDatabase): The Xapian writable database.
        ixer (xapian.TermGenerator): The Xapian term generator.
        db (xapian.Database): The Xapian database.
        enq (xapian.Enquire): The Xapian enquire object.
        qp (xapian.QueryParser): The Xapian query parser.

    """

    def indexer(self, **kwargs):
        """
        Initializes the Xapian indexer.

        Args:
            **kwargs: Additional keyword arguments.

        """
        path = os.path.join(self.options.dir, f"{self.options.indexname}_xapian")
        self.database = xapian.WritableDatabase(path, xapian.DB_CREATE_OR_OPEN)
        self.ixer = xapian.TermGenerator()

    def index_document(self, d):
        """
        Indexes a document in the Xapian database.

        Args:
            d (dict): The document to be indexed.

        """
        if hasattr(self.bench, "process_document_xapian"):
            self.bench.process_document_xapian(d)
        doc = xapian.Document()
        doc.add_value(0, d.get(self.bench.spec.headline_field, "-"))
        doc.set_data(d[self.bench.spec.main_field])
        self.ixer.set_document(doc)
        self.ixer.index_text(d[self.bench.spec.main_field])
        self.database.add_document(doc)

    def finish(self, **kwargs):
        """
        Flushes the Xapian database.

        Args:
            **kwargs: Additional keyword arguments.

        """
        self.database.flush()

    def searcher(self):
        """
        Initializes the Xapian searcher.

        """
        path = os.path.join(self.options.dir, f"{self.options.indexname}_xappy")
        self.db = xapian.Database(path)
        self.enq = xapian.Enquire(self.db)
        self.qp = xapian.QueryParser()
        self.qp.set_database(self.db)

    def query(self):
        """
        Parses and returns the query.

        Returns:
            xapian.Query: The parsed query.

        """
        return self.qp.parse_query(" ".join(self.args))

    def find(self, q):
        """
        Finds and returns the matching documents for the given query.

        Args:
            q (xapian.Query): The query to search for.

        Returns:
            xapian.MSet: The matching documents.

        """
        self.enq.set_query(q)
        return self.enq.get_mset(0, int(self.options.limit))

    def findterms(self, terms):
        """
        Finds and returns the matching documents for each term in the given list.

        Args:
            terms (list): The list of terms to search for.

        Yields:
            xapian.MSet: The matching documents for each term.

        """
        limit = int(self.options.limit)
        for term in terms:
            q = self.qp.parse_query(term)
            self.enq.set_query(q)
            yield self.enq.get_mset(0, limit)

    def results(self, matches):
        """
        Processes and yields the results from the given matches.

        Args:
            matches (xapian.MSet): The matches to process.

        Yields:
            dict: The processed result for each match.

        """
        hf = self.bench.spec.headline_field
        mf = self.bench.spec.main_field
        for m in matches:
            yield self._process_result(
                {hf: m.document.get_value(0), mf: m.document.get_data()}
            )


class SolrModule(Module):
    """
    A module for interacting with Apache Solr.

    This module provides methods for indexing documents, searching for documents,
    and retrieving search results from an Apache Solr server.

    Args:
        Module (class): The base class for all modules.

    Attributes:
        solr_doclist (list): A list to store the documents to be indexed.
        conn (pysolr.Solr): A connection object to interact with the Solr server.
        solr (pysolr.Solr): A connection object to interact with the Solr server for searching.

    """

    def indexer(self, **kwargs):
        """
        Initializes the SolrModule for indexing.

        This method initializes the SolrModule by creating a connection to the Solr server,
        deleting all existing documents in the server, and committing the changes.

        Args:
            **kwargs: Additional keyword arguments.

        """

        self.solr_doclist = []
        self.conn = pysolr.Solr(self.options.url)
        self.conn.delete("*:*")
        self.conn.commit()

    def index_document(self, d):
        """
        Adds a document to the list of documents to be indexed.

        This method adds a document to the list of documents to be indexed.
        If the number of documents in the list reaches the batch size specified in the options,
        the documents are added to the Solr server and the list is cleared.

        Args:
            d (dict): The document to be indexed.

        """

        self.solr_doclist.append(d)
        if len(self.solr_doclist) >= int(self.options.batch):
            self.conn.add(self.solr_doclist, commit=False)
            self.solr_doclist = []

    def finish(self, **kwargs):
        """
        Finalizes the indexing process.

        This method finalizes the indexing process by adding any remaining documents in the list
        to the Solr server, optimizing the server, and cleaning up resources.

        Args:
            **kwargs: Additional keyword arguments.

        """

        if self.solr_doclist:
            self.conn.add(self.solr_doclist)
        del self.solr_doclist
        self.conn.optimize(block=True)

    def searcher(self):
        """
        Initializes the SolrModule for searching.

        This method initializes the SolrModule by creating a connection to the Solr server
        specifically for searching.

        """

        self.solr = pysolr.Solr(self.options.url)

    def query(self):
        """
        Constructs a query string.

        This method constructs a query string by joining the arguments passed to the script.

        Returns:
            str: The constructed query string.

        """

        return " ".join(self.args)

    def find(self, q):
        """
        Executes a search query.

        This method executes a search query on the Solr server using the provided query string.

        Args:
            q (str): The query string.

        Returns:
            pysolr.Results: The search results.

        """

        return self.solr.search(q, limit=int(self.options.limit))

    def findterms(self, terms):
        """
        Executes search queries for each term.

        This method executes search queries on the Solr server for each term in the provided list.
        The search queries are constructed by appending the term to the "body:" field.

        Args:
            terms (list): The list of terms to search for.

        Yields:
            pysolr.Results: The search results for each term.

        """

        limit = int(self.options.limit)
        for term in terms:
            yield self.solr.search("body:" + term, limit=limit)


class ZcatalogModule(Module):
    """
    A module for indexing and searching documents using ZCatalog.

    This module provides functionality for indexing and searching documents using ZCatalog,
    which is a powerful indexing and search system for Python applications.

    Usage:
    1. Create an instance of ZcatalogModule.
    2. Call the `indexer` method to set up the indexing environment.
    3. Call the `index_document` method to index a document.
    4. Call the `finish` method to commit the changes and clean up resources.
    5. Call the `searcher` method to set up the searching environment.
    6. Call the `query` method to specify the search query.
    7. Call the `find` method to retrieve search results.
    8. Call the `findterms` method to retrieve search results for each term in a list.
    9. Call the `results` method to process and iterate over search results.

    Note: This module requires the ZODB package to be installed.

    Attributes:
    - cat: The ZCatalog instance used for indexing and searching.
    - zcatalog_count: The count of indexed documents.

    """

    def indexer(self, **kwargs):
        """
        Set up the indexing environment.

        This method creates the necessary directory and storage for indexing,
        initializes the ZCatalog instance, and commits the changes.

        Args:
        - kwargs: Additional keyword arguments.

        """

        import transaction
        from zcatalog import catalog
        from ZODB.DB import DB
        from ZODB.FileStorage import FileStorage

        directory = os.path.join(self.options.dir, f"{self.options.indexname}_zcatalog")
        if os.path.exists(directory):
            rmtree(directory)
        os.mkdir(directory)

        storage = FileStorage(os.path.join(directory, "index"))
        db = DB(storage)
        conn = db.open()

        self.cat = catalog.Catalog()
        self.bench.spec.zcatalog_setup(self.cat)
        conn.root()["cat"] = self.cat
        transaction.commit()

        self.zcatalog_count = 0

    def index_document(self, d):
        """
        Index a document.

        This method indexes a document by processing it with the `process_document_zcatalog`
        method (if available), creating a ZDoc instance, and indexing the document using the
        ZCatalog instance. It also commits the changes periodically based on the `zcatalog_count`
        attribute.

        Args:
        - d: The document to be indexed.

        """

        if hasattr(self.bench, "process_document_zcatalog"):
            self.bench.process_document_zcatalog(d)
        doc = ZDoc(d)
        self.cat.index_doc(doc)
        self.zcatalog_count += 1
        if self.zcatalog_count >= 100:
            import transaction

            transaction.commit()
            self.zcatalog_count = 0

    def finish(self, **kwargs):
        """
        Finish indexing and clean up resources.

        This method commits the changes made during indexing and cleans up resources.

        Args:
        - kwargs: Additional keyword arguments.

        """

        import transaction

        transaction.commit()
        del self.zcatalog_count

    def searcher(self):
        """
        Set up the searching environment.

        This method sets up the searching environment by opening the ZODB connection,
        retrieving the ZCatalog instance, and assigning it to the `cat` attribute.

        """

        from ZODB.DB import DB
        from ZODB.FileStorage import FileStorage

        path = os.path.join(
            self.options.dir, f"{self.options.indexname}_zcatalog", "index"
        )
        storage = FileStorage(path)
        db = DB(storage)
        conn = db.open()

        self.cat = conn.root()["cat"]

    def query(self):
        """
        Get the search query.

        This method returns the search query as a string.

        Returns:
        - The search query.

        """

        return " ".join(self.args)

    def find(self, q):
        """
        Find search results.

        This method performs a search using the ZCatalog instance and the specified query.

        Args:
        - q: The search query.

        Returns:
        - The search results.

        """

        return self.cat.searchResults(body=q)

    def findterms(self, terms):
        """
        Find search results for each term.

        This method performs a search for each term in the specified list using the ZCatalog instance.

        Args:
        - terms: The list of terms to search for.

        Yields:
        - The search results for each term.

        """

        for term in terms:
            yield self.cat.searchResults(body=term)

    def results(self, r):
        """
        Process and iterate over search results.

        This method processes and iterates over the search results, retrieving the headline and main
        fields for each hit.

        Args:
        - r: The search results.

        Yields:
        - The processed search results.

        """

        hf = self.bench.spec.headline_field
        mf = self.bench.spec.main_field
        for hit in r:
            # Have to access the attributes for them to be retrieved
            yield self._process_result({hf: getattr(hit, hf), mf: getattr(hit, mf)})


class NucularModule(Module):
    """
    A module for indexing and searching documents using the Nucular library.
    """

    def indexer(self, create=True):
        """
        Indexes a document using the Nucular library.

        Args:
            create (bool, optional): Whether to create a new index. Defaults to True.
        """
        import shutil

        from nucular import Nucular  # type: ignore # type: ignore @UnresolvedImport

        directory = os.path.join(self.options.dir, f"{self.options.indexname}_nucular")
        if create:
            if os.path.exists(directory):
                shutil.rmtree(directory)
            os.mkdir(directory)
        self.archive = Nucular.Nucular(directory)
        if create:
            self.archive.create()
        self.count = 0

    def index_document(self, d):
        """
        Indexes a document.

        Args:
            d (dict): The document to be indexed.
        """
        try:
            self.archive.indexDictionary(str(self.count), d)
        except ValueError:
            print("d=", d)
            raise
        self.count += 1
        if not self.count % int(self.options.batch):
            self.archive.store(lazy=True)
            self.indexer(create=False)

    def finish(self, **kwargs):
        """
        Finishes the indexing process.
        """
        self.archive.store(lazy=False)
        self.archive.aggregateRecent(fast=False, verbose=True)
        self.archive.moveTransientToBase(verbose=True)
        self.archive.cleanUp()

    def searcher(self):
        """
        Initializes the searcher for querying the indexed documents.
        """
        from nucular import Nucular  # type: ignore # type: ignore @UnresolvedImport

        directory = os.path.join(
            self.options.directory, f"{self.options.indexname}_nucular"
        )
        self.archive = Nucular.Nucular(directory)

    def query(self):
        """
        Constructs a query string from the arguments.

        Returns:
            str: The constructed query string.
        """
        return " ".join(self.args)

    def find(self, q):
        """
        Finds documents matching the given query.

        Args:
            q (str): The query string.

        Returns:
            list: A list of dictionaries representing the matching documents.
        """
        return self.archive.dictionaries(q)

    def findterms(self, terms):
        """
        Finds documents containing the given terms.

        Args:
            terms (list): A list of terms to search for.

        Yields:
            list: A list of dictionaries representing the matching documents for each term.
        """
        for term in terms:
            q = self.archive.Query()
            q.anyWord(term)
            yield q.resultDictionaries()


class Bench:
    """
    The Bench class provides methods for indexing and searching documents using different libraries.
    """

    libs = {
        "whoosh": WhooshModule,
        "xappy": XappyModule,
        "xapian": XapianModule,
        "solr": SolrModule,
        "zcatalog": ZcatalogModule,
        "nucular": NucularModule,
    }

    def index(self, lib):
        """
        Indexes documents using the specified library.

        Args:
            lib: The library to use for indexing.

        Returns:
            None

        Raises:
            None

        Example:
            bench = Bench()
            bench.index(MyLibrary())
        """

        print(f"Indexing with {lib}...")

        options = self.options
        every = None if options.every is None else int(options.every)
        merge = options.merge
        chunk = int(options.chunk)
        skip = int(options.skip)
        upto = int(options.upto)
        count = 0
        skipc = skip

        starttime = chunkstarttime = now()

        lib.indexer()

        for d in self.spec.documents():
            skipc -= 1
            if not skipc:
                lib.index_document(d)
                count += 1
                skipc = skip
                if chunk and not count % chunk:
                    t = now()
                    sofar = t - starttime
                    print(
                        "Done %d docs, %0.3f secs for %d, %0.3f total, %0.3f docs/s"
                        % (count, t - chunkstarttime, chunk, sofar, count / sofar)
                    )
                    chunkstarttime = t
                if count > upto:
                    break
                if every and not count % every:
                    print("----Commit")
                    lib.finish(merge=merge)
                    lib.indexer(create=False)

        spooltime = now()
        print("Spool time:", spooltime - starttime)
        lib.finish(merge=merge)
        committime = now()
        print("Commit time:", committime - spooltime)
        totaltime = committime - starttime
        print(
            "Total time to index %d documents: %0.3f secs (%0.3f minutes)"
            % (count, totaltime, totaltime / 60.0)
        )
        print(f"Indexed {count / totaltime:0.3f} docs/s")

    def search(self, lib):
        """
        Perform a search using the given library.

        Args:
            lib: The library object to use for searching.

        Returns:
            None

        Raises:
            None
        """
        lib.searcher()

        t = now()
        q = lib.query()
        print("Query:", q)
        r = lib.find(q)
        print("Search time:", now() - t)

        t = now()
        self.spec.print_results(lib.results(r))
        print("Print time:", now() - t)

    def search_file(self, lib):
        """
        Searches for terms in a file using the specified library.

        Args:
            lib (str): The name of the library to use for searching.

        Returns:
            None

        Raises:
            FileNotFoundError: If the termfile specified in the options does not exist.

        """
        f = open(self.options.termfile, "rb")
        terms = [line.strip() for line in f]
        f.close()

        print(f"Searching {len(terms)} terms with {lib}")
        lib.searcher()
        starttime = now()
        for _ in lib.findterms(terms):
            pass
        searchtime = now() - starttime
        print("Search time:", searchtime, "searches/s:", float(len(terms)) / searchtime)

    def _parser(self, name):
        """
        Create an OptionParser object with predefined options for command-line parsing.

        Parameters:
        - name (str): The name used as a prefix for the index name.

        Returns:
        - OptionParser: The OptionParser object with predefined options.

        The _parser function creates an OptionParser object and adds several options to it.
        These options are used for command-line parsing in the bench.py script.

        Options:
        - -x, --lib: Name of the library to use to index/search. Default is "whoosh".
        - -d, --dir: Directory in which to store index. Default is the current directory.
        - -s, --setup: Set up any support files or caches. Default is False.
        - -i, --index: Index the documents. Default is False.
        - -n, --name: Index name prefix. Default is "{name}_index".
        - -U, --url: Solr URL. Default is "http://localhost:8983/solr".
        - -m, --mb: Max. memory usage, in MB. Default is "128".
        - -c, --chunk: Number of documents to index between progress messages. Default is 1000.
        - -B, --batch: Batch size for batch adding documents. Default is 1000.
        - -k, --skip: Index every Nth document. Default is 1.
        - -e, --commit-every: Commit every NUM documents. Default is None.
        - -M, --no-merge: Don't merge segments when doing multiple commits. Default is True.
        - -u, --upto: Index up to this document number. Default is 600000.
        - -p, --procs: Number of processors to use. Default is 0.
        - -l, --limit: Maximum number of search results to retrieve. Default is 10.
        - -b, --body: Show the body text in search results. Default is False.
        - -g, --gen: Generate a list at most N terms present in all libraries. Default is None.
        - -f, --file: Search using the list of terms in this file. Default is None.
        - -t, --tempdir: Whoosh temp dir. Default is None.
        - -P, --pool: Whoosh pool class. Default is None.
        - -X, --xms: Experimental Whoosh feature. Default is False.
        - -Z, --storebody: Store the body text in index. Default is False.
        - -q, --snippets: Show highlighted snippets. Default is False.
        - -O, --no-optimize: Turn off searcher optimization. Default is True.
        """
        p = OptionParser()
        p.add_option(
            "-x",
            "--lib",
            dest="lib",
            help="Name of the library to use to index/search.",
            default="whoosh",
        )
        p.add_option(
            "-d",
            "--dir",
            dest="dir",
            metavar="DIRNAME",
            help="Directory in which to store index.",
            default=".",
        )
        p.add_option(
            "-s",
            "--setup",
            dest="setup",
            action="store_true",
            help="Set up any support files or caches.",
            default=False,
        )
        p.add_option(
            "-i",
            "--index",
            dest="index",
            action="store_true",
            help="Index the documents.",
            default=False,
        )
        p.add_option(
            "-n",
            "--name",
            dest="indexname",
            metavar="PREFIX",
            help="Index name prefix.",
            default=f"{name}_index",
        )
        p.add_option(
            "-U",
            "--url",
            dest="url",
            metavar="URL",
            help="Solr URL",
            default="http://localhost:8983/solr",
        )
        p.add_option(
            "-m", "--mb", dest="limitmb", help="Max. memory usage, in MB", default="128"
        )
        p.add_option(
            "-c",
            "--chunk",
            dest="chunk",
            help="Number of documents to index between progress messages.",
            default=1000,
        )
        p.add_option(
            "-B",
            "--batch",
            dest="batch",
            help="Batch size for batch adding documents.",
            default=1000,
        )
        p.add_option(
            "-k",
            "--skip",
            dest="skip",
            metavar="N",
            help="Index every Nth document.",
            default=1,
        )
        p.add_option(
            "-e",
            "--commit-every",
            dest="every",
            metavar="NUM",
            help="Commit every NUM documents",
            default=None,
        )
        p.add_option(
            "-M",
            "--no-merge",
            dest="merge",
            action="store_false",
            help="Don't merge segments when doing multiple commits",
            default=True,
        )
        p.add_option(
            "-u",
            "--upto",
            dest="upto",
            metavar="N",
            help="Index up to this document number.",
            default=600000,
        )
        p.add_option(
            "-p",
            "--procs",
            dest="procs",
            metavar="NUMBER",
            help="Number of processors to use.",
            default=0,
        )
        p.add_option(
            "-l",
            "--limit",
            dest="limit",
            metavar="N",
            help="Maximum number of search results to retrieve.",
            default=10,
        )
        p.add_option(
            "-b",
            "--body",
            dest="showbody",
            action="store_true",
            help="Show the body text in search results.",
            default=False,
        )
        p.add_option(
            "-g",
            "--gen",
            dest="generate",
            metavar="N",
            help="Generate a list at most N terms present in all libraries.",
            default=None,
        )
        p.add_option(
            "-f",
            "--file",
            dest="termfile",
            metavar="FILENAME",
            help="Search using the list of terms in this file.",
            default=None,
        )
        p.add_option(
            "-t",
            "--tempdir",
            dest="tempdir",
            metavar="DIRNAME",
            help="Whoosh temp dir",
            default=None,
        )
        p.add_option(
            "-P",
            "--pool",
            dest="pool",
            metavar="CLASSNAME",
            help="Whoosh pool class",
            default=None,
        )
        p.add_option(
            "-X",
            "--xms",
            dest="xms",
            action="store_true",
            help="Experimental Whoosh feature",
            default=False,
        )
        p.add_option(
            "-Z",
            "--storebody",
            dest="storebody",
            action="store_true",
            help="Store the body text in index",
            default=False,
        )
        p.add_option(
            "-q",
            "--snippets",
            dest="snippets",
            action="store_true",
            help="Show highlighted snippets",
            default=False,
        )
        p.add_option(
            "-O",
            "--no-optimize",
            dest="optimize",
            action="store_false",
            help="Turn off searcher optimization",
            default=True,
        )

        return p

    def run(self, specclass):
        """
        Runs the benchmarking process.

        Args:
            specclass: The benchmark specification class.

        Raises:
            ValueError: If the specified library is unknown.

        Notes:
            This method parses the command line arguments, initializes the benchmark options and arguments,
            creates an instance of the specified library, and executes the benchmark action based on the
            command line options.

        Example:
            To run the benchmark using a specific specification class:

            ```
            bench = Benchmark()
            bench.run(MySpecClass)
            ```
        """
        parser = self._parser(specclass.name)
        options, args = parser.parse_args()
        self.options = options
        self.args = args

        if options.lib not in self.libs:
            raise ValueError(f"Unknown library: {options.lib!r}")
        lib = self.libs[options.lib](self, options, args)

        self.spec = specclass(options, args)

        if options.setup:
            self.spec.setup()

        action = self.search
        if options.index:
            action = self.index
        if options.termfile:
            action = self.search_file
        if options.generate:
            action = self.generate_search_file

        action(lib)
