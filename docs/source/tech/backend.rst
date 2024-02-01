==============================
How to implement a new backend
==============================

Index
=====

* Subclass :class:`whoosh-reloaded.index.Index`.

* Indexes must implement the following methods.

  * :meth:`whoosh-reloaded.index.Index.is_empty`

  * :meth:`whoosh-reloaded.index.Index.doc_count`

  * :meth:`whoosh-reloaded.index.Index.reader`

  * :meth:`whoosh-reloaded.index.Index.writer`

* Indexes that require/support locking must implement the following methods.

  * :meth:`whoosh-reloaded.index.Index.lock`

  * :meth:`whoosh-reloaded.index.Index.unlock`

* Indexes that support deletion must implement the following methods.

  * :meth:`whoosh-reloaded.index.Index.delete_document`

  * :meth:`whoosh-reloaded.index.Index.doc_count_all` -- if the backend has delayed
    deletion.

* Indexes that require/support versioning/transactions *may* implement the following methods.

  * :meth:`whoosh-reloaded.index.Index.latest_generation`

  * :meth:`whoosh-reloaded.index.Index.up_to_date`

  * :meth:`whoosh-reloaded.index.Index.last_modified`

* Index *may* implement the following methods (the base class's versions are no-ops).

  * :meth:`whoosh-reloaded.index.Index.optimize`

  * :meth:`whoosh-reloaded.index.Index.close`


IndexWriter
===========

* Subclass :class:`whoosh-reloaded.writing.IndexWriter`.

* IndexWriters must implement the following methods.

  * :meth:`whoosh-reloaded.writing.IndexWriter.add_document`

  * :meth:`whoosh-reloaded.writing.IndexWriter.add_reader`

* Backends that support deletion must implement the following methods.

  * :meth:`whoosh-reloaded.writing.IndexWriter.delete_document`

* IndexWriters that work as transactions must implement the following methods.

  * :meth:`whoosh-reloaded.reading.IndexWriter.commit` -- Save the additions/deletions done with
    this IndexWriter to the main index, and release any resources used by the IndexWriter.

  * :meth:`whoosh-reloaded.reading.IndexWriter.cancel` -- Throw away any additions/deletions done
    with this IndexWriter, and release any resources used by the IndexWriter.


IndexReader
===========

* Subclass :class:`whoosh-reloaded.reading.IndexReader`.

* IndexReaders must implement the following methods.

  * :meth:`whoosh-reloaded.reading.IndexReader.__contains__`

  * :meth:`whoosh-reloaded.reading.IndexReader.__iter__`

  * :meth:`whoosh-reloaded.reading.IndexReader.iter_from`

  * :meth:`whoosh-reloaded.reading.IndexReader.stored_fields`

  * :meth:`whoosh-reloaded.reading.IndexReader.doc_count_all`

  * :meth:`whoosh-reloaded.reading.IndexReader.doc_count`

  * :meth:`whoosh-reloaded.reading.IndexReader.doc_field_length`

  * :meth:`whoosh-reloaded.reading.IndexReader.field_length`

  * :meth:`whoosh-reloaded.reading.IndexReader.max_field_length`

  * :meth:`whoosh-reloaded.reading.IndexReader.postings`

  * :meth:`whoosh-reloaded.reading.IndexReader.has_vector`

  * :meth:`whoosh-reloaded.reading.IndexReader.vector`

  * :meth:`whoosh-reloaded.reading.IndexReader.doc_frequency`

  * :meth:`whoosh-reloaded.reading.IndexReader.frequency`

* Backends that support deleting documents should implement the following
  methods.

  * :meth:`whoosh-reloaded.reading.IndexReader.has_deletions`
  * :meth:`whoosh-reloaded.reading.IndexReader.is_deleted`

* Backends that support versioning should implement the following methods.

  * :meth:`whoosh-reloaded.reading.IndexReader.generation`

* If the IndexReader object does not keep the schema in the ``self.schema``
  attribute, it needs to override the following methods.

  * :meth:`whoosh-reloaded.reading.IndexReader.field`

  * :meth:`whoosh-reloaded.reading.IndexReader.field_names`

  * :meth:`whoosh-reloaded.reading.IndexReader.scorable_names`

  * :meth:`whoosh-reloaded.reading.IndexReader.vector_names`

* IndexReaders *may* implement the following methods.

  * :meth:`whoosh-reloaded.reading.DocReader.close` -- closes any open resources associated with the
    reader.


Matcher
=======

The :meth:`whoosh-reloaded.reading.IndexReader.postings` method returns a
:class:`whoosh-reloaded.matching.Matcher` object. You will probably need to implement
a custom Matcher class for reading from your posting lists.

* Subclass :class:`whoosh-reloaded.matching.Matcher`.

* Implement the following methods at minimum.

  * :meth:`whoosh-reloaded.matching.Matcher.is_active`

  * :meth:`whoosh-reloaded.matching.Matcher.copy`

  * :meth:`whoosh-reloaded.matching.Matcher.id`

  * :meth:`whoosh-reloaded.matching.Matcher.next`

  * :meth:`whoosh-reloaded.matching.Matcher.value`

  * :meth:`whoosh-reloaded.matching.Matcher.value_as`

  * :meth:`whoosh-reloaded.matching.Matcher.score`

* Depending on the implementation, you *may* implement the following methods
  more efficiently.

  * :meth:`whoosh-reloaded.matching.Matcher.skip_to`

  * :meth:`whoosh-reloaded.matching.Matcher.weight`

* If the implementation supports quality, you should implement the following
  methods.

  * :meth:`whoosh-reloaded.matching.Matcher.supports_quality`

  * :meth:`whoosh-reloaded.matching.Matcher.quality`

  * :meth:`whoosh-reloaded.matching.Matcher.block_quality`

  * :meth:`whoosh-reloaded.matching.Matcher.skip_to_quality`
