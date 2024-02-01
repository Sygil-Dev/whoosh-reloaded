==============================
How to implement a new backend
==============================

Index
=====

* Subclass :class:` whoosh_reloaded.index.Index`.

* Indexes must implement the following methods.

  * :meth:` whoosh_reloaded.index.Index.is_empty`

  * :meth:` whoosh_reloaded.index.Index.doc_count`

  * :meth:` whoosh_reloaded.index.Index.reader`

  * :meth:` whoosh_reloaded.index.Index.writer`

* Indexes that require/support locking must implement the following methods.

  * :meth:` whoosh_reloaded.index.Index.lock`

  * :meth:` whoosh_reloaded.index.Index.unlock`

* Indexes that support deletion must implement the following methods.

  * :meth:` whoosh_reloaded.index.Index.delete_document`

  * :meth:` whoosh_reloaded.index.Index.doc_count_all` -- if the backend has delayed
    deletion.

* Indexes that require/support versioning/transactions *may* implement the following methods.

  * :meth:` whoosh_reloaded.index.Index.latest_generation`

  * :meth:` whoosh_reloaded.index.Index.up_to_date`

  * :meth:` whoosh_reloaded.index.Index.last_modified`

* Index *may* implement the following methods (the base class's versions are no-ops).

  * :meth:` whoosh_reloaded.index.Index.optimize`

  * :meth:` whoosh_reloaded.index.Index.close`


IndexWriter
===========

* Subclass :class:` whoosh_reloaded.writing.IndexWriter`.

* IndexWriters must implement the following methods.

  * :meth:` whoosh_reloaded.writing.IndexWriter.add_document`

  * :meth:` whoosh_reloaded.writing.IndexWriter.add_reader`

* Backends that support deletion must implement the following methods.

  * :meth:` whoosh_reloaded.writing.IndexWriter.delete_document`

* IndexWriters that work as transactions must implement the following methods.

  * :meth:` whoosh_reloaded.reading.IndexWriter.commit` -- Save the additions/deletions done with
    this IndexWriter to the main index, and release any resources used by the IndexWriter.

  * :meth:` whoosh_reloaded.reading.IndexWriter.cancel` -- Throw away any additions/deletions done
    with this IndexWriter, and release any resources used by the IndexWriter.


IndexReader
===========

* Subclass :class:` whoosh_reloaded.reading.IndexReader`.

* IndexReaders must implement the following methods.

  * :meth:` whoosh_reloaded.reading.IndexReader.__contains__`

  * :meth:` whoosh_reloaded.reading.IndexReader.__iter__`

  * :meth:` whoosh_reloaded.reading.IndexReader.iter_from`

  * :meth:` whoosh_reloaded.reading.IndexReader.stored_fields`

  * :meth:` whoosh_reloaded.reading.IndexReader.doc_count_all`

  * :meth:` whoosh_reloaded.reading.IndexReader.doc_count`

  * :meth:` whoosh_reloaded.reading.IndexReader.doc_field_length`

  * :meth:` whoosh_reloaded.reading.IndexReader.field_length`

  * :meth:` whoosh_reloaded.reading.IndexReader.max_field_length`

  * :meth:` whoosh_reloaded.reading.IndexReader.postings`

  * :meth:` whoosh_reloaded.reading.IndexReader.has_vector`

  * :meth:` whoosh_reloaded.reading.IndexReader.vector`

  * :meth:` whoosh_reloaded.reading.IndexReader.doc_frequency`

  * :meth:` whoosh_reloaded.reading.IndexReader.frequency`

* Backends that support deleting documents should implement the following
  methods.

  * :meth:` whoosh_reloaded.reading.IndexReader.has_deletions`
  * :meth:` whoosh_reloaded.reading.IndexReader.is_deleted`

* Backends that support versioning should implement the following methods.

  * :meth:` whoosh_reloaded.reading.IndexReader.generation`

* If the IndexReader object does not keep the schema in the ``self.schema``
  attribute, it needs to override the following methods.

  * :meth:` whoosh_reloaded.reading.IndexReader.field`

  * :meth:` whoosh_reloaded.reading.IndexReader.field_names`

  * :meth:` whoosh_reloaded.reading.IndexReader.scorable_names`

  * :meth:` whoosh_reloaded.reading.IndexReader.vector_names`

* IndexReaders *may* implement the following methods.

  * :meth:` whoosh_reloaded.reading.DocReader.close` -- closes any open resources associated with the
    reader.


Matcher
=======

The :meth:` whoosh_reloaded.reading.IndexReader.postings` method returns a
:class:` whoosh_reloaded.matching.Matcher` object. You will probably need to implement
a custom Matcher class for reading from your posting lists.

* Subclass :class:` whoosh_reloaded.matching.Matcher`.

* Implement the following methods at minimum.

  * :meth:` whoosh_reloaded.matching.Matcher.is_active`

  * :meth:` whoosh_reloaded.matching.Matcher.copy`

  * :meth:` whoosh_reloaded.matching.Matcher.id`

  * :meth:` whoosh_reloaded.matching.Matcher.next`

  * :meth:` whoosh_reloaded.matching.Matcher.value`

  * :meth:` whoosh_reloaded.matching.Matcher.value_as`

  * :meth:` whoosh_reloaded.matching.Matcher.score`

* Depending on the implementation, you *may* implement the following methods
  more efficiently.

  * :meth:` whoosh_reloaded.matching.Matcher.skip_to`

  * :meth:` whoosh_reloaded.matching.Matcher.weight`

* If the implementation supports quality, you should implement the following
  methods.

  * :meth:` whoosh_reloaded.matching.Matcher.supports_quality`

  * :meth:` whoosh_reloaded.matching.Matcher.quality`

  * :meth:` whoosh_reloaded.matching.Matcher.block_quality`

  * :meth:` whoosh_reloaded.matching.Matcher.skip_to_quality`
