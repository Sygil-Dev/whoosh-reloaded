# ===============================================================================
# Copyright 2009 Matt Chaput
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ===============================================================================

from marshal import loads
from threading import Lock

from whoosh.fields import FieldConfigurationError
from whoosh.filedb import misc
from whoosh.filedb.filepostings import FilePostingReader
from whoosh.filedb.filetables import (
    FileListReader,
    FileTableReader,
    LengthReader,
    StructHashReader,
)

# from whoosh.postings import Exclude
from whoosh.reading import IndexReader, TermNotFound
from whoosh.util import byte_to_length, protected

# Reader class


class SegmentReader(IndexReader):
    def __init__(self, storage, segment, schema):
        self.storage = storage
        self.segment = segment
        self.schema = schema

        storedfieldnames = schema.stored_field_names()

        def decode_storedfields(value):
            return dict(zip(storedfieldnames, loads(value)))

        # Term index
        tf = storage.open_file(segment.termsindex_filename)
        self.termsindex = FileTableReader(
            tf,
            keycoder=misc.encode_termkey,
            keydecoder=misc.decode_termkey,
            valuedecoder=misc.decode_terminfo,
        )

        # Term postings file, vector index, and vector postings: lazy load
        self.postfile = None
        self.vectorindex = None
        self.vpostfile = None

        # Stored fields file
        sf = storage.open_file(segment.storedfields_filename, mapped=False)
        self.storedfields = FileListReader(sf, valuedecoder=decode_storedfields)

        # Field length file
        scorables = schema.scorable_fields()
        if scorables:
            self.indices = {fieldnum: i for i, fieldnum in enumerate(scorables)}
            lengthcount = segment.doc_count_all() * len(self.indices)
            flf = storage.open_file(segment.fieldlengths_filename)
            self.fieldlengths = flf.read_array("B", lengthcount)
            flf.close()
        else:
            self.fieldlengths = []

        # Copy methods from underlying segment
        self.has_deletions = segment.has_deletions
        self.is_deleted = segment.is_deleted
        self.doc_count = segment.doc_count

        self.dc = segment.doc_count_all()
        self.is_closed = False
        self._sync_lock = Lock()

    def _open_vectors(self):
        if self.vectorindex:
            return

        storage, segment = self.storage, self.segment

        # Vector index
        vf = storage.open_file(segment.vectorindex_filename)
        self.vectorindex = StructHashReader(vf, "!IH", "!I")

        # Vector postings file
        self.vpostfile = storage.open_file(segment.vectorposts_filename, mapped=False)

    def _open_postfile(self):
        if self.postfile:
            return
        self.postfile = self.storage.open_file(
            self.segment.termposts_filename, mapped=False
        )

    def __repr__(self):
        return f"{self.__class__.__name__}({self.segment})"

    @protected
    def __contains__(self, term):
        return (self.schema.to_number(term[0]), term[1]) in self.termsindex

    def close(self):
        self.storedfields.close()
        self.termsindex.close()
        if self.postfile:
            self.postfile.close()
        if self.vectorindex:
            self.vectorindex.close()
        # if self.fieldlengths:
        #    self.fieldlengths.close()
        self.is_closed = True

    def doc_count_all(self):
        return self.dc

    @protected
    def stored_fields(self, docnum):
        return self.storedfields[docnum]

    @protected
    def all_stored_fields(self):
        is_deleted = self.segment.is_deleted
        for docnum in range(self.segment.doc_count_all()):
            if not is_deleted(docnum):
                yield self.storedfields[docnum]

    def field_length(self, fieldnum):
        return self.segment.field_length(fieldnum)

    @protected
    def doc_field_length(self, docnum, fieldnum, default=0):
        index = self.indices[fieldnum]
        pos = index * self.dc + docnum
        return byte_to_length(self.fieldlengths[pos])

    def max_field_length(self, fieldnum):
        return self.segment.max_field_length(fieldnum)

    @protected
    def has_vector(self, docnum, fieldnum):
        self._open_vectors()
        return (docnum, fieldnum) in self.vectorindex

    @protected
    def __iter__(self):
        for (fn, t), (totalfreq, _, postcount) in self.termsindex:
            yield (fn, t, postcount, totalfreq)

    @protected
    def iter_from(self, fieldnum, text):
        tt = self.termsindex
        for (fn, t), (totalfreq, _, postcount) in tt.items_from((fieldnum, text)):
            yield (fn, t, postcount, totalfreq)

    @protected
    def _term_info(self, fieldnum, text):
        try:
            return self.termsindex[(fieldnum, text)]
        except KeyError:
            raise TermNotFound(f"{fieldnum}:{text!r}")

    def doc_frequency(self, fieldid, text):
        try:
            fieldnum = self.schema.to_number(fieldid)
            return self._term_info(fieldnum, text)[2]
        except TermNotFound:
            return 0

    def frequency(self, fieldid, text):
        try:
            fieldnum = self.schema.to_number(fieldid)
            return self._term_info(fieldnum, text)[0]
        except TermNotFound:
            return 0

    @protected
    def lexicon(self, fieldid):
        # The base class has a lexicon() implementation that uses iter_from()
        # and throws away the value, but overriding to use
        # FileTableReader.keys_from() is much, much faster.

        tt = self.termsindex
        fieldid = self.schema.to_number(fieldid)
        for fn, t in tt.keys_from((fieldid, "")):
            if fn != fieldid:
                return
            yield t

    @protected
    def expand_prefix(self, fieldid, prefix):
        # The base class has an expand_prefix() implementation that uses
        # iter_from() and throws away the value, but overriding to use
        # FileTableReader.keys_from() is much, much faster.

        tt = self.termsindex
        fieldid = self.schema.to_number(fieldid)
        for fn, t in tt.keys_from((fieldid, prefix)):
            if fn != fieldid or not t.startswith(prefix):
                return
            yield t

    def postings(self, fieldid, text, exclude_docs=frozenset()):
        schema = self.schema
        fieldnum = schema.to_number(fieldid)
        format = schema[fieldnum].format

        try:
            offset = self.termsindex[(fieldnum, text)][1]
        except KeyError:
            raise TermNotFound(f"{fieldid}:{text!r}")

        if self.segment.deleted and exclude_docs:
            exclude_docs = self.segment.deleted | exclude_docs
        elif self.segment.deleted:
            exclude_docs = self.segment.deleted

        self._open_postfile()
        postreader = FilePostingReader(self.postfile, offset, format)
        # if exclude_docs:
        #    postreader = Exclude(postreader, exclude_docs)
        return postreader

    def vector(self, docnum, fieldid):
        schema = self.schema
        fieldnum = schema.to_number(fieldid)
        vformat = schema[fieldnum].vector
        if not vformat:
            raise Exception(f"No vectors are stored for field {fieldid!r}")

        self._open_vectors()
        offset = self.vectorindex.get((docnum, fieldnum))
        if offset is None:
            raise Exception(f"No vector found for document {docnum} field {fieldid!r}")

        return FilePostingReader(self.vpostfile, offset, vformat, stringids=True)
