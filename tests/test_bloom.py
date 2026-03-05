"""Tests for Bloom filter integration in Whoosh.

Tests cover:
1. BloomFilter standalone functionality (add, contains, serialize, merge)
2. Integration with W3Codec writer/reader (negative term lookup optimization)
3. End-to-end indexing and searching with Bloom filters enabled/disabled
4. Backward compatibility (indexes without Bloom files)
"""

import math

import pytest

from whoosh import fields, index, query
from whoosh.codec import default_codec
from whoosh.codec.whoosh3 import W3Codec
from whoosh.fields import ID, KEYWORD, TEXT, Schema
from whoosh.filedb.filestore import RamStorage
from whoosh.reading import TermNotFound
from whoosh.support.bloom import BloomFilter, _optimal_num_bits, _optimal_num_hashes
from whoosh.util.testing import TempIndex, TempStorage


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def b(s):
    return s.encode("latin-1")


def _make_codec(**kwargs):
    st = RamStorage()
    codec = default_codec(**kwargs)
    seg = codec.new_segment(st, "test")
    return st, codec, seg


# ===========================================================================
# Unit tests for BloomFilter class
# ===========================================================================


class TestBloomFilterUnit:
    """Pure unit tests for the BloomFilter data structure."""

    def test_basic_add_and_contains(self):
        bf = BloomFilter(expected_items=100, false_positive_rate=0.01)
        bf.add(b"hello")
        bf.add(b"world")

        assert b"hello" in bf
        assert b"world" in bf
        assert b"missing" not in bf

    def test_no_false_negatives(self):
        """Items added must always be found (no false negatives)."""
        items = [f"term_{i}".encode() for i in range(500)]
        bf = BloomFilter(expected_items=500, false_positive_rate=0.01)
        for item in items:
            bf.add(item)
        for item in items:
            assert item in bf, f"{item!r} should be in the Bloom filter"

    def test_false_positive_rate(self):
        """False positive rate should be within expected bounds."""
        n = 1000
        bf = BloomFilter(expected_items=n, false_positive_rate=0.05)
        for i in range(n):
            bf.add(f"item_{i}".encode())

        # Test with items NOT added
        false_positives = 0
        test_count = 10000
        for i in range(n, n + test_count):
            if f"item_{i}".encode() in bf:
                false_positives += 1

        fp_rate = false_positives / test_count
        # Allow 3x the target rate as tolerance for randomness
        assert fp_rate < 0.15, (
            f"False positive rate {fp_rate:.3f} is too high (expected < 0.15)"
        )

    def test_empty_filter(self):
        bf = BloomFilter(expected_items=100)
        assert b"anything" not in bf
        assert bf.count == 0

    def test_string_keys(self):
        """String keys should be automatically encoded to UTF-8."""
        bf = BloomFilter(expected_items=100)
        bf.add("café")
        assert "café" in bf
        assert "café".encode("utf-8") in bf

    def test_count(self):
        bf = BloomFilter(expected_items=100)
        assert bf.count == 0
        bf.add(b"one")
        assert bf.count == 1
        bf.add(b"two")
        assert bf.count == 2

    def test_properties(self):
        bf = BloomFilter(expected_items=1000, false_positive_rate=0.01)
        assert bf.num_bits > 0
        assert bf.num_hashes > 0
        assert bf.size_bytes == (bf.num_bits + 7) // 8

    def test_estimated_false_positive_rate(self):
        bf = BloomFilter(expected_items=1000, false_positive_rate=0.01)
        assert bf.estimated_false_positive_rate() == 0.0  # Empty filter
        for i in range(100):
            bf.add(f"item_{i}".encode())
        rate = bf.estimated_false_positive_rate()
        assert 0.0 < rate < 1.0

    def test_repr(self):
        bf = BloomFilter(expected_items=100)
        r = repr(bf)
        assert "BloomFilter" in r
        assert "num_bits" in r


class TestBloomFilterSerialization:
    """Tests for Bloom filter serialization / deserialization."""

    def test_roundtrip_bytes(self):
        bf = BloomFilter(expected_items=500, false_positive_rate=0.01)
        items = [f"key_{i}".encode() for i in range(200)]
        for item in items:
            bf.add(item)

        data = bf.to_bytes()
        bf2 = BloomFilter.from_bytes(data)

        # All items should still be found
        for item in items:
            assert item in bf2
        # Non-existent items should (usually) not be found
        assert b"never_added_xyz" not in bf2

    def test_serialized_parameters_match(self):
        bf = BloomFilter(expected_items=500, false_positive_rate=0.01)
        bf.add(b"test")
        data = bf.to_bytes()
        bf2 = BloomFilter.from_bytes(data)
        assert bf2.num_bits == bf.num_bits
        assert bf2.num_hashes == bf.num_hashes
        assert bf2.size_bytes == bf.size_bytes

    def test_invalid_magic(self):
        with pytest.raises(ValueError, match="Invalid Bloom filter magic"):
            BloomFilter.from_bytes(b"XXXX" + b"\x00" * 20)

    def test_truncated_data(self):
        bf = BloomFilter(expected_items=100)
        bf.add(b"test")
        data = bf.to_bytes()
        with pytest.raises(ValueError):
            BloomFilter.from_bytes(data[:5])

    def test_data_too_short(self):
        with pytest.raises(ValueError, match="too short"):
            BloomFilter.from_bytes(b"BL")


class TestBloomFilterMerge:
    """Tests for merging two Bloom filters."""

    def test_merge(self):
        bf1 = BloomFilter(expected_items=100, false_positive_rate=0.01)
        bf2 = BloomFilter(expected_items=100, false_positive_rate=0.01)
        bf1.add(b"alpha")
        bf2.add(b"beta")

        bf1.merge(bf2)
        assert b"alpha" in bf1
        assert b"beta" in bf1

    def test_merge_mismatched_sizes(self):
        bf1 = BloomFilter(num_bits=128, num_hashes=3)
        bf2 = BloomFilter(num_bits=256, num_hashes=3)
        with pytest.raises(ValueError, match="num_bits differ"):
            bf1.merge(bf2)

    def test_merge_mismatched_hashes(self):
        bf1 = BloomFilter(num_bits=128, num_hashes=3)
        bf2 = BloomFilter(num_bits=128, num_hashes=5)
        with pytest.raises(ValueError, match="num_hashes differ"):
            bf1.merge(bf2)


class TestBloomFilterOptimalParams:
    """Tests for optimal parameter calculations."""

    def test_optimal_num_bits(self):
        m = _optimal_num_bits(1000, 0.01)
        # For n=1000, p=0.01: m ≈ 9585
        assert 9000 < m < 10000

    def test_optimal_num_bits_edge_cases(self):
        assert _optimal_num_bits(0, 0.01) == 64  # Minimum
        assert _optimal_num_bits(-1, 0.01) == 64

    def test_optimal_num_hashes(self):
        k = _optimal_num_hashes(9585, 1000)
        # Optimal k ≈ 6.64 -> ceil = 7
        assert 5 <= k <= 8

    def test_optimal_num_hashes_edge(self):
        assert _optimal_num_hashes(100, 0) == 1  # Minimum


# ===========================================================================
# Integration tests: W3Codec with Bloom filters
# ===========================================================================


class TestBloomCodecIntegration:
    """Tests for Bloom filter integration with the W3Codec writer/reader."""

    def test_bloom_file_created(self):
        """Writer should create a .blm file when Bloom is enabled."""
        st = RamStorage()
        codec = W3Codec(bloom_enabled=True)
        seg = codec.new_segment(st, "test")

        fieldobj = fields.TEXT()
        tw = codec.field_writer(st, seg)
        tw.start_field("content", fieldobj)
        tw.start_term(b"hello")
        tw.add(0, 1.0, b"", 3)
        tw.finish_term()
        tw.start_term(b"world")
        tw.add(1, 1.0, b"", 3)
        tw.finish_term()
        tw.finish_field()
        tw.close()

        blmname = seg.make_filename(W3Codec.BLOOM_EXT)
        assert st.file_exists(blmname), "Bloom filter file should exist"
        assert st.file_length(blmname) > 0, "Bloom filter file should not be empty"

    def test_bloom_file_not_created_when_disabled(self):
        """No .blm file when Bloom is disabled."""
        st = RamStorage()
        codec = W3Codec(bloom_enabled=False)
        seg = codec.new_segment(st, "test")

        fieldobj = fields.TEXT()
        tw = codec.field_writer(st, seg)
        tw.start_field("content", fieldobj)
        tw.start_term(b"hello")
        tw.add(0, 1.0, b"", 3)
        tw.finish_term()
        tw.finish_field()
        tw.close()

        blmname = seg.make_filename(W3Codec.BLOOM_EXT)
        assert not st.file_exists(blmname), "No Bloom file when disabled"

    def test_negative_lookup_via_bloom(self):
        """Terms not in the index should be quickly rejected by the Bloom filter."""
        st = RamStorage()
        codec = W3Codec(bloom_enabled=True, bloom_false_positive_rate=0.001)
        seg = codec.new_segment(st, "test")

        fieldobj = fields.TEXT()
        tw = codec.field_writer(st, seg)
        tw.start_field("content", fieldobj)
        for i in range(100):
            tw.start_term(f"term{i}".encode())
            tw.add(i, 1.0, b"", 3)
            tw.finish_term()
        tw.finish_field()
        tw.close()

        tr = codec.terms_reader(st, seg)
        # Existing terms should be found
        for i in range(100):
            assert ("content", f"term{i}".encode()) in tr

        # Non-existing terms should NOT be found (no false negatives)
        for i in range(100, 200):
            assert ("content", f"term{i}".encode()) not in tr

        tr.close()

    def test_term_info_raises_for_missing_with_bloom(self):
        """term_info should raise TermNotFound for absent terms (via Bloom)."""
        st = RamStorage()
        codec = W3Codec(bloom_enabled=True)
        seg = codec.new_segment(st, "test")

        fieldobj = fields.TEXT()
        tw = codec.field_writer(st, seg)
        tw.start_field("content", fieldobj)
        tw.start_term(b"exists")
        tw.add(0, 1.0, b"", 3)
        tw.finish_term()
        tw.finish_field()
        tw.close()

        tr = codec.terms_reader(st, seg)
        # Should work for existing terms
        ti = tr.term_info("content", b"exists")
        assert ti is not None

        # Should raise for missing terms
        with pytest.raises(TermNotFound):
            tr.term_info("content", b"does_not_exist")

        tr.close()

    def test_frequency_returns_zero_for_missing_with_bloom(self):
        """frequency() should return 0 for missing terms via Bloom short-circuit."""
        st = RamStorage()
        codec = W3Codec(bloom_enabled=True)
        seg = codec.new_segment(st, "test")

        fieldobj = fields.TEXT()
        tw = codec.field_writer(st, seg)
        tw.start_field("content", fieldobj)
        tw.start_term(b"hello")
        tw.add(0, 2.5, b"", 3)
        tw.finish_term()
        tw.finish_field()
        tw.close()

        tr = codec.terms_reader(st, seg)
        assert tr.frequency("content", b"missing_term") == 0
        tr.close()

    def test_doc_frequency_returns_zero_for_missing_with_bloom(self):
        """doc_frequency() should return 0 for missing terms via Bloom."""
        st = RamStorage()
        codec = W3Codec(bloom_enabled=True)
        seg = codec.new_segment(st, "test")

        fieldobj = fields.TEXT()
        tw = codec.field_writer(st, seg)
        tw.start_field("content", fieldobj)
        tw.start_term(b"hello")
        tw.add(0, 1.0, b"", 3)
        tw.finish_term()
        tw.finish_field()
        tw.close()

        tr = codec.terms_reader(st, seg)
        assert tr.doc_frequency("content", b"missing_term") == 0
        tr.close()

    def test_bloom_with_multiple_fields(self):
        """Bloom filter should work correctly across multiple indexed fields."""
        st = RamStorage()
        codec = W3Codec(bloom_enabled=True)
        seg = codec.new_segment(st, "test")

        fieldobj = fields.TEXT()
        tw = codec.field_writer(st, seg)

        tw.start_field("title", fieldobj)
        tw.start_term(b"alpha")
        tw.add(0, 1.0, b"", 3)
        tw.finish_term()
        tw.finish_field()

        tw.start_field("body", fieldobj)
        tw.start_term(b"beta")
        tw.add(0, 1.0, b"", 5)
        tw.finish_term()
        tw.finish_field()

        tw.close()

        tr = codec.terms_reader(st, seg)
        # Each field has its own terms
        assert ("title", b"alpha") in tr
        assert ("body", b"beta") in tr
        # Cross-field lookups should fail
        assert ("title", b"beta") not in tr
        assert ("body", b"alpha") not in tr
        # Completely missing terms
        assert ("title", b"gamma") not in tr
        assert ("body", b"gamma") not in tr
        tr.close()

    def test_backward_compatibility_no_bloom_file(self):
        """Reader should work fine if there's no .blm file (old indexes)."""
        st = RamStorage()
        # Write with Bloom disabled (simulates old index without .blm)
        codec_write = W3Codec(bloom_enabled=False)
        seg = codec_write.new_segment(st, "test")

        fieldobj = fields.TEXT()
        tw = codec_write.field_writer(st, seg)
        tw.start_field("content", fieldobj)
        tw.start_term(b"hello")
        tw.add(0, 1.0, b"", 3)
        tw.finish_term()
        tw.finish_field()
        tw.close()

        # Read with Bloom enabled — should gracefully handle missing .blm
        codec_read = W3Codec(bloom_enabled=True)
        tr = codec_read.terms_reader(st, seg)
        assert ("content", b"hello") in tr
        assert ("content", b"missing") not in tr
        tr.close()


# ===========================================================================
# End-to-end tests: full indexing and searching with Bloom filters
# ===========================================================================


class TestBloomEndToEnd:
    """Full end-to-end tests with indexing and searching."""

    def test_search_existing_terms(self):
        schema = Schema(title=TEXT(stored=True), content=TEXT)
        with TempIndex(schema, "bloom_e2e_exist") as ix:
            with ix.writer() as w:
                w.add_document(title="Doc One", content="alpha beta gamma")
                w.add_document(title="Doc Two", content="delta epsilon alpha")

            with ix.searcher() as s:
                results = s.search(query.Term("content", "alpha"))
                assert len(results) == 2
                results = s.search(query.Term("content", "gamma"))
                assert len(results) == 1

    def test_search_nonexistent_terms(self):
        schema = Schema(title=TEXT(stored=True), content=TEXT)
        with TempIndex(schema, "bloom_e2e_noexist") as ix:
            with ix.writer() as w:
                w.add_document(title="Doc One", content="alpha beta gamma")

            with ix.searcher() as s:
                results = s.search(query.Term("content", "zzz_nonexistent"))
                assert len(results) == 0
                results = s.search(query.Term("content", "xyz_missing"))
                assert len(results) == 0

    def test_contains_operator(self):
        schema = Schema(content=TEXT)
        with TempIndex(schema, "bloom_e2e_contains") as ix:
            with ix.writer() as w:
                w.add_document(content="hello world foo bar")

            with ix.reader() as r:
                assert ("content", "hello") in r
                assert ("content", "world") in r
                assert ("content", "missing") not in r
                assert ("content", "nonexistent") not in r

    def test_doc_frequency_and_frequency(self):
        schema = Schema(content=TEXT)
        with TempIndex(schema, "bloom_e2e_freq") as ix:
            with ix.writer() as w:
                w.add_document(content="hello hello world")
                w.add_document(content="hello foo")

            with ix.reader() as r:
                # Existing terms
                assert r.doc_frequency("content", "hello") == 2
                assert r.frequency("content", "hello") > 0
                # Non-existing terms (Bloom should short-circuit)
                assert r.doc_frequency("content", "missing") == 0
                assert r.frequency("content", "missing") == 0

    def test_multiple_segments(self):
        """Bloom filters should work per-segment in multi-segment indexes."""
        schema = Schema(content=TEXT(stored=True))
        with TempIndex(schema, "bloom_e2e_multiseg") as ix:
            # Write two separate segments
            with ix.writer() as w:
                w.add_document(content="alpha beta")
            with ix.writer() as w:
                w.add_document(content="gamma delta")

            with ix.searcher() as s:
                results = s.search(query.Term("content", "alpha"))
                assert len(results) == 1
                results = s.search(query.Term("content", "gamma"))
                assert len(results) == 1
                results = s.search(query.Term("content", "nonexistent"))
                assert len(results) == 0

    def test_index_optimization_preserves_bloom(self):
        """After optimizing (merging segments), Bloom should still work."""
        schema = Schema(content=TEXT(stored=True))
        with TempIndex(schema, "bloom_e2e_optimize") as ix:
            with ix.writer() as w:
                w.add_document(content="alpha beta")
            with ix.writer() as w:
                w.add_document(content="gamma delta")

            # Optimize to merge all segments
            ix.optimize()

            with ix.searcher() as s:
                results = s.search(query.Term("content", "alpha"))
                assert len(results) == 1
                results = s.search(query.Term("content", "gamma"))
                assert len(results) == 1
                results = s.search(query.Term("content", "nonexistent"))
                assert len(results) == 0

    def test_keyword_field_with_bloom(self):
        schema = Schema(tag=KEYWORD(stored=True, commas=True))
        with TempIndex(schema, "bloom_e2e_kw") as ix:
            with ix.writer() as w:
                w.add_document(tag="python,search,index")
                w.add_document(tag="bloom,filter,search")

            with ix.searcher() as s:
                results = s.search(query.Term("tag", "search"))
                assert len(results) == 2
                results = s.search(query.Term("tag", "bloom"))
                assert len(results) == 1
                results = s.search(query.Term("tag", "java"))
                assert len(results) == 0

    def test_id_field_with_bloom(self):
        schema = Schema(path=ID(stored=True, unique=True), content=TEXT)
        with TempIndex(schema, "bloom_e2e_id") as ix:
            with ix.writer() as w:
                w.add_document(path="/a/b", content="hello world")
                w.add_document(path="/c/d", content="foo bar")

            with ix.searcher() as s:
                results = s.search(query.Term("path", "/a/b"))
                assert len(results) == 1
                results = s.search(query.Term("path", "/nonexistent"))
                assert len(results) == 0
