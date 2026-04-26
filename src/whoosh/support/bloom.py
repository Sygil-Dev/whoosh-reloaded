# Copyright 2026 Whoosh Reloaded contributors. All rights reserved.
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
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
# INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.

"""
Bloom Filter implementation for fast negative lookups.

A Bloom filter is a probabilistic data structure that can definitively say
when an element is NOT in a set (no false negatives) but may occasionally
report an element as present when it is not (false positives). This makes
it ideal for avoiding expensive disk reads when looking up terms that do
not exist in the index.

Usage::

    from whoosh.support.bloom import BloomFilter

    # Create a Bloom filter sized for expected number of items
    bf = BloomFilter(expected_items=10000, false_positive_rate=0.01)

    # Add items
    bf.add(b"hello")
    bf.add(b"world")

    # Test membership (fast, in-memory)
    assert b"hello" in bf       # True (definitely present)
    assert b"missing" not in bf  # True (definitely absent)

    # Serialize / deserialize
    data = bf.to_bytes()
    bf2 = BloomFilter.from_bytes(data)
"""

import math
import struct
from array import array
from hashlib import md5, sha256

# Header format: magic(4) + version(1) + num_hashes(1) + size_bytes(4) + num_bits(4)
_BLOOM_HEADER = struct.Struct("!4sBBII")
_BLOOM_MAGIC = b"BLM1"
_BLOOM_VERSION = 1


def _optimal_num_bits(n, p):
    """Calculate the optimal number of bits for a Bloom filter.

    :param n: expected number of items.
    :param p: desired false positive rate (0 < p < 1).
    :returns: number of bits (m).
    """
    if n <= 0:
        return 64  # minimum sensible size
    if p <= 0 or p >= 1:
        p = 0.01
    m = -1.0 * (n * math.log(p)) / (math.log(2) ** 2)
    return max(64, int(math.ceil(m)))


def _optimal_num_hashes(m, n):
    """Calculate the optimal number of hash functions.

    :param m: number of bits in the filter.
    :param n: expected number of items.
    :returns: number of hash functions (k).
    """
    if n <= 0:
        return 1
    k = (m / n) * math.log(2)
    return max(1, min(int(math.ceil(k)), 16))


def _hash_key(key, seed):
    """Generate a deterministic hash position for the given key and seed.

    Uses a double-hashing scheme based on MD5 and SHA-256 to produce
    multiple independent hash values from two base hashes.

    :param key: bytes to hash.
    :param seed: integer seed (0-based index of the hash function).
    :returns: non-negative integer hash value.
    """
    # Double hashing: h(i) = h1 + i * h2
    # This gives us k independent hash functions from just two base hashes
    if not isinstance(key, bytes):
        key = key.encode("utf-8")
    h1 = int.from_bytes(md5(key).digest()[:8], "big")
    h2 = int.from_bytes(sha256(key).digest()[:8], "big")
    return h1 + seed * h2


class BloomFilter:
    """A space-efficient probabilistic data structure for set membership testing.

    The Bloom filter guarantees no false negatives: if ``__contains__`` returns
    ``False``, the element is definitely not in the set. It may return
    ``True`` for elements that were never added (false positives), with a
    probability controlled by the ``false_positive_rate`` parameter.

    This implementation uses a double-hashing scheme for generating multiple
    hash functions from two base hash computations, keeping CPU cost low.
    """

    __slots__ = ("_bits", "_num_bits", "_num_hashes", "_count")

    def __init__(self, expected_items=1000, false_positive_rate=0.01,
                 num_bits=None, num_hashes=None):
        """Create a new Bloom filter.

        :param expected_items: estimated number of items to be added.
        :param false_positive_rate: desired false positive probability (0..1).
        :param num_bits: override the number of bits (for deserialization).
        :param num_hashes: override the number of hash functions.
        """
        if num_bits is not None and num_hashes is not None:
            # Direct construction (used by from_bytes)
            self._num_bits = num_bits
            self._num_hashes = num_hashes
        else:
            self._num_bits = _optimal_num_bits(expected_items, false_positive_rate)
            self._num_hashes = _optimal_num_hashes(self._num_bits, expected_items)

        # Use a bytearray as the backing store for the bit array
        num_bytes = (self._num_bits + 7) // 8
        self._bits = bytearray(num_bytes)
        self._count = 0

    @property
    def num_bits(self):
        """The number of bits in the filter."""
        return self._num_bits

    @property
    def num_hashes(self):
        """The number of hash functions used."""
        return self._num_hashes

    @property
    def count(self):
        """The number of items added (approximate, counting duplicates)."""
        return self._count

    @property
    def size_bytes(self):
        """The memory footprint of the bit array in bytes."""
        return len(self._bits)

    def estimated_false_positive_rate(self):
        """Estimate the current false positive rate based on items added.

        :returns: estimated false positive probability.
        """
        if self._count == 0:
            return 0.0
        # p = (1 - e^(-kn/m))^k
        exponent = -self._num_hashes * self._count / self._num_bits
        return (1.0 - math.exp(exponent)) ** self._num_hashes

    def add(self, key):
        """Add a key to the Bloom filter.

        :param key: bytes or string to add.
        """
        if not isinstance(key, bytes):
            key = key.encode("utf-8")

        bits = self._bits
        num_bits = self._num_bits
        for i in range(self._num_hashes):
            pos = _hash_key(key, i) % num_bits
            byte_idx = pos >> 3
            bit_mask = 1 << (pos & 7)
            bits[byte_idx] |= bit_mask
        self._count += 1

    def __contains__(self, key):
        """Test if a key might be in the set.

        :param key: bytes or string to test.
        :returns: True if possibly present, False if definitely absent.
        """
        if not isinstance(key, bytes):
            key = key.encode("utf-8")

        bits = self._bits
        num_bits = self._num_bits
        for i in range(self._num_hashes):
            pos = _hash_key(key, i) % num_bits
            byte_idx = pos >> 3
            bit_mask = 1 << (pos & 7)
            if not (bits[byte_idx] & bit_mask):
                return False
        return True

    def to_bytes(self):
        """Serialize the Bloom filter to bytes.

        :returns: bytes representation of the filter.
        """
        header = _BLOOM_HEADER.pack(
            _BLOOM_MAGIC,
            _BLOOM_VERSION,
            self._num_hashes,
            len(self._bits),
            self._num_bits,
        )
        return header + bytes(self._bits)

    @classmethod
    def from_bytes(cls, data):
        """Deserialize a Bloom filter from bytes.

        :param data: bytes previously produced by ``to_bytes()``.
        :returns: a new BloomFilter instance.
        :raises ValueError: if the data is corrupt or has wrong magic.
        """
        hdr_size = _BLOOM_HEADER.size
        if len(data) < hdr_size:
            raise ValueError("Bloom filter data too short")

        magic, version, num_hashes, size_bytes, num_bits = _BLOOM_HEADER.unpack(
            data[:hdr_size]
        )
        if magic != _BLOOM_MAGIC:
            raise ValueError(f"Invalid Bloom filter magic: {magic!r}")
        if version != _BLOOM_VERSION:
            raise ValueError(f"Unsupported Bloom filter version: {version}")

        expected_size = hdr_size + size_bytes
        if len(data) < expected_size:
            raise ValueError(
                f"Bloom filter data truncated: expected {expected_size}, "
                f"got {len(data)}"
            )

        bf = cls.__new__(cls)
        bf._num_bits = num_bits
        bf._num_hashes = num_hashes
        bf._bits = bytearray(data[hdr_size:hdr_size + size_bytes])
        bf._count = -1  # Unknown when deserialized
        return bf

    def write_to_file(self, dbfile):
        """Write the Bloom filter to an open file-like object.

        :param dbfile: a file-like object with a ``write()`` method.
        """
        dbfile.write(self.to_bytes())

    @classmethod
    def read_from_file(cls, dbfile, length=None):
        """Read a Bloom filter from an open file-like object.

        :param dbfile: a file-like object with ``read()`` method.
        :param length: optional total length to read; if None, reads header
            first to determine size.
        :returns: a new BloomFilter instance.
        """
        if length is not None:
            data = dbfile.read(length)
        else:
            # Read header first to get size
            hdr_data = dbfile.read(_BLOOM_HEADER.size)
            if len(hdr_data) < _BLOOM_HEADER.size:
                raise ValueError("Bloom filter data too short")
            _, _, _, size_bytes, _ = _BLOOM_HEADER.unpack(hdr_data)
            bit_data = dbfile.read(size_bytes)
            data = hdr_data + bit_data
        return cls.from_bytes(data)

    def merge(self, other):
        """Merge another Bloom filter into this one (OR operation).

        Both filters must have the same parameters (num_bits, num_hashes).

        :param other: another BloomFilter with identical parameters.
        :raises ValueError: if parameters don't match.
        """
        if self._num_bits != other._num_bits:
            raise ValueError(
                f"Cannot merge: num_bits differ "
                f"({self._num_bits} vs {other._num_bits})"
            )
        if self._num_hashes != other._num_hashes:
            raise ValueError(
                f"Cannot merge: num_hashes differ "
                f"({self._num_hashes} vs {other._num_hashes})"
            )
        for i in range(len(self._bits)):
            self._bits[i] |= other._bits[i]

    def __repr__(self):
        return (
            f"BloomFilter(num_bits={self._num_bits}, "
            f"num_hashes={self._num_hashes}, "
            f"size_bytes={self.size_bytes}, "
            f"count={self._count})"
        )
