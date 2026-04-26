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

"""Legacy bit-array and dynamic set implementations.

This module provides :class:`BitVector`, a fixed-size array of bits with
set-like operations, and :class:`BitSet`, a hybrid container that
automatically switches its backing store between a ``set`` and a
:class:`BitVector` depending on density.
"""

import operator
from array import array

#: Popcount lookup table: number of set bits in each byte value (0-255).
BYTE_COUNTS = array("B", [bin(i).count("1") for i in range(256)])


class BitVector:
    """Implements a memory-efficient fixed-size array of bits.

    >>> bv = BitVector(10)
    >>> bv
    <BitVector 0000000000>
    >>> bv[5] = True
    >>> bv
    <BitVector 0000010000>

    You can initialise a ``BitVector`` using an iterable of integers
    representing bit positions to turn on.

    >>> bv2 = BitVector(10, [2, 4, 7])
    >>> bv2
    <BitVector 0010100100>
    >>> bv2[2]
    True

    ``BitVector`` supports bit-wise logic operations ``&`` (and), ``|`` (or),
    and ``^`` (xor) between itself and another ``BitVector`` of equal size, or
    itself and a collection of integers (usually a ``set`` or ``frozenset``).

    >>> bv | bv2
    <BitVector 0010110100>

    Note that ``BitVector.__len__()`` returns the number of "on" bits, not
    the size of the bit array.  This is to make ``BitVector`` interchangeable
    with a ``set``/``frozenset`` of integers.  To get the size, use
    ``BitVector.size``.
    """

    def __init__(self, size, source=None, bits=None):
        self.size = size

        if bits:
            self.bits = bits
        else:
            self.bits = array("B", ([0x00] * ((size >> 3) + 1)))

        if source:
            _set = self.set
            for num in source:
                _set(num)

        self.bcount = None

    def __eq__(self, other):
        if isinstance(other, BitVector):
            return self.bits == other.bits
        return False

    def __repr__(self):
        return f"<BitVector {self.__str__()}>"

    def __len__(self):
        # This returns the count of "on" bits instead of the size to
        # make BitVector exchangeable with a set() object.
        return self.count()

    def __contains__(self, index):
        return self[index]

    def __iter__(self):
        get = self.__getitem__
        for i in range(0, self.size):
            if get(i):
                yield i

    def __str__(self):
        get = self.__getitem__
        return "".join("1" if get(i) else "0" for i in range(0, self.size))

    def __nonzero__(self):
        return self.count() > 0

    def __getitem__(self, index):
        return self.bits[index >> 3] & (1 << (index & 7)) != 0

    def __setitem__(self, index, value):
        if value:
            self.set(index)
        else:
            self.clear(index)

    def _logic(self, op, bitv):
        if self.size != bitv.size:
            raise ValueError("Can't combine bitvectors of different sizes")
        res = BitVector(size=self.size)
        lpb = map(op, self.bits, bitv.bits)
        res.bits = array("B", lpb)
        return res

    def union(self, other):
        return self.__or__(other)

    def intersection(self, other):
        return self.__and__(other)

    def __and__(self, other):
        if not isinstance(other, BitVector):
            other = BitVector(self.size, source=other)
        return self._logic(operator.__and__, other)

    def __or__(self, other):
        if not isinstance(other, BitVector):
            other = BitVector(self.size, source=other)
        return self._logic(operator.__or__, other)

    def __ror__(self, other):
        return self.__or__(other)

    def __rand__(self, other):
        return self.__and__(other)

    def __xor__(self, other):
        if not isinstance(other, BitVector):
            other = BitVector(self.size, source=other)
        return self._logic(operator.__xor__, other)

    def __invert__(self):
        return BitVector(
            self.size, source=(x for x in range(self.size) if x not in self)
        )

    def count(self):
        """Returns the number of "on" bits in the bit array."""

        if self.bcount is None:
            self.bcount = sum(BYTE_COUNTS[b & 0xFF] for b in self.bits)
        return self.bcount

    def set(self, index):
        """Turns the bit at the given position on."""

        if index >= self.size:
            raise IndexError(
                f"Position {repr(index)} greater than the size of the vector"
            )
        self.bits[index >> 3] |= 1 << (index & 7)
        self.bcount = None

    def clear(self, index):
        """Turns the bit at the given position off."""

        self.bits[index >> 3] &= ~(1 << (index & 7))
        self.bcount = None

    def set_from(self, iterable):
        """Takes an iterable of integers representing positions, and turns
        on the bits at those positions.
        """

        _set = self.set
        for index in iterable:
            _set(index)

    def copy(self):
        """Returns a copy of this BitVector."""

        return BitVector(self.size, bits=self.bits)


class BitSet:
    """A set-like object for holding positive integers.  It is dynamically
    backed by either a ``set`` or :class:`BitVector` depending on how many
    numbers are in the set.

    Provides ``add``, ``remove``, ``union``, ``intersection``,
    ``__contains__``, ``__len__``, ``__iter__``, ``__and__``, ``__or__``, and
    ``__nonzero__`` methods.
    """

    def __init__(self, size, source=None):
        self.size = size

        self._back = ()
        self._switch(size > 256)

        if source:
            add = self.add
            for num in source:
                add(num)

    def _switch(self, toset):
        if toset:
            self._back = set(self._back)
            self.add = self._set_add
            self.remove = self._back.remove
        else:
            self._back = BitVector(self.size)
            self.add = self._back.set
            self.remove = self._vec_remove

        self.__contains__ = self._back.__contains__
        self.__len__ = self._back.__len__
        self.__iter__ = self._back.__iter__
        self.__nonzero__ = self._back.__nonzero__

    def as_set(self):
        return frozenset(self._back)

    def union(self, other):
        return self.__or__(other)

    def intersection(self, other):
        return self.__and__(other)

    def __and__(self, other):
        self._back = self._back.intersection(other)

    def __or__(self, other):
        self._back = self._back.union(other)

    def _set_add(self, num):
        self._back.add(num)
        if len(self._back) * 4 > self.size // 8 + 32:
            self._switch(False)

    def _vec_remove(self, num):
        self._back.clear(num)
        if len(self._back) * 4 < self.size // 8 - 32:
            self._switch(True)
