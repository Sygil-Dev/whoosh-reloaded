"""
An implementation of an object that acts like a collection of on/off bits.
"""

import operator
from array import array

#: Table of the number of '1' bits in each byte (0-255)
BYTE_COUNTS = array("B", [bin(byte).count("1") for byte in range(256)])


class BitVector:
    """
    Implements a memory-efficient array of bits.

    >>> bv = BitVector(10)
    >>> bv
    <BitVector 0000000000>
    >>> bv[5] = True
    >>> bv
    <BitVector 0000010000>

    You can initialize the BitVector using an iterable of integers representing bit
    positions to turn on.

    >>> bv2 = BitVector(10, [2, 4, 7])
    >>> bv2
    <BitVector 00101001000>
    >>> bv[2]
    True

    BitVector supports bit-wise logic operations & (and), | (or), and ^ (xor)
    between itself and another BitVector of equal size, or itself and a collection of
    integers (usually a set() or frozenset()).

    >>> bv | bv2
    <BitVector 00101101000>

    Note that ``BitVector.__len__()`` returns the number of "on" bits, not
    the size of the bit array. This is to make BitVector interchangeable with
    a set()/frozenset() of integers. To get the size, use BitVector.size.
    """

    def __init__(self, size, source=None, bits=None):
        """
        Initializes a BitVector object.

        Args:
            size (int): The size of the BitVector.
            source (iterable, optional): An iterable of integers representing bit positions to turn on. Defaults to None.
            bits (array, optional): An array of bytes representing the bit values. Defaults to None.
        """
        self.size = size

        if bits:
            self.bits = bits
        else:
            self.bits = array("B", ([0x00] * ((size >> 3) + 1)))

        if source:
            set_var = self.set
            for num in source:
                set_var(num)

        self.bcount = None

    def __eq__(self, other):
        """
        Checks if two BitVector objects are equal.

        Args:
            other (BitVector): The other BitVector object to compare.

        Returns:
            bool: True if the BitVector objects are equal, False otherwise.
        """
        if isinstance(other, BitVector):
            return self.bits == other.bits
        return False

    def __repr__(self):
        """
        Returns a string representation of the BitVector object.

        Returns:
            str: A string representation of the BitVector object.
        """
        return f"<BitVector {self.__str__()}>"

    def __len__(self):
        """
        Returns the number of "on" bits in the BitVector.

        Returns:
            int: The number of "on" bits in the BitVector.
        """
        return self.count()

    def __contains__(self, index):
        """
        Checks if a given index is present in the BitVector.

        Args:
            index (int): The index to check.

        Returns:
            bool: True if the index is present in the BitVector, False otherwise.
        """
        return self[index]

    def __iter__(self):
        """
        Returns an iterator over the "on" bits in the BitVector.

        Yields:
            int: The indices of the "on" bits in the BitVector.
        """
        get = self.__getitem__
        for i in range(0, self.size):
            if get(i):
                yield i

    def __str__(self):
        """
        Returns a string representation of the BitVector object.

        Returns:
            str: A string representation of the BitVector object.
        """
        get = self.__getitem__
        return "".join("1" if get(i) else "0" for i in range(0, self.size))

    def __nonzero__(self):
        """
        Checks if the BitVector has any "on" bits.

        Returns:
            bool: True if the BitVector has any "on" bits, False otherwise.
        """
        return self.count() > 0

    def __getitem__(self, index):
        """
        Returns the value of the bit at the given index.

        Args:
            index (int): The index of the bit to retrieve.

        Returns:
            bool: True if the bit is "on", False otherwise.
        """
        return self.bits[index >> 3] & (1 << (index & 7)) != 0

    def __setitem__(self, index, value):
        """
        Sets the value of the bit at the given index.

        Args:
            index (int): The index of the bit to set.
            value (bool): The value to set the bit to.
        """
        if value:
            self.set(index)
        else:
            self.clear(index)

    def _logic(self, op, bitv):
        """
        Performs a bit-wise logic operation between two BitVector objects.

        Args:
            op (function): The bit-wise logic operation to perform.
            bitv (BitVector): The other BitVector object to perform the operation with.

        Returns:
            BitVector: The result of the bit-wise logic operation.
        """
        if self.size != bitv.size:
            raise ValueError("Can't combine bitvectors of different sizes")
        res = BitVector(size=self.size)
        lpb = map(op, self.bits, bitv.bits)
        res.bits = array("B", lpb)
        return res

    def union(self, other):
        """
        Performs a union operation between two BitVector objects.

        Args:
            other (BitVector): The other BitVector object to perform the union with.

        Returns:
            BitVector: The result of the union operation.
        """
        return self.__or__(other)

    def intersection(self, other):
        """
        Performs an intersection operation between two BitVector objects.

        Args:
            other (BitVector): The other BitVector object to perform the intersection with.

        Returns:
            BitVector: The result of the intersection operation.
        """
        return self.__and__(other)

    def __and__(self, other):
        """
        Performs a bit-wise AND operation between two BitVector objects.

        Args:
            other (BitVector): The other BitVector object to perform the AND operation with.

        Returns:
            BitVector: The result of the bit-wise AND operation.
        """
        if not isinstance(other, BitVector):
            other = BitVector(self.size, source=other)
        return self._logic(operator.__and__, other)

    def __or__(self, other):
        """
        Performs a bit-wise OR operation between two BitVector objects.

        Args:
            other (BitVector): The other BitVector object to perform the OR operation with.

        Returns:
            BitVector: The result of the bit-wise OR operation.
        """
        if not isinstance(other, BitVector):
            other = BitVector(self.size, source=other)
        return self._logic(operator.__or__, other)

    def __ror__(self, other):
        """
        Performs a bit-wise OR operation between a BitVector object and another object.

        Args:
            other (BitVector): The other object to perform the OR operation with.

        Returns:
            BitVector: The result of the bit-wise OR operation.
        """
        return self.__or__(other)

    def __rand__(self, other):
        """
        Performs a bit-wise AND operation between a BitVector object and another object.

        Args:
            other (BitVector): The other object to perform the AND operation with.

        Returns:
            BitVector: The result of the bit-wise AND operation.
        """
        return self.__and__(other)

    def __xor__(self, other):
        """
        Performs a bit-wise XOR operation between two BitVector objects.

        Args:
            other (BitVector): The other BitVector object to perform the XOR operation with.

        Returns:
            BitVector: The result of the bit-wise XOR operation.
        """
        if not isinstance(other, BitVector):
            other = BitVector(self.size, source=other)
        return self._logic(operator.__xor__, other)

    def __invert__(self):
        """
        Performs a bit-wise inversion operation on the BitVector.

        Returns:
            BitVector: The result of the bit-wise inversion operation.
        """
        return BitVector(
            self.size, source=(x for x in range(self.size) if x not in self)
        )

    def count(self):
        """
        Returns the number of "on" bits in the BitVector.

        Returns:
            int: The number of "on" bits in the BitVector.
        """
        if self.bcount is None:
            self.bcount = sum(BYTE_COUNTS[b & 0xFF] for b in self.bits)
        return self.bcount

    def set(self, index):
        """
        Turns the bit at the given position on.

        Args:
            index (int): The index of the bit to turn on.
        """
        if index >= self.size:
            raise IndexError(
                f"Position {repr(index)} greater than the size of the vector"
            )
        self.bits[index >> 3] |= 1 << (index & 7)
        self.bcount = None

    def clear(self, index):
        """
        Turns the bit at the given position off.

        Args:
            index (int): The index of the bit to turn off.
        """
        self.bits[index >> 3] &= ~(1 << (index & 7))
        self.bcount = None

    def set_from(self, iterable):
        """
        Turns on the bits at the positions specified by an iterable of integers.

        Args:
            iterable (iterable): An iterable of integers representing positions.
        """
        set_var = self.set
        for index in iterable:
            set_var(index)

    def copy(self):
        """
        Returns a copy of the BitVector.

        Returns:
            BitVector: A copy of the BitVector.
        """
        return BitVector(self.size, bits=self.bits)


class BitSet:
    """A set-like object for holding positive integers. It is dynamically
    backed by either a set or BitVector depending on how many numbers are in
    the set.

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
            self._back = BitVector()
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
