from __future__ import annotations

from abc import ABC, abstractmethod
from array import array
from itertools import islice
from struct import Struct
from typing import TYPE_CHECKING, ClassVar, Literal

from whoosh.system import (
    emptybytes,
    pack_byte,
    pack_uint_le,
    pack_ushort_le,
    unpack_uint_le,
)

if TYPE_CHECKING:
    from collections.abc import Generator, Iterable, Iterator, Sequence

    from whoosh.filedb.structfile import StructFile


def delta_encode(nums: Iterable[int]) -> Generator[int]:
    base = 0
    for n in nums:
        yield n - base
        base = n


def delta_decode(nums: Iterable[int]) -> Generator[int]:
    base = 0
    for n in nums:
        base += n
        yield base


class GrowableArray:
    def __init__(self, inittype: str = "B", allow_longs: bool = True):
        self.array: array[int] | list[int] = array(inittype)
        self._allow_longs = allow_longs

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.array!r})"

    def __len__(self) -> int:
        return len(self.array)

    def __iter__(self) -> Iterator[int]:
        return iter(self.array)

    def _retype(self, maxnum: int) -> None:
        if maxnum < 2**16:
            newtype = "H"
        elif maxnum < 2**31:
            newtype = "i"
        elif maxnum < 2**32:
            newtype = "I"
        elif self._allow_longs:
            newtype = "q"
        else:
            raise OverflowError(f"{maxnum!r} is too big to fit in an array")

        try:
            self.array = array(newtype, iter(self.array))
        except ValueError:
            self.array = list(self.array)

    def append(self, n: int) -> None:
        try:
            self.array.append(n)
        except OverflowError:
            self._retype(n)
            self.array.append(n)

    def extend(self, ns: Iterable[int]) -> None:
        for n in ns:
            self.append(n)

    @property
    def typecode(
        self,
    ) -> Literal["b", "B", "h", "H", "i", "I", "l", "L", "q", "Q", "f", "d", "u", "w"]:
        if isinstance(self.array, array):
            return self.array.typecode
        else:
            return "q"

    def to_file(self, dbfile: StructFile) -> None:
        if isinstance(self.array, array):
            dbfile.write_array(self.array)
        else:
            write_long = dbfile.write_long
            for n in self.array:
                write_long(n)


# Number list encoding base class


class NumberEncoding(ABC):
    maxint: ClassVar[int | None] = None

    @abstractmethod
    def write_nums(self, f: StructFile, numbers: Iterable[int]) -> None:
        pass

    @abstractmethod
    def read_nums(self, f: StructFile, n: int) -> Generator[int]:
        pass

    def write_deltas(self, f: StructFile, numbers: Iterable[int]) -> None:
        return self.write_nums(f, list(delta_encode(numbers)))

    def read_deltas(self, f: StructFile, n: int) -> Generator[int]:
        return delta_decode(self.read_nums(f, n))

    def get(self, f: StructFile, pos: int, i: int) -> None | int:
        f.seek(pos)
        if i < 0:
            return None
        return next(islice(self.read_nums(f, i + 1), i, None), None)


# Fixed width encodings


class FixedEncoding(NumberEncoding):
    _struct: ClassVar[Struct]

    @classmethod
    def encode(cls, n: int) -> bytes:
        return cls._struct.pack(n)

    @classmethod
    def decode(cls, data: bytes) -> int:
        return cls._struct.unpack(data)[0]

    @classmethod
    def size(cls) -> int:
        return cls._struct.size

    def write_nums(self, f: StructFile, numbers: Iterable[int]) -> None:
        encode = type(self).encode
        for n in numbers:
            f.write(encode(n))

    def read_nums(self, f: StructFile, n: int) -> Generator[int]:
        cls = type(self)
        size = cls.size()
        decode = cls.decode
        for _ in range(n):
            yield decode(f.read(size))

    def get(self, f: StructFile, pos: int, i: int) -> int:
        cls = type(self)
        size = cls.size()
        f.seek(pos + i * size)
        return cls.decode(f.read(size))


class ByteEncoding(FixedEncoding):
    _struct = Struct("!B")
    maxint = 255


class UShortEncoding(FixedEncoding):
    _struct = Struct("<H")
    maxint = 2**16 - 1


class UIntEncoding(FixedEncoding):
    _struct = Struct("<I")
    maxint = 2**32 - 1


# High-bit encoded variable-length integer


class Varints(NumberEncoding):
    maxint = None

    def write_nums(self, f: StructFile, numbers: Iterable[int]) -> None:
        for n in numbers:
            f.write_varint(n)

    def read_nums(self, f: StructFile, n: int) -> Generator[int]:
        for _ in range(n):
            yield f.read_varint()


# Simple16 algorithm for storing arrays of positive integers (usually delta
# encoded lists of sorted integers)
#
# 1. http://www2008.org/papers/pdf/p387-zhangA.pdf
# 2. http://www2009.org/proceedings/pdf/p401.pdf


class Simple16(NumberEncoding):
    # The maximum possible integer value Simple16 can encode is < 2^28.
    # Therefore, in order to use Simple16, the application must have its own
    # code to encode numbers in the range of [2^28, 2^32). A simple way is just
    # write those numbers as 32-bit integers (that is, no compression for very
    # big numbers).
    _numsize: ClassVar[int] = 16
    _bitsize: ClassVar[int] = 28
    maxint = 2**_bitsize - 1

    # Number of stored numbers per code
    _num: ClassVar[tuple[int, ...]] = (
        28,
        21,
        21,
        21,
        14,
        9,
        8,
        7,
        6,
        6,
        5,
        5,
        4,
        3,
        2,
        1,
    )
    # Number of bits for each number per code
    _bits: ClassVar[tuple[tuple[int, ...], ...]] = (
        (1,) * 28,
        (2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1),
        (1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1),
        (1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2),
        (2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2),
        (4, 3, 3, 3, 3, 3, 3, 3, 3),
        (3, 4, 4, 4, 4, 3, 3, 3),
        (4, 4, 4, 4, 4, 4, 4),
        (5, 5, 5, 5, 4, 4),
        (4, 4, 5, 5, 5, 5),
        (6, 6, 6, 5, 5),
        (5, 5, 6, 6, 6),
        (7, 7, 7, 7),
        (10, 9, 9),
        (14, 14),
        (28,),
    )

    def write_nums(self, f: StructFile, numbers: Iterable[int]) -> None:
        items = list(numbers)
        i = 0
        while i < len(items):
            value, taken = self._compress(items, i, len(items) - i)
            f.write_uint_le(value)
            i += taken

    @classmethod
    def _compress(
        cls,
        inarray: Sequence[int],
        inoffset: int,
        n: int,
    ) -> tuple[int, int]:
        numsize = cls._numsize
        bitsize = cls._bitsize
        num_table = cls._num
        bits_table = cls._bits

        for key in range(numsize):
            value = key << bitsize
            num = min(num_table[key], n)
            bits = 0

            j = 0
            while j < num and inarray[inoffset + j] < (1 << bits_table[key][j]):
                x = inarray[inoffset + j]
                value |= x << bits
                bits += bits_table[key][j]
                j += 1

            if j == num:
                return value, num

        raise ValueError("Could not encode values with Simple16")

    def read_nums(self, f: StructFile, n: int) -> Generator[int]:
        i = 0
        while i < n:
            value = unpack_uint_le(f.read(4))[0]
            for v in self._decompress(value, n - i):
                yield v
                i += 1

    @classmethod
    def _decompress(cls, value: int, n: int) -> Generator[int]:
        bitsize = cls._bitsize
        num_table = cls._num
        bits_table = cls._bits

        key = value >> bitsize
        num = min(num_table[key], n)
        bits = 0
        for j in range(num):
            v = value >> bits
            yield v & (0xFFFFFFFF >> (32 - bits_table[key][j]))
            bits += bits_table[key][j]

    def get(self, f: StructFile, pos: int, i: int) -> int:
        f.seek(pos)
        base = 0
        value = unpack_uint_le(f.read(4))[0]
        key = value >> self._bitsize
        num = self._num[key]
        while i > base + num:
            base += num
            value = unpack_uint_le(f.read(4))[0]
            key = value >> self._bitsize
            num = self._num[key]

        offset = i - base
        if offset:
            value = value >> sum(self._bits[key][:offset])
        return value & (2 ** self._bits[key][offset] - 1)


# Google Packed Ints algorithm: a set of four numbers is preceded by a "key"
# byte, which encodes how many bytes each of the next four integers use
# (stored in the byte as four 2-bit numbers)


class GInts(NumberEncoding):
    maxint = 2**32 - 1

    # Number of future bytes to expect after a "key" byte value of N -- used to
    # skip ahead from a key byte
    _lens: ClassVar[array[int]] = array(
        "B",
        [
            4,
            5,
            6,
            7,
            5,
            6,
            7,
            8,
            6,
            7,
            8,
            9,
            7,
            8,
            9,
            10,
            5,
            6,
            7,
            8,
            6,
            7,
            8,
            9,
            7,
            8,
            9,
            10,
            8,
            9,
            10,
            11,
            6,
            7,
            8,
            9,
            7,
            8,
            9,
            10,
            8,
            9,
            10,
            11,
            9,
            10,
            11,
            12,
            7,
            8,
            9,
            10,
            8,
            9,
            10,
            11,
            9,
            10,
            11,
            12,
            10,
            11,
            12,
            13,
            5,
            6,
            7,
            8,
            6,
            7,
            8,
            9,
            7,
            8,
            9,
            10,
            8,
            9,
            10,
            11,
            6,
            7,
            8,
            9,
            7,
            8,
            9,
            10,
            8,
            9,
            10,
            11,
            9,
            10,
            11,
            12,
            7,
            8,
            9,
            10,
            8,
            9,
            10,
            11,
            9,
            10,
            11,
            12,
            10,
            11,
            12,
            13,
            8,
            9,
            10,
            11,
            9,
            10,
            11,
            12,
            10,
            11,
            12,
            13,
            11,
            12,
            13,
            14,
            6,
            7,
            8,
            9,
            7,
            8,
            9,
            10,
            8,
            9,
            10,
            11,
            9,
            10,
            11,
            12,
            7,
            8,
            9,
            10,
            8,
            9,
            10,
            11,
            9,
            10,
            11,
            12,
            10,
            11,
            12,
            13,
            8,
            9,
            10,
            11,
            9,
            10,
            11,
            12,
            10,
            11,
            12,
            13,
            11,
            12,
            13,
            14,
            9,
            10,
            11,
            12,
            10,
            11,
            12,
            13,
            11,
            12,
            13,
            14,
            12,
            13,
            14,
            15,
            7,
            8,
            9,
            10,
            8,
            9,
            10,
            11,
            9,
            10,
            11,
            12,
            10,
            11,
            12,
            13,
            8,
            9,
            10,
            11,
            9,
            10,
            11,
            12,
            10,
            11,
            12,
            13,
            11,
            12,
            13,
            14,
            9,
            10,
            11,
            12,
            10,
            11,
            12,
            13,
            11,
            12,
            13,
            14,
            12,
            13,
            14,
            15,
            10,
            11,
            12,
            13,
            11,
            12,
            13,
            14,
            12,
            13,
            14,
            15,
            13,
            14,
            15,
            16,
        ],
    )

    def key_to_sizes(self, key: int) -> list[int]:
        """Returns a list of the sizes of the next four numbers given a key
        byte.
        """

        return [(key >> (i * 2) & 3) + 1 for i in range(4)]

    def write_nums(self, f: StructFile, numbers: Iterable[int]) -> None:
        buf = emptybytes
        count = 0
        key = 0
        for v in numbers:
            shift = count * 2
            if v < 256:
                buf += pack_byte(v)
            elif v < 65536:
                key |= 1 << shift
                buf += pack_ushort_le(v)
            elif v < 16777216:
                key |= 2 << shift
                buf += pack_uint_le(v)[:3]
            else:
                key |= 3 << shift
                buf += pack_uint_le(v)

            count += 1
            if count == 4:
                f.write_byte(key)
                f.write(buf)
                count = 0
                key = 0
                buf = emptybytes  # Clear the buffer

        # Write out leftovers in the buffer
        if count:
            f.write_byte(key)
            f.write(buf)

    def read_nums(self, f: StructFile, n: int) -> Generator[int]:
        """Read N integers from the bytes stream dbfile. Expects that the file
        is positioned at a key byte.
        """

        count = 0
        key = None
        for _ in range(n):
            if count == 0:
                key = f.read_byte()
            assert key is not None
            code = key >> (count * 2) & 3
            if code == 0:
                yield f.read_byte()
            elif code == 1:
                yield f.read_ushort_le()
            elif code == 2:
                yield unpack_uint_le(f.read(3) + "\x00")[0]
            else:
                yield f.read_uint_le()

            count = (count + 1) % 4


#    def get(self, f, pos, i):
#        f.seek(pos)
#        base = 0
#        key = f.read_byte()
#        while i > base + 4:
#            base += 4
#            f.seek(self._lens[key], 1)
#            key = f.read_byte()
#
#        for n in self.read_nums(f, (i + 1) - base):
#            pass
#        return n
