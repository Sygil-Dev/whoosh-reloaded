"""

From a post by Patrick Maupin on the Python mailing list:
https://mail.python.org/pipermail/python-list/2003-November/237481.html
"""

from array import array

from whoosh.system import _LONG_SIZE

_bitsperlong = _LONG_SIZE * 8


class BitStreamReader:
    def __init__(self, source):
        """
        Initializes a BitStreamReader object.

        Parameters:
        - source: The source data to read from.

        The BitStreamReader reads binary data from the given source and provides methods to seek, tell, and read bits from the data.
        """

        self._totalbits = len(source) * _bitsperlong
        self._position = 0

        # Pad to longword boundary, then make an array

        source += -len(source) % _LONG_SIZE * chr(0)
        bits = array("L")
        bits.fromstring(source)
        self._bitstream = bits

    def seek(self, offset):
        """
        Sets the current position in the bitstream.

        Parameters:
        - offset: The new position to set.

        The offset is specified in bits from the beginning of the bitstream.
        """

        self._position = offset

    def tell(self):
        """
        Returns the current position in the bitstream.

        Returns:
        - The current position in bits from the beginning of the bitstream.
        """

        return self._position

    def read(self, numbits):
        """
        Reads the specified number of bits from the bitstream.

        Parameters:
        - numbits: The number of bits to read.

        Returns:
        - The value of the read bits.

        Raises:
        - IndexError: If the specified number of bits exceeds the available bits in the bitstream.

        The read method reads the specified number of bits from the current position in the bitstream and advances the position accordingly.
        """

        position = self._position

        if position < 0 or position + numbits > self._totalbits:
            raise IndexError("Invalid bitarray._position/numbits")

        longaddress, bitoffset = divmod(position, _bitsperlong)

        # We may read bits in the final word after ones we care
        # about, so create a mask to remove them later.

        finalmask = (1 << numbits) - 1

        # We may read bits in the first word before the ones we
        # care about, so bump the total bits to read by this
        # amount, so we read enough higher-order bits.

        numbits += bitoffset

        # Read and concatenate every long containing a bit we need

        outval, outshift = 0, 0
        while numbits > 0:
            outval += self._bitstream[longaddress] << outshift
            longaddress += 1
            outshift += _bitsperlong
            numbits -= _bitsperlong

        # numbits is now basically a negative number which tells us
        # how many bits to back up from our current position.

        self._position = longaddress * _bitsperlong + numbits

        # Shift right to strip off the low-order bits we
        # don't want, then 'and' with the mask to strip
        # off the high-order bits we don't want.

        return (outval >> bitoffset) & finalmask
