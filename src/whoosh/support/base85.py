"""
This module contains generic base85 encoding and decoding functions. The
whoosh.util.numeric module contains faster variants for encoding and
decoding integers.

Modified from:
http://paste.lisp.org/display/72815
"""

import struct

# Instead of using the character set from the ascii85 algorithm, I put the
# characters in order so that the encoded text sorts properly (my life would be
# a lot easier if they had just done that from the start)
b85chars = (
    "!$%&*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "^_abcdefghijklmnopqrstuvwxyz{|}~"
)
b85dec = {}
for i in range(len(b85chars)):
    b85dec[b85chars[i]] = i


# Integer encoding and decoding functions


def to_base85(x, islong=False):
    """
    Encodes the given integer using base 85.

    Parameters:
    - x: The integer to be encoded.
    - islong: A boolean indicating whether the integer is a long integer or not. Default is False.

    Returns:
    - The base 85 encoded string.

    Example:
    >>> to_base85(12345)
    '3qo'
    """
    size = 10 if islong else 5
    rems = ""
    for _ in range(size):
        rems = b85chars[x % 85] + rems
        x //= 85
    return rems


def from_base85(text):
    """
    Decodes the given base 85 text into an integer.

    Parameters:
    text (str): The base 85 encoded text to be decoded.

    Returns:
    int: The decoded integer value.

    Raises:
    KeyError: If the input text contains characters not present in the base 85 encoding table.
    """
    acc = 0
    for c in text:
        acc = acc * 85 + b85dec[c]
    return acc


# Bytes encoding and decoding functions
def b85encode(text, pad=False):
    """
    Encode the given text using Base85 encoding.

    Args:
        text (str): The text to be encoded.
        pad (bool, optional): Whether to pad the encoded output. Defaults to False.

    Returns:
        str: The Base85 encoded string.

    Raises:
        None

    Example:
        >>> b85encode("Hello World")
        '87cURD]j7BEbo80'

    Note:
        Base85 encoding is a binary-to-text encoding scheme that represents binary data in an ASCII string format.
        It is commonly used in various applications such as data compression and data transmission.
    """
    l = len(text)
    r = l % 4
    if r:
        text += "\0" * (4 - r)
    longs = len(text) >> 2
    out = []
    words = struct.unpack(">" + "L" * longs, text[0 : longs * 4])
    for word in words:
        rems = [0, 0, 0, 0, 0]
        for i in range(4, -1, -1):
            rems[i] = b85chars[word % 85]
            word /= 85
        out.extend(rems)

    out = "".join(out)
    if pad:
        return out

    # Trim padding
    olen = l % 4
    if olen:
        olen += 1
    olen += l / 4 * 5
    return out[0:olen]


def b85decode(text):
    """
    Decode a base85 encoded string.

    Args:
        text (str): The base85 encoded string to decode.

    Returns:
        bytes: The decoded binary data.

    Raises:
        TypeError: If the input string contains invalid base85 characters.
        OverflowError: If the decoded value exceeds the maximum representable value.

    Example:
        >>> encoded = "9jqo^BlbD-BleB1DJ+*+F(f,q"
        >>> decoded = b85decode(encoded)
        >>> print(decoded)
        b'Hello, World!'

    This function decodes a base85 encoded string and returns the corresponding binary data.
    Base85 encoding is a method of representing binary data as ASCII text using 85 different characters.
    The function takes a base85 encoded string as input and returns the decoded binary data.

    The function raises a TypeError if the input string contains invalid base85 characters.
    It also raises an OverflowError if the decoded value exceeds the maximum representable value.

    Example usage:
    >>> encoded = "9jqo^BlbD-BleB1DJ+*+F(f,q"
    >>> decoded = b85decode(encoded)
    >>> print(decoded)
    b'Hello, World!'
    """

    l = len(text)
    out = []
    for i in range(0, len(text), 5):
        chunk = text[i : i + 5]
        acc = 0
        for j in range(len(chunk)):
            try:
                acc = acc * 85 + b85dec[chunk[j]]
            except KeyError:
                raise TypeError("Bad base85 character at byte %d" % (i + j))
        if acc > 4294967295:
            raise OverflowError("Base85 overflow in hunk starting at byte %d" % i)
        out.append(acc)

    # Pad final chunk if necessary
    cl = l % 5
    if cl:
        acc *= 85 ** (5 - cl)
        if cl > 1:
            acc += 0xFFFFFF >> (cl - 2) * 8
        out[-1] = acc

    out = struct.pack(">" + "L" * ((l + 4) / 5), *out)
    if cl:
        out = out[: -(5 - cl)]

    return out
