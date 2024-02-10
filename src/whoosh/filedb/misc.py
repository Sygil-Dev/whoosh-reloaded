# ===============================================================================
# Copyright 2010 Matt Chaput
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

from marshal import dumps as mdumps
from marshal import loads as mloads
from pickle import dumps, loads
from struct import Struct

from whoosh.system import (
    _SHORT_SIZE,
    pack_uint,
    pack_ushort,
    unpack_uint,
    unpack_ushort,
)
from whoosh.util import utf8decode, utf8encode


def encode_termkey(term):
    """
    Encodes a term key.

    This function takes a term tuple consisting of a field number and text, and encodes it into a byte string.
    The field number is packed as an unsigned short, followed by the UTF-8 encoded text.

    Parameters:
    term (tuple): A tuple containing the field number and text.

    Returns:
    bytes: The encoded term key as a byte string.

    Example:
    >>> term = (1, "example")
    >>> encode_termkey(term)
    b'\x00\x01example'
    """
    fieldnum, text = term
    return pack_ushort(fieldnum) + utf8encode(text)[0]


def decode_termkey(key):
    """
    Decode a term key.

    Args:
        key (bytes): The term key to decode.

    Returns:
        tuple: A tuple containing the decoded term key. The first element is an
        unsigned short integer, and the second element is a Unicode string.

    Raises:
        IndexError: If the key is too short to be decoded.

    Example:
        >>> key = b'\x00\x01hello'
        >>> decode_termkey(key)
        (1, 'hello')
    """
    return (unpack_ushort(key[:_SHORT_SIZE])[0], utf8decode(key[_SHORT_SIZE:])[0])


_terminfo_struct = Struct("!III")  # frequency, offset, postcount
_pack_terminfo = _terminfo_struct.pack
encode_terminfo = lambda cf_offset_df: _pack_terminfo(*cf_offset_df)
decode_terminfo = _terminfo_struct.unpack

encode_docnum = pack_uint
decode_docnum = lambda x: unpack_uint(x)[0]

enpickle = lambda data: dumps(data, -1)
depickle = loads

enmarshal = mdumps
demarshal = mloads
