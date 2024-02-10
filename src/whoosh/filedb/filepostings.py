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

import types
from array import array
from struct import Struct

from whoosh.matching import Matcher, ReadTooFar
from whoosh.support import unicode
from whoosh.system import _FLOAT_SIZE, _INT_SIZE
from whoosh.util import byte_to_length, length_to_byte, utf8decode, utf8encode
from whoosh.writing import PostingWriter


class BlockInfo:
    """
    Represents information about a block in a file-based posting list.

    Attributes:
        nextoffset (int): The offset of the next block in the file.
        postcount (int): The number of postings in the block.
        maxweight (int): The maximum weight of the postings in the block.
        maxwol (float): The maximum weight of a single posting in the block.
        minlength (int): The minimum length of the terms in the block.
        maxid (int or str): The maximum term ID in the block.
        dataoffset (int): The offset of the block's data in the file.
    """

    __slots__ = (
        "nextoffset",
        "postcount",
        "maxweight",
        "maxwol",
        "minlength",
        "maxid",
        "dataoffset",
    )

    # nextblockoffset, unused, postcount, maxweight, maxwol, unused, minlength
    _struct = Struct("!IiiBfffB")

    def __init__(
        self,
        nextoffset=None,
        postcount=None,
        maxweight=None,
        maxwol=None,
        minlength=None,
        maxid=None,
        dataoffset=None,
    ):
        """
        Initializes a new instance of the BlockInfo class.

        Args:
            nextoffset (int, optional): The offset of the next block in the file.
            postcount (int, optional): The number of postings in the block.
            maxweight (int, optional): The maximum weight of the postings in the block.
            maxwol (float, optional): The maximum weight of a single posting in the block.
            minlength (int, optional): The minimum length of the terms in the block.
            maxid (int or str, optional): The maximum term ID in the block.
            dataoffset (int, optional): The offset of the block's data in the file.
        """
        self.nextoffset = nextoffset
        self.postcount = postcount
        self.maxweight = maxweight
        self.maxwol = maxwol
        self.minlength = minlength
        self.maxid = maxid
        self.dataoffset = dataoffset

    def __repr__(self):
        """
        Returns a string representation of the BlockInfo object.

        Returns:
            str: A string representation of the BlockInfo object.
        """
        return (
            "<%s nextoffset=%r postcount=%r maxweight=%r"
            " maxwol=%r minlength=%r"
            " maxid=%r dataoffset=%r>"
            % (
                self.__class__.__name__,
                self.nextoffset,
                self.postcount,
                self.maxweight,
                self.maxwol,
                self.minlength,
                self.maxid,
                self.dataoffset,
            )
        )

    def to_file(self, file):
        """
        Writes the BlockInfo object to a file.

        Args:
            file (file-like object): The file to write to.
        """
        file.write(
            self._struct.pack(
                self.nextoffset,
                0,
                self.postcount,
                self.maxweight,
                self.maxwol,
                0,
                length_to_byte(self.minlength),
            )
        )

        maxid = self.maxid
        if isinstance(maxid, unicode):
            file.write_string(utf8encode(maxid)[0])
        else:
            file.write_uint(maxid)

    def _read_id(self, file):
        """
        Reads the maximum term ID from a file.

        Args:
            file (file-like object): The file to read from.
        """
        self.maxid = file.read_uint()

    @staticmethod
    def from_file(file, stringids=False):
        """
        Creates a new BlockInfo object from a file.

        Args:
            file (file-like object): The file to read from.
            stringids (bool, optional): Whether the term IDs are stored as strings.

        Returns:
            BlockInfo: A new BlockInfo object.
        """
        (
            nextoffset,
            xi1,
            postcount,
            maxweight,
            maxwol,
            xf1,
            minlength,
        ) = BlockInfo._struct.unpack(file.read(BlockInfo._struct.size))
        assert postcount > 0
        minlength = byte_to_length(minlength)

        if stringids:
            maxid = utf8decode(file.read_string())[0]
        else:
            maxid = file.read_uint()

        dataoffset = file.tell()
        return BlockInfo(
            nextoffset=nextoffset,
            postcount=postcount,
            maxweight=maxweight,
            maxwol=maxwol,
            maxid=maxid,
            minlength=minlength,
            dataoffset=dataoffset,
        )


class FilePostingWriter(PostingWriter):
    """
    A class for writing posting lists to a file-based index.

    Args:
        schema (Schema): The schema of the index.
        postfile (file): The file object to write the posting lists to.
        stringids (bool, optional): Whether the document ids are strings. Defaults to False.
        blocklimit (int, optional): The maximum number of postings to store in a block. Defaults to 128.

    Raises:
        ValueError: If the blocklimit argument is greater than 255 or less than 1.

    Attributes:
        schema (Schema): The schema of the index.
        postfile (file): The file object to write the posting lists to.
        stringids (bool): Whether the document ids are strings.
        blocklimit (int): The maximum number of postings to store in a block.
        inblock (bool): Indicates if currently inside a block.
        fieldnum (int): The field number being written.
        format (Codec): The codec for the field being written.
        blockcount (int): The number of blocks written.
        posttotal (int): The total number of postings written.
        startoffset (int): The offset in the file where the current block starts.
        blockids (list): The list of document ids in the current block.
        blockweights (list): The list of weights in the current block.
        blockvalues (list): The list of values in the current block.
        blockoffset (int): The offset in the file where the current block is written.

    """

    def __init__(self, schema, postfile, stringids=False, blocklimit=128):
        self.schema = schema
        self.postfile = postfile
        self.stringids = stringids

        if blocklimit > 255:
            raise ValueError("blocklimit argument must be <= 255")
        elif blocklimit < 1:
            raise ValueError("blocklimit argument must be > 0")
        self.blocklimit = blocklimit
        self.inblock = False

    def _reset_block(self):
        """
        Resets the current block's data structures.
        """
        if self.stringids:
            self.blockids = []
        else:
            self.blockids = array("I")
        self.blockweights = array("f")
        self.blockvalues = []
        self.blockoffset = self.postfile.tell()

    def start(self, fieldnum):
        """
        Starts a new block for writing postings.

        Args:
            fieldnum (int): The field number being written.

        Returns:
            int: The offset in the file where the block starts.

        Raises:
            ValueError: If called while already inside a block.

        """
        if self.inblock:
            raise ValueError("Cannot call start() while already in a block")

        self.fieldnum = fieldnum
        self.format = self.schema[fieldnum].format
        self.blockcount = 0
        self.posttotal = 0
        self.startoffset = self.postfile.tell()

        # Placeholder for block count
        self.postfile.write_uint(0)

        self._reset_block()
        self.inblock = True

        return self.startoffset

    def write(self, id, valuestring):
        """
        Writes a posting to the current block.

        Args:
            id: The document id.
            valuestring: The value associated with the document.

        """
        self.blockids.append(id)
        self.blockvalues.append(valuestring)
        self.blockweights.append(self.format.decode_weight(valuestring))
        if len(self.blockids) >= self.blocklimit:
            self._write_block()

    def finish(self):
        """
        Finishes writing the current block.

        Returns:
            int: The total number of postings written.

        Raises:
            ValueError: If called when not in a block.

        """
        if not self.inblock:
            raise ValueError("Called finish() when not in a block")

        if self.blockids:
            self._write_block()

        # Seek back to the start of this list of posting blocks and write the
        # number of blocks
        pf = self.postfile
        pf.flush()
        offset = pf.tell()
        pf.seek(self.startoffset)
        pf.write_uint(self.blockcount)
        pf.seek(offset)

        self.inblock = False
        return self.posttotal

    def close(self):
        """
        Closes the posting writer.

        """
        if hasattr(self, "blockids") and self.blockids:
            self.finish()
        self.postfile.close()

    def _write_block(self):
        """
        Writes the current block to the file.

        """
        posting_size = self.format.posting_size
        dfl_fn = self.dfl_fn
        fieldnum = self.fieldnum
        stringids = self.stringids
        pf = self.postfile
        ids = self.blockids
        values = self.blockvalues
        weights = self.blockweights
        postcount = len(ids)

        # Write the blockinfo
        maxid = ids[-1]
        maxweight = max(weights)
        maxwol = 0.0
        minlength = 0
        if dfl_fn and self.schema[fieldnum].scorable:
            lens = [dfl_fn(id, fieldnum) for id in ids]
            minlength = min(lens)
            assert minlength > 0
            maxwol = max(w / l for w, l in zip(weights, lens))

        blockinfo_start = pf.tell()
        blockinfo = BlockInfo(
            nextoffset=0,
            maxweight=maxweight,
            maxwol=maxwol,
            minlength=minlength,
            postcount=postcount,
            maxid=maxid,
        )
        blockinfo.to_file(pf)

        # Write the IDs
        if stringids:
            for id in ids:
                pf.write_string(utf8encode(id)[0])
        else:
            pf.write_array(ids)

        # Write the weights
        pf.write_array(weights)

        # If the size of a posting value in this format is not fixed
        # (represented by a number less than zero), write an array of value
        # lengths
        if posting_size < 0:
            lengths = array("I")
            for valuestring in values:
                lengths.append(len(valuestring))
            pf.write_array(lengths)

        # Write the values
        if posting_size != 0:
            pf.write("".join(values))

        # Seek back and write the pointer to the next block
        pf.flush()
        nextoffset = pf.tell()
        pf.seek(blockinfo_start)
        pf.write_uint(nextoffset)
        pf.seek(nextoffset)

        self.posttotal += postcount
        self._reset_block()
        self.blockcount += 1


class FilePostingReader(Matcher):
    """
    A class for reading posting data from a file-like object.

    This class is responsible for reading posting data from a file-like object and providing
    convenient methods to access the IDs, values, and weights of the postings.

    Args:
        postfile (file-like object): The file-like object representing the posting file.
        offset (int): The offset in the file where the posting data starts.
        format (PostingFormat): The format of the posting data.
        scorefns (tuple, optional): A tuple of score functions (score, quality, block_quality).
            Defaults to None.
        stringids (bool, optional): Indicates whether the IDs are stored as strings.
            Defaults to False.

    Attributes:
        postfile (file-like object): The file-like object representing the posting file.
        startoffset (int): The offset in the file where the posting data starts.
        format (PostingFormat): The format of the posting data.
        _scorefns (tuple): A tuple of score functions (score, quality, block_quality).
        stringids (bool): Indicates whether the IDs are stored as strings.
        blockcount (int): The number of blocks in the posting file.
        baseoffset (int): The offset in the file where the posting data starts.
        _active (bool): Indicates whether the FilePostingReader object is active.
        currentblock (int): The index of the current block being read.
        ids (list): The IDs of the postings in the current block.
        values (list): The values of the postings in the current block.
        weights (list): The weights of the postings in the current block.
        i (int): The index of the current posting within the current block.

    Methods:
        copy(): Creates a copy of the FilePostingReader object.
        is_active(): Checks if the FilePostingReader object is active.
        id(): Returns the ID of the current posting.
        value(): Returns the value of the current posting.
        weight(): Returns the weight of the current posting.
        all_ids(): Generator that yields all the IDs in the posting file.
        next(): Moves to the next posting in the posting file.
        skip_to(id): Skips to the posting with the specified ID.

    """

    def __init__(self, postfile, offset, format, scorefns=None, stringids=False):
        """
        Initializes a FilePostingReader object.

        Args:
            postfile (file-like object): The file-like object representing the posting file.
            offset (int): The offset in the file where the posting data starts.
            format (PostingFormat): The format of the posting data.
            scorefns (tuple, optional): A tuple of score functions (score, quality, block_quality).
                Defaults to None.
            stringids (bool, optional): Indicates whether the IDs are stored as strings.
                Defaults to False.

        Raises:
            None

        Returns:
            None
        """
        self.postfile = postfile
        self.startoffset = offset
        self.format = format
        # Bind the score and quality functions to this object as methods

        self._scorefns = scorefns
        if scorefns:
            sfn, qfn, bqfn = scorefns
            if sfn:
                self.score = types.MethodType(sfn, self, self.__class__)
            if qfn:
                self.quality = types.MethodType(qfn, self, self.__class__)
            if bqfn:
                self.block_quality = types.MethodType(bqfn, self, self.__class__)

        self.stringids = stringids

        self.blockcount = postfile.get_uint(offset)
        self.baseoffset = offset + _INT_SIZE
        self._active = True
        self.currentblock = -1
        self._next_block()

    def copy(self):
        """
        Creates a copy of the FilePostingReader object.

        Args:
            None

        Raises:
            None

        Returns:
            FilePostingReader: A copy of the FilePostingReader object.
        """
        return self.__class__(
            self.postfile,
            self.startoffset,
            self.format,
            scorefns=self._scorefns,
            stringids=self.stringids,
        )

    def is_active(self):
        """
        Checks if the FilePostingReader object is active.

        Args:
            None

        Raises:
            None

        Returns:
            bool: True if the FilePostingReader object is active, False otherwise.
        """
        return self._active

    def id(self):
        """
        Returns the ID of the current posting.

        Args:
            None

        Raises:
            None

        Returns:
            int or str: The ID of the current posting.
        """
        return self.ids[self.i]

    def value(self):
        """
        Returns the value of the current posting.

        Args:
            None

        Raises:
            None

        Returns:
            object: The value of the current posting.
        """
        return self.values[self.i]

    def weight(self):
        """
        Returns the weight of the current posting.

        Args:
            None

        Raises:
            None

        Returns:
            float: The weight of the current posting.
        """
        return self.weights[self.i]

    def all_ids(self):
        """
        Generator that yields all the IDs in the posting file.

        Args:
            None

        Raises:
            None

        Yields:
            int or str: The IDs in the posting file.
        """
        nextoffset = self.baseoffset
        for _ in range(self.blockcount):
            blockinfo = self._read_blockinfo(nextoffset)
            nextoffset = blockinfo.nextoffset
            ids, __ = self._read_ids(blockinfo.dataoffset, blockinfo.postcount)
            yield from ids

    def next(self):
        """
        Moves to the next posting in the posting file.

        Args:
            None

        Raises:
            None

        Returns:
            bool: True if there is a next posting, False otherwise.
        """
        if self.i == self.blockinfo.postcount - 1:
            self._next_block()
            return True
        else:
            self.i += 1
            return False

    def skip_to(self, id):
        """
        Skips to the posting with the specified ID.

        Args:
            id (int or str): The ID to skip to.

        Raises:
            ReadTooFar: If the skip operation goes beyond the end of the posting file.

        Returns:
            None
        """
        if not self.is_active():
            raise ReadTooFar

        i = self.i
        # If we're already in the block with the target ID, do nothing
        if id <= self.ids[i]:
            return

        # Skip to the block that would contain the target ID
        if id > self.blockinfo.maxid:
            self._skip_to_block(lambda: id > self.blockinfo.maxid)
        if not self._active:
            return

        # Iterate through the IDs in the block until we find or pass the
        # target
        ids = self.ids
        i = self.i
        while ids[i] < id:
            i += 1
            if i == len(ids):
                self._active = False
                return
        self.i = i

    def _read_blockinfo(self, offset):
        """
        Reads the block information from the posting file.

        Args:
            offset (int): The offset in the posting file where the block information starts.

        Raises:
            None

        Returns:
            BlockInfo: The block information.
        """
        pf = self.postfile
        pf.seek(offset)
        return BlockInfo.from_file(pf, self.stringids)

    def _read_ids(self, offset, postcount):
        """
        Reads the IDs from the posting file.

        Args:
            offset (int): The offset in the posting file where the IDs start.
            postcount (int): The number of IDs to read.

        Raises:
            None

        Returns:
            tuple: A tuple containing the IDs and the offset after reading.
        """
        pf = self.postfile
        pf.seek(offset)

        if self.stringids:
            rs = pf.read_string
            ids = [utf8decode(rs())[0] for _ in range(postcount)]
        else:
            ids = pf.read_array("I", postcount)

        return (ids, pf.tell())

    def _read_weights(self, offset, postcount):
        """
        Reads the weights from the posting file.

        Args:
            offset (int): The offset in the posting file where the weights start.
            postcount (int): The number of weights to read.

        Raises:
            None

        Returns:
            tuple: A tuple containing the weights and the offset after reading.
        """
        weights = self.postfile.get_array(offset, "f", postcount)
        return (weights, offset + _FLOAT_SIZE * postcount)

    def _read_values(self, startoffset, endoffset, postcount):
        """
        Reads the values from the posting file.

        Args:
            startoffset (int): The offset in the posting file where the values start.
            endoffset (int): The offset in the posting file where the values end.
            postcount (int): The number of values to read.

        Raises:
            None

        Returns:
            list: A list of values.
        """
        pf = self.postfile
        posting_size = self.format.posting_size

        if posting_size != 0:
            valueoffset = startoffset
            if posting_size < 0:
                # Read the array of lengths for the values
                lengths = pf.get_array(startoffset, "I", postcount)
                valueoffset += _INT_SIZE * postcount

            allvalues = pf.map[valueoffset:endoffset]

            # Chop up the block string into individual valuestrings
            if posting_size > 0:
                # Format has a fixed posting size, just chop up the values
                # equally
                values = [
                    allvalues[i * posting_size : i * posting_size + posting_size]
                    for i in range(postcount)
                ]
            else:
                # Format has a variable posting size, use the array of lengths
                # to chop up the values.
                pos = 0
                values = []
                for length in lengths:
                    values.append(allvalues[pos : pos + length])
                    pos += length
        else:
            # Format does not store values (i.e. Existence), just create fake
            # values
            values = (None,) * postcount

        return values

    def _consume_block(self):
        """
        Consumes the current block by reading the IDs, weights, and values.

        Args:
            None

        Raises:
            None

        Returns:
            None
        """
        postcount = self.blockinfo.postcount
        self.ids, woffset = self._read_ids(self.blockinfo.dataoffset, postcount)
        self.weights, voffset = self._read_weights(woffset, postcount)
        self.values = self._read_values(voffset, self.blockinfo.nextoffset, postcount)
        self.i = 0

    def _next_block(self, consume=True):
        """
        Moves to the next block in the posting file.

        Args:
            consume (bool, optional): Indicates whether to consume the block by reading the IDs, weights, and values.
                Defaults to True.

        Raises:
            None

        Returns:
            None
        """
        self.currentblock += 1
        if self.currentblock == self.blockcount:
            self._active = False
            return

        if self.currentblock == 0:
            self.blockinfo = self._read_blockinfo(self.baseoffset)
        else:
            self.blockinfo = self._read_blockinfo(self.blockinfo.nextoffset)

        if consume:
            self._consume_block()
            pos = self.baseoffset
        else:
            pos = self.blockinfo.nextoffset

        self.blockinfo = self._read_blockinfo(pos)
        if consume:
            self._consume_block()

    def _skip_to_block(self, targetfn):
        """
        Skips to the block that satisfies the target function.

        Args:
            targetfn (function): The target function that determines whether to skip to the next block.

        Raises:
            None

        Returns:
            int: The number of blocks skipped.
        """
        skipped = 0
        while self._active and targetfn():
            self._next_block(consume=False)
            skipped += 1

        if self._active:
            self._consume_block()

        return skipped

    def supports_quality(self):
        """
        Checks if the FilePostingReader object supports quality scoring.

        Args:
            None

        Raises:
            None

        Returns:
            bool: True if the FilePostingReader object supports quality scoring, False otherwise.
        """
        return True

    def skip_to_quality(self, minquality):
        """
        Skips to the block with the minimum quality score.

        Args:
            minquality (float): The minimum quality score.

        Raises:
            None

        Returns:
            int: The number of blocks skipped.
        """
        bq = self.block_quality
        if bq() > minquality:
            return 0
        return self._skip_to_block(lambda: bq() <= minquality)

    def quality(self):
        """
        Raises a ValueError indicating that no quality function is given.

        Args:
            None

        Raises:
            ValueError: No quality function given.

        Returns:
            None
        """
        raise ValueError("No quality function given")

    def block_quality(self):
        """
        Raises a ValueError indicating that no block_quality function is given.

        Args:
            None

        Raises:
            ValueError: No block_quality function given.

        Returns:
            None
        """
        raise ValueError("No block_quality function given")

    def score(self):
        """
        Raises a ValueError indicating that no score function is given.

        Args:
            None

        Raises:
            ValueError: No score function given.

        Returns:
            None
        """
        raise ValueError("No score function given")
