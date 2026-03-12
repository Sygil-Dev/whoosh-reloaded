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

"""Probabilistic skip list data structure for efficient document ID lookups.

A skip list is a layered linked list that allows O(log n) search by
maintaining express lanes of forward pointers at multiple levels. This
implementation is used by :class:`~whoosh.matching.mcore.SkipListMatcher` to
accelerate ``skip_to`` operations over sorted posting lists.
"""

import random

_rng = random.Random(42)


class SkipNode:
    """A single node in the skip list.

    Each node stores a document ID and an array of forward pointers, one per
    level the node participates in.
    """

    __slots__ = ("doc_id", "forward")

    def __init__(self, doc_id, level):
        self.doc_id = doc_id
        self.forward = [None] * (level + 1)


class SkipList:
    """Probabilistic skip list over a sorted sequence of document IDs.

    :param ids: A sorted iterable of integer document IDs.
    :param max_level: The maximum number of express-lane levels.
    :param p: The probability that a node is promoted to the next level.
    """

    def __init__(self, ids, max_level=16, p=0.5):
        self.max_level = max_level
        self.p = p
        self.header = SkipNode(-1, max_level)
        self.level = 0
        self.size = len(ids)
        self._build(ids)

    def _random_level(self):
        """Return a random level for a new node."""

        lvl = 0
        while _rng.random() < self.p and lvl < self.max_level:
            lvl += 1
        return lvl

    def _build(self, ids):
        """Bulk-insert all *ids* into the skip list in a single pass."""

        update = [self.header] * (self.max_level + 1)
        for doc_id in ids:
            lvl = self._random_level()
            if lvl > self.level:
                self.level = lvl

            node = SkipNode(doc_id, lvl)
            for i in range(lvl + 1):
                node.forward[i] = update[i].forward[i]
                update[i].forward[i] = node
                update[i] = node

    def skip_to(self, target_id):
        """Return the first node whose ``doc_id`` is >= *target_id*, or
        ``None`` if no such node exists.
        """

        current = self.header
        for i in range(self.level, -1, -1):
            while (
                current.forward[i] is not None
                and current.forward[i].doc_id < target_id
            ):
                current = current.forward[i]

        current = current.forward[0]
        return current

    def __len__(self):
        return self.size

    def __iter__(self):
        node = self.header.forward[0]
        while node is not None:
            yield node.doc_id
            node = node.forward[0]

    def __contains__(self, doc_id):
        node = self.skip_to(doc_id)
        return node is not None and node.doc_id == doc_id
