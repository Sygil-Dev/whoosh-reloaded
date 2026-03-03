import random

_rng = random.Random(42)


class SkipNode:
    __slots__ = ("doc_id", "forward")

    def __init__(self, doc_id, level):
        self.doc_id = doc_id
        self.forward = [None] * (level + 1)

class SkipList:
    def __init__(self, ids, max_level=16, p = 0.5):
        self.max_level = max_level
        self.p = p
        self.header = SkipNode(-1, max_level)
        self.level = 0
        self.size = len(ids)
        self._build(ids)

    def _random_level(self):
        level = 0
        while _rng.random() < self.p and level < self.max_level:
            level += 1
        return level
    
    def _build(self, ids):
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
