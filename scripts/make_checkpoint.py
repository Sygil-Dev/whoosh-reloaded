#!python

"""
This script creates a "checkpoint" index using the Whoosh library. The checkpoint index captures the index format created by a certain version of Whoosh.

Usage: make_checkpoint.py <dir>

Parameters:
    <dir> (str): The directory where the checkpoint index will be created.

The script generates a checkpoint index with the following fields:
- path: A unique identifier for each document.
- num: An integer field stored in the index.
- frac: A float field stored in the index.
- dt: A datetime field stored in the index.
- tag: A keyword field.
- title: A text field stored in the index.
- ngrams: A field for generating n-grams from the title.

The script creates a directory if it doesn't exist and initializes the index with the specified schema. It then adds documents to the index with randomly generated data. The number of documents and the data for each document are determined by the loop iterations.

Finally, the script deletes specific documents from the index and prints the total number of documents in the index.

Note: The Whoosh library must be installed in order to run this script.
"""

import os.path
import random
import sys
from datetime import datetime, timezone

from whoosh import fields, index

if len(sys.argv) < 2:
    print("USAGE: make_checkpoint.py <dir>")
    sys.exit(1)
indexdir = sys.argv[1]
print("Creating checkpoint index in", indexdir)

schema = fields.Schema(
    path=fields.ID(stored=True, unique=True),
    num=fields.NUMERIC(int, stored=True),
    frac=fields.NUMERIC(float, stored=True),
    dt=fields.DATETIME(stored=True),
    tag=fields.KEYWORD,
    title=fields.TEXT(stored=True),
    ngrams=fields.NGRAMWORDS,
)

words = (
    "alfa bravo charlie delta echo foxtrot golf hotel india"
    "juliet kilo lima mike november oskar papa quebec romeo"
    "sierra tango"
).split()

if not os.path.exists(indexdir):
    os.makedirs(indexdir)

ix = index.create_in(indexdir, schema)
counter = 0
frac = 0.0
for segnum in range(3):
    with ix.writer() as w:
        for num in range(100):
            frac += 0.15
            path = f"{segnum}/{num}"
            title = " ".join(random.choice(words) for _ in range(100))
            dt = datetime(
                year=2000 + counter,
                month=(counter % 12) + 1,
                day=15,
                tzinfo=timezone.utc,
            )

            w.add_document(
                path=path,
                num=counter,
                frac=frac,
                dt=dt,
                tag=words[counter % len(words)],
                title=title,
                ngrams=title,
            )
            counter += 1

with ix.writer() as w:
    for path in ("0/42", "1/6", "2/80"):
        print("Deleted", path, w.delete_by_term("path", path))

print(counter, ix.doc_count())
