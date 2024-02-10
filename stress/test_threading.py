import random
import threading
import time

from whoosh import fields, query
from whoosh.util.testing import TempStorage


def test_readwrite():
    schema = fields.Schema(id=fields.ID(stored=True), content=fields.TEXT)
    with TempStorage("threading") as st:
        domain = (
            "alfa",
            "bravo",
            "charlie",
            "delta",
            "echo",
            "foxtrot",
            "golf",
            "hotel",
            "india",
            "juliet",
            "kilo",
            "lima",
            "mike",
            "november",
            "oscar",
            "papa",
            "quebec",
            "romeo",
            "sierra",
            "tango",
            "uniform",
            "victor",
            "whiskey",
            "xray",
            "yankee",
            "zulu",
        )

        class WriterThread(threading.Thread):
            def run(self):
                ix = st.create_index(dir, schema)
                num = 0

                for i in range(50):
                    print(i)
                    w = ix.writer()
                    for _ in range(random.randint(1, 100)):
                        content = " ".join(random.sample(domain, random.randint(5, 20)))
                        w.add_document(id=str(num), content=content)
                        num += 1
                    w.commit()

                    time.sleep(0.1)

        class SearcherThread(threading.Thread):
            def run(self):
                print(self.name + " starting")
                for _ in range(10):
                    ix = st.open_index()
                    s = ix.searcher()
                    q = query.Term("content", random.choice(domain))
                    s.search(q, limit=10)
                    s.close()
                    ix.close()
                    time.sleep(0.1)
                print(self.name + " done")

        wt = WriterThread()
        wt.start()
        time.sleep(0.5)
        for _ in range(20):
            SearcherThread().start()
            time.sleep(0.5)
        wt.join()
