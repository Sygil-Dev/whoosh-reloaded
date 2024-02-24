import copy
from datetime import datetime, timedelta, timezone
from itertools import permutations, zip_longest

import pytest
from whoosh import analysis, fields, index, qparser, query, scoring
from whoosh.codec.whoosh3 import W3Codec
from whoosh.filedb.filestore import RamStorage
from whoosh.util.testing import TempIndex


def make_index():
    s = fields.Schema(key=fields.ID(stored=True), name=fields.TEXT, value=fields.TEXT)
    st = RamStorage()
    ix = st.create_index(s)

    w = ix.writer()
    w.add_document(key="A", name="Yellow brown", value="Blue red green render purple?")
    w.add_document(key="B", name="Alpha beta", value="Gamma delta epsilon omega.")
    w.add_document(key="C", name="One two", value="Three rendered four five.")
    w.add_document(key="D", name="Quick went", value="Every red town.")
    w.add_document(
        key="E", name="Yellow uptown", value="Interest rendering outer photo!"
    )
    w.commit()

    return ix


def _get_keys(stored_fields):
    return sorted([d.get("key") for d in stored_fields])


def _docs(q, s):
    return _get_keys([s.stored_fields(docnum) for docnum in q.docs(s)])


def _run_query(q, target):
    ix = make_index()
    with ix.searcher() as s:
        assert target == _docs(q, s)


def test_empty_index():
    schema = fields.Schema(key=fields.ID(stored=True), value=fields.TEXT)
    st = RamStorage()
    with pytest.raises(index.EmptyIndexError):
        st.open_index(schema=schema)


def test_docs_method():
    ix = make_index()
    with ix.searcher() as s:
        assert _get_keys(s.documents(name="yellow")) == ["A", "E"]
        assert _get_keys(s.documents(value="red")) == ["A", "D"]
        assert _get_keys(s.documents()) == ["A", "B", "C", "D", "E"]


def test_term():
    _run_query(query.Term("name", "yellow"), ["A", "E"])
    _run_query(query.Term("value", "zeta"), [])
    _run_query(query.Term("value", "red"), ["A", "D"])


def test_require():
    _run_query(
        query.Require(query.Term("value", "red"), query.Term("name", "yellow")),
        ["A"],
    )


def test_and():
    _run_query(
        query.And([query.Term("value", "red"), query.Term("name", "yellow")]),
        ["A"],
    )
    # Missing
    _run_query(
        query.And([query.Term("value", "ochre"), query.Term("name", "glonk")]), []
    )


def test_or():
    _run_query(
        query.Or([query.Term("value", "red"), query.Term("name", "yellow")]),
        ["A", "D", "E"],
    )
    # Missing
    _run_query(
        query.Or([query.Term("value", "ochre"), query.Term("name", "glonk")]), []
    )
    _run_query(query.Or([]), [])


def test_ors():
    domain = "alfa bravo charlie delta".split()
    s = fields.Schema(num=fields.STORED, text=fields.TEXT)
    st = RamStorage()
    ix = st.create_index(s)
    with ix.writer() as w:
        for i, ls in enumerate(permutations(domain)):
            w.add_document(num=i, text=" ".join(ls))

    with ix.searcher() as s:
        qs = [query.Term("text", word) for word in domain]
        for i in range(1, len(domain)):
            q = query.Or(qs[:i])
            r1 = [(hit.docnum, hit.score) for hit in s.search(q, limit=None)]

            q.binary_matcher = True
            r2 = [(hit.docnum, hit.score) for hit in s.search(q, limit=None)]

            for item1, item2 in zip_longest(r1, r2):
                assert item1[0] == item2[0]
                assert item1[1] == item2[1]


def test_not():
    _run_query(
        query.And(
            [
                query.Or([query.Term("value", "red"), query.Term("name", "yellow")]),
                query.Not(query.Term("name", "quick")),
            ]
        ),
        ["A", "E"],
    )


def test_topnot():
    _run_query(query.Not(query.Term("value", "red")), ["B", "C", "E"])
    _run_query(query.Not(query.Term("name", "yellow")), ["B", "C", "D"])


def test_andnot():
    _run_query(
        query.AndNot(query.Term("name", "yellow"), query.Term("value", "purple")),
        ["E"],
    )


def test_andnot2():
    schema = fields.Schema(a=fields.ID(stored=True))
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(a="bravo")
    w.add_document(a="echo")
    w.add_document(a="juliet")
    w.commit()
    w = ix.writer()
    w.add_document(a="kilo")
    w.add_document(a="foxtrot")
    w.add_document(a="charlie")
    w.commit(merge=False)
    w = ix.writer()
    w.delete_by_term("a", "echo")
    w.add_document(a="alfa")
    w.add_document(a="india")
    w.add_document(a="delta")
    w.commit(merge=False)

    with ix.searcher() as s:
        q = query.TermRange("a", "bravo", "k")
        qr = [hit["a"] for hit in s.search(q)]
        assert " ".join(sorted(qr)) == "bravo charlie delta foxtrot india juliet"

        oq = query.Or([query.Term("a", "bravo"), query.Term("a", "delta")])
        oqr = [hit["a"] for hit in s.search(oq)]
        assert " ".join(sorted(oqr)) == "bravo delta"

        anq = query.AndNot(q, oq)

        m = anq.matcher(s)
        r = s.search(anq)
        assert list(anq.docs(s)) == sorted(hit.docnum for hit in r)
        assert " ".join(sorted(hit["a"] for hit in r)) == "charlie foxtrot india juliet"


def test_variations():
    _run_query(query.Variations("value", "render"), ["A", "C", "E"])


def test_wildcard():
    _run_query(
        query.Or(
            [query.Wildcard("value", "*red*"), query.Wildcard("name", "*yellow*")]
        ),
        ["A", "C", "D", "E"],
    )
    # Missing
    _run_query(query.Wildcard("value", "glonk*"), [])


def test_not2():
    schema = fields.Schema(name=fields.ID(stored=True), value=fields.TEXT)
    storage = RamStorage()
    ix = storage.create_index(schema)
    writer = ix.writer()
    writer.add_document(name="a", value="alfa bravo charlie delta echo")
    writer.add_document(name="b", value="bravo charlie delta echo foxtrot")
    writer.add_document(name="c", value="charlie delta echo foxtrot golf")
    writer.add_document(name="d", value="delta echo golf hotel india")
    writer.add_document(name="e", value="echo golf hotel india juliet")
    writer.commit()

    with ix.searcher() as s:
        p = qparser.QueryParser("value", None)
        results = s.search(p.parse("echo NOT golf"))
        assert sorted([d["name"] for d in results]) == ["a", "b"]

        results = s.search(p.parse("echo NOT bravo"))
        assert sorted([d["name"] for d in results]) == ["c", "d", "e"]

    ix.delete_by_term("value", "bravo")

    with ix.searcher() as s:
        results = s.search(p.parse("echo NOT charlie"))
        assert sorted([d["name"] for d in results]) == ["d", "e"]


#    def test_or_minmatch():
#        schema = fields.Schema(k=fields.STORED, v=fields.TEXT)
#        st = RamStorage()
#        ix = st.create_index(schema)
#
#        w = ix.writer()
#        w.add_document(k=1, v="alfa bravo charlie delta echo")
#        w.add_document(k=2, v="bravo charlie delta echo foxtrot")
#        w.add_document(k=3, v="charlie delta echo foxtrot golf")
#        w.add_document(k=4, v="delta echo foxtrot golf hotel")
#        w.add_document(k=5, v="echo foxtrot golf hotel india")
#        w.add_document(k=6, v="foxtrot golf hotel india juliet")
#        w.commit()
#
#        s = ix.searcher()
#        q = Or([Term("v", "echo"), Term("v", "foxtrot")], minmatch=2)
#        r = s.search(q)
#        assert sorted(d["k"] for d in r), [2, 3, 4, 5])


def test_range():
    schema = fields.Schema(id=fields.ID(stored=True), content=fields.TEXT)
    st = RamStorage()
    ix = st.create_index(schema)

    w = ix.writer()
    w.add_document(id="A", content="alfa bravo charlie delta echo")
    w.add_document(id="B", content="bravo charlie delta echo foxtrot")
    w.add_document(id="C", content="charlie delta echo foxtrot golf")
    w.add_document(id="D", content="delta echo foxtrot golf hotel")
    w.add_document(id="E", content="echo foxtrot golf hotel india")
    w.commit()

    with ix.searcher() as s:
        qp = qparser.QueryParser("content", schema)

        q = qp.parse("charlie [delta TO foxtrot]")
        assert q.__class__ == query.And
        assert q[0].__class__ == query.Term
        assert q[1].__class__ == query.TermRange
        assert q[1].start == "delta"
        assert q[1].end == "foxtrot"
        assert not q[1].startexcl
        assert not q[1].endexcl
        ids = sorted([d["id"] for d in s.search(q)])
        assert ids == ["A", "B", "C"]

        q = qp.parse("foxtrot {echo TO hotel]")
        assert q.__class__ == query.And
        assert q[0].__class__ == query.Term
        assert q[1].__class__ == query.TermRange
        assert q[1].start == "echo"
        assert q[1].end == "hotel"
        assert q[1].startexcl
        assert not q[1].endexcl
        ids = sorted([d["id"] for d in s.search(q)])
        assert ids == ["B", "C", "D", "E"]

        q = qp.parse("{bravo TO delta}")
        assert q.__class__ == query.TermRange
        assert q.start == "bravo"
        assert q.end == "delta"
        assert q.startexcl
        assert q.endexcl
        ids = sorted([d["id"] for d in s.search(q)])
        assert ids == ["A", "B", "C"]

        # Shouldn't match anything
        q = qp.parse("[1 to 10]")
        assert q.__class__ == query.TermRange
        assert len(s.search(q)) == 0


def test_range_clusiveness():
    schema = fields.Schema(id=fields.ID(stored=True))
    st = RamStorage()
    ix = st.create_index(schema)
    w = ix.writer()
    for letter in "abcdefg":
        w.add_document(id=letter)
    w.commit()

    with ix.searcher() as s:

        def check(startexcl, endexcl, string):
            q = query.TermRange("id", "b", "f", startexcl, endexcl)
            r = "".join(sorted(d["id"] for d in s.search(q)))
            assert r == string

        check(False, False, "bcdef")
        check(True, False, "cdef")
        check(True, True, "cde")
        check(False, True, "bcde")


def test_open_ranges():
    schema = fields.Schema(id=fields.ID(stored=True))
    st = RamStorage()
    ix = st.create_index(schema)
    w = ix.writer()
    for letter in "abcdefg":
        w.add_document(id=letter)
    w.commit()

    with ix.searcher() as s:
        qp = qparser.QueryParser("id", schema)

        def check(qstring, result):
            q = qp.parse(qstring)
            r = "".join(sorted([d["id"] for d in s.search(q)]))
            assert r == result

        check("[b TO]", "bcdefg")
        check("[TO e]", "abcde")
        check("[b TO d]", "bcd")
        check("{b TO]", "cdefg")
        check("[TO e}", "abcd")
        check("{b TO d}", "c")


def test_open_numeric_ranges():
    domain = range(0, 1000, 7)

    schema = fields.Schema(num=fields.NUMERIC(stored=True))
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    for i in domain:
        w.add_document(num=i)
    w.commit()

    qp = qparser.QueryParser("num", schema)
    with ix.searcher() as s:
        q = qp.parse("[100 to]")
        r = [hit["num"] for hit in s.search(q, limit=None)]
        assert r == [n for n in domain if n >= 100]

        q = qp.parse("[to 500]")
        r = [hit["num"] for hit in s.search(q, limit=None)]
        assert r == [n for n in domain if n <= 500]


def test_open_date_ranges():
    basedate = datetime(2011, 1, 24, 6, 25, 0, 0, tzinfo=timezone.utc)
    domain = [basedate + timedelta(days=n) for n in range(-20, 20)]

    schema = fields.Schema(date=fields.DATETIME(stored=True))
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    for d in domain:
        w.add_document(date=d)
    w.commit()

    with ix.searcher() as s:
        # Without date parser
        qp = qparser.QueryParser("date", schema)
        q = qp.parse("[2011-01-10 to]")
        r = [hit["date"] for hit in s.search(q, limit=None)]
        assert len(r) > 0
        target = [
            d for d in domain if d >= datetime(2011, 1, 10, 6, 25, tzinfo=timezone.utc)
        ]
        assert r == target

        q = qp.parse("[to 2011-01-30]")
        r = [hit["date"] for hit in s.search(q, limit=None)]
        assert len(r) > 0
        target = [
            d for d in domain if d <= datetime(2011, 1, 30, 6, 25, tzinfo=timezone.utc)
        ]
        assert r == target

        # With date parser
        from whoosh.qparser.dateparse import DateParserPlugin

        qp.add_plugin(DateParserPlugin(basedate))

        q = qp.parse("[10 jan 2011 to]")
        r = [hit["date"] for hit in s.search(q, limit=None)]
        assert len(r) > 0
        target = [
            d for d in domain if d >= datetime(2011, 1, 10, 6, 25, tzinfo=timezone.utc)
        ]
        assert r == target

        q = qp.parse("[to 30 jan 2011]")
        r = [hit["date"] for hit in s.search(q, limit=None)]
        assert len(r) > 0
        target = [
            d for d in domain if d <= datetime(2011, 1, 30, 6, 25, tzinfo=timezone.utc)
        ]
        assert r == target


def test_negated_unlimited_ranges():
    # Whoosh should treat "[to]" as if it was "*"
    schema = fields.Schema(
        id=fields.ID(stored=True), num=fields.NUMERIC, date=fields.DATETIME
    )
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    from string import ascii_letters

    domain = ascii_letters

    dt = datetime.now(tz=timezone.utc)
    for i, letter in enumerate(domain):
        w.add_document(id=letter, num=i, date=dt + timedelta(days=i))
    w.commit()

    with ix.searcher() as s:
        qp = qparser.QueryParser("id", schema)

        nq = qp.parse("NOT [to]")
        assert nq.__class__ == query.Not
        q = nq.query
        assert q.__class__ == query.Every
        assert "".join(h["id"] for h in s.search(q, limit=None)) == domain
        assert not list(nq.docs(s))

        nq = qp.parse("NOT num:[to]")
        assert nq.__class__ == query.Not
        q = nq.query
        assert q.__class__ == query.NumericRange
        assert q.start is None
        assert q.end is None
        assert "".join(h["id"] for h in s.search(q, limit=None)) == domain
        assert not list(nq.docs(s))

        nq = qp.parse("NOT date:[to]")
        assert nq.__class__ == query.Not
        q = nq.query
        assert q.__class__ == query.Every
        assert "".join(h["id"] for h in s.search(q, limit=None)) == domain
        assert not list(nq.docs(s))


def test_keyword_or():
    schema = fields.Schema(a=fields.ID(stored=True), b=fields.KEYWORD)
    st = RamStorage()
    ix = st.create_index(schema)

    w = ix.writer()
    w.add_document(a="First", b="ccc ddd")
    w.add_document(a="Second", b="aaa ddd")
    w.add_document(a="Third", b="ccc eee")
    w.commit()

    qp = qparser.QueryParser("b", schema)
    with ix.searcher() as s:
        qr = qp.parse("b:ccc OR b:eee")
        assert qr.__class__ == query.Or
        r = s.search(qr)
        assert len(r) == 2
        assert r[0]["a"] == "Third"
        assert r[1]["a"] == "First"


def test_merged():
    schema = fields.Schema(id=fields.ID(stored=True), content=fields.TEXT)
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(id="alfa", content="alfa")
            w.add_document(id="bravo", content="bravo")

        with ix.searcher() as s:
            r = s.search(query.Term("content", "bravo"))
            assert len(r) == 1
            assert r[0]["id"] == "bravo"

        with ix.writer() as w:
            w.add_document(id="charlie", content="charlie")
            w.optimize = True

        assert len(ix._segments()) == 1

        with ix.searcher() as s:
            r = s.search(query.Term("content", "bravo"))
            assert len(r) == 1
            assert r[0]["id"] == "bravo"


def test_multireader():
    sc = fields.Schema(id=fields.ID(stored=True), content=fields.TEXT)
    st = RamStorage()
    ix = st.create_index(sc)
    w = ix.writer()
    w.add_document(id="alfa", content="alfa")
    w.add_document(id="bravo", content="bravo")
    w.add_document(id="charlie", content="charlie")
    w.add_document(id="delta", content="delta")
    w.add_document(id="echo", content="echo")
    w.add_document(id="foxtrot", content="foxtrot")
    w.add_document(id="golf", content="golf")
    w.add_document(id="hotel", content="hotel")
    w.add_document(id="india", content="india")
    w.commit()

    with ix.searcher() as s:
        r = s.search(query.Term("content", "bravo"))
        assert len(r) == 1
        assert r[0]["id"] == "bravo"

    w = ix.writer()
    w.add_document(id="juliet", content="juliet")
    w.add_document(id="kilo", content="kilo")
    w.add_document(id="lima", content="lima")
    w.add_document(id="mike", content="mike")
    w.add_document(id="november", content="november")
    w.add_document(id="oscar", content="oscar")
    w.add_document(id="papa", content="papa")
    w.add_document(id="quebec", content="quebec")
    w.add_document(id="romeo", content="romeo")
    w.commit()
    assert len(ix._segments()) == 2

    # r = ix.reader()
    # assert r.__class__.__name__ == "MultiReader"
    # pr = r.postings("content", "bravo")

    with ix.searcher() as s:
        r = s.search(query.Term("content", "bravo"))
        assert len(r) == 1
        assert r[0]["id"] == "bravo"


def test_posting_phrase():
    schema = fields.Schema(name=fields.ID(stored=True), value=fields.TEXT)
    storage = RamStorage()
    ix = storage.create_index(schema)
    writer = ix.writer()
    writer.add_document(name="A", value="Little Miss Muffet sat on a tuffet")
    writer.add_document(name="B", value="Miss Little Muffet tuffet")
    writer.add_document(name="C", value="Miss Little Muffet tuffet sat")
    writer.add_document(
        name="D",
        value="Gibberish blonk falunk miss muffet sat " + "tuffet garbonzo",
    )
    writer.add_document(name="E", value="Blah blah blah pancakes")
    writer.commit()

    with ix.searcher() as s:

        def names(results):
            return sorted([fields["name"] for fields in results])

        q = query.Phrase("value", ["little", "miss", "muffet", "sat", "tuffet"])
        m = q.matcher(s)
        assert m.__class__.__name__ == "SpanNear2Matcher"

        r = s.search(q)
        assert names(r) == ["A"]
        assert len(r) == 1

        q = query.Phrase("value", ["miss", "muffet", "sat", "tuffet"])
        assert names(s.search(q)) == ["A", "D"]

        q = query.Phrase("value", ["falunk", "gibberish"])
        r = s.search(q)
        assert not names(r)
        assert len(r) == 0

        q = query.Phrase("value", ["gibberish", "falunk"], slop=2)
        assert names(s.search(q)) == ["D"]

        q = query.Phrase("value", ["blah"] * 4)
        assert not names(s.search(q))  # blah blah blah blah

        q = query.Phrase("value", ["blah"] * 3)
        m = q.matcher(s)
        assert names(s.search(q)) == ["E"]


def test_phrase_score():
    schema = fields.Schema(name=fields.ID(stored=True), value=fields.TEXT)
    storage = RamStorage()
    ix = storage.create_index(schema)
    writer = ix.writer()
    writer.add_document(name="A", value="Little Miss Muffet sat on a tuffet")
    writer.add_document(
        name="D",
        value="Gibberish blonk falunk miss muffet sat " + "tuffet garbonzo",
    )
    writer.add_document(name="E", value="Blah blah blah pancakes")
    writer.add_document(name="F", value="Little miss muffet little miss muffet")
    writer.commit()

    with ix.searcher() as s:
        q = query.Phrase("value", ["little", "miss", "muffet"])
        m = q.matcher(s)
        assert m.id() == 0
        score1 = m.weight()
        assert score1 > 0
        m.next()
        assert m.id() == 3
        assert m.weight() > score1


def test_stop_phrase():
    schema = fields.Schema(title=fields.TEXT(stored=True))
    storage = RamStorage()
    ix = storage.create_index(schema)
    writer = ix.writer()
    writer.add_document(title="Richard of York")
    writer.add_document(title="Lily the Pink")
    writer.commit()

    with ix.searcher() as s:
        qp = qparser.QueryParser("title", schema)
        q = qp.parse("richard of york")
        assert str(q) == "(title:richard AND title:york)"
        assert len(s.search(q)) == 1
        # q = qp.parse("lily the pink")
        # assert len(s.search(q)), 1)
        assert len(s.find("title", "lily the pink")) == 1


def test_phrase_order():
    tfield = fields.TEXT(stored=True, analyzer=analysis.SimpleAnalyzer())
    schema = fields.Schema(text=tfield)
    storage = RamStorage()
    ix = storage.create_index(schema)

    writer = ix.writer()
    for ls in permutations(["ape", "bay", "can", "day"], 4):
        writer.add_document(text=" ".join(ls))
    writer.commit()

    with ix.searcher() as s:

        def result(q):
            r = s.search(q, limit=None, sortedby=None)
            return sorted([d["text"] for d in r])

        q = query.Phrase("text", ["bay", "can", "day"])
        assert result(q) == ["ape bay can day", "bay can day ape"]


def test_phrase_sameword():
    schema = fields.Schema(id=fields.STORED, text=fields.TEXT)
    storage = RamStorage()
    ix = storage.create_index(schema)

    writer = ix.writer()
    writer.add_document(id=1, text="The film Linda Linda Linda is good")
    writer.add_document(id=2, text="The model Linda Evangelista is pretty")
    writer.commit()

    with ix.searcher() as s:
        r = s.search(query.Phrase("text", ["linda", "linda", "linda"]), limit=None)
        assert len(r) == 1
        assert r[0]["id"] == 1


def test_phrase_multi():
    schema = fields.Schema(id=fields.STORED, text=fields.TEXT)
    ix = RamStorage().create_index(schema)

    domain = "alfa bravo charlie delta echo".split()
    w = None
    for i, ls in enumerate(permutations(domain)):
        if w is None:
            w = ix.writer()
        w.add_document(id=i, text=" ".join(ls))
        if not i % 30:
            w.commit()
            w = None
    if w is not None:
        w.commit()

    with ix.searcher() as s:
        q = query.Phrase("text", ["alfa", "bravo"])
        _ = s.search(q)


def test_missing_field_scoring():
    schema = fields.Schema(
        name=fields.TEXT(stored=True), hobbies=fields.TEXT(stored=True)
    )
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(name="Frank", hobbies="baseball, basketball")

        with ix.reader() as r:
            assert r.field_length("hobbies") == 2
            assert r.field_length("name") == 1

        with ix.writer() as w:
            w.add_document(name="Jonny")

        with ix.searcher() as s:
            assert s.field_length("hobbies") == 2
            assert s.field_length("name") == 2

            parser = qparser.MultifieldParser(["name", "hobbies"], schema)
            q = parser.parse("baseball")
            result = s.search(q)
            assert len(result) == 1


def test_search_fieldname_underscores():
    s = fields.Schema(my_name=fields.ID(stored=True), my_value=fields.TEXT)
    st = RamStorage()
    ix = st.create_index(s)

    w = ix.writer()
    w.add_document(my_name="Green", my_value="It's not easy being green")
    w.add_document(my_name="Red", my_value="Hopping mad like a playground ball")
    w.commit()

    qp = qparser.QueryParser("my_value", schema=s)
    with ix.searcher() as s:
        r = s.search(qp.parse("my_name:Green"))
        assert r[0]["my_name"] == "Green"


def test_short_prefix():
    s = fields.Schema(name=fields.ID, value=fields.TEXT)
    qp = qparser.QueryParser("value", schema=s)
    q = qp.parse("s*")
    assert q.__class__.__name__ == "Prefix"
    assert q.text == "s"


def test_weighting():
    from whoosh.scoring import BaseScorer, Weighting

    schema = fields.Schema(id=fields.ID(stored=True), n_comments=fields.STORED)
    st = RamStorage()
    ix = st.create_index(schema)

    w = ix.writer()
    w.add_document(id="1", n_comments=5)
    w.add_document(id="2", n_comments=12)
    w.add_document(id="3", n_comments=2)
    w.add_document(id="4", n_comments=7)
    w.commit()

    # Fake Weighting implementation
    class CommentWeighting(Weighting):
        def scorer(self, searcher, fieldname, text, qf=1):
            return self.CommentScorer(searcher.stored_fields)

        class CommentScorer(BaseScorer):
            def __init__(self, stored_fields):
                self.stored_fields = stored_fields

            def score(self, matcher):
                sf = self.stored_fields(matcher.id())
                ncomments = sf.get("n_comments", 0)
                return ncomments

    with ix.searcher(weighting=CommentWeighting()) as s:
        q = query.TermRange("id", "1", "4", constantscore=False)

        r = s.search(q)
        ids = [fs["id"] for fs in r]
        assert ids == ["2", "4", "1", "3"]


def test_dismax():
    schema = fields.Schema(
        id=fields.STORED, f1=fields.TEXT, f2=fields.TEXT, f3=fields.TEXT
    )
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(
        id=1,
        f1="alfa bravo charlie delta",
        f2="alfa alfa alfa",
        f3="alfa echo foxtrot hotel india",
    )
    w.commit()

    with ix.searcher(weighting=scoring.Frequency()) as s:
        assert list(s.documents(f1="alfa")) == [{"id": 1}]
        assert list(s.documents(f2="alfa")) == [{"id": 1}]
        assert list(s.documents(f3="alfa")) == [{"id": 1}]

        qs = [
            query.Term("f1", "alfa"),
            query.Term("f2", "alfa"),
            query.Term("f3", "alfa"),
        ]
        dm = query.DisjunctionMax(qs)
        r = s.search(dm)
        assert r.score(0) == 3.0


def test_deleted_wildcard():
    schema = fields.Schema(id=fields.ID(stored=True))
    st = RamStorage()
    ix = st.create_index(schema)

    w = ix.writer()
    w.add_document(id="alfa")
    w.add_document(id="bravo")
    w.add_document(id="charlie")
    w.add_document(id="delta")
    w.add_document(id="echo")
    w.add_document(id="foxtrot")
    w.commit()

    w = ix.writer()
    w.delete_by_term("id", "bravo")
    w.delete_by_term("id", "delta")
    w.delete_by_term("id", "echo")
    w.commit()

    with ix.searcher() as s:
        r = s.search(query.Every("id"))
        assert sorted([d["id"] for d in r]) == ["alfa", "charlie", "foxtrot"]


def test_missing_wildcard():
    schema = fields.Schema(id=fields.ID(stored=True), f1=fields.TEXT, f2=fields.TEXT)
    st = RamStorage()
    ix = st.create_index(schema)

    w = ix.writer()
    w.add_document(id="1", f1="alfa", f2="apple")
    w.add_document(id="2", f1="bravo")
    w.add_document(id="3", f1="charlie", f2="candy")
    w.add_document(id="4", f2="donut")
    w.add_document(id="5")
    w.commit()

    with ix.searcher() as s:
        r = s.search(query.Every("id"))
        assert sorted([d["id"] for d in r]) == ["1", "2", "3", "4", "5"]

        r = s.search(query.Every("f1"))
        assert sorted([d["id"] for d in r]) == ["1", "2", "3"]

        r = s.search(query.Every("f2"))
        assert sorted([d["id"] for d in r]) == ["1", "3", "4"]


def test_finalweighting():
    from whoosh.scoring import Frequency

    schema = fields.Schema(
        id=fields.ID(stored=True), summary=fields.TEXT, n_comments=fields.STORED
    )
    st = RamStorage()
    ix = st.create_index(schema)

    w = ix.writer()
    w.add_document(id="1", summary="alfa bravo", n_comments=5)
    w.add_document(id="2", summary="alfa", n_comments=12)
    w.add_document(id="3", summary="bravo", n_comments=2)
    w.add_document(id="4", summary="bravo bravo", n_comments=7)
    w.commit()

    class CommentWeighting(Frequency):
        use_final = True

        def final(self, searcher, docnum, score):
            ncomments = searcher.stored_fields(docnum).get("n_comments", 0)
            return ncomments

    with ix.searcher(weighting=CommentWeighting()) as s:
        q = qparser.QueryParser("summary", None).parse("alfa OR bravo")
        r = s.search(q)
        ids = [fs["id"] for fs in r]
        assert ["2", "4", "1", "3"] == ids


def test_outofdate():
    schema = fields.Schema(id=fields.ID(stored=True))
    st = RamStorage()
    ix = st.create_index(schema)

    w = ix.writer()
    w.add_document(id="1")
    w.add_document(id="2")
    w.commit()

    s = ix.searcher()
    assert s.up_to_date()

    w = ix.writer()
    w.add_document(id="3")
    w.add_document(id="4")

    assert s.up_to_date()
    w.commit()
    assert not s.up_to_date()

    s = s.refresh()
    assert s.up_to_date()
    s.close()


def test_find_missing():
    schema = fields.Schema(id=fields.ID, text=fields.KEYWORD(stored=True))
    ix = RamStorage().create_index(schema)

    w = ix.writer()
    w.add_document(id="1", text="alfa")
    w.add_document(id="2", text="bravo")
    w.add_document(text="charlie")
    w.add_document(id="4", text="delta")
    w.add_document(text="echo")
    w.add_document(id="6", text="foxtrot")
    w.add_document(text="golf")
    w.commit()

    with ix.searcher() as s:
        qp = qparser.QueryParser("text", schema)
        q = qp.parse("NOT id:*")
        r = s.search(q, limit=None)
        assert [h["text"] for h in r] == ["charlie", "echo", "golf"]


def test_ngram_phrase():
    f = fields.NGRAM(minsize=2, maxsize=2, phrase=True)
    schema = fields.Schema(text=f, path=fields.ID(stored=True))
    ix = RamStorage().create_index(schema)
    writer = ix.writer()
    writer.add_document(
        text=(
            "\u9AD8\u6821\u307E\u3067\u306F\u6771\u4EAC"
            "\u3067\u3001\u5927\u5B66\u304B\u3089\u306F"
            "\u4EAC\u5927\u3067\u3059\u3002"
        ),
        path="sample",
    )
    writer.commit()

    with ix.searcher() as s:
        p = qparser.QueryParser("text", schema)

        q = p.parse("\u6771\u4EAC\u5927\u5B66")
        assert len(s.search(q)) == 1

        q = p.parse('"\u6771\u4EAC\u5927\u5B66"')
        assert len(s.search(q)) == 0

        q = p.parse('"\u306F\u6771\u4EAC\u3067"')
        assert len(s.search(q)) == 1


def test_ordered():
    domain = "alfa bravo charlie delta echo foxtrot".split(" ")

    schema = fields.Schema(f=fields.TEXT(stored=True))
    ix = RamStorage().create_index(schema)
    writer = ix.writer()
    for ls in permutations(domain):
        writer.add_document(f=" ".join(ls))
    writer.commit()

    with ix.searcher() as s:
        q = query.Ordered(
            [
                query.Term("f", "alfa"),
                query.Term("f", "charlie"),
                query.Term("f", "echo"),
            ]
        )
        r = s.search(q)
        for hit in r:
            ls = hit["f"].split()
            assert "alfa" in ls
            assert "charlie" in ls
            assert "echo" in ls
            a = ls.index("alfa")
            c = ls.index("charlie")
            e = ls.index("echo")
            assert a < c and c < e, repr(ls)


def test_otherwise():
    schema = fields.Schema(id=fields.STORED, f=fields.TEXT)
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(id=1, f="alfa one two")
    w.add_document(id=2, f="alfa three four")
    w.add_document(id=3, f="bravo four five")
    w.add_document(id=4, f="bravo six seven")
    w.commit()

    with ix.searcher() as s:
        q = query.Otherwise(query.Term("f", "alfa"), query.Term("f", "six"))
        assert [d["id"] for d in s.search(q)] == [1, 2]

        q = query.Otherwise(query.Term("f", "tango"), query.Term("f", "four"))
        assert [d["id"] for d in s.search(q)] == [2, 3]

        q = query.Otherwise(query.Term("f", "tango"), query.Term("f", "nine"))
        assert [d["id"] for d in s.search(q)] == []


def test_fuzzyterm():
    schema = fields.Schema(id=fields.STORED, f=fields.TEXT)
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(id=1, f="alfa bravo charlie delta")
    w.add_document(id=2, f="bravo charlie delta echo")
    w.add_document(id=3, f="charlie delta echo foxtrot")
    w.add_document(id=4, f="delta echo foxtrot golf")
    w.commit()

    with ix.searcher() as s:
        q = query.FuzzyTerm("f", "brave")
        assert [d["id"] for d in s.search(q)] == [1, 2]


def test_fuzzyterm2():
    schema = fields.Schema(id=fields.STORED, f=fields.TEXT(spelling=True))
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(id=1, f="alfa bravo charlie delta")
    w.add_document(id=2, f="bravo charlie delta echo")
    w.add_document(id=3, f="charlie delta echo foxtrot")
    w.add_document(id=4, f="delta echo foxtrot golf")
    w.commit()

    with ix.searcher() as s:
        assert list(s.reader().terms_within("f", "brave", 1)) == ["bravo"]
        q = query.FuzzyTerm("f", "brave")
        assert [d["id"] for d in s.search(q)] == [1, 2]


def test_multireader_not():
    schema = fields.Schema(id=fields.STORED, f=fields.TEXT)

    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(id=0, f="alfa bravo chralie")
    w.add_document(id=1, f="bravo chralie delta")
    w.add_document(id=2, f="charlie delta echo")
    w.add_document(id=3, f="delta echo foxtrot")
    w.add_document(id=4, f="echo foxtrot golf")
    w.commit()

    with ix.searcher() as s:
        q = query.And([query.Term("f", "delta"), query.Not(query.Term("f", "delta"))])
        r = s.search(q)
        assert len(r) == 0

    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(id=5, f="alfa bravo chralie")
    w.add_document(id=6, f="bravo chralie delta")
    w.commit(merge=False)
    w = ix.writer()
    w.add_document(id=7, f="charlie delta echo")
    w.add_document(id=8, f="delta echo foxtrot")
    w.commit(merge=False)
    w = ix.writer()
    w.add_document(id=9, f="echo foxtrot golf")
    w.add_document(id=10, f="foxtrot golf delta")
    w.commit(merge=False)
    assert len(ix._segments()) > 1

    with ix.searcher() as s:
        q = query.And([query.Term("f", "delta"), query.Not(query.Term("f", "delta"))])
        r = s.search(q)
        assert len(r) == 0


def test_boost_phrase():
    schema = fields.Schema(
        title=fields.TEXT(field_boost=5.0, stored=True), text=fields.TEXT
    )
    ix = RamStorage().create_index(schema)
    domain = "alfa bravo charlie delta".split()
    w = ix.writer()
    for ls in permutations(domain):
        t = " ".join(ls)
        w.add_document(title=t, text=t)
    w.commit()

    q = query.Or(
        [
            query.Term("title", "alfa"),
            query.Term("title", "bravo"),
            query.Phrase("text", ["bravo", "charlie", "delta"]),
        ]
    )

    def boost_phrases(q):
        if isinstance(q, query.Phrase):
            q.boost *= 1000.0
            return q
        else:
            return q.apply(boost_phrases)

    q = boost_phrases(q)

    with ix.searcher() as s:
        r = s.search(q, limit=None)
        for hit in r:
            if "bravo charlie delta" in hit["title"]:
                assert hit.score > 100.0


def test_filter():
    schema = fields.Schema(id=fields.STORED, path=fields.ID, text=fields.TEXT)
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(id=1, path="/a/1", text="alfa bravo charlie")
    w.add_document(id=2, path="/b/1", text="bravo charlie delta")
    w.add_document(id=3, path="/c/1", text="charlie delta echo")
    w.commit(merge=False)
    w = ix.writer()
    w.add_document(id=4, path="/a/2", text="delta echo alfa")
    w.add_document(id=5, path="/b/2", text="echo alfa bravo")
    w.add_document(id=6, path="/c/2", text="alfa bravo charlie")
    w.commit(merge=False)
    w = ix.writer()
    w.add_document(id=7, path="/a/3", text="bravo charlie delta")
    w.add_document(id=8, path="/b/3", text="charlie delta echo")
    w.add_document(id=9, path="/c/3", text="delta echo alfa")
    w.commit(merge=False)

    with ix.searcher() as s:
        fq = query.Or([query.Prefix("path", "/a"), query.Prefix("path", "/b")])
        r = s.search(query.Term("text", "alfa"), filter=fq)
        assert [d["id"] for d in r] == [1, 4, 5]

        r = s.search(query.Term("text", "bravo"), filter=fq)
        assert [d["id"] for d in r] == [
            1,
            2,
            5,
            7,
        ]


def test_fieldboost():
    schema = fields.Schema(id=fields.STORED, a=fields.TEXT, b=fields.TEXT)
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(id=0, a="alfa bravo charlie", b="echo foxtrot india")
    w.add_document(id=1, a="delta bravo charlie", b="alfa alfa alfa")
    w.add_document(id=2, a="alfa alfa alfa", b="echo foxtrot india")
    w.add_document(id=3, a="alfa sierra romeo", b="alfa tango echo")
    w.add_document(id=4, a="bravo charlie delta", b="alfa foxtrot india")
    w.add_document(id=5, a="alfa alfa echo", b="tango tango tango")
    w.add_document(id=6, a="alfa bravo echo", b="alfa alfa tango")
    w.commit()

    def field_booster(fieldname, factor=2.0):
        "Returns a function which will boost the given field in a query tree"

        def booster_fn(obj):
            if obj.is_leaf() and obj.field() == fieldname:
                obj = copy.deepcopy(obj)
                obj.boost *= factor
                return obj
            else:
                return obj

        return booster_fn

    with ix.searcher() as s:
        q = query.Or([query.Term("a", "alfa"), query.Term("b", "alfa")])
        q = q.accept(field_booster("a", 100.0))
        assert str(q) == "(a:alfa^100.0 OR b:alfa)"
        r = s.search(q)
        assert [hit["id"] for hit in r] == [2, 5, 6, 3, 0, 1, 4]


def test_andmaybe_quality():
    schema = fields.Schema(
        id=fields.STORED, title=fields.TEXT(stored=True), year=fields.NUMERIC
    )
    ix = RamStorage().create_index(schema)

    domain = [
        ("Alpha Bravo Charlie Delta", 2000),
        ("Echo Bravo Foxtrot", 2000),
        ("Bravo Golf Hotel", 2002),
        ("Bravo India", 2002),
        ("Juliet Kilo Bravo", 2004),
        ("Lima Bravo Mike", 2004),
    ]
    w = ix.writer()
    for title, year in domain:
        w.add_document(title=title, year=year)
    w.commit()

    with ix.searcher() as s:
        qp = qparser.QueryParser("title", ix.schema)
        q = qp.parse("title:bravo ANDMAYBE year:2004")

        titles = [hit["title"] for hit in s.search(q, limit=None)[:2]]
        assert "Juliet Kilo Bravo" in titles

        titles = [hit["title"] for hit in s.search(q, limit=2)]
        assert "Juliet Kilo Bravo" in titles


def test_collect_limit():
    schema = fields.Schema(id=fields.STORED, text=fields.TEXT)
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(id="a", text="alfa bravo charlie delta echo")
    w.add_document(id="b", text="bravo charlie delta echo foxtrot")
    w.add_document(id="c", text="charlie delta echo foxtrot golf")
    w.add_document(id="d", text="delta echo foxtrot golf hotel")
    w.add_document(id="e", text="echo foxtrot golf hotel india")
    w.commit()

    with ix.searcher() as s:
        r = s.search(query.Term("text", "golf"), limit=10)
        assert len(r) == 3
        count = 0
        for _ in r:
            count += 1
        assert count == 3

    w = ix.writer()
    w.add_document(id="f", text="foxtrot golf hotel india juliet")
    w.add_document(id="g", text="golf hotel india juliet kilo")
    w.add_document(id="h", text="hotel india juliet kilo lima")
    w.add_document(id="i", text="india juliet kilo lima mike")
    w.add_document(id="j", text="juliet kilo lima mike november")
    w.commit(merge=False)

    with ix.searcher() as s:
        r = s.search(query.Term("text", "golf"), limit=20)
        assert len(r) == 5
        count = 0
        for _ in r:
            count += 1
        assert count == 5


def test_scorer():
    schema = fields.Schema(key=fields.TEXT(stored=True))
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(key="alfa alfa alfa")
    w.add_document(key="alfa alfa alfa alfa")
    w.add_document(key="alfa alfa")
    w.commit()
    w = ix.writer()
    w.add_document(key="alfa alfa alfa alfa alfa alfa")
    w.add_document(key="alfa")
    w.add_document(key="alfa alfa alfa alfa alfa")
    w.commit(merge=False)


#    dw = scoring.DebugModel()
#    s = ix.searcher(weighting=dw)
#    r = s.search(query.Term("key", "alfa"))
#    log = dw.log
#    assert log, [('key', 'alfa', 0, 3.0, 3),
#                       ('key', 'alfa', 1, 4.0, 4),
#                       ('key', 'alfa', 2, 2.0, 2),
#                       ('key', 'alfa', 0, 6.0, 6),
#                       ('key', 'alfa', 1, 1.0, 1),
#                       ('key', 'alfa', 2, 5.0, 5)])


def test_pos_scorer():
    ana = analysis.SimpleAnalyzer()
    schema = fields.Schema(id=fields.STORED, key=fields.TEXT(analyzer=ana))
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(id=0, key="0 0 1 0 0 0")
    w.add_document(id=1, key="0 0 0 1 0 0")
    w.add_document(id=2, key="0 1 0 0 0 0")
    w.commit()
    w = ix.writer()
    w.add_document(id=3, key="0 0 0 0 0 1")
    w.add_document(id=4, key="1 0 0 0 0 0")
    w.add_document(id=5, key="0 0 0 0 1 0")
    w.commit(merge=False)

    def pos_score_fn(searcher, fieldname, text, matcher):
        poses = matcher.value_as("positions")
        return 1.0 / (poses[0] + 1)

    pos_weighting = scoring.FunctionWeighting(pos_score_fn)

    s = ix.searcher(weighting=pos_weighting)
    r = s.search(query.Term("key", "1"))
    assert [hit["id"] for hit in r] == [4, 2, 0, 1, 5, 3]


# def test_too_many_prefix_positions():
#     schema = fields.Schema(id=fields.STORED, text=fields.TEXT)
#     ix = RamStorage().create_index(schema)
#     with ix.writer() as w:
#         for i in range(200):
#             text = "a%s" % i
#             w.add_document(id=i, text=text)
#
#     q = query.Prefix("text", "a")
#     q.TOO_MANY_CLAUSES = 100
#
#     with ix.searcher() as s:
#         m = q.matcher(s)
#         assert m.supports("positions")
#         items = list(m.items_as("positions"))
#         assert [(i, [0]) for i in range(200)] == items


def test_collapse():
    from whoosh import collectors

    # id, text, size, tag
    domain = [
        ("a", "blah blah blah", 5, "x"),
        ("b", "blah", 3, "y"),
        ("c", "blah blah blah blah", 2, "z"),
        ("d", "blah blah", 4, "x"),
        ("e", "bloop", 1, "-"),
        ("f", "blah blah blah blah blah", 6, "x"),
        ("g", "blah", 8, "w"),
        ("h", "blah blah", 7, "="),
    ]

    schema = fields.Schema(
        id=fields.STORED,
        text=fields.TEXT,
        size=fields.NUMERIC,
        tag=fields.KEYWORD(sortable=True),
    )
    ix = RamStorage().create_index(schema)
    with ix.writer(codec=W3Codec()) as w:
        for id, text, size, tag in domain:
            w.add_document(id=id, text=text, size=size, tag=tag)

    with ix.searcher() as s:
        q = query.Term("text", "blah")
        r = s.search(q, limit=None)
        assert " ".join(hit["id"] for hit in r) == "f c a d h b g"

        col = s.collector(limit=3)
        col = collectors.CollapseCollector(col, "tag")
        s.search_with_collector(q, col)
        r = col.results()
        assert " ".join(hit["id"] for hit in r) == "f c h"

        col = s.collector(limit=None)
        col = collectors.CollapseCollector(col, "tag")
        s.search_with_collector(q, col)
        r = col.results()
        assert " ".join(hit["id"] for hit in r) == "f c h b g"

        r = s.search(query.Every(), sortedby="size")
        assert " ".join(hit["id"] for hit in r) == "e c b d a f h g"

        col = s.collector(sortedby="size")
        col = collectors.CollapseCollector(col, "tag")
        s.search_with_collector(query.Every(), col)
        r = col.results()
        assert " ".join(hit["id"] for hit in r) == "e c b d h g"


def test_collapse_nocolumn():
    from whoosh import collectors

    # id, text, size, tag
    domain = [
        ("a", "blah blah blah", 5, "x"),
        ("b", "blah", 3, "y"),
        ("c", "blah blah blah blah", 2, "z"),
        ("d", "blah blah", 4, "x"),
        ("e", "bloop", 1, "-"),
        ("f", "blah blah blah blah blah", 6, "x"),
        ("g", "blah", 8, "w"),
        ("h", "blah blah", 7, "="),
    ]

    schema = fields.Schema(
        id=fields.STORED, text=fields.TEXT, size=fields.NUMERIC, tag=fields.KEYWORD
    )
    ix = RamStorage().create_index(schema)
    with ix.writer() as w:
        for id, text, size, tag in domain:
            w.add_document(id=id, text=text, size=size, tag=tag)

    with ix.searcher() as s:
        q = query.Term("text", "blah")
        r = s.search(q, limit=None)
        assert " ".join(hit["id"] for hit in r) == "f c a d h b g"

        col = s.collector(limit=3)
        col = collectors.CollapseCollector(col, "tag")
        s.search_with_collector(q, col)
        r = col.results()
        assert " ".join(hit["id"] for hit in r) == "f c h"

        col = s.collector(limit=None)
        col = collectors.CollapseCollector(col, "tag")
        s.search_with_collector(q, col)
        r = col.results()
        assert " ".join(hit["id"] for hit in r) == "f c h b g"

        r = s.search(query.Every(), sortedby="size")
        assert " ".join(hit["id"] for hit in r) == "e c b d a f h g"

        col = s.collector(sortedby="size")
        col = collectors.CollapseCollector(col, "tag")
        s.search_with_collector(query.Every(), col)
        r = col.results()
        assert " ".join(hit["id"] for hit in r) == "e c b d h g"


def test_collapse_length():
    domain = (
        "alfa apple agnostic aplomb arc "
        "bravo big braid beer "
        "charlie crouch car "
        "delta dog "
        "echo "
        "foxtrot fold flip "
        "golf gym goop"
    ).split()

    schema = fields.Schema(key=fields.ID(sortable=True), word=fields.ID(stored=True))
    ix = RamStorage().create_index(schema)
    with ix.writer(codec=W3Codec()) as w:
        for word in domain:
            w.add_document(key=word[0], word=word)

    with ix.searcher() as s:
        q = query.Every()

        def check(r):
            words = " ".join(hit["word"] for hit in r)
            assert words == "alfa bravo charlie delta echo foxtrot golf"
            assert r.scored_length() == 7
            assert len(r) == 7

        r = s.search(q, collapse="key", collapse_limit=1, limit=None)
        check(r)

        r = s.search(q, collapse="key", collapse_limit=1, limit=50)
        check(r)

        r = s.search(q, collapse="key", collapse_limit=1, limit=10)
        check(r)


def test_collapse_length_nocolumn():
    domain = (
        "alfa apple agnostic aplomb arc "
        "bravo big braid beer "
        "charlie crouch car "
        "delta dog "
        "echo "
        "foxtrot fold flip "
        "golf gym goop"
    ).split()

    schema = fields.Schema(key=fields.ID(), word=fields.ID(stored=True))
    ix = RamStorage().create_index(schema)
    with ix.writer() as w:
        for word in domain:
            w.add_document(key=word[0], word=word)

    with ix.searcher() as s:
        q = query.Every()

        def check(r):
            words = " ".join(hit["word"] for hit in r)
            assert words == "alfa bravo charlie delta echo foxtrot golf"
            assert r.scored_length() == 7
            assert len(r) == 7

        r = s.search(q, collapse="key", collapse_limit=1, limit=None)
        check(r)

        r = s.search(q, collapse="key", collapse_limit=1, limit=50)
        check(r)

        r = s.search(q, collapse="key", collapse_limit=1, limit=10)
        check(r)


def test_collapse_order():
    from whoosh import sorting

    schema = fields.Schema(
        id=fields.STORED,
        price=fields.NUMERIC(sortable=True),
        rating=fields.NUMERIC(sortable=True),
        tag=fields.ID(sortable=True),
    )
    ix = RamStorage().create_index(schema)
    with ix.writer(codec=W3Codec()) as w:
        w.add_document(id="a", price=10, rating=1, tag="x")
        w.add_document(id="b", price=80, rating=3, tag="y")
        w.add_document(id="c", price=60, rating=1, tag="z")
        w.add_document(id="d", price=30, rating=2)
        w.add_document(id="e", price=50, rating=3, tag="x")
        w.add_document(id="f", price=20, rating=1, tag="y")
        w.add_document(id="g", price=50, rating=2, tag="z")
        w.add_document(id="h", price=90, rating=5)
        w.add_document(id="i", price=50, rating=5, tag="x")
        w.add_document(id="j", price=40, rating=1, tag="y")
        w.add_document(id="k", price=50, rating=4, tag="z")
        w.add_document(id="l", price=70, rating=2)

    with ix.searcher() as s:

        def check(kwargs, target):
            r = s.search(query.Every(), limit=None, **kwargs)
            assert " ".join(hit["id"] for hit in r) == target

        price = sorting.FieldFacet("price", reverse=True)
        rating = sorting.FieldFacet("rating", reverse=True)
        tag = sorting.FieldFacet("tag")

        check({"sortedby": price}, "h b l c e g i k j d f a")
        check({"sortedby": price, "collapse": tag}, "h b l c e d")
        check(
            {"sortedby": price, "collapse": tag, "collapse_order": rating},
            "h b l i k d",
        )


def test_collapse_order_nocolumn():
    from whoosh import sorting

    schema = fields.Schema(
        id=fields.STORED,
        price=fields.NUMERIC(),
        rating=fields.NUMERIC(),
        tag=fields.ID(),
    )
    ix = RamStorage().create_index(schema)
    with ix.writer() as w:
        w.add_document(id="a", price=10, rating=1, tag="x")
        w.add_document(id="b", price=80, rating=3, tag="y")
        w.add_document(id="c", price=60, rating=1, tag="z")
        w.add_document(id="d", price=30, rating=2)
        w.add_document(id="e", price=50, rating=3, tag="x")
        w.add_document(id="f", price=20, rating=1, tag="y")
        w.add_document(id="g", price=50, rating=2, tag="z")
        w.add_document(id="h", price=90, rating=5)
        w.add_document(id="i", price=50, rating=5, tag="x")
        w.add_document(id="j", price=40, rating=1, tag="y")
        w.add_document(id="k", price=50, rating=4, tag="z")
        w.add_document(id="l", price=70, rating=2)

    with ix.searcher() as s:

        def check(kwargs, target):
            r = s.search(query.Every(), limit=None, **kwargs)
            assert " ".join(hit["id"] for hit in r) == target

        price = sorting.FieldFacet("price", reverse=True)
        rating = sorting.FieldFacet("rating", reverse=True)
        tag = sorting.FieldFacet("tag")

        check({"sortedby": price}, "h b l c e g i k j d f a")
        check({"sortedby": price, "collapse": tag}, "h b l c e d")
        check(
            {"sortedby": price, "collapse": tag, "collapse_order": rating},
            "h b l i k d",
        )


def test_coord():
    from whoosh.matching import CoordMatcher

    schema = fields.Schema(id=fields.STORED, hits=fields.STORED, tags=fields.KEYWORD)
    ix = RamStorage().create_index(schema)
    with ix.writer() as w:
        w.add_document(id=0, hits=0, tags="blah blah blah blah")
        w.add_document(id=1, hits=0, tags="echo echo blah blah")
        w.add_document(id=2, hits=1, tags="bravo charlie delta echo")
        w.add_document(id=3, hits=2, tags="charlie delta echo foxtrot")
        w.add_document(id=4, hits=3, tags="delta echo foxtrot golf")
        w.add_document(id=5, hits=3, tags="echo foxtrot golf hotel")
        w.add_document(id=6, hits=2, tags="foxtrot golf hotel india")
        w.add_document(id=7, hits=1, tags="golf hotel india juliet")
        w.add_document(id=8, hits=0, tags="foxtrot foxtrot foo foo")
        w.add_document(id=9, hits=0, tags="foo foo foo foo")

    og = qparser.OrGroup.factory(0.99)
    qp = qparser.QueryParser("tags", schema, group=og)
    q = qp.parse("golf foxtrot echo")
    assert q.__class__ == query.Or
    assert q.scale == 0.99

    with ix.searcher() as s:
        m = q.matcher(s)
        assert type(m) == CoordMatcher

        r = s.search(q, optimize=False)
        assert [hit["id"] for hit in r] == [4, 5, 3, 6, 1, 8, 2, 7]


def test_keyword_search():
    schema = fields.Schema(tags=fields.KEYWORD)
    ix = RamStorage().create_index(schema)
    with ix.writer() as w:
        w.add_document(tags="keyword1 keyword2 keyword3 keyword4 keyword5")

    with ix.searcher() as s:
        r = s.search_page(query.Term("tags", "keyword3"), 1)
        assert r


def test_groupedby_with_terms():
    schema = fields.Schema(content=fields.TEXT, organism=fields.ID)
    ix = RamStorage().create_index(schema)

    with ix.writer() as w:
        w.add_document(
            organism="mus",
            content="IPFSTD1 IPFSTD_kdwq134 Kaminski-all Study00:00:00",
        )
        w.add_document(
            organism="mus", content="IPFSTD1 IPFSTD_kdwq134 Kaminski-all Study"
        )
        w.add_document(organism="hs", content="This is the first document we've added!")

    with ix.searcher() as s:
        q = qparser.QueryParser("content", schema=ix.schema).parse("IPFSTD1")
        r = s.search(q, groupedby=["organism"], terms=True)
        assert len(r) == 2
        assert r.groups("organism") == {"mus": [1, 0]}
        assert r.has_matched_terms()
        assert r.matched_terms() == {("content", b"ipfstd1")}


def test_buffered_refresh():
    from whoosh import writing

    schema = fields.Schema(foo=fields.ID())
    ix = RamStorage().create_index(schema)

    with writing.BufferedWriter(ix, period=1000) as writer:
        writer.add_document(foo="1")
        writer.add_document(foo="2")

        with writer.searcher() as searcher:
            assert searcher.doc_count() == 2
            assert not searcher.up_to_date()
            searcher = searcher.refresh()
            assert searcher.doc_count() == 2


def test_score_length():
    schema = fields.Schema(a=fields.TEXT, b=fields.TEXT)
    ix = RamStorage().create_index(schema)
    with ix.writer() as w:
        w.add_document(a="alfa bravo charlie")
        w.add_document(b="delta echo foxtrot")
        w.add_document(a="golf hotel india")

    with ix.writer() as w:
        w.merge = False
        w.add_document(b="juliet kilo lima")
        # In the second segment, there is an "a" field here, but in the
        # corresponding document in the first segment, the field doesn't exist,
        # so if the scorer is getting segment offsets wrong, scoring this
        # document will error
        w.add_document(a="mike november oskar")
        w.add_document(b="papa quebec romeo")

    with ix.searcher() as s:
        assert not s.is_atomic()
        p = s.postings("a", "mike")
        while p.is_active():
            docnum = p.id()
            score = p.score()
            p.next()


def test_terms_with_filter():
    schema = fields.Schema(text=fields.TEXT)
    ix = RamStorage().create_index(schema)
    with ix.writer() as w:
        w.add_document(text="alfa bravo charlie delta")
        w.add_document(text="bravo charlie delta echo")
        w.add_document(text="charlie delta echo foxtrot")
        w.add_document(text="delta echo foxtrot golf")
        w.add_document(text="echo foxtrot golf hotel")
        w.add_document(text="foxtrot golf hotel alfa")
        w.add_document(text="golf hotel alfa bravo")
        w.add_document(text="hotel alfa bravo charlie")

    with ix.searcher() as s:
        workingset = {1, 2, 3}
        q = query.Term("text", "foxtrot")
        r = s.search_page(q, pagenum=1, pagelen=5, terms=True, filter=workingset)

        assert r.scored_length() == 2
        assert [hit.docnum for hit in r] == [2, 3]


def test_terms_to_bytes():
    schema = fields.Schema(a=fields.TEXT, b=fields.NUMERIC, id=fields.STORED)
    ix = RamStorage().create_index(schema)
    with ix.writer() as w:
        w.add_document(id=0, a="alfa bravo", b=100)
        w.add_document(id=1, a="bravo charlie", b=200)
        w.add_document(id=2, a="charlie delta", b=100)
        w.add_document(id=3, a="delta echo", b=200)

    with ix.searcher() as s:
        t1 = query.Term("b", 200)
        t2 = query.Term("a", "bravo")
        q = query.And([t1, t2])
        r = s.search(q)
        assert [hit["id"] for hit in r] == [1]


def test_issue_334():
    schema = fields.Schema(
        kind=fields.ID(stored=True),
        name=fields.ID(stored=True),
        returns=fields.ID(stored=True),
    )
    ix = RamStorage().create_index(schema)

    with ix.writer() as w:
        with w.group():
            w.add_document(kind="class", name="Index")
            w.add_document(kind="method", name="add document", returns="void")
            w.add_document(kind="method", name="add reader", returns="void")
            w.add_document(kind="method", name="close", returns="void")
        with w.group():
            w.add_document(kind="class", name="Accumulator")
            w.add_document(kind="method", name="add", returns="void")
            w.add_document(kind="method", name="get result", returns="number")
        with w.group():
            w.add_document(kind="class", name="Calculator")
            w.add_document(kind="method", name="add", returns="number")
            w.add_document(kind="method", name="add all", returns="number")
            w.add_document(kind="method", name="add some", returns="number")
            w.add_document(kind="method", name="multiply", returns="number")
            w.add_document(kind="method", name="close", returns="void")
        with w.group():
            w.add_document(kind="class", name="Deleter")
            w.add_document(kind="method", name="add", returns="void")
            w.add_document(kind="method", name="delete", returns="void")

    with ix.searcher() as s:
        pq = query.Term("kind", "class")
        cq = query.Term("name", "Calculator")

        q = query.NestedChildren(pq, cq) & query.Term("returns", "void")
        r = s.search(q)
        assert len(r) == 1
        assert r[0]["name"] == "close"


def test_find_decimals():
    from decimal import Decimal

    schema = fields.Schema(
        name=fields.KEYWORD(stored=True), num=fields.NUMERIC(Decimal, decimal_places=5)
    )
    ix = RamStorage().create_index(schema)

    with ix.writer() as w:
        w.add_document(name="alfa", num=Decimal("1.5"))
        w.add_document(name="bravo", num=Decimal("2.1"))
        w.add_document(name="charlie", num=Decimal("5.3"))
        w.add_document(name="delta", num=Decimal(3))
        w.add_document(name="echo", num=Decimal("3.00001"))
        w.add_document(name="foxtrot", num=Decimal("3"))

    qp = qparser.QueryParser("name", ix.schema)
    q = qp.parse("num:3.0")
    assert isinstance(q, query.Term)

    with ix.searcher() as s:
        r = s.search(q)
        names = " ".join(sorted(hit["name"] for hit in r))
        assert names == "delta foxtrot"


def test_limit_scores():
    domain = "alfa bravo charlie delta echo foxtrot golf".split()

    schema = fields.Schema(desc=fields.TEXT, parent=fields.KEYWORD(stored=True))
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            count = 0
            for words in permutations(domain, 4):
                count += 1
                w.add_document(desc=" ".join(words), parent=str(count))

        with ix.searcher() as s:
            q = query.And(
                [query.Term("desc", "delta", boost=30.0), query.Term("parent", "545")]
            )
            r = s.search(q, limit=500)
            assert r.scored_length() == 1
            limited_score = r[0].score

            r = s.search(q, limit=None)
            assert r.scored_length() == 1
            unlimited_score = r[0].score

            assert limited_score == unlimited_score


def test_function_weighting():
    def pos_score_fn(searcher, fieldname, text, matcher):
        spans = matcher.spans()
        return 1.0 / (spans[0].start + 1)

    pos_weighting = scoring.FunctionWeighting(pos_score_fn)

    schema = fields.Schema(id=fields.STORED, text=fields.TEXT)

    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(id="a", text="aa bb")
            w.add_document(id="b", text="bb aa bb")
            w.add_document(id="c", text="bb bb aa bb")
            w.add_document(id="d", text="bb bb bb aa bb")
            w.add_document(id="e", text="bb bb bb bb aa bb")
            w.add_document(id="f", text="bb bb bb bb bb aa bb")

        with ix.writer() as w:
            w.add_document(id="g", text="aa bb")
            w.add_document(id="h", text="bb aa bb")
            w.add_document(id="i", text="bb bb aa bb")
            w.add_document(id="j", text="bb bb bb aa bb")
            w.add_document(id="k", text="bb bb bb bb aa bb")
            w.add_document(id="l", text="bb bb bb bb bb aa bb")

        with ix.writer() as w:
            w.add_document(id="m", text="aa bb")
            w.add_document(id="n", text="bb aa bb")
            w.add_document(id="o", text="bb bb aa bb")
            w.add_document(id="p", text="bb bb bb aa bb")
            w.add_document(id="q", text="bb bb bb bb aa bb")
            w.add_document(id="r", text="bb bb bb bb bb aa bb")

        with ix.writer() as w:
            w.add_document(id="s", text="aa bb")
            w.add_document(id="t", text="bb aa bb")
            w.add_document(id="u", text="bb bb aa bb")
            w.add_document(id="v", text="bb bb bb aa bb")
            w.add_document(id="w", text="bb bb bb bb aa bb")
            w.add_document(id="x", text="bb bb bb bb bb aa bb")

        with ix.searcher(weighting=pos_weighting) as s:
            q = query.Term("text", "aa")
            m = q.matcher(s, s.context())
            assert not m.supports_block_quality()

            r = s.search(q, limit=5)
            ids = "".join([hit["id"] for hit in r])
            assert ids == "agmsb"

            q = query.Or(
                [query.Term("text", "aa"), query.Term("text", "bb")], scale=2.0
            )
            m = q.matcher(s, s.context())
            assert not m.supports_block_quality()
