import copy

import pytest
from whoosh import fields, qparser, query
from whoosh.filedb.filestore import RamStorage
from whoosh.qparser import QueryParser
from whoosh.query import (
    And,
    AndMaybe,
    ConstantScoreQuery,
    DateRange,
    DisjunctionMax,
    Every,
    FuzzyTerm,
    Not,
    NullQuery,
    NumericRange,
    Or,
    Phrase,
    Prefix,
    Require,
    Term,
    TermRange,
    Variations,
    Wildcard,
)
from whoosh.query.spans import SpanContains, SpanFirst, SpanNear, SpanNot, SpanOr
from whoosh.util.testing import TempIndex


def u(s):
    return s.decode("ascii") if isinstance(s, bytes) else s


def test_all_terms():
    q = QueryParser("a", None).parse('hello b:there c:"my friend"')
    ts = q.all_terms(phrases=False)
    assert sorted(ts) == [("a", "hello"), ("b", "there")]
    ts = q.all_terms(phrases=True)
    assert sorted(ts) == [("a", "hello"), ("b", "there"), ("c", "friend"), ("c", "my")]


def test_existing_terms():
    s = fields.Schema(key=fields.ID, value=fields.TEXT)
    ix = RamStorage().create_index(s)

    w = ix.writer()
    w.add_document(key="a", value="alfa bravo charlie delta echo")
    w.add_document(key="b", value="foxtrot golf hotel india juliet")
    w.commit()

    r = ix.reader()
    q = QueryParser("value", None).parse('alfa hotel tango "sierra bravo"')

    ts = q.existing_terms(r, phrases=False)
    assert sorted(ts) == [("value", b"alfa"), ("value", b"hotel")]

    ts = q.existing_terms(r)
    assert sorted(ts) == [
        ("value", b"alfa"),
        ("value", b"bravo"),
        ("value", b"hotel"),
    ]


def test_wildcard_existing_terms():
    s = fields.Schema(key=fields.ID, value=fields.TEXT)
    ix = RamStorage().create_index(s)

    w = ix.writer()
    w.add_document(key="a", value="alfa bravo bear charlie delta")
    w.add_document(key="a", value="boggle echo render rendering renders")
    w.commit()
    r = ix.reader()
    qp = QueryParser("value", ix.schema)

    def words(terms):
        z = []
        for t in terms:
            assert t[0] == "value"
            z.append(t[1])
        return b" ".join(sorted(z))

    q = qp.parse("b*")
    ts = q.existing_terms(r)
    assert ts == set()
    ts = q.existing_terms(r, expand=True)
    assert words(ts) == b"bear boggle bravo"

    q = qp.parse("[a TO f]")
    ts = q.existing_terms(r)
    assert ts == set()
    ts = q.existing_terms(r, expand=True)
    assert words(ts) == b"alfa bear boggle bravo charlie delta echo"

    q = query.Variations("value", "render")
    ts = q.existing_terms(r, expand=False)
    assert ts == {("value", b"render")}
    ts = q.existing_terms(r, expand=True)
    assert words(ts) == b"render rendering renders"


def test_replace():
    q = And(
        [
            Or([Term("a", "b"), Term("b", "c")], boost=1.2),
            Variations("a", "b", boost=2.0),
        ]
    )
    q = q.replace("a", "b", "BB")
    assert q == And(
        [
            Or([Term("a", "BB"), Term("b", "c")], boost=1.2),
            Variations("a", "BB", boost=2.0),
        ]
    )


def test_apply():
    def visit(q):
        if isinstance(q, (Term, Variations, FuzzyTerm)):
            q.text = q.text.upper()
            return q
        return q.apply(visit)

    before = And([Not(Term("a", "b")), Variations("a", "c"), Not(FuzzyTerm("a", "d"))])
    after = visit(before)
    assert after == And(
        [Not(Term("a", "B")), Variations("a", "C"), Not(FuzzyTerm("a", "D"))]
    )

    def term2var(q):
        if isinstance(q, Term):
            return Variations(q.fieldname, q.text)
        else:
            return q.apply(term2var)

    q = And([Term("f", "alfa"), Or([Term("f", "bravo"), Not(Term("f", "charlie"))])])
    q = term2var(q)
    assert q == And(
        [
            Variations("f", "alfa"),
            Or([Variations("f", "bravo"), Not(Variations("f", "charlie"))]),
        ]
    )


def test_accept():
    def boost_phrases(q):
        if isinstance(q, Phrase):
            q.boost *= 2.0
        return q

    before = And(
        [
            Term("a", "b"),
            Or([Term("c", "d"), Phrase("a", ["e", "f"])]),
            Phrase("a", ["g", "h"], boost=0.25),
        ]
    )
    after = before.accept(boost_phrases)
    assert after == And(
        [
            Term("a", "b"),
            Or([Term("c", "d"), Phrase("a", ["e", "f"], boost=2.0)]),
            Phrase("a", ["g", "h"], boost=0.5),
        ]
    )

    before = Phrase("a", ["b", "c"], boost=2.5)
    after = before.accept(boost_phrases)
    assert after == Phrase("a", ["b", "c"], boost=5.0)


def test_simplify():
    s = fields.Schema(k=fields.ID, v=fields.TEXT)
    ix = RamStorage().create_index(s)

    w = ix.writer()
    w.add_document(k="1", v="aardvark apple allan alfa bear bee")
    w.add_document(k="2", v="brie glue geewhiz goop julia")
    w.commit()

    r = ix.reader()
    q1 = And([Prefix("v", "b", boost=2.0), Term("v", "juliet")])
    q2 = And(
        [
            Or(
                [
                    Term("v", "bear", boost=2.0),
                    Term("v", "bee", boost=2.0),
                    Term("v", "brie", boost=2.0),
                ]
            ),
            Term("v", "juliet"),
        ]
    )
    assert q1.simplify(r) == q2


def test_merge_ranges():
    q = And([TermRange("f1", "a", None), TermRange("f1", None, "z")])
    assert q.normalize() == TermRange("f1", "a", "z")

    q = And([NumericRange("f1", None, "aaaaa"), NumericRange("f1", "zzzzz", None)])
    assert q.normalize() == q

    q = And([TermRange("f1", "a", "z"), TermRange("f1", "b", "x")])
    assert q.normalize() == TermRange("f1", "a", "z")

    q = And([TermRange("f1", "a", "m"), TermRange("f1", "f", "q")])
    assert q.normalize() == TermRange("f1", "f", "m")

    q = Or([TermRange("f1", "a", "m"), TermRange("f1", "f", "q")])
    assert q.normalize() == TermRange("f1", "a", "q")

    q = Or([TermRange("f1", "m", None), TermRange("f1", None, "n")])
    assert q.normalize() == Every("f1")

    q = And([Every("f1"), Term("f1", "a"), Variations("f1", "b")])
    assert q.normalize() == Every("f1")

    q = Or(
        [
            Term("f1", "q"),
            TermRange("f1", "m", None),
            TermRange("f1", None, "n"),
        ]
    )
    assert q.normalize() == Every("f1")

    q = And([Or([Term("f1", "a"), Term("f1", "b")]), Every("f1")])
    assert q.normalize() == Every("f1")

    q = And([Term("f1", "a"), And([Or([Every("f1")])])])
    assert q.normalize() == Every("f1")


def test_normalize_compound():
    def oq():
        return Or([Term("a", "a"), Term("a", "b")])

    def nq(level):
        if level == 0:
            return oq()
        else:
            return Or([nq(level - 1), nq(level - 1), nq(level - 1)])

    q = nq(5)
    q = q.normalize()
    assert q == Or([Term("a", "a"), Term("a", "b")])


def test_duplicates():
    q = And([Term("a", "b"), Term("a", "b")])
    assert q.normalize() == Term("a", "b")

    q = And([Prefix("a", "b"), Prefix("a", "b")])
    assert q.normalize() == Prefix("a", "b")

    q = And([Variations("a", "b"), And([Variations("a", "b"), Term("a", "b")])])
    assert q.normalize() == And([Variations("a", "b"), Term("a", "b")])

    q = And([Term("a", "b"), Prefix("a", "b"), Term("a", "b", boost=1.1)])
    assert q.normalize() == q

    # Wildcard without * or ? normalizes to Term
    q = And([Wildcard("a", "b"), And([Wildcard("a", "b"), Term("a", "b")])])
    assert q.normalize() == Term("a", "b")


# TODO: FIX THIS


def test_query_copy_hash():
    def do(q1, q2):
        q1a = copy.deepcopy(q1)
        assert q1 == q1a
        assert hash(q1) == hash(q1a)
        assert q1 != q2

    do(Term("a", "b", boost=1.1), Term("a", "b", boost=1.5))
    do(
        And([Term("a", "b"), Term("c", "d")], boost=1.1),
        And([Term("a", "b"), Term("c", "d")], boost=1.5),
    )
    do(
        Or([Term("a", "b", boost=1.1), Term("c", "d")]),
        Or([Term("a", "b", boost=1.8), Term("c", "d")], boost=1.5),
    )
    do(
        DisjunctionMax([Term("a", "b", boost=1.8), Term("c", "d")]),
        DisjunctionMax([Term("a", "b", boost=1.1), Term("c", "d")], boost=1.5),
    )
    do(Not(Term("a", "b", boost=1.1)), Not(Term("a", "b", boost=1.5)))
    do(Prefix("a", "b", boost=1.1), Prefix("a", "b", boost=1.5))
    do(Wildcard("a", "b*x?", boost=1.1), Wildcard("a", "b*x?", boost=1.5))
    do(
        FuzzyTerm("a", "b", constantscore=True),
        FuzzyTerm("a", "b", constantscore=False),
    )
    do(FuzzyTerm("a", "b", boost=1.1), FuzzyTerm("a", "b", boost=1.5))
    do(TermRange("a", "b", "c"), TermRange("a", "b", "d"))
    do(TermRange("a", None, "c"), TermRange("a", None, None))
    do(
        TermRange("a", "b", "c", boost=1.1),
        TermRange("a", "b", "c", boost=1.5),
    )
    do(
        TermRange("a", "b", "c", constantscore=True),
        TermRange("a", "b", "c", constantscore=False),
    )
    do(NumericRange("a", 1, 5), NumericRange("a", 1, 6))
    do(NumericRange("a", None, 5), NumericRange("a", None, None))
    do(NumericRange("a", 3, 6, boost=1.1), NumericRange("a", 3, 6, boost=1.5))
    do(
        NumericRange("a", 3, 6, constantscore=True),
        NumericRange("a", 3, 6, constantscore=False),
    )
    # do(DateRange)
    do(Variations("a", "render"), Variations("a", "renders"))
    do(
        Variations("a", "render", boost=1.1),
        Variations("a", "renders", boost=1.5),
    )
    do(Phrase("a", ["b", "c", "d"]), Phrase("a", ["b", "c", "e"]))
    do(
        Phrase("a", ["b", "c", "d"], boost=1.1),
        Phrase("a", ["b", "c", "d"], boost=1.5),
    )
    do(
        Phrase("a", ["b", "c", "d"], slop=1),
        Phrase("a", ["b", "c", "d"], slop=2),
    )
    # do(Ordered)
    do(Every(), Every("a"))
    do(Every("a"), Every("b"))
    do(Every("a", boost=1.1), Every("a", boost=1.5))
    do(NullQuery, Term("a", "b"))
    do(ConstantScoreQuery(Term("a", "b")), ConstantScoreQuery(Term("a", "c")))
    do(
        ConstantScoreQuery(Term("a", "b"), score=2.0),
        ConstantScoreQuery(Term("a", "c"), score=2.1),
    )
    do(
        Require(Term("a", "b"), Term("c", "d")),
        Require(Term("a", "b", boost=1.1), Term("c", "d")),
    )
    # do(Require)
    # do(AndMaybe)
    # do(AndNot)
    # do(Otherwise)

    do(SpanFirst(Term("a", "b"), limit=1), SpanFirst(Term("a", "b"), limit=2))
    do(
        SpanNear(Term("a", "b"), Term("c", "d")),
        SpanNear(Term("a", "b"), Term("c", "e")),
    )
    do(
        SpanNear(Term("a", "b"), Term("c", "d"), slop=1),
        SpanNear(Term("a", "b"), Term("c", "d"), slop=2),
    )
    do(
        SpanNear(Term("a", "b"), Term("c", "d"), mindist=1),
        SpanNear(Term("a", "b"), Term("c", "d"), mindist=2),
    )
    do(
        SpanNear(Term("a", "b"), Term("c", "d"), ordered=True),
        SpanNear(Term("a", "b"), Term("c", "d"), ordered=False),
    )
    do(
        SpanNot(Term("a", "b"), Term("a", "c")),
        SpanNot(Term("a", "b"), Term("a", "d")),
    )
    do(
        SpanOr([Term("a", "b"), Term("a", "c"), Term("a", "d")]),
        SpanOr([Term("a", "b"), Term("a", "c"), Term("a", "e")]),
    )
    do(
        SpanContains(Term("a", "b"), Term("a", "c")),
        SpanContains(Term("a", "b"), Term("a", "d")),
    )
    # do(SpanBefore)
    # do(SpanCondition)


def test_requires():
    a = Term("f", "a")
    b = Term("f", "b")
    assert And([a, b]).requires() == {a, b}
    assert Or([a, b]).requires() == set()
    assert AndMaybe(a, b).requires() == {a}
    assert a.requires() == {a}


def test_highlight_daterange():
    from datetime import datetime, timezone

    schema = fields.Schema(
        id=fields.ID(unique=True, stored=True),
        title=fields.TEXT(stored=True),
        content=fields.TEXT(stored=True),
        released=fields.DATETIME(stored=True),
    )
    ix = RamStorage().create_index(schema)

    w = ix.writer()
    w.update_document(
        id="1",
        title="Life Aquatic",
        content="A nautic film crew sets out to kill a gigantic shark.",
        released=datetime(2004, 12, 25, tzinfo=timezone.utc),
    )
    w.update_document(
        id="2",
        title="Darjeeling Limited",
        content=(
            "Three brothers meet in India for a life changing train " + "journey."
        ),
        released=datetime(2007, 10, 27, tzinfo=timezone.utc),
    )
    w.commit()

    s = ix.searcher()
    r = s.search(Term("content", "train"), terms=True)
    assert len(r) == 1
    assert r[0]["id"] == "2"
    assert (
        r[0].highlights("content")
        == 'for a life changing <b class="match term0">train</b> journey'
    )

    r = s.search(DateRange("released", datetime(2007, 1, 1, tzinfo=timezone.utc), None))
    assert len(r) == 1
    assert r[0].highlights("content") == ""


def test_patterns():
    domain = (
        "aaron able acre adage aether after ago ahi aim ajax akimbo "
        "alembic all amiga amount ampere"
    ).split()
    schema = fields.Schema(word=fields.KEYWORD(stored=True))
    ix = RamStorage().create_index(schema)
    with ix.writer() as w:
        for word in domain:
            w.add_document(word=word)

    with ix.reader() as r:
        assert list(r.field_terms("word")) == domain

        assert list(r.expand_prefix("word", "al")) == [b"alembic", b"all"]
        q = query.Prefix("word", "al")
        assert str(q.simplify(r)) == "(word:alembic OR word:all)"

        q = query.Wildcard("word", "a*[ae]")
        assert (
            str(q.simplify(r))
            == "(word:able OR word:acre OR word:adage OR word:amiga OR word:ampere)"
        )
        assert q._find_prefix(q.text) == "a"

        q = query.Regex("word", "am.*[ae]")
        assert str(q.simplify(r)) == "(word:amiga OR word:ampere)"
        assert q._find_prefix(q.text) == "am"

        q = query.Regex("word", "able|ago")
        assert str(q.simplify(r)) == "(word:able OR word:ago)"
        assert q._find_prefix(q.text) == ""

        # special case: ? may mean "zero occurences"
        q = query.Regex("word", "ah?i")
        assert str(q.simplify(r)) == "(word:ahi OR word:aim)"
        assert q._find_prefix(q.text) == "a"

        # special case: * may mean "zero occurences"
        q = query.Regex("word", "ah*i")
        assert str(q.simplify(r)) == "(word:ahi OR word:aim)"
        assert q._find_prefix(q.text) == "a"


def test_or_nots1():
    # Issue #285
    schema = fields.Schema(a=fields.KEYWORD(stored=True), b=fields.KEYWORD(stored=True))
    st = RamStorage()
    ix = st.create_index(schema)
    with ix.writer() as w:
        w.add_document(a="alfa", b="charlie")

    with ix.searcher() as s:
        q = query.And(
            [
                query.Term("a", "alfa"),
                query.Or(
                    [
                        query.Not(query.Term("b", "bravo")),
                        query.Not(query.Term("b", "charlie")),
                    ]
                ),
            ]
        )
        r = s.search(q)
        assert len(r) == 1


def test_or_nots2():
    # Issue #286
    schema = fields.Schema(a=fields.KEYWORD(stored=True), b=fields.KEYWORD(stored=True))
    st = RamStorage()
    ix = st.create_index(schema)
    with ix.writer() as w:
        w.add_document(b="bravo")

    with ix.searcher() as s:
        q = query.Or([query.Term("a", "alfa"), query.Not(query.Term("b", "alfa"))])
        r = s.search(q)
        assert len(r) == 1


def test_or_nots3():
    schema = fields.Schema(
        title=fields.TEXT(stored=True), itemtype=fields.ID(stored=True)
    )
    with TempIndex(schema, "ornot") as ix:
        w = ix.writer()
        w.add_document(title="a1", itemtype="a")
        w.add_document(title="a2", itemtype="a")
        w.add_document(title="b1", itemtype="b")
        w.commit()

        q = Term("itemtype", "a") | Not(Term("itemtype", "a"))

        with ix.searcher() as s:
            r = " ".join([hit["title"] for hit in s.search(q)])
            assert r == "a1 a2 b1"


def test_ornot_andnot():
    schema = fields.Schema(id=fields.NUMERIC(stored=True), a=fields.KEYWORD())
    st = RamStorage()
    ix = st.create_index(schema)

    with ix.writer() as w:
        w.add_document(id=0, a="word1 word1")
        w.add_document(id=1, a="word1 word2")
        w.add_document(id=2, a="word1 foo")
        w.add_document(id=3, a="foo word2")
        w.add_document(id=4, a="foo bar")

    with ix.searcher() as s:
        qp = qparser.QueryParser("a", ix.schema)
        q1 = qp.parse("NOT word1 NOT word2")
        q2 = qp.parse("NOT (word1 OR word2)")

        r1 = [hit["id"] for hit in s.search(q1, sortedby="id")]
        r2 = [hit["id"] for hit in s.search(q2, sortedby="id")]

        assert r1 == r2 == [4]


def test_none_in_compounds():
    with pytest.raises(query.QueryError):
        _ = query.And([query.Term("a", "b"), None, query.Term("c", "d")])


def test_issue_355():
    schema = fields.Schema(seats=fields.NUMERIC(bits=8, stored=True))
    ix = RamStorage().create_index(schema)
    with ix.writer() as w:
        w.add_document(seats=0)
        w.add_document(seats=10)
        w.add_document(seats=20)

    with ix.searcher() as s:
        # Passing a bytestring for a numeric field
        q = Term("seats", b"maker")
        r1 = [hit["seats"] for hit in s.search(q, limit=5)]

        # Passing a unicode string for a numeric field
        q = Term("seats", "maker")
        r2 = [hit["seats"] for hit in s.search(q, limit=5)]

        # Passing a value too large for the numeric field
        q = Term("seats", 260)
        r3 = [hit["seats"] for hit in s.search(q, limit=5)]

        assert r1 == r2 == r3 == []


def test_sequence():
    schema = fields.Schema(id=fields.STORED, text=fields.TEXT)
    ix = RamStorage().create_index(schema)
    with ix.writer() as w:
        w.add_document(id=0, text="alfa bravo charlie delta echo")
        w.add_document(id=1, text="bravo charlie delta echo alfa")
        w.add_document(id=2, text="charlie delta echo bravo")
        w.add_document(id=3, text="delta echo charlie")
        w.add_document(id=4, text="echo delta")

    with ix.searcher() as s:
        seq = query.Sequence([query.Term("text", "echo"), query.Term("text", "alfa")])
        q = query.And([query.Term("text", "bravo"), seq])

        r = s.search(q, limit=4)
        assert len(r) == 1
        assert r[0]["id"] == 1


def test_andmaybe():
    schema = fields.Schema(id=fields.STORED, text=fields.TEXT)
    ix = RamStorage().create_index(schema)
    with ix.writer() as w:
        w.add_document(id=0, text="alfa bravo charlie delta echo")
        w.add_document(id=1, text="bravo charlie delta echo alfa")
        w.add_document(id=2, text="charlie delta echo bravo")
        w.add_document(id=3, text="delta echo charlie")
        w.add_document(id=4, text="echo delta")

    qp = qparser.QueryParser("text", schema)
    q = qp.parse('bravo ANDMAYBE "echo alfa"')

    with ix.searcher() as s:
        r = s.search(q)
        assert len(r) == 3
        assert [hit["id"] for hit in r] == [1, 2, 0]


def test_numeric_filter():
    schema = fields.Schema(status=fields.NUMERIC, tags=fields.TEXT)
    ix = RamStorage().create_index(schema)

    # Add a single document with status = -2
    with ix.writer() as w:
        w.add_document(status=-2, tags="alfa bravo")

    with ix.searcher() as s:
        # No document should match the filter
        fq = query.NumericRange("status", 0, 2)
        fr = s.search(fq)
        assert fr.scored_length() == 0

        # Make sure the query would otherwise match
        q = query.Term("tags", "alfa")
        r = s.search(q)
        assert r.scored_length() == 1

        # Check the query doesn't match with the filter
        r = s.search(q, filter=fq)
        assert r.scored_length() == 0


def test_andnot_reverse():
    # Bitbucket issue 419
    docs = ["ruby", "sapphire", "ruby + sapphire"]
    schema = fields.Schema(name=fields.TEXT(stored=True))
    q = query.AndNot(query.Term("name", "ruby"), query.Term("name", "sapphire"))

    with TempIndex(schema) as ix:
        with ix.writer() as w:
            for name in docs:
                w.add_document(name=u(name))

        with ix.searcher() as s:
            names_fw = [hit["name"] for hit in s.search(q, limit=None)]

    with TempIndex(schema) as ix:
        with ix.writer() as w:
            for name in reversed(docs):
                w.add_document(name=u(name))

        with ix.searcher() as s:
            names_rv = [hit["name"] for hit in s.search(q, limit=None)]

    assert len(names_fw) == len(names_rv) == 1
    assert names_fw == names_rv


# NumericRange with valid fieldname, start, and end
def test_valid_fieldname_start_end():
    from whoosh.query.ranges import NumericRange

    nr = NumericRange("number", 10, 5925)
    assert nr.fieldname == "number"
    assert nr.start == 10
    assert nr.end == 5925
    assert nr.startexcl == False
    assert nr.endexcl == False
    assert nr.boost == 1.0
    assert nr.constantscore == True


# NumericRange with valid fieldname, start, end, startexcl=True, and endexcl=True
def test_valid_fieldname_start_end_startexcl_endexcl():
    from whoosh.query.ranges import NumericRange

    nr = NumericRange("number", 10, 5925, startexcl=True, endexcl=True)
    assert nr.fieldname == "number"
    assert nr.start == 10
    assert nr.end == 5925
    assert nr.startexcl == True
    assert nr.endexcl == True
    assert nr.boost == 1.0
    assert nr.constantscore == True


# NumericRange with valid fieldname, start, end, boost=2.0, and constantscore=False
def test_valid_fieldname_start_end_boost_constantscore():
    from whoosh.query.ranges import NumericRange

    nr = NumericRange("number", 10, 5925, boost=2.0, constantscore=False)
    assert nr.fieldname == "number"
    assert nr.start == 10
    assert nr.end == 5925
    assert nr.startexcl == False
    assert nr.endexcl == False
    assert nr.boost == 2.0
    assert nr.constantscore == False


# NumericRange with valid fieldname, start=None, and end=None
def test_valid_fieldname_start_none_end_none():
    from whoosh.query.ranges import NumericRange

    nr = NumericRange("number", 0, 0)
    assert nr.fieldname == "number"
    assert nr.start == 0
    assert nr.end == 0
    assert nr.startexcl == False
    assert nr.endexcl == False
    assert nr.boost == 1.0
    assert nr.constantscore == True


# NumericRange with valid fieldname, start=0, and end=0
def test_valid_fieldname_start_zero_end_zero():
    from whoosh.query.ranges import NumericRange

    nr = NumericRange("number", 0, 0)
    assert nr.fieldname == "number"
    assert nr.start == 0
    assert nr.end == 0
    assert nr.startexcl == False
    assert nr.endexcl == False
    assert nr.boost == 1.0
    assert nr.constantscore == True


# NumericRange with valid fieldname, start=-1, and end=1
def test_valid_fieldname_start_minus_one_end_one():
    from whoosh.query.ranges import NumericRange

    nr = NumericRange("number", -1, 1)
    assert nr.fieldname == "number"
    assert nr.start == -1
    assert nr.end == 1
    assert nr.startexcl == False
    assert nr.endexcl == False
    assert nr.boost == 1.0
    assert nr.constantscore == True


# NumericRange with valid fieldname, start=1, and end=-1
def test_valid_fieldname_start_end():
    from whoosh.query.ranges import NumericRange

    nr = NumericRange("fieldname", 1, -1)
    assert nr.fieldname == "fieldname"
    assert nr.start == 1
    assert nr.end == -1
    assert nr.startexcl == False
    assert nr.endexcl == False
    assert nr.boost == 1.0
    assert nr.constantscore == True


# NumericRange with valid fieldname, start=1.5, and end=2.5
def test_valid_fieldname_start_end_float():
    from whoosh.query.ranges import NumericRange

    nr = NumericRange("fieldname", 1.5, 2.5)
    assert nr.fieldname == "fieldname"
    assert nr.start == 1.5
    assert nr.end == 2.5
    assert nr.startexcl == False
    assert nr.endexcl == False
    assert nr.boost == 1.0
    assert nr.constantscore == True


# NumericRange with valid fieldname, start=1.5, and end=2.5, startexcl=True, and endexcl=True
def test_valid_fieldname_start_end_excl():
    from whoosh.query.ranges import NumericRange

    nr = NumericRange("fieldname", 1.5, 2.5, startexcl=True, endexcl=True)
    assert nr.fieldname == "fieldname"
    assert nr.start == 1.5
    assert nr.end == 2.5
    assert nr.startexcl == True
    assert nr.endexcl == True
    assert nr.boost == 1.0
    assert nr.constantscore == True


# NumericRange with valid fieldname, start=1.5, and end=2.5, boost=2.0, and constantscore=False
def test_valid_fieldname_start_end_boost_constantscore():
    from whoosh.query.ranges import NumericRange

    nr = NumericRange("fieldname", 1.5, 2.5, boost=2.0, constantscore=False)
    assert nr.fieldname == "fieldname"
    assert nr.start == 1.5
    assert nr.end == 2.5
    assert nr.startexcl == False
    assert nr.endexcl == False
    assert nr.boost == 2.0
    assert nr.constantscore == False


# NumericRange with invalid boost
def test_invalid_boost():
    from whoosh.query.ranges import NumericRange

    nr = NumericRange("number", 10, 5925, boost="invalid")
    assert nr.boost == "invalid"


# NumericRange with valid start and invalid end


# NumericRange with invalid startexcl and valid endexcl
def test_invalid_startexcl_valid_endexcl():
    """
    Test that NumericRange works with invalid startexcl and valid endexcl.
    """
    from whoosh.query.ranges import NumericRange

    nr = NumericRange("number", 10, 5925, startexcl=True, endexcl=False)

    assert nr.fieldname == "number"
    assert nr.start == 10
    assert nr.end == 5925
    assert nr.startexcl == True
    assert nr.endexcl == False
    assert nr.boost == 1.0
    assert nr.constantscore == True


# NumericRange with invalid constantscore
def test_invalid_constantscore():
    """
    Test that NumericRange does not raise an exception when constantscore is set to False.
    """
    from whoosh.query.ranges import NumericRange

    NumericRange("number", 10, 5925, constantscore=False)


# NumericRange with valid startexcl and invalid endexcl
def test_valid_startexcl_invalid_endexcl():
    from whoosh.query.ranges import NumericRange

    nr = NumericRange("number", 10, 5925, startexcl=True, endexcl=True)
    assert nr.startexcl == True
    assert nr.endexcl == True


# NumbericRange with negative boost field
def test_numeric_range_with_negative_boost():
    """
    Test that NumericRange works with a negative boost field.
    """
    from whoosh.query.ranges import NumericRange

    nr = NumericRange("number", 10, 5925, boost=-1.0)

    assert nr.fieldname == "number"
    assert nr.start == 10
    assert nr.end == 5925
    assert nr.startexcl == False
    assert nr.endexcl == False
    assert nr.boost == -1.0
    assert nr.constantscore == True
