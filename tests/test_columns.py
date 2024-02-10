import inspect
import random
import sys
from io import BytesIO
from pickle import dumps, loads

import pytest
from whoosh import columns, fields, query
from whoosh.codec.whoosh3 import W3Codec
from whoosh.filedb import compound
from whoosh.filedb.filestore import RamStorage
from whoosh.matching import ConstantScoreMatcher
from whoosh.query import ColumnMatcher, ColumnQuery
from whoosh.util.testing import TempIndex, TempStorage


def b(s):
    return s.encode("latin-1")


def test_pickleability():
    # Ignore base classes
    ignore = (columns.Column, columns.WrappedColumn, columns.ListColumn)
    # Required arguments
    init_args = {
        "ClampedNumericColumn": (columns.NumericColumn("B"),),
        "FixedBytesColumn": (5,),
        "FixedBytesListColumn": (5,),
        "NumericColumn": ("i",),
        "PickleColumn": (columns.VarBytesColumn(),),
        "StructColumn": ("=if", (0, 0.0)),
    }

    coltypes = [
        c
        for _, c in inspect.getmembers(columns, inspect.isclass)
        if issubclass(c, columns.Column) and c not in ignore
    ]

    for coltype in coltypes:
        args = init_args.get(coltype.__name__, ())
        try:
            inst = coltype(*args)
        except TypeError:
            e = sys.exc_info()[1]
            raise TypeError(f"Error instantiating {coltype!r}: {e}")
        _ = loads(dumps(inst, -1))


def test_multistream():
    domain = [
        ("a", "12345"),
        ("b", "abc"),
        ("c", "AaBbC"),
        ("a", "678"),
        ("c", "cDdEeF"),
        ("b", "defgh"),
        ("b", "ijk"),
        ("c", "fGgHh"),
        ("a", "9abc"),
    ]

    st = RamStorage()
    msw = compound.CompoundWriter(st)
    files = {name: msw.create_file(name) for name in "abc"}
    for name, data in domain:
        files[name].write(b(data))
    f = st.create_file("test")
    msw.save_as_compound(f)

    f = st.open_file("test")
    msr = compound.CompoundStorage(f)
    assert msr.open_file("a").read() == b"123456789abc"
    assert msr.open_file("b").read() == b"abcdefghijk"
    assert msr.open_file("c").read() == b"AaBbCcDdEeFfGgHh"


def test_random_multistream():
    letters = "abcdefghijklmnopqrstuvwxyz"

    def randstring(n):
        s = "".join(random.choice(letters) for _ in range(n))
        return s.encode("latin1")

    domain = {randstring(random.randint(5, 10)): randstring(2500) for _ in range(100)}
    outfiles = {name: BytesIO(value) for name, value in domain.items()}

    with TempStorage() as st:
        msw = compound.CompoundWriter(st, buffersize=1024)
        mfiles = {name: msw.create_file(name) for name in domain}
        while outfiles:
            name = random.choice(list(outfiles))
            v = outfiles[name].read(1000)
            mfiles[name].write(v)
            if len(v) < 1000:
                del outfiles[name]
        f = st.create_file("test")
        msw.save_as_compound(f)

        f = st.open_file("test")
        msr = compound.CompoundStorage(f)
        for name, value in domain.items():
            assert msr.open_file(name).read() == value
        msr.close()


def _rt(c, values, default):
    # Continuous
    st = RamStorage()
    f = st.create_file("test1")
    f.write(b"hello")
    w = c.writer(f)
    for docnum, v in enumerate(values):
        w.add(docnum, v)
    w.finish(len(values))
    length = f.tell() - 5
    f.close()

    f = st.open_file("test1")
    r = c.reader(f, 5, length, len(values))
    assert values == list(r)
    for x in range(len(values)):
        assert values[x] == r[x]
    f.close()

    # Sparse
    doccount = len(values) * 7 + 15
    target = [default] * doccount

    f = st.create_file("test2")
    f.write(b"hello")
    w = c.writer(f)
    for docnum, v in zip(range(10, doccount, 7), values):
        target[docnum] = v
        w.add(docnum, v)
    w.finish(doccount)
    length = f.tell() - 5
    f.close()

    f = st.open_file("test2")
    r = c.reader(f, 5, length, doccount)
    assert target == list(r)
    for x in range(doccount):
        assert target[x] == r[x]

    lr = r.load()
    assert target == list(lr)
    f.close()


def test_roundtrip():
    _rt(columns.VarBytesColumn(), [b"a", b"ccc", b"bbb", b"e", b"dd"], b"")
    _rt(
        columns.FixedBytesColumn(5),
        [b"aaaaa", b"eeeee", b"ccccc", b"bbbbb", b"eeeee"],
        b"\x00" * 5,
    )
    _rt(
        columns.RefBytesColumn(),
        [b"a", b"ccc", b"bb", b"ccc", b"a", b"bb"],
        b"",
    )
    _rt(
        columns.RefBytesColumn(3),
        [b"aaa", b"bbb", b"ccc", b"aaa", b"bbb", b"ccc"],
        b"\x00" * 3,
    )
    _rt(
        columns.StructColumn("ifH", (0, 0.0, 0)),
        [
            (100, 1.5, 15000),
            (-100, -5.0, 0),
            (5820, 6.5, 462),
            (-57829, -1.5, 6),
            (0, 0, 0),
        ],
        (0, 0.0, 0),
    )

    numcol = columns.NumericColumn
    _rt(numcol("b"), [10, -20, 30, -25, 15], 0)
    _rt(numcol("B"), [10, 20, 30, 25, 15], 0)
    _rt(numcol("h"), [1000, -2000, 3000, -15000, 32000], 0)
    _rt(numcol("H"), [1000, 2000, 3000, 15000, 50000], 0)
    _rt(numcol("i"), [2**16, -(2**20), 2**24, -(2**28), 2**30], 0)
    _rt(numcol("I"), [2**16, 2**20, 2**24, 2**28, 2**31 & 0xFFFFFFFF], 0)
    _rt(numcol("q"), [10, -20, 30, -25, 15], 0)
    _rt(numcol("Q"), [2**35, 2**40, 2**48, 2**52, 2**63], 0)
    _rt(numcol("f"), [1.5, -2.5, 3.5, -4.5, 1.25], 0)
    _rt(numcol("d"), [1.5, -2.5, 3.5, -4.5, 1.25], 0)

    c = columns.BitColumn(compress_at=10)
    _rt(c, [bool(random.randint(0, 1)) for _ in range(70)], False)
    _rt(c, [bool(random.randint(0, 1)) for _ in range(90)], False)

    c = columns.PickleColumn(columns.VarBytesColumn())
    _rt(c, [None, True, False, 100, -7, "hello"], None)

    c = columns.VarBytesListColumn()
    _rt(c, [[b"garnet", b"amethyst"], [b"pearl"]], [])
    _c = columns.VarBytesListColumn()

    c = columns.FixedBytesListColumn(4)
    _rt(c, [[b"garn", b"amet"], [b"pear"]], [])


def test_multivalue():
    schema = fields.Schema(
        s=fields.TEXT(sortable=True), n=fields.NUMERIC(sortable=True)
    )
    ix = RamStorage().create_index(schema)
    with ix.writer(codec=W3Codec()) as w:
        w.add_document(s="alfa foxtrot charlie".split(), n=[100, 200, 300])
        w.add_document(s="juliet bravo india".split(), n=[10, 20, 30])

    with ix.reader() as r:
        scr = r.column_reader("s")
        assert list(scr) == ["alfa", "juliet"]

        ncr = r.column_reader("n")
        assert list(ncr) == [100, 10]


def test_column_field():
    schema = fields.Schema(
        a=fields.TEXT(sortable=True), b=fields.COLUMN(columns.RefBytesColumn())
    )
    with TempIndex(schema, "columnfield") as ix:
        cd = b"charlie delta"
        with ix.writer(codec=W3Codec()) as w:
            w.add_document(a="alfa bravo", b=cd)
            w.add_document(a="bravo charlie", b=b"delta echo")
            w.add_document(a="charlie delta", b=b"echo foxtrot")

        with ix.reader() as r:
            assert r.has_column("a")
            assert r.has_column("b")

            cra = r.column_reader("a")
            assert cra[0] == "alfa bravo"
            assert type(cra[0]) == str

            crb = r.column_reader("b")
            assert crb[0] == cd
            assert type(crb[0]) == bytes


def test_column_query():
    schema = fields.Schema(
        id=fields.STORED, a=fields.ID(sortable=True), b=fields.NUMERIC(sortable=True)
    )
    with TempIndex(schema, "ColumnQuery") as ix:
        with ix.writer(codec=W3Codec()) as w:
            w.add_document(id=1, a="alfa", b=10)
            w.add_document(id=2, a="bravo", b=20)
            w.add_document(id=3, a="charlie", b=30)
            w.add_document(id=4, a="delta", b=40)
            w.add_document(id=5, a="echo", b=50)
            w.add_document(id=6, a="foxtrot", b=60)

        with ix.searcher() as s:

            def check(q):
                return [s.stored_fields(docnum)["id"] for docnum in q.docs(s)]

            q = ColumnQuery("a", "bravo")
            assert check(q) == [2]

            q = ColumnQuery("b", 30)
            assert check(q) == [3]

            q = ColumnQuery("a", lambda v: v != "delta")
            assert check(q) == [1, 2, 3, 5, 6]

            q = ColumnQuery("b", lambda v: v > 30)
            assert check(q) == [4, 5, 6]


def test_ref_switch():
    import warnings

    col = columns.RefBytesColumn()

    def rw(size):
        st = RamStorage()

        f = st.create_file("test")
        cw = col.writer(f)
        for i in range(size):
            cw.add(i, hex(i).encode("latin1"))
        cw.finish(size)
        length = f.tell()
        f.close()

        f = st.open_file("test")
        cr = col.reader(f, 0, length, size)
        for i in range(size):
            v = cr[i]
            # Column ignores additional unique values after 65535
            if i <= 65535 - 1:
                assert v == hex(i).encode("latin1")
            else:
                assert v == b""
        f.close()

    rw(255)

    # warnings.catch_warnings is not available in Python 2.5
    if hasattr(warnings, "catch_warnings"):
        # Column warns on additional unique values after 65535
        with warnings.catch_warnings(record=True) as w:
            # Cause all warnings to always be triggered.
            warnings.simplefilter("always")
            rw(65537)

            assert len(w) == 2
            assert issubclass(w[-1].category, UserWarning)


def test_varbytes_offsets():
    values = "alfa bravo charlie delta echo foxtrot golf hotel".split()
    vlen = len(values)

    # Without offsets:
    col = columns.VarBytesColumn(allow_offsets=False)
    schema = fields.Schema(name=fields.ID(sortable=col))
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            for i in range(5000):
                w.add_document(name=values[i % vlen])

        with ix.reader() as r:
            cr = r.column_reader("name")
            assert isinstance(cr, columns.TranslatingColumnReader)
            assert not cr.raw_column().had_stored_offsets
            for i in (10, 100, 1000, 3000):
                assert cr[i] == values[i % vlen]

    # With offsets
    col = columns.VarBytesColumn(allow_offsets=True, write_offsets_cutoff=4096)
    schema = fields.Schema(name=fields.ID(sortable=col))
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            for i in range(5000):
                w.add_document(name=values[i % vlen])

        with ix.reader() as r:
            cr = r.column_reader("name")
            assert isinstance(cr, columns.TranslatingColumnReader)
            assert cr.raw_column().had_stored_offsets
            for i in (10, 100, 1000, 3000):
                assert cr[i] == values[i % vlen]


# Initializes the 'fieldname' and 'condition' attributes with the values passed as parameters.
def test_initializes_fieldname_and_condition_attributes():
    fieldname = "test_field"
    condition = lambda x: x > 0
    query = ColumnQuery(fieldname, condition)
    assert query.fieldname == fieldname
    assert query.condition == condition


# If 'condition' is a callable, sets it as the 'condition' attribute.
def test_sets_condition_attribute_if_condition_is_callable():
    fieldname = "test_field"
    condition = lambda x: x > 0
    query = ColumnQuery(fieldname, condition)
    assert query.condition == condition


# If 'condition' is not a callable, creates a lambda function that compares the document values to it (using '==') and sets it as the 'comp' attribute.
def test_creates_lambda_function_if_condition_is_not_callable():
    fieldname = "test_field"
    condition = 10
    query = ColumnQuery(fieldname, condition)
    assert query.condition == 10


# If 'fieldname' is not a string, it should not raise a TypeError.
def test_raises_typeerror_if_fieldname_is_not_string():
    fieldname = 10
    condition = lambda x: x > 0
    query = ColumnQuery(fieldname, condition)
    assert query.fieldname == fieldname
    assert query.condition == condition


# If 'condition' is not a callable and not a hashable type, the columns.ColumnQuery object should be created without raising any exception.
def test_behavior_if_condition_is_not_callable_and_not_hashable():
    fieldname = "test_field"
    condition = []
    query = ColumnQuery(fieldname, condition)
    assert query.fieldname == fieldname
    assert query.condition == condition


# If 'condition' is a callable and it raises an exception when called with a document value, raises that exception.
def test_raises_exception_if_condition_callable_raises_exception():
    fieldname = "test_field"
    condition = lambda x: 1 / x
    with pytest.raises(ZeroDivisionError):
        query = ColumnQuery(fieldname, condition)
        query.condition(0)


# If 'condition' is a callable and it returns a non-boolean value when called with a document value, does not raise a TypeError.
def test_does_not_raise_typeerror_if_condition_callable_returns_non_boolean_value():
    fieldname = "test_field"
    condition = lambda x: "True"
    query = ColumnQuery(fieldname, condition)
    assert isinstance(query, ColumnQuery)


# If 'condition' is a callable and it returns True for all document values, returns a ConstantScoreMatcher that matches all documents.
def test_returns_constantscorematcher_matching_all_documents_if_condition_callable_returns_true_for_all_values():
    from unittest.mock import Mock

    fieldname = "test_field"
    condition = lambda x: True
    query = ColumnQuery(fieldname, condition)
    searcher = Mock()
    creader = Mock()
    creader.__len__ = Mock(return_value=10)
    creader.__getitem__ = Mock(side_effect=lambda i: i)
    searcher.reader.return_value.column_reader.return_value = creader
    assert isinstance(query.matcher(searcher), ConstantScoreMatcher)


# If 'condition' is a callable and it is very slow, the matcher may take a long time to initialize.
def test_matcher_initialization_may_take_long_time_if_condition_callable_is_very_slow():
    import time
    from unittest.mock import Mock, patch

    fieldname = "test_field"
    condition = lambda x: time.sleep(10)
    query = ColumnQuery(fieldname, condition)
    searcher = Mock()
    searcher.reader.return_value.has_column.return_value = True
    with patch.object(query, "matcher") as matcher_mock:
        query.matcher(searcher)
        matcher_mock.assert_called_once_with(searcher)


# Initializes the '_i' attribute to 0.
def test_initializes_i_attribute_to_0():
    condition = lambda x: x > 0
    creader = []  # Define creader variable
    matcher = ColumnMatcher(creader, condition)
    assert matcher._i == 0


# Initializes the 'creader' attribute with the value passed as parameter.
def test_initializes_creader_attribute():
    condition = lambda x: x > 0
    creader = [1, 2, 3, 4, 5]
    matcher = ColumnMatcher(creader, condition)
    assert matcher.creader == creader


# Initializes the 'condition' attribute with the value passed as parameter.
def test_initializes_condition_attribute():
    condition = lambda x: x > 0
    creader = []
    matcher = ColumnMatcher(creader, condition)
    assert matcher.condition == condition


# Returns True if the '_i' attribute is less than the length of the 'creader' attribute.
def test_returns_true_if_i_attribute_is_less_than_length_of_creader_attribute():
    condition = lambda x: x > 0
    creader = [1, 2, 3, 4, 5]
    matcher = ColumnMatcher(creader, condition)
    assert matcher.is_active() == True


# Returns False if the '_i' attribute is equal to or greater than the length of the 'creader' attribute.
def test_returns_false_if_i_attribute_is_equal_to_or_greater_than_length_of_creader_attribute():
    condition = lambda x: x > 0
    creader = [1, 2, 3, 4, 5]
    matcher = ColumnMatcher(creader, condition)
    matcher._i = len(creader)
    assert matcher.is_active() == False


# test if the `is_leaf` function is True
def test_is_leaf_true():
    fieldname = "test_field"
    condition = lambda x: x > 0
    query = ColumnQuery(fieldname, condition)
    assert query.is_leaf() == True
