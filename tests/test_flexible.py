from whoosh import fields
from whoosh.util.testing import TempIndex


def test_addfield():
    schema = fields.Schema(id=fields.ID(stored=True), content=fields.TEXT)
    with TempIndex(schema, "addfield") as ix:
        w = ix.writer()
        w.add_document(id="a", content="alfa")
        w.add_document(id="b", content="bravo")
        w.add_document(id="c", content="charlie")
        w.commit()

        ix.add_field("added", fields.KEYWORD(stored=True))

        w = ix.writer()
        w.add_document(id="d", content="delta", added="fourth")
        w.add_document(id="e", content="echo", added="fifth")
        w.commit(merge=False)

        with ix.searcher() as s:
            assert ("id", "d") in s.reader()
            assert s.document(id="d") == {"id": "d", "added": "fourth"}
            assert s.document(id="b") == {"id": "b"}


def test_addfield_spelling():
    schema = fields.Schema(id=fields.ID(stored=True), content=fields.TEXT)
    with TempIndex(schema, "addfield") as ix:
        w = ix.writer()
        w.add_document(id="a", content="alfa")
        w.add_document(id="b", content="bravo")
        w.add_document(id="c", content="charlie")
        w.commit()

        ix.add_field("added", fields.KEYWORD(stored=True))

        w = ix.writer()
        w.add_document(id="d", content="delta", added="fourth")
        w.add_document(id="e", content="echo", added="fifth")
        w.commit(merge=False)

        with ix.searcher() as s:
            assert s.document(id="d") == {"id": "d", "added": "fourth"}
            assert s.document(id="b") == {"id": "b"}


def test_removefield():
    schema = fields.Schema(
        id=fields.ID(stored=True), content=fields.TEXT, city=fields.KEYWORD(stored=True)
    )
    with TempIndex(schema, "removefield") as ix:
        w = ix.writer()
        w.add_document(id="b", content="bravo", city="baghdad")
        w.add_document(id="c", content="charlie", city="cairo")
        w.add_document(id="d", content="delta", city="dakar")
        w.commit()

        with ix.searcher() as s:
            assert s.document(id="c") == {"id": "c", "city": "cairo"}

        w = ix.writer()
        w.remove_field("content")
        w.remove_field("city")
        w.commit()

        ixschema = ix._current_schema()
        assert ixschema.names() == ["id"]
        assert ixschema.stored_names() == ["id"]

        with ix.searcher() as s:
            assert ("content", b"charlie") not in s.reader()
            assert s.document(id="c") == {"id": "c"}


def test_optimize_away():
    schema = fields.Schema(
        id=fields.ID(stored=True), content=fields.TEXT, city=fields.KEYWORD(stored=True)
    )
    with TempIndex(schema, "optimizeaway") as ix:
        w = ix.writer()
        w.add_document(id="b", content="bravo", city="baghdad")
        w.add_document(id="c", content="charlie", city="cairo")
        w.add_document(id="d", content="delta", city="dakar")
        w.commit()

        with ix.searcher() as s:
            assert s.document(id="c") == {"id": "c", "city": "cairo"}

        w = ix.writer()
        w.remove_field("content")
        w.remove_field("city")
        w.commit(optimize=True)

        with ix.searcher() as s:
            assert ("content", "charlie") not in s.reader()
            assert s.document(id="c") == {"id": "c"}


if __name__ == "__main__":
    test_addfield()
