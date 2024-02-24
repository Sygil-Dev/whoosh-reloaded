from whoosh import analysis, classify, fields, formats, query, reading
from whoosh.filedb.filestore import RamStorage
from whoosh.util.testing import TempIndex

domain = [
    "A volume that is a signed distance field used for collision calculations.  The turbulence is damped near the collision object to prevent particles from passing through.",
    "When particles cross the SDF boundary they have their velocities reversed according to the SDF normal and are pushed outside of the SDF.",
    "The distance at which the particles start to slow down due to a collision object.",
    "There are several different ways to update a particle system in response to an external velocity field. They are broadly categorized as Force, Velocity, and Position updates.",
    "Instead of applying a force in the direction of the velocity field, the force is applied relative to the difference between the particle's velocity and the velocity field.  This effectively adds an implicit drag that causes the particles to match the velocity field.",
    "In Velocity Blend mode, the amount to mix in the field velocity every timestep.",
    "In Velocity Blend mode, the amount to add the curlnoise velocity to the particle's velocity.  This can be useful in addition to advectbyvolume to layer turbulence on a velocity field.",
]

text = "How do I use a velocity field for particles"


def create_index():
    analyzer = analysis.StandardAnalyzer()
    vector_format = formats.Frequency()
    schema = fields.Schema(
        path=fields.ID(stored=True),
        content=fields.TEXT(analyzer=analyzer, vector=vector_format),
        extra=fields.TEXT(stored=True),
    )

    ix = RamStorage().create_index(schema)

    w = ix.writer()
    from string import ascii_lowercase

    for letter, content in zip(ascii_lowercase, domain):
        w.add_document(path=f"/{letter}", content=content, extra="")
    w.commit()

    return ix


def test_add_text(model=classify.Bo1Model):
    ix = create_index()
    with ix.reader() as r:
        exp = classify.Expander(r, "content", model=model)
        exp.add_text(text)
        assert {t[0] for t in exp.expanded_terms(3)} == {
            "particles",
            "velocity",
            "field",
        }
        exp = classify.Expander(r, "extra", model=model)
        exp.add_text(text)
        assert exp.expanded_terms(3) == []


def test_keyterms(model=classify.Bo1Model):
    ix = create_index()
    with ix.searcher() as s:
        docnum = s.document_number(path="/a")
        keys = list(s.key_terms([docnum], "content", numterms=3, model=model))
        assert [t[0] for t in keys] == ["collision", "calculations", "damped"]
        keys = list(s.key_terms([docnum], "extra", numterms=3, model=model))
        assert keys == []


def test_keyterms_from_text(model=classify.Bo2Model):
    ix = create_index()
    with ix.searcher() as s:
        keys = list(s.key_terms_from_text("content", text, model=model))
        assert {t[0] for t in keys} == {"particles", "velocity", "field"}
        keys = list(s.key_terms_from_text("extra", text, model=model))
        assert keys == []


def test_more_like_this(model=classify.Bo2Model):
    docs = [
        "alfa bravo charlie delta echo foxtrot golf",
        "delta echo foxtrot golf hotel india juliet",
        "echo foxtrot golf hotel india juliet kilo",
        "foxtrot golf hotel india juliet kilo lima",
        "golf hotel india juliet kilo lima mike",
        "foxtrot golf hotel india alfa bravo charlie",
    ]

    def _check(schema, **kwargs):
        ix = RamStorage().create_index(schema)
        with ix.writer() as w:
            for i, text in enumerate(docs, 1):
                w.add_document(id=str(i), text=text)

        with ix.searcher() as s:
            docnum = s.document_number(id="1")
            r = s.more_like(docnum, "text", model=model, **kwargs)
            assert [hit["id"] for hit in r] == ["6", "2", "3"]

    schema = fields.Schema(id=fields.ID(stored=True), text=fields.TEXT(stored=True))
    _check(schema)

    ana = analysis.StandardAnalyzer()
    schema = fields.Schema(
        id=fields.ID(stored=True),
        text=fields.TEXT(analyzer=ana, vector=formats.Frequency()),
    )
    _check(schema)

    schema = fields.Schema(id=fields.ID(stored=True), text=fields.TEXT)
    _check(schema, text=docs[0])


def test_more_like(model=classify.Bo2Model):
    schema = fields.Schema(id=fields.ID(stored=True), text=fields.TEXT(stored=True))
    with TempIndex(schema, "morelike") as ix:
        with ix.writer() as w:
            w.add_document(id="1", text="alfa bravo charlie")
            w.add_document(id="2", text="bravo charlie delta")
            w.add_document(id="3", text="echo")
            w.add_document(id="4", text="delta echo foxtrot")
            w.add_document(id="5", text="echo echo echo")
            w.add_document(id="6", text="foxtrot golf hotel")
            w.add_document(id="7", text="golf hotel india")

        with ix.searcher() as s:
            docnum = s.document_number(id="3")
            r = s.more_like(docnum, "text", model=model)
            assert [hit["id"] for hit in r] == ["5", "4"]


def test_empty_more_like(model=classify.Bo1Model):
    schema = fields.Schema(text=fields.TEXT)
    with TempIndex(schema, "emptymore") as ix:
        with ix.searcher() as s:
            assert s.doc_count() == 0
            q = query.Term("a", "b")
            r = s.search(q)
            assert r.scored_length() == 0
            assert r.key_terms("text", model=model) == []

            ex = classify.Expander(s.reader(), "text", model=model)
            assert ex.expanded_terms(1) == []


def test_fake_more_like(model=classify.Bo1Model):
    schema = fields.Schema(text=fields.TEXT)
    reader = reading.EmptyReader(schema)
    ex = classify.Expander(reader, "text", model=model)
    assert ex.expanded_terms(1) == []


def test_bo2model():
    test_empty_more_like(classify.Bo2Model)
    test_add_text(classify.Bo2Model)
    test_keyterms_from_text(classify.Bo2Model)
    test_more_like_this(classify.Bo2Model)
    test_more_like(classify.Bo2Model)
    test_keyterms(classify.Bo2Model)
    test_fake_more_like(classify.Bo2Model)


def test_klmodel():
    test_empty_more_like(classify.KLModel)
    test_add_text(classify.KLModel)
    test_keyterms_from_text(classify.KLModel)
    test_more_like_this(classify.KLModel)
    test_more_like(classify.KLModel)
    test_keyterms(classify.KLModel)
    test_fake_more_like(classify.KLModel)
