import struct

from nose.tools import assert_equal  # type: ignore @UnresolvedImport
from whoosh import formats
from whoosh.filedb.filepostings import FilePostingReader, FilePostingWriter
from whoosh.util.testing import TempStorage


def test_huge_postfile():
    with TempStorage("hugeindex") as st:
        pf = st.create_file("test.pst")

        gb5 = 5 * 1024 * 1024 * 1024
        pf.seek(gb5)
        pf.write("\x00\x00\x00\x00")
        assert_equal(pf.tell(), gb5 + 4)

        fpw = FilePostingWriter(pf)
        f = formats.Frequency(None)
        offset = fpw.start(f)
        for i in range(10):
            fpw.write(i, float(i), struct.pack("!I", i), 10)
        posttotal = fpw.finish()
        assert_equal(posttotal, 10)
        fpw.close()

        pf = st.open_file("test.pst")
        pfr = FilePostingReader(pf, offset, f)
        i = 0
        while pfr.is_active():
            assert_equal(pfr.id(), i)
            assert_equal(pfr.weight(), float(i))
            assert_equal(pfr.value(), struct.pack("!I", i))
            pfr.next()
            i += 1
        pf.close()
