import fnmatch
import logging
import os
import re

from whoosh import analysis, fields, index, qparser, scoring
from whoosh.util import now

log = logging.getLogger(__name__)


# Functions for reading MARC format

LEADER = (" " * 10) + "22" + (" " * 8) + "4500"
LEADER_LEN = len(LEADER)
DIRECTORY_ENTRY_LEN = 12
SUBFIELD_INDICATOR = "\x1F"
END_OF_FIELD = "\x1E"
END_OF_RECORD = "\x1D"
isbn_regex = re.compile(r"[-0-9xX]+")


def read_file(dbfile, tags=None):
    """
    Reads records from a database file.

    Args:
        dbfile (file): The file object representing the database file.
        tags (list, optional): A list of tags to filter the records. Defaults to None.

    Yields:
        tuple: A tuple containing the parsed record and its position in the file.

    Raises:
        ValueError: If the length of the record is invalid.

    """
    while True:
        pos = dbfile.tell()
        first5 = dbfile.read(5)
        if not first5:
            return
        if len(first5) < 5:
            raise ValueError("Invalid length")
        length = int(first5)
        chunk = dbfile.read(length - 5)
        yield parse_record(first5 + chunk, tags), pos


def read_record(filename, pos, tags=None):
    """
    Read a MARC21 record from a file.

    Args:
        filename (str): The path to the MARC21 file.
        pos (int): The position in the file where the record starts.
        tags (list[str], optional): A list of tags to include in the parsed record.
            If None, all tags will be included. Defaults to None.

    Returns:
        dict: A dictionary representing the parsed MARC21 record.

    Raises:
        FileNotFoundError: If the specified file does not exist.
        ValueError: If the specified position is invalid.

    """
    f = open(filename, "rb")
    f.seek(pos)
    first5 = f.read(5)
    length = int(first5)
    chunk = f.read(length - 5)
    return parse_record(first5 + chunk, tags)


def parse_record(data, tags=None):
    """
    Parse a MARC21 record from the given data.

    Args:
        data (str): The MARC21 record data.
        tags (list[str], optional): List of tags to include in the parsed result. If not provided, all tags will be included.

    Returns:
        dict: A dictionary representing the parsed MARC21 record, where the keys are the tags and the values are the corresponding data.

    Raises:
        AssertionError: If the length of the leader is not equal to LEADER_LEN.
        AssertionError: If the dataoffset is not greater than 0.
        AssertionError: If the dataoffset is not less than the length of the data.
        AssertionError: If the difference between dirend and dirstart is not divisible by DIRECTORY_ENTRY_LEN.

    Example:
        data = "..."
        tags = ["245", "260"]
        result = parse_record(data, tags)
        # Returns:
        # {
        #     "245": ["Title"],
        #     "260": ["Publisher"]
        # }
    """
    leader = data[:LEADER_LEN]
    assert len(leader) == LEADER_LEN

    dataoffset = int(data[12:17])
    assert dataoffset > 0
    assert dataoffset < len(data)

    # dataoffset - 1 to avoid END-OF-FIELD byte
    dirstart = LEADER_LEN
    dirend = dataoffset - 1

    # Number of fields in record
    assert (dirend - dirstart) % DIRECTORY_ENTRY_LEN == 0
    field_count = (dirend - dirstart) // DIRECTORY_ENTRY_LEN

    result = {}
    for i in range(field_count):
        start = dirstart + i * DIRECTORY_ENTRY_LEN
        end = start + DIRECTORY_ENTRY_LEN
        tag = data[start : start + 3]
        if tags and tag not in tags:
            continue

        entry = data[start:end]
        elen = int(entry[3:7])
        offset = dataoffset + int(entry[7:12])
        edata = data[offset : offset + elen - 1]

        if not (tag < "010" and tag.isdigit()):
            edata = edata.split(SUBFIELD_INDICATOR)[1:]
            if tag in result:
                result[tag].extend(edata)
            else:
                result[tag] = edata
        else:
            result[tag] = edata
    return result


def subfield(vs, code):
    """
    Extracts the value of a subfield from a list of subfields.

    Parameters:
    - vs (list): The list of subfields to search in.
    - code (str): The code of the subfield to extract.

    Returns:
    - str or None: The value of the subfield if found, None otherwise.
    """
    for v in vs:
        if v.startswith(code):
            return v[1:]
    return None


def joinsubfields(vs):
    """
    Joins the subfields of a MARC21 record.

    This function takes a list of subfields and joins them into a single string,
    excluding any subfields starting with "6".

    Args:
        vs (list): A list of subfields.

    Returns:
        str: The joined subfields as a single string.

    Example:
        >>> subfields = ['a', 'b', 'c', '6d', 'e']
        >>> joinsubfields(subfields)
        'a b c e'
    """
    return " ".join(v[1:] for v in vs if v and v[0] != "6")


def getfields(d, *tags):
    """
    Retrieve the values from a dictionary `d` for the given `tags`.

    Args:
        d (dict): The dictionary to retrieve values from.
        tags (str): Variable number of tags to retrieve values for.

    Returns:
        generator: A generator that yields the values for the given tags.

    Example:
        >>> d = {'tag1': 'value1', 'tag2': 'value2', 'tag3': 'value3'}
        >>> fields = getfields(d, 'tag1', 'tag3')
        >>> list(fields)
        ['value1', 'value3']
    """
    return (d[tag] for tag in tags if tag in d)


def title(d):
    """
    Extracts the title from a MARC21 record dictionary.

    Args:
        d (dict): The MARC21 record dictionary.

    Returns:
        str: The extracted title, or None if no title is found.
    """
    title = None
    if "245" in d:
        svs = d["245"]
        title = subfield(svs, "a")
        if title:
            t2 = subfield(svs, "b")
            if t2:
                title += t2
    return title


def isbn(d):
    """
    Extracts the ISBN number from the MARC21 record.

    Parameters:
    - d (dict): The MARC21 record dictionary.

    Returns:
    - str: The extracted ISBN number without hyphens.

    Example:
    >>> record = {
    ...     "020": {
    ...         "a": "978-0132350884"
    ...     }
    ... }
    >>> isbn(record)
    '9780132350884'
    """
    if "020" in d:
        num = subfield(d["020"], "a")
        if num:
            match = isbn_regex.search(num)
            if match:
                return match.group(0).replace("-", "")


def author(d):
    """
    Returns the author information from the given dictionary.

    Parameters:
    - d (dict): The dictionary containing the MARC21 record.

    Returns:
    - str: The author information.

    Raises:
    - KeyError: If the dictionary does not contain any author fields (100, 110, or 111).
    """
    if "100" in d:
        return joinsubfields(d["100"])
    elif "110" in d:
        return joinsubfields(d["110"])
    elif "111" in d:
        return joinsubfields(d["111"])


def uniform_title(d):
    """
    Returns the uniform title from the MARC21 record dictionary.

    Parameters:
    - d (dict): The MARC21 record dictionary.

    Returns:
    - str: The uniform title.

    Raises:
    - None.

    Examples:
    >>> record = {"130": ["Uniform Title"]}
    >>> uniform_title(record)
    'Uniform Title'

    >>> record = {"240": ["Uniform Title"]}
    >>> uniform_title(record)
    'Uniform Title'
    """
    if "130" in d:
        return joinsubfields(d["130"])
    elif "240" in d:
        return joinsubfields(d["240"])


subjectfields = (
    "600 610 611 630 648 650 651 653 654 655 656 657 658 662 690 691 696 697 698 699"
).split()


def subjects(d):
    """
    Returns a string containing the joined subfields of the given document's subject fields.

    Parameters:
    - d: The document to extract subject fields from.

    Returns:
    A string containing the joined subfields of the subject fields.
    """
    return " ".join(joinsubfields(vs) for vs in getfields(d, *subjectfields))


def physical(d):
    """
    Returns the physical description of a MARC21 record.

    Parameters:
    - d (dict): The MARC21 record dictionary.

    Returns:
    - str: The physical description of the record.
    """
    return joinsubfields(d["300"])


def location(d):
    """
    Returns the location of a record in the MARC21 format.

    Parameters:
    - d (dict): The MARC21 record dictionary.

    Returns:
    - str: The location of the record.
    """
    return joinsubfields(d["852"])


def publisher(d):
    """
    Extracts the publisher information from the MARC21 record.

    Args:
        d (dict): The MARC21 record dictionary.

    Returns:
        str: The publisher information, or None if not found.
    """
    if "260" in d:
        return subfield(d["260"], "b")


def pubyear(d):
    """
    Extracts the publication year from a MARC21 record.

    Args:
        d (dict): The MARC21 record dictionary.

    Returns:
        str: The publication year, or None if not found.
    """
    if "260" in d:
        return subfield(d["260"], "c")


def uni(v):
    """
    Converts a byte string to a Unicode string.

    Parameters:
    v (bytes): The byte string to be converted.

    Returns:
    str: The converted Unicode string.

    Raises:
    None

    Examples:
    >>> uni(b'hello')
    'hello'
    >>> uni(None)
    ''
    """
    return "" if v is None else v.decode("utf-8", "replace")


# Indexing and searching
def make_index(basedir, ixdir, procs=4, limitmb=128, multisegment=True, glob="*.mrc"):
    """
    Create an index for MARC21 records.

    Args:
        basedir (str): The base directory containing the MARC21 files.
        ixdir (str): The directory to store the index.
        procs (int, optional): The number of processors to use for indexing. Defaults to 4.
        limitmb (int, optional): The memory limit per processor in megabytes. Defaults to 128.
        multisegment (bool, optional): Whether to use multisegment indexing. Defaults to True.
        glob (str, optional): The file pattern to match for indexing. Defaults to "*.mrc".

    Returns:
        None

    Raises:
        OSError: If the specified `ixdir` directory does not exist and cannot be created.

    Notes:
        This function creates an index for MARC21 records using the Whoosh library. It takes the base directory
        containing the MARC21 files (`basedir`), the directory to store the index (`ixdir`), and optional parameters
        for configuring the indexing process.

        The `procs` parameter specifies the number of processors to use for indexing. By default, it is set to 4.

        The `limitmb` parameter sets the memory limit per processor in megabytes. The default value is 128.

        The `multisegment` parameter determines whether to use multisegment indexing. If set to True (default), the
        index will be split into multiple segments for better performance.

        The `glob` parameter specifies the file pattern to match for indexing. By default, it is set to "*.mrc".

        If the specified `ixdir` directory does not exist, it will be created before creating the index.

        The function uses a multi-lingual stop words list for text analysis and defines a schema for the index
        containing fields for title, author, subject, file, and position.

        The MARC fields to extract are specified in the `mfields` set.

        The function prints the indexing configuration and starts the indexing process. It creates the index in the
        specified `ixdir` directory and uses the Whoosh writer to add documents to the index.

        After indexing is complete, the function returns None.
    """
    if not os.path.exists(ixdir):
        os.mkdir(ixdir)

    # Multi-lingual stop words
    stoplist = analysis.STOP_WORDS | set(
        "de la der und le die et en al no von di du da " "del zur ein".split()
    )
    # Schema
    ana = analysis.stemming_analyzer(stoplist=stoplist)
    schema = fields.Schema(
        title=fields.TEXT(analyzer=ana),
        author=fields.TEXT(phrase=False),
        subject=fields.TEXT(analyzer=ana, phrase=False),
        file=fields.STORED,
        pos=fields.STORED,
    )

    # MARC fields to extract
    mfields = set(subjectfields)  # Subjects
    mfields.update("100 110 111".split())  # Author
    mfields.add("245")  # Title

    print(f"Indexing with {procs} processor(s) and {limitmb} MB per processor")
    c = 0
    t = now()
    ix = index.create_in(ixdir, schema)
    with ix.writer(procs=procs, limitmb=limitmb, multisegment=multisegment) as w:
        filenames = [
            filename
            for filename in os.listdir(basedir)
            if fnmatch.fnmatch(filename, glob)
        ]
        for filename in filenames:
            path = os.path.join(basedir, filename)
            print("Indexing", path)
            f = open(path, "rb")
            for x, pos in read_file(f, mfields):
                w.add_document(
                    title=uni(title(x)),
                    author=uni(author(x)),
                    subject=uni(subjects(x)),
                    file=filename,
                    pos=pos,
                )
                c += 1
            f.close()
        print("Committing...")
    print("Indexed %d records in %0.02f minutes" % (c, (now() - t) / 60.0))


def print_record(no, basedir, filename, pos):
    """
    Print the record information.

    Args:
        no (int): The record number.
        basedir (str): The base directory.
        filename (str): The name of the file.
        pos (int): The position of the record.

    Returns:
        None

    Raises:
        None

    """
    path = os.path.join(basedir, filename)
    record = read_record(path, pos)
    print("% 5d. %s" % (no + 1, title(record)))
    print("      ", author(record))
    print("      ", subjects(record))
    isbn_num = isbn(record)
    if isbn_num:
        print(" ISBN:", isbn_num)
    print()


def search(qstring, ixdir, basedir, limit=None, optimize=True, scores=True):
    """
    Perform a search on the index using the given query string.

    Args:
        qstring (str): The query string to search for.
        ixdir (str): The directory path where the index is located.
        basedir (str): The base directory path.
        limit (int, optional): The maximum number of results to return. Defaults to None.
        optimize (bool, optional): Whether to optimize the search. Defaults to True.
        scores (bool, optional): Whether to include scores in the search results. Defaults to True.

    Returns:
        None

    Raises:
        None

    """
    ix = index.open_dir(ixdir)
    qp = qparser.QueryParser("title", ix.schema)
    q = qp.parse(qstring)

    with ix.searcher(weighting=scoring.PL2()) as s:
        if scores:
            r = s.search(q, limit=limit, optimize=optimize)
            for hit in r:
                print_record(hit.rank, basedir, hit["file"], hit["pos"])
            print(f"Found {len(r)} records in {r.runtime:0.06f} seconds")
        else:
            t = now()
            for i, docnum in enumerate(s.docs_for_query(q)):
                if not limit or i < limit:
                    fields = s.stored_fields(docnum)
                    print_record(i, basedir, fields["file"], fields["pos"])
            print("Found %d records in %0.06f seconds" % (i, now() - t))


if __name__ == "__main__":
    from optparse import OptionParser

    p = OptionParser(usage="usage: %prog [options] query")
    # Common options
    p.add_option(
        "-f",
        "--filedir",
        metavar="DIR",
        dest="basedir",
        help="Directory containing the .mrc files to index",
        default="data/HLOM",
    )
    p.add_option(
        "-d",
        "--dir",
        metavar="DIR",
        dest="ixdir",
        help="Directory containing the index",
        default="marc_index",
    )

    # Indexing options
    p.add_option(
        "-i",
        "--index",
        dest="index",
        help="Index the records",
        action="store_true",
        default=False,
    )
    p.add_option(
        "-p",
        "--procs",
        metavar="NPROCS",
        dest="procs",
        help="Number of processors to use",
        default="1",
    )
    p.add_option(
        "-m",
        "--mb",
        metavar="MB",
        dest="limitmb",
        help="Limit the indexer to this many MB of memory per writer",
        default="128",
    )
    p.add_option(
        "-M",
        "--merge-segments",
        dest="multisegment",
        help="If indexing with multiproc, merge the segments after indexing",
        action="store_false",
        default=True,
    )
    p.add_option(
        "-g",
        "--match",
        metavar="GLOB",
        dest="glob",
        help="Only index file names matching the given pattern",
        default="*.mrc",
    )

    # Search options
    p.add_option(
        "-l",
        "--limit",
        metavar="NHITS",
        dest="limit",
        help="Maximum number of search results to print (0=no limit)",
        default="10",
    )
    p.add_option(
        "-O",
        "--no-optimize",
        dest="optimize",
        help="Turn off searcher optimization (for debugging)",
        action="store_false",
        default=True,
    )
    p.add_option(
        "-s",
        "--scoring",
        dest="scores",
        help="Score the results",
        action="store_true",
        default=False,
    )

    options, args = p.parse_args()

    if options.index:
        make_index(
            options.basedir,
            options.ixdir,
            procs=int(options.procs),
            limitmb=int(options.limitmb),
            multisegment=options.multisegment,
            glob=options.glob,
        )

    if args:
        qstring = " ".join(args).decode("utf-8")
        limit = int(options.limit)
        if limit < 1:
            limit = None
        search(
            qstring,
            options.ixdir,
            options.basedir,
            limit=limit,
            optimize=options.optimize,
            scores=options.scores,
        )
