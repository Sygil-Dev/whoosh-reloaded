# module pyparsing.py
#
# Copyright (c) 2003-2009  Paul T. McGuire
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
# CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
# TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# from __future__ import generators

__doc__ = """
pyparsing module - Classes and methods to define and execute parsing grammars

The pyparsing module is an alternative approach to creating and executing simple grammars,
vs. the traditional lex/yacc approach, or the use of regular expressions.  With pyparsing, you
don't need to learn a new syntax for defining grammars or matching expressions - the parsing module
provides a library of classes that you use to construct the grammar directly in Python.

Here is a program to parse "Hello, World!" (or any greeting of the form "<salutation>, <addressee>!")::

    from pyparsing import Word, alphas

    # define grammar of a greeting
    greet = Word( alphas ) + "," + Word( alphas ) + "!"

    hello = "Hello, World!"
    print hello, "->", greet.parse_string( hello )

The program outputs the following::

    Hello, World! -> ['Hello', ',', 'World', '!']

The Python representation of the grammar is quite readable, owing to the self-explanatory
class names, and the use of '+', '|' and '^' operators.

The parsed results returned from parse_string() can be accessed as a nested list, a dictionary, or an
object with named attributes.

The pyparsing module handles some of the problems that are typically vexing when writing text parsers:
 - extra or missing whitespace (the above program will also handle "Hello,World!", "Hello  ,  World  !", etc.)
 - quoted strings
 - embedded comments
"""

__version__ = "1.5.2"
__versionTime__ = "17 February 2009 19:45"
__author__ = "Paul McGuire <ptmcg@users.sourceforge.net>"

import copy
import re
import sre_constants
import string
import sys
import warnings
from weakref import ref as wkref

from whoosh.support import unicode

# ~ sys.stderr.write( "testing pyparsing module, version %s, %s\n" % (__version__,__versionTime__ ) )

__all__ = [
    "And",
    "CaselessKeyword",
    "CaselessLiteral",
    "CharsNotIn",
    "Combine",
    "Dict",
    "Each",
    "Empty",
    "FollowedBy",
    "Forward",
    "GoToColumn",
    "Group",
    "Keyword",
    "LineEnd",
    "LineStart",
    "Literal",
    "MatchFirst",
    "NoMatch",
    "NotAny",
    "OneOrMore",
    "OnlyOnce",
    "Optional",
    "Or",
    "ParseBaseException",
    "ParseElementEnhance",
    "ParseException",
    "ParseExpression",
    "ParseFatalException",
    "ParseResults",
    "ParseSyntaxException",
    "ParserElement",
    "QuotedString",
    "RecursiveGrammarException",
    "Regex",
    "SkipTo",
    "StringEnd",
    "StringStart",
    "Suppress",
    "Token",
    "TokenConverter",
    "Upcase",
    "White",
    "Word",
    "WordEnd",
    "WordStart",
    "ZeroOrMore",
    "alphanums",
    "alphas",
    "alphas8bit",
    "anyCloseTag",
    "anyOpenTag",
    "cStyleComment",
    "col",
    "commaSeparatedList",
    "commonHTMLEntity",
    "counted_array",
    "cppStyleComment",
    "dblQuotedString",
    "dblSlashComment",
    "delimited_list",
    "dict_of",
    "downcase_tokens",
    "empty",
    "get_tokens_end_loc",
    "hexnums",
    "htmlComment",
    "javaStyleComment",
    "keep_original_text",
    "line",
    "lineEnd",
    "lineStart",
    "lineno",
    "make_html_tags",
    "make_xml_tags",
    "match_only_at_col",
    "match_previous_expr",
    "match_previous_literal",
    "nested_expr",
    "null_debug_action",
    "nums",
    "one_of",
    "opAssoc",
    "operator_precedence",
    "printables",
    "punc8bit",
    "pythonStyleComment",
    "quotedString",
    "remove_quotes",
    "replaceHTMLEntity",
    "replace_with",
    "restOfLine",
    "sglQuotedString",
    "srange",
    "stringEnd",
    "stringStart",
    "trace_parse_action",
    "unicodeString",
    "upcase_tokens",
    "with_attribute",
    "indented_block",
    "original_text_for",
]


# _MAX_INT = sys.maxsize
# basestring = str

# _ustr = str
# unichr = chr

# _str2dict = set


def _xml_escape(data):
    """Escape &, <, >, ", ', etc. in a string of data."""

    # ampersand must be replaced first
    from_symbols = "&><\"'"
    to_symbols = ["&" + s + ";" for s in "amp gt lt quot apos".split()]
    for from_, to_ in zip(from_symbols, to_symbols):
        data = data.replace(from_, to_)
    return data


class _Constants:
    pass


alphas = string.ascii_lowercase + string.ascii_uppercase

nums = string.digits
hexnums = nums + "ABCDEFabcdef"
alphanums = alphas + nums
_bslash = chr(92)
printables = "".join([c for c in string.printable if c not in string.whitespace])


class ParseBaseException(Exception):
    """base exception class for all parsing runtime exceptions"""

    # Performance tuning: we construct a *lot* of these, so keep this
    # constructor as small and fast as possible
    def __init__(self, pstr, loc=0, msg=None, elem=None):
        self.loc = loc
        if msg is None:
            self.msg = pstr
            self.pstr = ""
        else:
            self.msg = msg
            self.pstr = pstr
        self.parserElement = elem

    def __getattr__(self, aname):
        """supported attributes by name are:
        - lineno - returns the line number of the exception text
        - col - returns the column number of the exception text
        - line - returns the line containing the exception text
        """
        if aname == "lineno":
            return lineno(self.loc, self.pstr)
        elif aname in ("col", "column"):
            return col(self.loc, self.pstr)
        elif aname == "line":
            return line(self.loc, self.pstr)
        else:
            raise AttributeError(aname)

    def __str__(self):
        return "%s (at char %d), (line:%d, col:%d)" % (
            self.msg,
            self.loc,
            self.lineno,
            self.column,
        )

    def __repr__(self):
        return str(self)

    def markInputline(self, marker_string=">!<"):
        """Extracts the exception line from the input string, and marks
        the location of the exception with a special symbol.
        """
        line_str = self.line
        line_column = self.column - 1
        if marker_string:
            line_str = "".join(
                [line_str[:line_column], marker_string, line_str[line_column:]]
            )
        return line_str.strip()

    def __dir__(self):
        return (
            "loc msg pstr parserElement lineno col line "
            "markInputLine __str__ __repr__".split()
        )


class ParseException(ParseBaseException):
    """exception thrown when parse expressions don't match class;
    supported attributes by name are:
     - lineno - returns the line number of the exception text
     - col - returns the column number of the exception text
     - line - returns the line containing the exception text
    """

    pass


class ParseFatalException(ParseBaseException):
    """user-throwable exception thrown when inconsistent parse content
    is found; stops all parsing immediately"""

    pass


class ParseSyntaxException(ParseFatalException):
    """just like ParseFatalException, but thrown internally when an
    ErrorStop indicates that parsing is to stop immediately because
    an unbacktrackable syntax error has been found"""

    def __init__(self, pe):
        super().__init__(pe.pstr, pe.loc, pe.msg, pe.parserElement)


# ~ class ReparseException(ParseBaseException):
# ~ """Experimental class - parse actions can raise this exception to cause
# ~ pyparsing to reparse the input string:
# ~ - with a modified input string, and/or
# ~ - with a modified start location
# ~ Set the values of the ReparseException in the constructor, and raise the
# ~ exception in a parse action to cause pyparsing to use the new string/location.
# ~ Setting the values as None causes no change to be made.
# ~ """
# ~ def __init_( self, newstring, restart_loc ):
# ~ self.newParseText = newstring
# ~ self.reparseLoc = restart_loc


class RecursiveGrammarException(Exception):
    """exception thrown by validate() if the grammar could be improperly recursive"""

    def __init__(self, parse_element_list):
        self.parseElementTrace = parse_element_list

    def __str__(self):
        return f"RecursiveGrammarException: {self.parseElementTrace}"


class _ParseResultsWithOffset:
    def __init__(self, p1, p2):
        self.tup = (p1, p2)

    def __getitem__(self, i):
        return self.tup[i]

    def __repr__(self):
        return repr(self.tup)

    def set_offset(self, i):
        self.tup = (self.tup[0], i)


class ParseResults:
    """Structured parse results, to provide multiple means of access to the parsed data:
    - as a list (len(results))
    - by list index (results[0], results[1], etc.)
    - by attribute (results.<results_name>)
    """

    __slots__ = (
        "__toklist",
        "__tokdict",
        "__doinit",
        "__name",
        "__parent",
        "__accum_names",
        "__weakref__",
    )

    def __new__(cls, toklist, name=None, as_list=True, modal=True):
        if isinstance(toklist, cls):
            return toklist
        retobj = object.__new__(cls)
        retobj.__doinit = True
        return retobj

    # Performance tuning: we construct a *lot* of these, so keep this
    # constructor as small and fast as possible
    def __init__(self, toklist, name=None, as_list=True, modal=True):
        if self.__doinit:
            self.__doinit = False
            self.__name = None
            self.__parent = None
            self.__accum_names = {}
            self.__toklist = (
                list(toklist) if not isinstance(toklist, list) else toklist[:]
            )
            self.__tokdict = {}

        if name:
            if not modal:
                self.__accum_names[name] = 0
            if isinstance(name, int):
                name = str(name)
            self.__name = name
            if toklist:
                if isinstance(toklist, str):
                    toklist = [toklist]
                if as_list:
                    if isinstance(toklist, ParseResults):
                        setattr(self, name, _ParseResultsWithOffset(toklist.copy(), 0))
                    else:
                        setattr(
                            self,
                            name,
                            _ParseResultsWithOffset(ParseResults(toklist[0]), 0),
                        )
                    getattr(self, name).__name = name
                else:
                    try:
                        setattr(self, name, toklist[0])
                    except (KeyError, TypeError, IndexError):
                        setattr(self, name, toklist)

    def __getitem__(self, i):
        if isinstance(i, (int, slice)):
            return self.__toklist[i]
        else:
            if i not in self.__accum_names:
                return self.__tokdict[i][-1][0]
            else:
                return ParseResults([v[0] for v in self.__tokdict[i]])

    def __setitem__(self, k, v):
        if isinstance(v, _ParseResultsWithOffset):
            self.__tokdict[k] = self.__tokdict.get(k, []) + [v]
            sub = v[0]
        elif isinstance(k, int):
            self.__toklist[k] = v
            sub = v
        else:
            self.__tokdict[k] = self.__tokdict.get(k, []) + [
                _ParseResultsWithOffset(v, 0)
            ]
            sub = v
        if isinstance(sub, ParseResults):
            sub.__parent = wkref(self)

    def __delitem__(self, i):
        if isinstance(i, (int, slice)):
            mylen = len(self.__toklist)
            del self.__toklist[i]

            # convert int to slice
            if isinstance(i, int):
                if i < 0:
                    i += mylen
                i = slice(i, i + 1)
            # get removed indices
            removed = list(range(*i.indices(mylen)))
            removed.reverse()
            # fixup indices in token dictionary
            for name in self.__tokdict:
                occurrences = self.__tokdict[name]
                for j in removed:
                    for k, (value, position) in enumerate(occurrences):
                        occurrences[k] = _ParseResultsWithOffset(
                            value, position - (position > j)
                        )
        else:
            del self.__tokdict[i]

    def __contains__(self, k):
        return k in self.__tokdict

    def __len__(self):
        return len(self.__toklist)

    def __bool__(self):
        return len(self.__toklist) > 0

    __nonzero__ = __bool__

    def __iter__(self):
        return iter(self.__toklist)

    def __reversed__(self):
        return iter(reversed(self.__toklist))

    def keys(self):
        """Returns all named result keys."""
        return self.__tokdict.keys()

    def pop(self, index=-1):
        """Removes and returns item at specified index (default=last).
        Will work with either numeric indices or dict-key indicies."""
        ret = self[index]
        del self[index]
        return ret

    def get(self, key, default_value=None):
        """Returns named result matching the given key, or if there is no
        such name, then returns the given default_value or None if no
        default_value is specified."""
        if key in self:
            return self[key]
        else:
            return default_value

    def insert(self, index, ins_str):
        self.__toklist.insert(index, ins_str)
        # fixup indices in token dictionary
        for name in self.__tokdict:
            occurrences = self.__tokdict[name]
            for k, (value, position) in enumerate(occurrences):
                occurrences[k] = _ParseResultsWithOffset(
                    value, position + (position > index)
                )

    def items(self):
        """Returns all named result keys and values as a list of tuples."""
        return [(k, self[k]) for k in self.__tokdict]

    def values(self):
        """Returns all named result values."""
        return [v[-1][0] for v in self.__tokdict.values()]

    def __getattr__(self, name):
        if name not in self.__slots__:
            if name in self.__tokdict:
                if name not in self.__accum_names:
                    return self.__tokdict[name][-1][0]
                else:
                    return ParseResults([v[0] for v in self.__tokdict[name]])
            else:
                return ""
        return None

    def __add__(self, other):
        ret = self.copy()
        ret += other
        return ret

    def __iadd__(self, other):
        if other.__tokdict:
            offset = len(self.__toklist)
            addoffset = lambda a: (a < 0 and offset) or (a + offset)
            otheritems = other.__tokdict.items()
            otherdictitems = [
                (k, _ParseResultsWithOffset(v[0], addoffset(v[1])))
                for (k, vlist) in otheritems
                for v in vlist
            ]
            for k, v in otherdictitems:
                self[k] = v
                if isinstance(v[0], ParseResults):
                    v[0].__parent = wkref(self)

        self.__toklist += other.__toklist
        self.__accum_names.update(other.__accum_names)
        del other
        return self

    def __repr__(self):
        return f"({repr(self.__toklist)}, {repr(self.__tokdict)})"

    def __str__(self):
        out = "["
        sep = ""
        for i in self.__toklist:
            if isinstance(i, ParseResults):
                out += sep + str(i)
            else:
                out += sep + repr(i)
            sep = ", "
        out += "]"
        return out

    def _as_string_list(self, sep=""):
        out = []
        for item in self.__toklist:
            if out and sep:
                out.append(sep)
            if isinstance(item, ParseResults):
                out += item._as_string_list()
            else:
                out.append(str(item))
        return out

    def as_list(self):
        """Returns the parse results as a nested list of matching tokens, all converted to strings."""
        out = []
        for res in self.__toklist:
            if isinstance(res, ParseResults):
                out.append(res.as_list())
            else:
                out.append(res)
        return out

    def as_dict(self):
        """Returns the named parse results as dictionary."""
        return dict(self.items())

    def copy(self):
        """Returns a new copy of a ParseResults object."""
        ret = ParseResults(self.__toklist)
        ret.__tokdict = self.__tokdict.copy()
        ret.__parent = self.__parent
        ret.__accum_names.update(self.__accum_names)
        ret.__name = self.__name
        return ret

    def as_xml(self, doctag=None, named_items_only=False, indent="", formatted=True):
        """Returns the parse results as XML. Tags are created for tokens and lists that have defined results names."""
        nl = "\n"
        out = []
        named_items = {v[1]: k for (k, vlist) in self.__tokdict.items() for v in vlist}
        next_level_indent = indent + "  "

        # collapse out indents if formatting is not desired
        if not formatted:
            indent = ""
            next_level_indent = ""
            nl = ""

        self_tag = None
        if doctag is not None:
            self_tag = doctag
        else:
            if self.__name:
                self_tag = self.__name

        if not self_tag:
            if named_items_only:
                return ""
            else:
                self_tag = "ITEM"

        out += [nl, indent, "<", self_tag, ">"]

        worklist = self.__toklist
        for i, res in enumerate(worklist):
            if isinstance(res, ParseResults):
                if i in named_items:
                    out += [
                        res.as_xml(
                            named_items[i],
                            named_items_only and doctag is None,
                            next_level_indent,
                            formatted,
                        )
                    ]
                else:
                    out += [
                        res.as_xml(
                            None,
                            named_items_only and doctag is None,
                            next_level_indent,
                            formatted,
                        )
                    ]
            else:
                # individual token, see if there is a name for it
                res_tag = None
                if i in named_items:
                    res_tag = named_items[i]
                if not res_tag:
                    if named_items_only:
                        continue
                    else:
                        res_tag = "ITEM"
                xml_body_text = _xml_escape(str(res))
                out += [
                    nl,
                    next_level_indent,
                    "<",
                    res_tag,
                    ">",
                    xml_body_text,
                    "</",
                    res_tag,
                    ">",
                ]

        out += [nl, indent, "</", self_tag, ">"]
        return "".join(out)

    def __lookup(self, sub):
        for k, vlist in self.__tokdict.items():
            for v, loc in vlist:
                if sub is v:
                    return k
        return None

    def get_name(self):
        """Returns the results name for this token expression."""
        if self.__name:
            return self.__name
        elif self.__parent:
            par = self.__parent()
            if par:
                return par.__lookup(self)
            else:
                return None
        elif (
            len(self) == 1
            and len(self.__tokdict) == 1
            and self.__tokdict.values()[0][0][1] in (0, -1)
        ):
            return self.__tokdict.keys()[0]
        else:
            return None

    def dump(self, indent="", depth=0):
        """Diagnostic method for listing out the contents of a ParseResults.
        Accepts an optional indent argument so that this string can be embedded
        in a nested display of other data."""
        out = []
        out.append(indent + str(self.as_list()))
        keys = self.items()
        keys.sort()
        for k, v in keys:
            if out:
                out.append("\n")
            out.append(f"{indent}{'  ' * depth}- {k}: ")
            if isinstance(v, ParseResults):
                if v.keys():
                    # ~ out.append('\n')
                    out.append(v.dump(indent, depth + 1))
                    # ~ out.append('\n')
                else:
                    out.append(str(v))
            else:
                out.append(str(v))
        # ~ out.append('\n')
        return "".join(out)

    # add support for pickle protocol
    def __getstate__(self):
        return (
            self.__toklist,
            (
                self.__tokdict.copy(),
                self.__parent is not None and self.__parent() or None,
                self.__accum_names,
                self.__name,
            ),
        )

    def __setstate__(self, state):
        self.__toklist = state[0]
        self.__tokdict, par, in_accum_names, self.__name = state[1]
        self.__accum_names = {}
        self.__accum_names.update(in_accum_names)
        if par is not None:
            self.__parent = wkref(par)
        else:
            self.__parent = None

    def __dir__(self):
        return dir(super()) + self.keys()


def col(loc, strg):
    """Returns current column within a string, counting newlines as line separators.
    The first column is number 1.

    Note: the default parsing behavior is to expand tabs in the input string
    before starting the parsing process.  See L{I{ParserElement.parse_string}<ParserElement.parse_string>} for more information
    on parsing strings containing <TAB>s, and suggested methods to maintain a
    consistent view of the parsed string, the parse location, and line and column
    positions within the parsed string.
    """
    return (
        (loc < len(strg) and strg[loc] == "\n") and 1 or loc - strg.rfind("\n", 0, loc)
    )


def lineno(loc, strg):
    """Returns current line number within a string, counting newlines as line separators.
    The first line is number 1.

    Note: the default parsing behavior is to expand tabs in the input string
    before starting the parsing process.  See L{I{ParserElement.parse_string}<ParserElement.parse_string>} for more information
    on parsing strings containing <TAB>s, and suggested methods to maintain a
    consistent view of the parsed string, the parse location, and line and column
    positions within the parsed string.
    """
    return strg.count("\n", 0, loc) + 1


def line(loc, strg):
    """Returns the line of text containing loc within a string, counting newlines as line separators."""
    last_cr = strg.rfind("\n", 0, loc)
    next_cr = strg.find("\n", loc)
    if next_cr > 0:
        return strg[last_cr + 1 : next_cr]
    else:
        return strg[last_cr + 1 :]


def _default_start_debug_action(instring, loc, expr):
    print(
        "Match "
        + str(expr)
        + " at loc "
        + str(loc)
        + "(%d,%d)" % (lineno(loc, instring), col(loc, instring))
    )


def _default_success_debug_action(instring, startloc, endloc, expr, toks):
    print("Matched " + str(expr) + " -> " + str(toks.as_list()))


def _default_exception_debug_action(instring, loc, expr, exc):
    print("Exception raised:" + str(exc))


def null_debug_action(*args):
    """'Do-nothing' debug action, to suppress debugging output during parsing."""
    pass


class ParserElement:
    """Abstract base level parser element class."""

    DEFAULT_WHITE_CHARS = " \n\t\r"

    def set_default_whitespace_chars(chars):
        """Overrides the default whitespace chars"""
        ParserElement.DEFAULT_WHITE_CHARS = chars

    set_default_whitespace_chars = staticmethod(set_default_whitespace_chars)

    def __init__(self, savelist=False):
        """Initialize the ParserElement.

        Args:
            savelist (bool, optional): Whether to save the results as a list. Defaults to False.
        """
        self.parse_action = []
        self.fail_action = None
        self.str_repr = None
        self.results_name = None
        self.saveas_list = savelist
        self.skip_whitespace = True
        self.white_chars = ParserElement.DEFAULT_WHITE_CHARS
        self.copy_default_white_chars = True
        self.may_return_empty = False  # used when checking for left-recursion
        self.keep_tabs = False
        self.ignore_exprs = []
        self.debug = False
        self.streamlined = False
        self.may_index_error = True  # used to optimize exception handling for subclasses that don't advance parse index
        self.errmsg = ""
        self.modal_results = True  # used to mark results names as modal (report only last) or cumulative (list all)
        self.debug_actions = (None, None, None)  # custom debug actions
        self.re = None
        self.call_preparse = True  # used to avoid redundant calls to pre_parse
        self.call_during_try = False

    def copy(self):
        """Make a copy of this ParserElement.

        Returns:
            ParserElement: A copy of the original ParserElement.
        """
        cpy = copy.copy(self)
        cpy.parse_action = self.parse_action[:]
        cpy.ignore_exprs = self.ignore_exprs[:]
        if self.copy_default_white_chars:
            cpy.white_chars = ParserElement.DEFAULT_WHITE_CHARS
        return cpy

    def set_name(self, name):
        """Define name for this expression, for use in debugging.

        Args:
            name (str): The name of the expression.

        Returns:
            ParserElement: The ParserElement object.
        """
        self.name = name
        self.errmsg = "Expected " + self.name
        if hasattr(self, "exception"):
            self.exception.msg = self.errmsg
        return self

    def set_results_name(self, name, list_all_matches=False):
        """Define name for referencing matching tokens as a nested attribute
        of the returned parse results.

        Args:
            name (str): The name of the results.
            list_all_matches (bool, optional): Whether to list all matches. Defaults to False.

        Returns:
            ParserElement: A copy of the original ParserElement with the results name set.
        """
        newself = self.copy()
        newself.results_name = name
        newself.modal_results = not list_all_matches
        return newself

    def set_break(self, break_flag=True):
        """Method to invoke the Python pdb debugger when this element is
        about to be parsed.

        Args:
            break_flag (bool, optional): Whether to enable the debugger. Defaults to True.

        Returns:
            ParserElement: The ParserElement object.
        """
        if break_flag:
            _parse_method = self._parse

            def breaker(instring, loc, do_actions=True, call_pre_parse=True):
                import pdb

                pdb.set_trace()
                return _parse_method(instring, loc, do_actions, call_pre_parse)

            breaker._originalParseMethod = _parse_method
            self._parse = breaker
        else:
            if hasattr(self._parse, "_originalParseMethod"):
                self._parse = self._parse._originalParseMethod
        return self

    def _normalize_parse_action_args(f):
        """Internal method used to decorate parse actions that take fewer than 3 arguments,
        so that all parse actions can be called as f(s,l,t).

        Args:
            f (callable): The parse action function.

        Returns:
            callable: The normalized parse action function.
        """
        STAR_ARGS = 4

        try:
            restore = None
            if isinstance(f, type):
                restore = f
                f = f.__init__

            if f.code.co_flags & STAR_ARGS:
                return f
            numargs = f.code.co_argcount

            if hasattr(f, "__self__"):
                numargs -= 1
            if restore:
                f = restore
        except AttributeError:
            try:
                # not a function, must be a callable object, get info from the
                # im_func binding of its bound __call__ method
                if f.__code__.co_flags & STAR_ARGS:
                    return f
                numargs = f.__code__.co_argcount

                if hasattr(f.__call__, "__self__"):
                    numargs -= 0
            except AttributeError:
                # not a bound method, get info directly from __call__ method
                if f.__call__.__code__.co_flags & STAR_ARGS:
                    return f
                numargs = f.__call__.__code__.co_argcount

                if hasattr(f.__call__, "__self__"):
                    numargs -= 1

        # ~ print ("adding function %s with %d args" % (f.func_name,numargs))
        if numargs == 3:
            return f
        else:
            if numargs > 3:

                def tmp(s, l, t):
                    return f(f.__call__.__self__, s, l, t)

            if numargs == 2:

                def tmp(_, l, t):
                    return f(l, t)

            elif numargs == 1:

                def tmp(_, __, t):
                    return f(t)

            else:

                def tmp(_, __, ___):
                    return f()

            try:
                tmp.__name__ = f.__name__
            except (AttributeError, TypeError):
                # no need for special handling if attribute doesnt exist
                pass
            try:
                tmp.__doc__ = f.__doc__
            except (AttributeError, TypeError):
                # no need for special handling if attribute doesnt exist
                pass
            try:
                tmp.__dict__.update(f.__dict__)
            except (AttributeError, TypeError):
                # no need for special handling if attribute doesnt exist
                pass
            return tmp

    _normalize_parse_action_args = staticmethod(_normalize_parse_action_args)

    def set_parse_action(self, *fns, **kwargs):
        """Define action to perform when successfully matching parse element definition.
        Parse action fn is a callable method with 0-3 arguments, called as fn(s,loc,toks),
        fn(loc,toks), fn(toks), or just fn(), where:
         - s   = the original string being parsed (see note below)
         - loc = the location of the matching substring
         - toks = a list of the matched tokens, packaged as a ParseResults object
        If the functions in fns modify the tokens, they can return them as the return
        value from fn, and the modified list of tokens will replace the original.
        Otherwise, fn does not need to return any value.

        Note: the default parsing behavior is to expand tabs in the input string
        before starting the parsing process.  See L{I{parse_string}<parse_string>} for more information
        on parsing strings containing <TAB>s, and suggested methods to maintain a
        consistent view of the parsed string, the parse location, and line and column
        positions within the parsed string.
        """
        self.parse_action = list(map(self._normalize_parse_action_args, list(fns)))
        self.call_during_try = "call_during_try" in kwargs and kwargs["call_during_try"]
        return self

    def add_parse_action(self, *fns, **kwargs):
        """Add parse action to expression's list of parse actions.

        Args:
            *fns (callable): The parse action functions.
            **kwargs: Additional keyword arguments.

        Returns:
            ParserElement: The ParserElement object.
        """
        self.parse_action += list(map(self._normalize_parse_action_args, list(fns)))
        self.call_during_try = self.call_during_try or (
            "call_during_try" in kwargs and kwargs["call_during_try"]
        )
        return self

    def set_fail_action(self, fn):
        """Define action to perform if parsing fails at this expression.
        Fail acton fn is a callable function that takes the arguments
        fn(s,loc,expr,err) where:
         - s = string being parsed
         - loc = location where expression match was attempted and failed
         - expr = the parse expression that failed
         - err = the exception thrown
        The function returns no value.  It may throw ParseFatalException
        if it is desired to stop parsing immediately."""
        self.fail_action = fn
        return self

    def _skip_ignorables(self, instring, loc):
        """Skip over ignored expressions.

        Args:
            instring (str): The input string.
            loc (int): The current location in the string.

        Returns:
            int: The updated location.
        """
        exprs_found = True
        while exprs_found:
            exprs_found = False
            for e in self.ignore_exprs:
                try:
                    while 1:
                        loc, dummy = e._parse(instring, loc)
                        exprs_found = True
                except ParseException:
                    pass
        return loc

    def pre_parse(self, instring, loc):
        """Perform pre-parsing operations.

        Args:
            instring (str): The input string.
            loc (int): The current location in the string.

        Returns:
            int: The updated location.
        """
        if self.ignore_exprs:
            loc = self._skip_ignorables(instring, loc)

        if self.skip_whitespace:
            wt = self.white_chars
            instrlen = len(instring)
            while loc < instrlen and instring[loc] in wt:
                loc += 1

        return loc

    def parse_impl(self, instring, loc, do_actions=True):
        """Implementation of the parsing logic.

        Args:
            instring (str): The input string.
            loc (int): The current location in the string.
            do_actions (bool, optional): Whether to perform parse actions. Defaults to True.

        Returns:
            tuple: The updated location and the list of matched tokens.
        """
        return loc, []

    def post_parse(self, instring, loc, tokenlist):
        """Perform post-parsing operations.

        Args:
            instring (str): The input string.
            loc (int): The current location in the string.
            tokenlist (list): The list of matched tokens.

        Returns:
            list: The updated list of tokens.
        """
        return tokenlist

    # ~ @profile
    def _parse_no_cache(self, instring, loc, do_actions=True, call_pre_parse=True):
        """Parse the input string without using the cache.

        Args:
            instring (str): The input string.
            loc (int): The current location in the string.
            do_actions (bool, optional): Whether to perform parse actions. Defaults to True.
            call_pre_parse (bool, optional): Whether to call the pre_parse method. Defaults to True.
        """
        # Implementation details omitted for brevity
        pass
        debugging = self.debug  # and do_actions )

        if debugging or self.fail_action:
            # ~ print ("Match",self,"at loc",loc,"(%d,%d)" % ( lineno(loc,instring), col(loc,instring) ))
            if self.debug_actions[0]:
                self.debug_actions[0](instring, loc, self)
            if call_pre_parse and self.call_preparse:
                preloc = self.pre_parse(instring, loc)
            else:
                preloc = loc
            tokens_start = loc
            try:
                try:
                    loc, tokens = self.parse_impl(instring, preloc, do_actions)
                except IndexError:
                    raise ParseException(instring, len(instring), self.errmsg, self)
            except ParseBaseException as err:
                # ~ print ("Exception raised:", err)
                if self.debug_actions[2]:
                    self.debug_actions[2](instring, tokens_start, self, err)
                if self.fail_action:
                    self.fail_action(instring, tokens_start, self, err)
                raise
        else:
            if call_pre_parse and self.call_preparse:
                preloc = self.pre_parse(instring, loc)
            else:
                preloc = loc
            tokens_start = loc
            if self.may_index_error or loc >= len(instring):
                try:
                    loc, tokens = self.parse_impl(instring, preloc, do_actions)
                except IndexError:
                    raise ParseException(instring, len(instring), self.errmsg, self)
            else:
                loc, tokens = self.parse_impl(instring, preloc, do_actions)

        tokens = self.post_parse(instring, loc, tokens)

        ret_tokens = ParseResults(
            tokens,
            self.results_name,
            as_list=self.saveas_list,
            modal=self.modal_results,
        )
        if self.parse_action and (do_actions or self.call_during_try):
            if debugging:
                try:
                    for fn in self.parse_action:
                        tokens = fn(instring, tokens_start, ret_tokens)
                        if tokens is not None:
                            ret_tokens = ParseResults(
                                tokens,
                                self.results_name,
                                as_list=self.saveas_list
                                and isinstance(tokens, (ParseResults, list)),
                                modal=self.modal_results,
                            )
                except ParseBaseException as err:
                    # print ("Exception raised in user parse action:", err)
                    if self.debug_actions[2]:
                        self.debug_actions[2](instring, tokens_start, self, err)
                    raise
            else:
                for fn in self.parse_action:
                    tokens = fn(instring, tokens_start, ret_tokens)
                    if tokens is not None:
                        ret_tokens = ParseResults(
                            tokens,
                            self.results_name,
                            as_list=self.saveas_list
                            and isinstance(tokens, (ParseResults, list)),
                            modal=self.modal_results,
                        )

        if debugging:
            # ~ print ("Matched",self,"->",ret_tokens.as_list())
            if self.debug_actions[1]:
                self.debug_actions[1](instring, tokens_start, loc, self, ret_tokens)

        return loc, ret_tokens

    def try_parse(self, instring, loc):
        try:
            return self._parse(instring, loc, do_actions=False)[0]
        except ParseFatalException:
            raise ParseException(instring, loc, self.errmsg, self)

    # this method gets repeatedly called during backtracking with the same arguments -
    # we can cache these arguments and save ourselves the trouble of re-parsing the contained expression
    def _parse_cache(self, instring, loc, do_actions=True, call_pre_parse=True):
        lookup = (self, instring, loc, call_pre_parse, do_actions)
        if lookup in ParserElement._expr_arg_cache:
            value = ParserElement._expr_arg_cache[lookup]
            if isinstance(value, Exception):
                raise value
            return value
        else:
            try:
                value = self._parse_no_cache(instring, loc, do_actions, call_pre_parse)
                ParserElement._expr_arg_cache[lookup] = (value[0], value[1].copy())
                return value
            except ParseBaseException as pe:
                ParserElement._expr_arg_cache[lookup] = pe
                raise

    _parse = _parse_no_cache

    # argument cache for optimizing repeated calls when backtracking through recursive expressions
    _expr_arg_cache = {}

    def reset_cache():
        ParserElement._expr_arg_cache.clear()

    reset_cache = staticmethod(reset_cache)

    _packrat_enabled = False

    def enable_packrat():
        """Enables "packrat" parsing, which adds memoizing to the parsing logic.
        Repeated parse attempts at the same string location (which happens
        often in many complex grammars) can immediately return a cached value,
        instead of re-executing parsing/validating code.  Memoizing is done of
        both valid results and parsing exceptions.

        This speedup may break existing programs that use parse actions that
        have side-effects.  For this reason, packrat parsing is disabled when
        you first import pyparsing.  To activate the packrat feature, your
        program must call the class method ParserElement.enable_packrat().  If
        your program uses psyco to "compile as you go", you must call
        enable_packrat before calling psyco.full().  If you do not do this,
        Python will crash.  For best results, call enable_packrat() immediately
        after importing pyparsing.
        """
        if not ParserElement._packrat_enabled:
            ParserElement._packrat_enabled = True
            ParserElement._parse = ParserElement._parse_cache

    enable_packrat = staticmethod(enable_packrat)

    def parse_string(self, instring, parse_all=False):
        """Execute the parse expression with the given string.
        This is the main interface to the client code, once the complete
        expression has been built.

        If you want the grammar to require that the entire input string be
        successfully parsed, then set parse_all to True (equivalent to ending
        the grammar with StringEnd()).

        Note: parse_string implicitly calls expandtabs() on the input string,
        in order to report proper column numbers in parse actions.
        If the input string contains tabs and
        the grammar uses parse actions that use the loc argument to index into the
        string being parsed, you can ensure you have a consistent view of the input
        string by:
         - calling parse_with_tabs on your grammar before calling parse_string
           (see L{I{parse_with_tabs}<parse_with_tabs>})
         - define your parse action using the full (s,loc,toks) signature, and
           reference the input string using the parse action's s argument
         - explictly expand the tabs in your input string before calling
           parse_string
        """
        ParserElement.reset_cache()
        if not self.streamlined:
            self.streamline()
            # ~ self.saveas_list = True
        for e in self.ignore_exprs:
            e.streamline()
        if not self.keep_tabs:
            instring = instring.expandtabs()
        try:
            loc, tokens = self._parse(instring, 0)
            if parse_all:
                loc = self.pre_parse(instring, loc)
                StringEnd()._parse(instring, loc)
        except ParseBaseException as exc:
            # catch and re-raise exception from here, clears out pyparsing internal stack trace
            raise exc
        else:
            return tokens

    def scan_string(self, instring, max_matches=sys.maxsize):
        """Scan the input string for expression matches.  Each match will return the
        matching tokens, start location, and end location.  May be called with optional
        max_matches argument, to clip scanning after 'n' matches are found.

        Note that the start and end locations are reported relative to the string
        being parsed.  See L{I{parse_string}<parse_string>} for more information on parsing
        strings with embedded tabs."""
        if not self.streamlined:
            self.streamline()
        for e in self.ignore_exprs:
            e.streamline()

        if not self.keep_tabs:
            instring = str(instring).expandtabs()
        instrlen = len(instring)
        loc = 0
        preparse_fn = self.pre_parse
        parse_fn = self._parse
        ParserElement.reset_cache()
        matches = 0
        try:
            while loc <= instrlen and matches < max_matches:
                try:
                    preloc = preparse_fn(instring, loc)
                    next_loc, tokens = parse_fn(instring, preloc, call_pre_parse=False)
                except ParseException:
                    loc = preloc + 1
                else:
                    matches += 1
                    yield tokens, preloc, next_loc
                    loc = next_loc
        except ParseBaseException as pe:
            raise pe

    def transform_string(self, instring):
        """Extension to scan_string, to modify matching text with modified tokens that may
        be returned from a parse action.  To use transform_string, define a grammar and
        attach a parse action to it that modifies the returned token list.
        Invoking transform_string() on a target string will then scan for matches,
        and replace the matched text patterns according to the logic in the parse
        action.  transform_string() returns the resulting transformed string."""
        out = []
        last_e = 0
        # force preservation of <TAB>s, to minimize unwanted transformation of string, and to
        # keep string locs straight between transform_string and scan_string
        self.keep_tabs = True
        try:
            for t, s, e in self.scan_string(instring):
                out.append(instring[last_e:s])
                if t:
                    if isinstance(t, ParseResults):
                        out += t.as_list()
                    elif isinstance(t, list):
                        out += t
                    else:
                        out.append(t)
                last_e = e
            out.append(instring[last_e:])
            return "".join(map(str, out))
        except ParseBaseException as pe:
            raise pe

    def search_string(self, instring, max_matches=sys.maxsize):
        """Another extension to scan_string, simplifying the access to the tokens found
        to match the given parse expression.  May be called with optional
        max_matches argument, to clip searching after 'n' matches are found.
        """
        try:
            return ParseResults(
                [t for t, s, e in self.scan_string(instring, max_matches)]
            )
        except ParseBaseException as pe:
            raise pe

    def __add__(self, other):
        """Implementation of + operator - returns And"""
        if isinstance(other, str):
            other = Literal(other)
        if not isinstance(other, ParserElement):
            warnings.warn(
                f"Cannot combine element of type {type(other)} with ParserElement",
                SyntaxWarning,
                stacklevel=2,
            )
            return None
        return And([self, other])

    def __radd__(self, other):
        """Implementation of + operator when left operand is not a ParserElement"""
        if isinstance(other, str):
            other = Literal(other)
        if not isinstance(other, ParserElement):
            warnings.warn(
                f"Cannot combine element of type {type(other)} with ParserElement",
                SyntaxWarning,
                stacklevel=2,
            )
            return None
        return other + self

    def __sub__(self, other):
        """Implementation of - operator, returns And with error stop"""
        if isinstance(other, str):
            other = Literal(other)
        if not isinstance(other, ParserElement):
            warnings.warn(
                f"Cannot combine element of type {type(other)} with ParserElement",
                SyntaxWarning,
                stacklevel=2,
            )
            return None
        return And([self, And._ErrorStop(), other])

    def __rsub__(self, other):
        """Implementation of - operator when left operand is not a ParserElement"""
        if isinstance(other, str):
            other = Literal(other)
        if not isinstance(other, ParserElement):
            warnings.warn(
                f"Cannot combine element of type {type(other)} with ParserElement",
                SyntaxWarning,
                stacklevel=2,
            )
            return None
        return other - self

    def __mul__(self, other):
        if isinstance(other, int):
            min_elements, opt_elements = other, 0
        elif isinstance(other, tuple):
            other = (other + (None, None))[:2]
            if other[0] is None:
                other = (0, other[1])
            if isinstance(other[0], int) and other[1] is None:
                if other[0] == 0:
                    return ZeroOrMore(self)
                if other[0] == 1:
                    return OneOrMore(self)
                else:
                    return self * other[0] + ZeroOrMore(self)
            elif isinstance(other[0], int) and isinstance(other[1], int):
                min_elements, opt_elements = other
                opt_elements -= min_elements
            else:
                raise TypeError(
                    "cannot multiply 'ParserElement' and ('%s','%s') objects",
                    type(other[0]),
                    type(other[1]),
                )
        else:
            raise TypeError(
                "cannot multiply 'ParserElement' and '%s' objects", type(other)
            )

        if min_elements < 0:
            raise ValueError("cannot multiply ParserElement by negative value")
        if opt_elements < 0:
            raise ValueError(
                "second tuple value must be greater or equal to first tuple value"
            )
        if min_elements == opt_elements == 0:
            raise ValueError("cannot multiply ParserElement by 0 or (0,0)")

        if opt_elements:

            def make_optional_list(n):
                if n > 1:
                    return Optional(self + make_optional_list(n - 1))
                else:
                    return Optional(self)

            if min_elements:
                if min_elements == 1:
                    ret = self + make_optional_list(opt_elements)
                else:
                    ret = And([self] * min_elements) + make_optional_list(opt_elements)
            else:
                ret = make_optional_list(opt_elements)
        else:
            if min_elements == 1:
                ret = self
            else:
                ret = And([self] * min_elements)
        return ret

    def __rmul__(self, other):
        return self.__mul__(other)

    def __or__(self, other):
        """Implementation of | operator - returns MatchFirst"""
        if isinstance(other, str):
            other = Literal(other)
        if not isinstance(other, ParserElement):
            warnings.warn(
                f"Cannot combine element of type {type(other)} with ParserElement",
                SyntaxWarning,
                stacklevel=2,
            )
            return None
        return MatchFirst([self, other])

    def __ror__(self, other):
        """Implementation of | operator when left operand is not a ParserElement"""
        if isinstance(other, str):
            other = Literal(other)
        if not isinstance(other, ParserElement):
            warnings.warn(
                f"Cannot combine element of type {type(other)} with ParserElement",
                SyntaxWarning,
                stacklevel=2,
            )
            return None
        return other | self

    def __xor__(self, other):
        """Implementation of ^ operator - returns Or"""
        if isinstance(other, str):
            other = Literal(other)
        if not isinstance(other, ParserElement):
            warnings.warn(
                f"Cannot combine element of type {type(other)} with ParserElement",
                SyntaxWarning,
                stacklevel=2,
            )
            return None
        return Or([self, other])

    def __rxor__(self, other):
        """Implementation of ^ operator when left operand is not a ParserElement

        Args:
            other (str or ParserElement): The right operand of the ^ operator.

        Returns:
            ParserElement: The result of the ^ operation.
        """
        if isinstance(other, str):
            other = Literal(other)
        if not isinstance(other, ParserElement):
            warnings.warn(
                f"Cannot combine element of type {type(other)} with ParserElement",
                SyntaxWarning,
                stacklevel=2,
            )
            return None
        return other ^ self

    def __and__(self, other):
        """Implementation of & operator - returns Each

        Args:
            other (str or ParserElement): The element to combine with.

        Returns:
            Each: A new `Each` object containing both `self` and `other`.
        """
        if isinstance(other, str):
            other = Literal(other)
        if not isinstance(other, ParserElement):
            warnings.warn(
                f"Cannot combine element of type {type(other)} with ParserElement",
                SyntaxWarning,
                stacklevel=2,
            )
            return None
        return Each([self, other])

    def __rand__(self, other):
        """Implementation of & operator when left operand is not a ParserElement

        Args:
            other (str or ParserElement): The left operand of the & operator.

        Returns:
            ParserElement: The result of combining the left operand with self using the & operator.
        """
        if isinstance(other, str):
            other = Literal(other)
        if not isinstance(other, ParserElement):
            warnings.warn(
                f"Cannot combine element of type {type(other)} with ParserElement",
                SyntaxWarning,
                stacklevel=2,
            )
            return None
        return other & self

    def __invert__(self):
        """Implementation of ~ operator - returns NotAny

        Returns:
            NotAny: A new instance of the NotAny class.
        """
        return NotAny(self)

    def __call__(self, name):
        """Shortcut for set_results_name, with list_all_matches=default::
            userdata = Word(alphas).set_results_name("name") + Word(nums+"-").set_results_name("socsecno")
        could be written as::
            userdata = Word(alphas)("name") + Word(nums+"-")("socsecno")

        Args:
                name (str): The name to assign to the parsed results.

        Returns:
                pyparsing.ParseResults: The modified pyparsing object with the specified name assigned to it.
        """
        return self.set_results_name(name)

    def suppress(self):
        """Suppresses the output of this ParserElement; useful to keep punctuation from
        cluttering up returned output.

        Returns:
            Suppress: A new ParserElement that suppresses the output of the original ParserElement.
        """
        return Suppress(self)

    def leave_whitespace(self):
        """
        Disables the skipping of whitespace before matching the characters in the
        ParserElement's defined pattern. This is normally only used internally by
        the pyparsing module, but may be needed in some whitespace-sensitive grammars.

        Returns:
            ParserElement: The ParserElement object with whitespace skipping disabled.
        """
        self.skip_whitespace = False
        return self

    def set_whitespace_chars(self, chars):
        """
        Overrides the default whitespace chars.

        Args:
            chars (str): The characters to be considered as whitespace.

        Returns:
            self: The current instance of the class.
        """
        self.skip_whitespace = True
        self.white_chars = chars
        self.copy_default_white_chars = False
        return self

    def parse_with_tabs(self):
        """
        Overrides default behavior to expand <TAB>s to spaces before parsing the input string.
        Must be called before parse_string when the input grammar contains elements that
        match <TAB> characters.

        Returns:
            self: The current instance of the class.
        """
        self.keep_tabs = True
        return self

    def ignore(self, other):
        """
        Define expression to be ignored (e.g., comments) while doing pattern matching.

        Parameters:
            other (str or pyparsing.ParserElement): The expression to be ignored.

        Returns:
            pyparsing.ParserElement: The current instance of the ParserElement.
        """
        if isinstance(other, Suppress):
            if other not in self.ignore_exprs:
                self.ignore_exprs.append(other)
        else:
            self.ignore_exprs.append(Suppress(other))
        return self

    def set_debug_actions(self, start_action, success_action, exception_action):
        """
        Enable display of debugging messages while doing pattern matching.

        Args:
            start_action (callable): The action to perform when pattern matching starts.
            success_action (callable): The action to perform when pattern matching succeeds.
            exception_action (callable): The action to perform when an exception occurs during pattern matching.

        Returns:
            self: The current instance of the class.

        """
        self.debug_actions = (
            start_action or _default_start_debug_action,
            success_action or _default_success_debug_action,
            exception_action or _default_exception_debug_action,
        )
        self.debug = True
        return self

    def set_debug(self, flag=True):
        """Enable or disable display of debugging messages while doing pattern matching.

        Args:
            flag (bool, optional): Set to True to enable debugging messages, False to disable. Defaults to True.

        Returns:
            self: The current instance of the class.
        """
        if flag:
            self.set_debug_actions(
                _default_start_debug_action,
                _default_success_debug_action,
                _default_exception_debug_action,
            )
        else:
            self.debug = False
        return self

    def __str__(self):
        return self.name

    def __repr__(self):
        return str(self)

    def streamline(self):
        """
        Streamlines the object by marking it as streamlined and resetting the string representation.

        Returns:
            The streamlined object.
        """
        self.streamlined = True
        self.str_repr = None
        return self

    def check_recursion(self, parse_element_list):
        """
        Check for recursion in the given parse element list.

        Args:
            parse_element_list (list): List of parse elements to check for recursion.

        Returns:
            bool: True if recursion is detected, False otherwise.
        """
        pass

    def validate(self, validate_trace=[]):
        """Check defined expressions for valid structure, check for infinite recursive definitions."""
        self.check_recursion([])

    def parse_file(self, file_or_filename, parse_all=False):
        """Execute the parse expression on the given file or filename.
        If a filename is specified (instead of a file object),
        the entire file is opened, read, and closed before parsing.
        """
        try:
            file_contents = file_or_filename.read()
        except AttributeError:
            f = open(file_or_filename, "rb")
            file_contents = f.read()
            f.close()
        try:
            return self.parse_string(file_contents, parse_all)
        except ParseBaseException as exc:
            # catch and re-raise exception from here, clears out pyparsing internal stack trace
            raise exc

    def get_exception(self):
        return ParseException("", 0, self.errmsg, self)

    def __getattr__(self, aname):
        if aname == "my_exception":
            self.my_exception = ret = self.get_exception()
            return ret
        else:
            raise AttributeError("no such attribute " + aname)

    def __eq__(self, other):
        if isinstance(other, ParserElement):
            return self is other or self.__dict__ == other.__dict__
        elif isinstance(other, str):
            try:
                self.parse_string(str(other), parse_all=True)
                return True
            except ParseBaseException:
                return False
        else:
            return super() == other

    def __ne__(self, other):
        return self != other

    def __hash__(self):
        return hash(id(self))

    def __req__(self, other):
        return self == other

    def __rne__(self, other):
        return self != other


class Token(ParserElement):
    """Abstract ParserElement subclass, for defining atomic matching patterns."""

    def __init__(self):
        super().__init__(savelist=False)
        # self.my_exception = ParseException("",0,"",self)

    def set_name(self, name):
        s = super().set_name(name)
        self.errmsg = "Expected " + self.name
        # s.my_exception.msg = self.errmsg
        return s


class Empty(Token):
    """An empty token, will always match."""

    def __init__(self):
        super().__init__()
        self.name = "Empty"
        self.may_return_empty = True
        self.may_index_error = False


class NoMatch(Token):
    """A token that will never match."""

    def __init__(self):
        super().__init__()
        self.name = "NoMatch"
        self.may_return_empty = True
        self.may_index_error = False
        self.errmsg = "Unmatchable token"
        # self.my_exception.msg = self.errmsg

    def parse_impl(self, instring, loc, do_actions=True):
        exc = self.my_exception
        exc.loc = loc
        exc.pstr = instring
        raise exc


class Literal(Token):
    """Token to exactly match a specified string."""

    def __init__(self, match_string):
        super().__init__()
        self.match = match_string
        self.matchLen = len(match_string)
        try:
            self.firstMatchChar = match_string[0]
        except IndexError:
            warnings.warn(
                "null string passed to Literal; use Empty() instead",
                SyntaxWarning,
                stacklevel=2,
            )
            self.__class__ = Empty
        self.name = f'"{str(self.match)}"'
        self.errmsg = "Expected " + self.name
        self.may_return_empty = False
        # self.my_exception.msg = self.errmsg
        self.may_index_error = False

    # Performance tuning: this routine gets called a *lot*
    # if this is a single character match string  and the first character matches,
    # short-circuit as quickly as possible, and avoid calling startswith
    # ~ @profile
    def parse_impl(self, instring, loc, do_actions=True):
        if instring[loc] == self.firstMatchChar and (
            self.matchLen == 1 or instring.startswith(self.match, loc)
        ):
            return loc + self.matchLen, self.match
        # ~ raise ParseException( instring, loc, self.errmsg )
        exc = self.my_exception
        exc.loc = loc
        exc.pstr = instring
        raise exc


_L = Literal


class Keyword(Token):
    """Token to exactly match a specified string as a keyword, that is, it must be
    immediately followed by a non-keyword character.  Compare with Literal::
      Literal("if") will match the leading 'if' in 'ifAndOnlyIf'.
      Keyword("if") will not; it will only match the leading 'if in 'if x=1', or 'if(y==2)'
    Accepts two optional constructor arguments in addition to the keyword string:
    ident_chars is a string of characters that would be valid identifier characters,
    defaulting to all alphanumerics + "_" and "$"; caseless allows case-insensitive
    matching, default is False.
    """

    DEFAULT_KEYWORD_CHARS = alphanums + "_$"

    def __init__(self, match_string, ident_chars=DEFAULT_KEYWORD_CHARS, caseless=False):
        super().__init__()
        self.match = match_string
        self.matchLen = len(match_string)
        try:
            self.firstMatchChar = match_string[0]
        except IndexError:
            warnings.warn(
                "null string passed to Keyword; use Empty() instead",
                SyntaxWarning,
                stacklevel=2,
            )
        self.name = f'"{self.match}"'
        self.errmsg = "Expected " + self.name
        self.may_return_empty = False
        # self.my_exception.msg = self.errmsg
        self.may_index_error = False
        self.caseless = caseless
        if caseless:
            self.caselessmatch = match_string.upper()
            ident_chars = ident_chars.upper()
        self.ident_chars = set(ident_chars)

    def parse_impl(self, instring, loc, do_actions=True):
        if self.caseless:
            if (
                (instring[loc : loc + self.matchLen].upper() == self.caselessmatch)
                and (
                    loc >= len(instring) - self.matchLen
                    or instring[loc + self.matchLen].upper() not in self.ident_chars
                )
                and (loc == 0 or instring[loc - 1].upper() not in self.ident_chars)
            ):
                return loc + self.matchLen, self.match
        else:
            if (
                instring[loc] == self.firstMatchChar
                and (self.matchLen == 1 or instring.startswith(self.match, loc))
                and (
                    loc >= len(instring) - self.matchLen
                    or instring[loc + self.matchLen] not in self.ident_chars
                )
                and (loc == 0 or instring[loc - 1] not in self.ident_chars)
            ):
                return loc + self.matchLen, self.match
        # ~ raise ParseException( instring, loc, self.errmsg )
        exc = self.my_exception
        exc.loc = loc
        exc.pstr = instring
        raise exc

    def copy(self):
        c = super().copy()
        c.ident_chars = Keyword.DEFAULT_KEYWORD_CHARS
        return c

    def setDefaultKeyword_chars(chars):
        """Overrides the default Keyword chars"""
        Keyword.DEFAULT_KEYWORD_CHARS = chars

    setDefaultKeyword_chars = staticmethod(setDefaultKeyword_chars)


class CaselessLiteral(Literal):
    """Token to match a specified string, ignoring case of letters.
    Note: the matched results will always be in the case of the given
    match string, NOT the case of the input text.
    """

    def __init__(self, match_string):
        super().__init__(match_string.upper())
        # Preserve the defining literal.
        self.returnString = match_string
        self.name = f"'{self.returnString}'"
        self.errmsg = "Expected " + self.name
        # self.my_exception.msg = self.errmsg

    def parse_impl(self, instring, loc, do_actions=True):
        if instring[loc : loc + self.matchLen].upper() == self.match:
            return loc + self.matchLen, self.returnString
        # ~ raise ParseException( instring, loc, self.errmsg )
        exc = self.my_exception
        exc.loc = loc
        exc.pstr = instring
        raise exc


class CaselessKeyword(Keyword):
    def __init__(self, match_string, ident_chars=Keyword.DEFAULT_KEYWORD_CHARS):
        super().__init__(match_string, ident_chars, caseless=True)

    def parse_impl(self, instring, loc, do_actions=True):
        if (instring[loc : loc + self.matchLen].upper() == self.caselessmatch) and (
            loc >= len(instring) - self.matchLen
            or instring[loc + self.matchLen].upper() not in self.ident_chars
        ):
            return loc + self.matchLen, self.match
        # ~ raise ParseException( instring, loc, self.errmsg )
        exc = self.my_exception
        exc.loc = loc
        exc.pstr = instring
        raise exc


class Word(Token):
    """Token for matching words composed of allowed character sets.
    Defined with string containing all allowed initial characters,
    an optional string containing allowed body characters (if omitted,
    defaults to the initial character set), and an optional minimum,
    maximum, and/or exact length.  The default value for min is 1 (a
    minimum value < 1 is not valid); the default values for max and exact
    are 0, meaning no maximum or exact length restriction.
    """

    def __init__(
        self, init_chars, body_chars=None, min=1, max=0, exact=0, as_keyword=False
    ):
        super().__init__()
        self.init_charsOrig = init_chars
        self.init_chars = set(init_chars)
        if body_chars:
            self.body_charsOrig = body_chars
            self.body_chars = set(body_chars)
        else:
            self.body_charsOrig = init_chars
            self.body_chars = set(init_chars)

        self.maxSpecified = max > 0

        if min < 1:
            raise ValueError(
                "cannot specify a minimum length < 1; use Optional(Word()) if zero-length word is permitted"
            )

        self.minLen = min

        if max > 0:
            self.maxLen = max
        else:
            self.maxLen = sys.maxsize

        if exact > 0:
            self.maxLen = exact
            self.minLen = exact

        self.name = str(self)
        self.errmsg = "Expected " + self.name
        # self.my_exception.msg = self.errmsg
        self.may_index_error = False
        self.as_keyword = as_keyword

        if " " not in self.init_charsOrig + self.body_charsOrig and (
            min == 1 and max == 0 and exact == 0
        ):
            if self.body_charsOrig == self.init_charsOrig:
                self.reString = f"[{_escape_regex_range_chars(self.init_charsOrig)}]+"
            elif len(self.body_charsOrig) == 1:
                self.reString = "{}[{}]*".format(
                    re.escape(self.init_charsOrig),
                    _escape_regex_range_chars(self.body_charsOrig),
                )
            else:
                self.reString = "[{}][{}]*".format(
                    _escape_regex_range_chars(self.init_charsOrig),
                    _escape_regex_range_chars(self.body_charsOrig),
                )
            if self.as_keyword:
                self.reString = r"\b" + self.reString + r"\b"
            try:
                self.re = re.compile(self.reString)
            except:
                self.re = None

    def parse_impl(self, instring, loc, do_actions=True):
        if self.re:
            result = self.re.match(instring, loc)
            if not result:
                exc = self.my_exception
                exc.loc = loc
                exc.pstr = instring
                raise exc

            loc = result.end()
            return loc, result.group()

        if instring[loc] not in self.init_chars:
            # ~ raise ParseException( instring, loc, self.errmsg )
            exc = self.my_exception
            exc.loc = loc
            exc.pstr = instring
            raise exc
        start = loc
        loc += 1
        instrlen = len(instring)
        bodychars = self.body_chars
        maxloc = start + self.maxLen
        maxloc = min(maxloc, instrlen)
        while loc < maxloc and instring[loc] in bodychars:
            loc += 1

        throw_rxception = False
        if loc - start < self.minLen:
            throw_rxception = True
        if self.maxSpecified and loc < instrlen and instring[loc] in bodychars:
            throw_rxception = True
        if self.as_keyword:
            if (start > 0 and instring[start - 1] in bodychars) or (
                loc < instrlen and instring[loc] in bodychars
            ):
                throw_rxception = True

        if throw_rxception:
            # ~ raise ParseException( instring, loc, self.errmsg )
            exc = self.my_exception
            exc.loc = loc
            exc.pstr = instring
            raise exc

        return loc, instring[start:loc]

    def __str__(self):
        try:
            return super().__str__()
        except:
            pass

        if self.str_repr is None:

            def chars_as_str(s):
                if len(s) > 4:
                    return s[:4] + "..."
                else:
                    return s

            if self.init_charsOrig != self.body_charsOrig:
                self.str_repr = f"W:({chars_as_str(self.init_charsOrig)},{chars_as_str(self.body_charsOrig)})"
            else:
                self.str_repr = f"W:({chars_as_str(self.init_charsOrig)})"

        return self.str_repr


class Regex(Token):
    """Token for matching strings that match a given regular expression.
    Defined with string specifying the regular expression in a form recognized by the inbuilt Python re module.
    """

    def __init__(self, pattern, flags=0):
        """The parameters pattern and flags are passed to the re.compile() function as-is. See the Python re module for an explanation of the acceptable patterns and flags."""
        super().__init__()

        if len(pattern) == 0:
            warnings.warn(
                "null string passed to Regex; use Empty() instead",
                SyntaxWarning,
                stacklevel=2,
            )

        self.pattern = pattern
        self.flags = flags

        try:
            self.re = re.compile(self.pattern, self.flags)
            self.reString = self.pattern
        except sre_constants.error:
            warnings.warn(
                f"invalid pattern ({pattern}) passed to Regex",
                SyntaxWarning,
                stacklevel=2,
            )
            raise

        self.name = str(self)
        self.errmsg = "Expected " + self.name
        # self.my_exception.msg = self.errmsg
        self.may_index_error = False
        self.may_return_empty = True

    def parse_impl(self, instring, loc, do_actions=True):
        result = self.re.match(instring, loc)
        if not result:
            exc = self.my_exception
            exc.loc = loc
            exc.pstr = instring
            raise exc

        loc = result.end()
        d = result.groupdict()
        ret = ParseResults(result.group())
        if d:
            for k in d:
                ret[k] = d[k]
        return loc, ret

    def __str__(self):
        try:
            return super().__str__()
        except:
            pass

        if self.str_repr is None:
            self.str_repr = f"Re:({repr(self.pattern)})"

        return self.str_repr


class QuotedString(Token):
    """Token for matching strings that are delimited by quoting characters."""

    def __init__(
        self,
        quote_char,
        esc_char=None,
        esc_quote=None,
        multiline=False,
        unquote_results=True,
        end_quote_char=None,
    ):
        """
        Defined with the following parameters:
         - quote_char - string of one or more characters defining the quote delimiting string
         - esc_char - character to escape quotes, typically backslash (default=None)
         - esc_quote - special quote sequence to escape an embedded quote string (such as SQL's "" to escape an embedded ") (default=None)
         - multiline - boolean indicating whether quotes can span multiple lines (default=False)
         - unquote_results - boolean indicating whether the matched text should be unquoted (default=True)
         - end_quote_char - string of one or more characters defining the end of the quote delimited string (default=None => same as quote_char)
        """
        super().__init__()

        # remove white space from quote chars - wont work anyway
        quote_char = quote_char.strip()
        if len(quote_char) == 0:
            warnings.warn(
                "quote_char cannot be the empty string", SyntaxWarning, stacklevel=2
            )
            raise SyntaxError()

        if end_quote_char is None:
            end_quote_char = quote_char
        else:
            end_quote_char = end_quote_char.strip()
            if len(end_quote_char) == 0:
                warnings.warn(
                    "end_quote_char cannot be the empty string",
                    SyntaxWarning,
                    stacklevel=2,
                )
                raise SyntaxError()

        self.quote_char = quote_char
        self.quote_charLen = len(quote_char)
        self.firstQuoteChar = quote_char[0]
        self.end_quote_char = end_quote_char
        self.end_quote_charLen = len(end_quote_char)
        self.esc_char = esc_char
        self.esc_quote = esc_quote
        self.unquote_results = unquote_results

        if multiline:
            self.flags = re.MULTILINE | re.DOTALL
            self.pattern = r"{}(?:[^{}{}]".format(
                re.escape(self.quote_char),
                _escape_regex_range_chars(self.end_quote_char[0]),
                (esc_char is not None and _escape_regex_range_chars(esc_char) or ""),
            )
        else:
            self.flags = 0
            self.pattern = r"{}(?:[^{}\n\r{}]".format(
                re.escape(self.quote_char),
                _escape_regex_range_chars(self.end_quote_char[0]),
                (esc_char is not None and _escape_regex_range_chars(esc_char) or ""),
            )
        if len(self.end_quote_char) > 1:
            self.pattern += (
                "|(?:"
                + ")|(?:".join(
                    [
                        "%s[^%s]"
                        % (
                            re.escape(self.end_quote_char[:i]),
                            _escape_regex_range_chars(self.end_quote_char[i]),
                        )
                        for i in range(len(self.end_quote_char) - 1, 0, -1)
                    ]
                )
                + ")"
            )
        if esc_quote:
            self.pattern += r"|(?:%s)" % re.escape(esc_quote)
        if esc_char:
            self.pattern += r"|(?:%s.)" % re.escape(esc_char)
            self.esc_charReplacePattern = re.escape(self.esc_char) + "(.)"
        self.pattern += r")*%s" % re.escape(self.end_quote_char)

        try:
            self.re = re.compile(self.pattern, self.flags)
            self.reString = self.pattern
        except sre_constants.error:
            warnings.warn(
                f"invalid pattern ({self.pattern}) passed to Regex",
                SyntaxWarning,
                stacklevel=2,
            )
            raise

        self.name = str(self)
        self.errmsg = "Expected " + self.name
        # self.my_exception.msg = self.errmsg
        self.may_index_error = False
        self.may_return_empty = True

    def parse_impl(self, instring, loc, do_actions=True):
        result = (
            instring[loc] == self.firstQuoteChar
            and self.re.match(instring, loc)
            or None
        )
        if not result:
            exc = self.my_exception
            exc.loc = loc
            exc.pstr = instring
            raise exc

        loc = result.end()
        ret = result.group()

        if self.unquote_results:
            # strip off quotes
            ret = ret[self.quote_charLen : -self.end_quote_charLen]

            if isinstance(ret, str):
                # replace escaped characters
                if self.esc_char:
                    ret = re.sub(self.esc_charReplacePattern, r"\g<1>", ret)

                # replace escaped quotes
                if self.esc_quote:
                    ret = ret.replace(self.esc_quote, self.end_quote_char)

        return loc, ret

    def __str__(self):
        try:
            return super().__str__()
        except:
            pass

        if self.str_repr is None:
            self.str_repr = f"quoted string, starting with {self.quote_char} ending with {self.end_quote_char}"

        return self.str_repr


class CharsNotIn(Token):
    """Token for matching words composed of characters *not* in a given set.
    Defined with string containing all disallowed characters, and an optional
    minimum, maximum, and/or exact length.  The default value for min is 1 (a
    minimum value < 1 is not valid); the default values for max and exact
    are 0, meaning no maximum or exact length restriction.
    """

    def __init__(self, not_chars, min=1, max=0, exact=0):
        super().__init__()
        self.skip_whitespace = False
        self.not_chars = not_chars

        if min < 1:
            raise ValueError(
                "cannot specify a minimum length < 1; use Optional(CharsNotIn()) if zero-length char group is permitted"
            )

        self.minLen = min

        if max > 0:
            self.maxLen = max
        else:
            self.maxLen = sys.maxsize

        if exact > 0:
            self.maxLen = exact
            self.minLen = exact

        self.name = str(self)
        self.errmsg = "Expected " + self.name
        self.may_return_empty = self.minLen == 0
        # self.my_exception.msg = self.errmsg
        self.may_index_error = False

    def parse_impl(self, instring, loc, do_actions=True):
        if instring[loc] in self.not_chars:
            # ~ raise ParseException( instring, loc, self.errmsg )
            exc = self.my_exception
            exc.loc = loc
            exc.pstr = instring
            raise exc

        start = loc
        loc += 1
        notchars = self.not_chars
        maxlen = min(start + self.maxLen, len(instring))
        while loc < maxlen and (instring[loc] not in notchars):
            loc += 1

        if loc - start < self.minLen:
            # ~ raise ParseException( instring, loc, self.errmsg )
            exc = self.my_exception
            exc.loc = loc
            exc.pstr = instring
            raise exc

        return loc, instring[start:loc]

    def __str__(self):
        try:
            return super().__str__()
        except:
            pass

        if self.str_repr is None:
            if len(self.not_chars) > 4:
                self.str_repr = f"!W:({self.not_chars[:4]}...)"
            else:
                self.str_repr = f"!W:({self.not_chars})"

        return self.str_repr


class White(Token):
    """Special matching class for matching whitespace.  Normally, whitespace is ignored
    by pyparsing grammars.  This class is included when some whitespace structures
    are significant.  Define with a string containing the whitespace characters to be
    matched; default is " \\t\\r\\n".  Also takes optional min, max, and exact arguments,
    as defined for the Word class."""

    whiteStrs = {
        " ": "<SPC>",
        "\t": "<TAB>",
        "\n": "<LF>",
        "\r": "<CR>",
        "\f": "<FF>",
    }

    def __init__(self, ws=" \t\r\n", min=1, max=0, exact=0):
        super().__init__()
        self.matchWhite = ws
        self.set_whitespace_chars(
            "".join([c for c in self.white_chars if c not in self.matchWhite])
        )
        # ~ self.leave_whitespace()
        self.name = "".join([White.whiteStrs[c] for c in self.matchWhite])
        self.may_return_empty = True
        self.errmsg = "Expected " + self.name
        # self.my_exception.msg = self.errmsg

        self.minLen = min

        if max > 0:
            self.maxLen = max
        else:
            self.maxLen = sys.maxsize

        if exact > 0:
            self.maxLen = exact
            self.minLen = exact

    def parse_impl(self, instring, loc, do_actions=True):
        if instring[loc] not in self.matchWhite:
            # ~ raise ParseException( instring, loc, self.errmsg )
            exc = self.my_exception
            exc.loc = loc
            exc.pstr = instring
            raise exc
        start = loc
        loc += 1
        maxloc = start + self.maxLen
        maxloc = min(maxloc, len(instring))
        while loc < maxloc and instring[loc] in self.matchWhite:
            loc += 1

        if loc - start < self.minLen:
            # ~ raise ParseException( instring, loc, self.errmsg )
            exc = self.my_exception
            exc.loc = loc
            exc.pstr = instring
            raise exc

        return loc, instring[start:loc]


class _PositionToken(Token):
    def __init__(self):
        super().__init__()
        self.name = self.__class__.__name__
        self.may_return_empty = True
        self.may_index_error = False


class GoToColumn(_PositionToken):
    """Token to advance to a specific column of input text; useful for tabular report scraping."""

    def __init__(self, colno):
        super().__init__()
        self.col = colno

    def pre_parse(self, instring, loc):
        if col(loc, instring) != self.col:
            instrlen = len(instring)
            if self.ignore_exprs:
                loc = self._skip_ignorables(instring, loc)
            while (
                loc < instrlen
                and instring[loc].isspace()
                and col(loc, instring) != self.col
            ):
                loc += 1
        return loc

    def parse_impl(self, instring, loc, do_actions=True):
        thiscol = col(loc, instring)
        if thiscol > self.col:
            raise ParseException(instring, loc, "Text not in expected column", self)
        newloc = loc + self.col - thiscol
        ret = instring[loc:newloc]
        return newloc, ret


class LineStart(_PositionToken):
    """Matches if current position is at the beginning of a line within the parse string"""

    def __init__(self):
        super().__init__()
        self.set_whitespace_chars(ParserElement.DEFAULT_WHITE_CHARS.replace("\n", ""))
        self.errmsg = "Expected start of line"
        # self.my_exception.msg = self.errmsg

    def pre_parse(self, instring, loc):
        preloc = super().pre_parse(instring, loc)
        if instring[preloc] == "\n":
            loc += 1
        return loc

    def parse_impl(self, instring, loc, do_actions=True):
        if not (
            loc == 0
            or (loc == self.pre_parse(instring, 0))
            or (instring[loc - 1] == "\n")
        ):  # col(loc, instring) != 1:
            # ~ raise ParseException( instring, loc, "Expected start of line" )
            exc = self.my_exception
            exc.loc = loc
            exc.pstr = instring
            raise exc
        return loc, []


class LineEnd(_PositionToken):
    """Matches if current position is at the end of a line within the parse string"""

    def __init__(self):
        super().__init__()
        self.set_whitespace_chars(ParserElement.DEFAULT_WHITE_CHARS.replace("\n", ""))
        self.errmsg = "Expected end of line"
        # self.my_exception.msg = self.errmsg

    def parse_impl(self, instring, loc, do_actions=True):
        if loc < len(instring):
            if instring[loc] == "\n":
                return loc + 1, "\n"
            else:
                # ~ raise ParseException( instring, loc, "Expected end of line" )
                exc = self.my_exception
                exc.loc = loc
                exc.pstr = instring
                raise exc
        elif loc == len(instring):
            return loc + 1, []
        else:
            exc = self.my_exception
            exc.loc = loc
            exc.pstr = instring
            raise exc


class StringStart(_PositionToken):
    """Matches if current position is at the beginning of the parse string"""

    def __init__(self):
        super().__init__()
        self.errmsg = "Expected start of text"
        # self.my_exception.msg = self.errmsg

    def parse_impl(self, instring, loc, do_actions=True):
        if loc != 0:
            # see if entire string up to here is just whitespace and ignoreables
            if loc != self.pre_parse(instring, 0):
                # ~ raise ParseException( instring, loc, "Expected start of text" )
                exc = self.my_exception
                exc.loc = loc
                exc.pstr = instring
                raise exc
        return loc, []


class StringEnd(_PositionToken):
    """Matches if current position is at the end of the parse string"""

    def __init__(self):
        super().__init__()
        self.errmsg = "Expected end of text"
        # self.my_exception.msg = self.errmsg

    def parse_impl(self, instring, loc, do_actions=True):
        if loc < len(instring):
            # ~ raise ParseException( instring, loc, "Expected end of text" )
            exc = self.my_exception
            exc.loc = loc
            exc.pstr = instring
            raise exc
        elif loc == len(instring):
            return loc + 1, []
        elif loc > len(instring):
            return loc, []


class WordStart(_PositionToken):
    """Matches if the current position is at the beginning of a Word, and
    is not preceded by any character in a given set of word_chars
    (default=printables). To emulate the \b behavior of regular expressions,
    use WordStart(alphanums). WordStart will also match at the beginning of
    the string being parsed, or at the beginning of a line.
    """

    def __init__(self, word_chars=printables):
        super().__init__()
        self.word_chars = set(word_chars)
        self.errmsg = "Not at the start of a word"

    def parse_impl(self, instring, loc, do_actions=True):
        if loc != 0 and (
            instring[loc - 1] in self.word_chars or instring[loc] not in self.word_chars
        ):
            exc = self.my_exception
            exc.loc = loc
            exc.pstr = instring
            raise exc
        return loc, []


class WordEnd(_PositionToken):
    """Matches if the current position is at the end of a Word, and
    is not followed by any character in a given set of word_chars
    (default=printables). To emulate the \b behavior of regular expressions,
    use WordEnd(alphanums). WordEnd will also match at the end of
    the string being parsed, or at the end of a line.
    """

    def __init__(self, word_chars=printables):
        super().__init__()
        self.word_chars = set(word_chars)
        self.skip_whitespace = False
        self.errmsg = "Not at the end of a word"

    def parse_impl(self, instring, loc, do_actions=True):
        instrlen = len(instring)
        if (
            instrlen > 0
            and loc < instrlen
            and (
                instring[loc] in self.word_chars
                or instring[loc - 1] not in self.word_chars
            )
        ):
            # ~ raise ParseException( instring, loc, "Expected end of word" )
            exc = self.my_exception
            exc.loc = loc
            exc.pstr = instring
            raise exc
        return loc, []


class ParseExpression(ParserElement):
    """Abstract subclass of ParserElement, for combining and post-processing parsed tokens."""

    def __init__(self, exprs, savelist=False):
        super().__init__(savelist)
        if isinstance(exprs, list):
            self.exprs = exprs
        elif isinstance(exprs, str):
            self.exprs = [Literal(exprs)]
        else:
            try:
                self.exprs = list(exprs)
            except TypeError:
                self.exprs = [exprs]
        self.call_preparse = False

    def __getitem__(self, i):
        return self.exprs[i]

    def append(self, other):
        self.exprs.append(other)
        self.str_repr = None
        return self

    def leave_whitespace(self):
        """Extends leave_whitespace defined in base class, and also invokes leave_whitespace on
        all contained expressions."""
        self.skip_whitespace = False
        self.exprs = [e.copy() for e in self.exprs]
        for e in self.exprs:
            e.leave_whitespace()
        return self

    def ignore(self, other):
        if isinstance(other, Suppress):
            if other not in self.ignore_exprs:
                super().ignore(other)
                for e in self.exprs:
                    e.ignore(self.ignore_exprs[-1])
        else:
            super().ignore(other)
            for e in self.exprs:
                e.ignore(self.ignore_exprs[-1])
        return self

    def __str__(self):
        try:
            return super().__str__()
        except:
            pass

        if self.str_repr is None:
            self.str_repr = f"{self.__class__.__name__}:({str(self.exprs)})"
        return self.str_repr

    def streamline(self):
        super().streamline()

        for e in self.exprs:
            e.streamline()

        # collapse nested And's of the form And( And( And( a,b), c), d) to And( a,b,c,d )
        # but only if there are no parse actions or results_names on the nested And's
        # (likewise for Or's and MatchFirst's)
        if len(self.exprs) == 2:
            other = self.exprs[0]
            if (
                isinstance(other, self.__class__)
                and not (other.parse_action)
                and other.results_name is None
                and not other.debug
            ):
                self.exprs = other.exprs[:] + [self.exprs[1]]
                self.str_repr = None
                self.may_return_empty |= other.may_return_empty
                self.may_index_error |= other.may_index_error

            other = self.exprs[-1]
            if (
                isinstance(other, self.__class__)
                and not (other.parse_action)
                and other.results_name is None
                and not other.debug
            ):
                self.exprs = self.exprs[:-1] + other.exprs[:]
                self.str_repr = None
                self.may_return_empty |= other.may_return_empty
                self.may_index_error |= other.may_index_error

        return self

    def set_results_name(self, name, list_all_matches=False):
        ret = super().set_results_name(name, list_all_matches)
        return ret

    def validate(self, validate_trace=[]):
        tmp = validate_trace[:] + [self]
        for e in self.exprs:
            e.validate(tmp)
        self.check_recursion([])


class And(ParseExpression):
    """Requires all given ParseExpressions to be found in the given order.
    Expressions may be separated by whitespace.
    May be constructed using the '+' operator.
    """

    class _ErrorStop(Empty):
        def __init__(self, *args, **kwargs):
            super(Empty, self).__init__(*args, **kwargs)
            self.leave_whitespace()

    def __init__(self, exprs, savelist=True):
        super().__init__(exprs, savelist)
        self.may_return_empty = True
        for e in self.exprs:
            if not e.may_return_empty:
                self.may_return_empty = False
                break
        self.set_whitespace_chars(exprs[0].white_chars)
        self.skip_whitespace = exprs[0].skip_whitespace
        self.call_preparse = True

    def parse_impl(self, instring, loc, do_actions=True):
        # pass False as last arg to _parse for first element, since we already
        # pre-parsed the string as part of our And pre-parsing
        loc, resultlist = self.exprs[0]._parse(
            instring, loc, do_actions, call_pre_parse=False
        )
        error_stop = False
        for e in self.exprs[1:]:
            if isinstance(e, And._ErrorStop):
                error_stop = True
                continue
            if error_stop:
                try:
                    loc, exprtokens = e._parse(instring, loc, do_actions)
                except ParseSyntaxException:
                    raise
                except ParseBaseException as pe:
                    raise ParseSyntaxException(pe)
                except IndexError:
                    raise ParseSyntaxException(
                        ParseException(instring, len(instring), self.errmsg, self)
                    )
            else:
                loc, exprtokens = e._parse(instring, loc, do_actions)
            if exprtokens or exprtokens.keys():
                resultlist += exprtokens
        return loc, resultlist

    def __iadd__(self, other):
        if isinstance(other, str):
            other = Literal(other)
        return self.append(other)  # And( [ self, other ] )

    def check_recursion(self, parse_element_list):
        sub_rec_check_list = parse_element_list[:] + [self]
        for e in self.exprs:
            e.check_recursion(sub_rec_check_list)
            if not e.may_return_empty:
                break

    def __str__(self):
        if hasattr(self, "name"):
            return self.name

        if self.str_repr is None:
            self.str_repr = "{" + " ".join([str(e) for e in self.exprs]) + "}"

        return self.str_repr


class Or(ParseExpression):
    """Requires that at least one ParseExpression is found.
    If two expressions match, the expression that matches the longest string will be used.
    May be constructed using the '^' operator.
    """

    def __init__(self, exprs, savelist=False):
        super().__init__(exprs, savelist)
        self.may_return_empty = False
        for e in self.exprs:
            if e.may_return_empty:
                self.may_return_empty = True
                break

    def parse_impl(self, instring, loc, do_actions=True):
        max_exc_loc = -1
        max_match_loc = -1
        max_exception = None
        for e in self.exprs:
            try:
                loc2 = e.try_parse(instring, loc)
            except ParseException as err:
                if err.loc > max_exc_loc:
                    max_exception = err
                    max_exc_loc = err.loc
            except IndexError:
                if len(instring) > max_exc_loc:
                    max_exception = ParseException(
                        instring, len(instring), e.errmsg, self
                    )
                    max_exc_loc = len(instring)
            else:
                if loc2 > max_match_loc:
                    max_match_loc = loc2
                    max_match_exp = e

        if max_match_loc < 0:
            if max_exception is not None:
                raise max_exception
            else:
                raise ParseException(
                    instring, loc, "no defined alternatives to match", self
                )

        return max_match_exp._parse(instring, loc, do_actions)

    def __ixor__(self, other):
        if isinstance(other, str):
            other = Literal(other)
        return self.append(other)  # Or( [ self, other ] )

    def __str__(self):
        if hasattr(self, "name"):
            return self.name

        if self.str_repr is None:
            self.str_repr = "{" + " ^ ".join([str(e) for e in self.exprs]) + "}"

        return self.str_repr

    def check_recursion(self, parse_element_list):
        sub_rec_check_list = parse_element_list[:] + [self]
        for e in self.exprs:
            e.check_recursion(sub_rec_check_list)


class MatchFirst(ParseExpression):
    """Requires that at least one ParseExpression is found.
    If two expressions match, the first one listed is the one that will match.
    May be constructed using the '|' operator.
    """

    def __init__(self, exprs, savelist=False):
        super().__init__(exprs, savelist)
        if exprs:
            self.may_return_empty = False
            for e in self.exprs:
                if e.may_return_empty:
                    self.may_return_empty = True
                    break
        else:
            self.may_return_empty = True

    def parse_impl(self, instring, loc, do_actions=True):
        max_exc_loc = -1
        max_exception = None
        for e in self.exprs:
            try:
                ret = e._parse(instring, loc, do_actions)
                return ret
            except ParseException as err:
                if err.loc > max_exc_loc:
                    max_exception = err
                    max_exc_loc = err.loc
            except IndexError:
                if len(instring) > max_exc_loc:
                    max_exception = ParseException(
                        instring, len(instring), e.errmsg, self
                    )
                    max_exc_loc = len(instring)

        # only got here if no expression matched, raise exception for match that made it the furthest
        else:
            if max_exception is not None:
                raise max_exception
            else:
                raise ParseException(
                    instring, loc, "no defined alternatives to match", self
                )

    def __ior__(self, other):
        if isinstance(other, str):
            other = Literal(other)
        return self.append(other)  # MatchFirst( [ self, other ] )

    def __str__(self):
        if hasattr(self, "name"):
            return self.name

        if self.str_repr is None:
            self.str_repr = "{" + " | ".join([str(e) for e in self.exprs]) + "}"

        return self.str_repr

    def check_recursion(self, parse_element_list):
        sub_rec_check_list = parse_element_list[:] + [self]
        for e in self.exprs:
            e.check_recursion(sub_rec_check_list)


class Each(ParseExpression):
    """Requires all given ParseExpressions to be found, but in any order.
    Expressions may be separated by whitespace.
    May be constructed using the '&' operator.
    """

    def __init__(self, exprs, savelist=True):
        super().__init__(exprs, savelist)
        self.may_return_empty = True
        for e in self.exprs:
            if not e.may_return_empty:
                self.may_return_empty = False
                break
        self.skip_whitespace = True
        self.initExprGroups = True

    def parse_impl(self, instring, loc, do_actions=True):
        if self.initExprGroups:
            self.optionals = [e.expr for e in self.exprs if isinstance(e, Optional)]
            self.multioptionals = [
                e.expr for e in self.exprs if isinstance(e, ZeroOrMore)
            ]
            self.multirequired = [
                e.expr for e in self.exprs if isinstance(e, OneOrMore)
            ]
            self.required = [
                e
                for e in self.exprs
                if not isinstance(e, (Optional, ZeroOrMore, OneOrMore))
            ]
            self.required += self.multirequired
            self.initExprGroups = False
        tmp_loc = loc
        tmp_reqd = self.required[:]
        tmp_opt = self.optionals[:]
        match_order = []

        keep_matching = True
        while keep_matching:
            tmp_exprs = tmp_reqd + tmp_opt + self.multioptionals + self.multirequired
            failed = []
            for e in tmp_exprs:
                try:
                    tmp_loc = e.try_parse(instring, tmp_loc)
                except ParseException:
                    failed.append(e)
                else:
                    match_order.append(e)
                    if e in tmp_reqd:
                        tmp_reqd.remove(e)
                    elif e in tmp_opt:
                        tmp_opt.remove(e)
            if len(failed) == len(tmp_exprs):
                keep_matching = False

        if tmp_reqd:
            missing = ", ".join([str(e) for e in tmp_reqd])
            raise ParseException(
                instring, loc, f"Missing one or more required elements ({missing})"
            )

        # add any unmatched Optionals, in case they have default values defined
        match_order += [
            e for e in self.exprs if isinstance(e, Optional) and e.expr in tmp_opt
        ]

        resultlist = []
        for e in match_order:
            loc, results = e._parse(instring, loc, do_actions)
            resultlist.append(results)

        final_results = ParseResults([])
        for r in resultlist:
            dups = {}
            for k in r.keys():
                if k in final_results.keys():
                    tmp = ParseResults(final_results[k])
                    tmp += ParseResults(r[k])
                    dups[k] = tmp
            final_results += ParseResults(r)
            for k, v in dups.items():
                final_results[k] = v
        return loc, final_results

    def __str__(self):
        if hasattr(self, "name"):
            return self.name

        if self.str_repr is None:
            self.str_repr = "{" + " & ".join([str(e) for e in self.exprs]) + "}"

        return self.str_repr

    def check_recursion(self, parse_element_list):
        sub_rec_check_list = parse_element_list[:] + [self]
        for e in self.exprs:
            e.check_recursion(sub_rec_check_list)


class ParseElementEnhance(ParserElement):
    """Abstract subclass of ParserElement, for combining and post-processing parsed tokens."""

    def __init__(self, expr, savelist=False):
        super().__init__(savelist)
        if isinstance(expr, str):
            expr = Literal(expr)
        self.expr = expr
        self.str_repr = None
        if expr is not None:
            self.may_index_error = expr.may_index_error
            self.may_return_empty = expr.may_return_empty
            self.set_whitespace_chars(expr.white_chars)
            self.skip_whitespace = expr.skip_whitespace
            self.saveas_list = expr.saveas_list
            self.call_preparse = expr.call_preparse
            self.ignore_exprs.extend(expr.ignore_exprs)

    def parse_impl(self, instring, loc, do_actions=True):
        if self.expr is not None:
            return self.expr._parse(instring, loc, do_actions, call_pre_parse=False)
        else:
            raise ParseException("", loc, self.errmsg, self)

    def leave_whitespace(self):
        self.skip_whitespace = False
        self.expr = self.expr.copy()
        if self.expr is not None:
            self.expr.leave_whitespace()
        return self

    def ignore(self, other):
        if isinstance(other, Suppress):
            if other not in self.ignore_exprs:
                super().ignore(other)
                if self.expr is not None:
                    self.expr.ignore(self.ignore_exprs[-1])
        else:
            super().ignore(other)
            if self.expr is not None:
                self.expr.ignore(self.ignore_exprs[-1])
        return self

    def streamline(self):
        super().streamline()
        if self.expr is not None:
            self.expr.streamline()
        return self

    def check_recursion(self, parse_element_list):
        if self in parse_element_list:
            raise RecursiveGrammarException(parse_element_list + [self])
        sub_rec_check_list = parse_element_list[:] + [self]
        if self.expr is not None:
            self.expr.check_recursion(sub_rec_check_list)

    def validate(self, validate_trace=[]):
        tmp = validate_trace[:] + [self]
        if self.expr is not None:
            self.expr.validate(tmp)
        self.check_recursion([])

    def __str__(self):
        try:
            return super().__str__()
        except:
            pass

        if self.str_repr is None and self.expr is not None:
            self.str_repr = f"{self.__class__.__name__}:({str(self.expr)})"
        return self.str_repr


class FollowedBy(ParseElementEnhance):
    """Lookahead matching of the given parse expression.  FollowedBy
    does *not* advance the parsing position within the input string, it only
    verifies that the specified parse expression matches at the current
    position.  FollowedBy always returns a null token list."""

    def __init__(self, expr):
        super().__init__(expr)
        self.may_return_empty = True

    def parse_impl(self, instring, loc, do_actions=True):
        self.expr.try_parse(instring, loc)
        return loc, []


class NotAny(ParseElementEnhance):
    """Lookahead to disallow matching with the given parse expression.  NotAny
    does *not* advance the parsing position within the input string, it only
    verifies that the specified parse expression does *not* match at the current
    position.  Also, NotAny does *not* skip over leading whitespace. NotAny
    always returns a null token list.  May be constructed using the '~' operator."""

    def __init__(self, expr):
        super().__init__(expr)
        # ~ self.leave_whitespace()
        self.skip_whitespace = False  # do NOT use self.leave_whitespace(), don't want to propagate to exprs
        self.may_return_empty = True
        self.errmsg = "Found unwanted token, " + str(self.expr)
        # self.my_exception = ParseException("",0,self.errmsg,self)

    def parse_impl(self, instring, loc, do_actions=True):
        try:
            self.expr.try_parse(instring, loc)
        except (ParseException, IndexError):
            pass
        else:
            # ~ raise ParseException(instring, loc, self.errmsg )
            exc = self.my_exception
            exc.loc = loc
            exc.pstr = instring
            raise exc
        return loc, []

    def __str__(self):
        if hasattr(self, "name"):
            return self.name

        if self.str_repr is None:
            self.str_repr = "~{" + str(self.expr) + "}"

        return self.str_repr


class ZeroOrMore(ParseElementEnhance):
    """Optional repetition of zero or more of the given expression."""

    def __init__(self, expr):
        super().__init__(expr)
        self.may_return_empty = True

    def parse_impl(self, instring, loc, do_actions=True):
        tokens = []
        try:
            loc, tokens = self.expr._parse(
                instring, loc, do_actions, call_pre_parse=False
            )
            has_ignore_exprs = len(self.ignore_exprs) > 0
            while 1:
                if has_ignore_exprs:
                    preloc = self._skip_ignorables(instring, loc)
                else:
                    preloc = loc
                loc, tmptokens = self.expr._parse(instring, preloc, do_actions)
                if tmptokens or tmptokens.keys():
                    tokens += tmptokens
        except (ParseException, IndexError):
            pass

        return loc, tokens

    def __str__(self):
        if hasattr(self, "name"):
            return self.name

        if self.str_repr is None:
            self.str_repr = "[" + str(self.expr) + "]..."

        return self.str_repr

    def set_results_name(self, name, list_all_matches=False):
        ret = super().set_results_name(name, list_all_matches)
        ret.saveas_list = True
        return ret


class OneOrMore(ParseElementEnhance):
    """Repetition of one or more of the given expression."""

    def parse_impl(self, instring, loc, do_actions=True):
        # must be at least one
        loc, tokens = self.expr._parse(instring, loc, do_actions, call_pre_parse=False)
        try:
            has_ignore_exprs = len(self.ignore_exprs) > 0
            while 1:
                if has_ignore_exprs:
                    preloc = self._skip_ignorables(instring, loc)
                else:
                    preloc = loc
                loc, tmptokens = self.expr._parse(instring, preloc, do_actions)
                if tmptokens or tmptokens.keys():
                    tokens += tmptokens
        except (ParseException, IndexError):
            pass

        return loc, tokens

    def __str__(self):
        if hasattr(self, "name"):
            return self.name

        if self.str_repr is None:
            self.str_repr = "{" + str(self.expr) + "}..."

        return self.str_repr

    def set_results_name(self, name, list_all_matches=False):
        ret = super().set_results_name(name, list_all_matches)
        ret.saveas_list = True
        return ret


class _NullToken:
    def __bool__(self):
        return False

    __nonzero__ = __bool__

    def __str__(self):
        return ""


_optionalNotMatched = _NullToken()


class Optional(ParseElementEnhance):
    """Optional matching of the given expression.
    A default return string can also be specified, if the optional expression
    is not found.
    """

    def __init__(self, exprs, default=_optionalNotMatched):
        super().__init__(exprs, savelist=False)
        self.default_value = default
        self.may_return_empty = True

    def parse_impl(self, instring, loc, do_actions=True):
        try:
            loc, tokens = self.expr._parse(
                instring, loc, do_actions, call_pre_parse=False
            )
        except (ParseException, IndexError):
            if self.default_value is not _optionalNotMatched:
                if self.expr.results_name:
                    tokens = ParseResults([self.default_value])
                    tokens[self.expr.results_name] = self.default_value
                else:
                    tokens = [self.default_value]
            else:
                tokens = []
        return loc, tokens

    def __str__(self):
        if hasattr(self, "name"):
            return self.name

        if self.str_repr is None:
            self.str_repr = "[" + str(self.expr) + "]"

        return self.str_repr


class SkipTo(ParseElementEnhance):
    """Token for skipping over all undefined text until the matched expression is found.
    If include is set to true, the matched expression is also parsed (the skipped text
    and matched expression are returned as a 2-element list).  The ignore
    argument is used to define grammars (typically quoted strings and comments) that
    might contain false matches.
    """

    def __init__(self, other, include=False, ignore=None, fail_on=None):
        super().__init__(other)
        self.ignore_expr = ignore
        self.may_return_empty = True
        self.may_index_error = False
        self.includeMatch = include
        self.as_list = False
        if fail_on is not None and isinstance(fail_on, str):
            self.fail_on = Literal(fail_on)
        else:
            self.fail_on = fail_on
        self.errmsg = "No match found for " + str(self.expr)
        # self.my_exception = ParseException("",0,self.errmsg,self)

    def parse_impl(self, instring, loc, do_actions=True):
        start_loc = loc
        instrlen = len(instring)
        expr = self.expr
        fail_parse = False
        while loc <= instrlen:
            try:
                if self.fail_on:
                    try:
                        self.fail_on.try_parse(instring, loc)
                    except ParseBaseException:
                        pass
                    else:
                        fail_parse = True
                        raise ParseException(
                            instring, loc, "Found expression " + str(self.fail_on)
                        )
                    fail_parse = False
                if self.ignore_expr is not None:
                    while 1:
                        try:
                            loc = self.ignore_expr.try_parse(instring, loc)
                            print("found ignore_expr, advance to", loc)
                        except ParseBaseException:
                            break
                expr._parse(instring, loc, do_actions=False, call_pre_parse=False)
                skip_text = instring[start_loc:loc]
                if self.includeMatch:
                    loc, mat = expr._parse(
                        instring, loc, do_actions, call_pre_parse=False
                    )
                    if mat:
                        skip_res = ParseResults(skip_text)
                        skip_res += mat
                        return loc, [skip_res]
                    else:
                        return loc, [skip_text]
                else:
                    return loc, [skip_text]
            except (ParseException, IndexError):
                if fail_parse:
                    raise
                else:
                    loc += 1
        exc = self.my_exception
        exc.loc = loc
        exc.pstr = instring
        raise exc


class Forward(ParseElementEnhance):
    """Forward declaration of an expression to be defined later -
    used for recursive grammars, such as algebraic infix notation.
    When the expression is known, it is assigned to the Forward variable using the '<<' operator.

    Note: take care when assigning to Forward not to overlook precedence of operators.
    Specifically, '|' has a lower precedence than '<<', so that::
       fwdExpr << a | b | c
    will actually be evaluated as::
       (fwdExpr << a) | b | c
    thereby leaving b and c out as parseable alternatives.  It is recommended that you
    explicitly group the values inserted into the Forward::
       fwdExpr << (a | b | c)
    """

    def __init__(self, other=None):
        super().__init__(other, savelist=False)

    def __lshift__(self, other):
        if isinstance(other, str):
            other = Literal(other)
        self.expr = other
        self.may_return_empty = other.may_return_empty
        self.str_repr = None
        self.may_index_error = self.expr.may_index_error
        self.may_return_empty = self.expr.may_return_empty
        self.set_whitespace_chars(self.expr.white_chars)
        self.skip_whitespace = self.expr.skip_whitespace
        self.saveas_list = self.expr.saveas_list
        self.ignore_exprs.extend(self.expr.ignore_exprs)
        return None

    def leave_whitespace(self):
        self.skip_whitespace = False
        return self

    def streamline(self):
        if not self.streamlined:
            self.streamlined = True
            if self.expr is not None:
                self.expr.streamline()
        return self

    def validate(self, validate_trace=[]):
        if self not in validate_trace:
            tmp = validate_trace[:] + [self]
            if self.expr is not None:
                self.expr.validate(tmp)
        self.check_recursion([])

    def __str__(self):
        if hasattr(self, "name"):
            return self.name

        self._revertClass = self.__class__
        self.__class__ = _ForwardNoRecurse
        try:
            if self.expr is not None:
                ret_string = str(self.expr)
            else:
                ret_string = "None"
        finally:
            self.__class__ = self._revertClass
        return self.__class__.__name__ + ": " + ret_string

    def copy(self):
        if self.expr is not None:
            return super().copy()
        else:
            ret = Forward()
            ret << self
            return ret


class _ForwardNoRecurse(Forward):
    def __str__(self):
        return "..."


class TokenConverter(ParseElementEnhance):
    """Abstract subclass of ParseExpression, for converting parsed results."""

    def __init__(self, expr, savelist=False):
        super().__init__(expr)  # , savelist )
        self.saveas_list = False


class Upcase(TokenConverter):
    """Converter to upper case all matching tokens."""

    def __init__(self, *args):
        super().__init__(*args)
        warnings.warn(
            "Upcase class is deprecated, use upcase_tokens parse action instead",
            DeprecationWarning,
            stacklevel=2,
        )

    def post_parse(self, instring, loc, tokenlist):
        return list(map(string.upper, tokenlist))


class Combine(TokenConverter):
    """Converter to concatenate all matching tokens to a single string.
    By default, the matching patterns must also be contiguous in the input string;
    this can be disabled by specifying 'adjacent=False' in the constructor.
    """

    def __init__(self, expr, join_string="", adjacent=True):
        super().__init__(expr)
        # suppress whitespace-stripping in contained parse expressions, but re-enable it on the Combine itself
        if adjacent:
            self.leave_whitespace()
        self.adjacent = adjacent
        self.skip_whitespace = True
        self.join_string = join_string

    def ignore(self, other):
        if self.adjacent:
            ParserElement.ignore(self, other)
        else:
            super().ignore(other)
        return self

    def post_parse(self, instring, loc, tokenlist):
        ret_toks = tokenlist.copy()
        del ret_toks[:]
        ret_toks += ParseResults(
            ["".join(tokenlist._as_string_list(self.join_string))],
            modal=self.modal_results,
        )

        if self.results_name and len(ret_toks.keys()) > 0:
            return [ret_toks]
        else:
            return ret_toks


class Group(TokenConverter):
    """Converter to return the matched tokens as a list - useful for returning tokens of ZeroOrMore and OneOrMore expressions."""

    def __init__(self, expr):
        super().__init__(expr)
        self.saveas_list = True

    def post_parse(self, instring, loc, tokenlist):
        return [tokenlist]


class Dict(TokenConverter):
    """Converter to return a repetitive expression as a list, but also as a dictionary.
    Each element can also be referenced using the first token in the expression as its key.
    Useful for tabular report scraping when the first column can be used as a item key.
    """

    def __init__(self, exprs):
        super().__init__(exprs)
        self.saveas_list = True

    def post_parse(self, instring, loc, tokenlist):
        for i, tok in enumerate(tokenlist):
            if len(tok) == 0:
                continue
            ikey = tok[0]
            if isinstance(ikey, int):
                ikey = str(tok[0]).strip()
            if len(tok) == 1:
                tokenlist[ikey] = _ParseResultsWithOffset("", i)
            elif len(tok) == 2 and not isinstance(tok[1], ParseResults):
                tokenlist[ikey] = _ParseResultsWithOffset(tok[1], i)
            else:
                dictvalue = tok.copy()  # ParseResults(i)
                del dictvalue[0]
                if len(dictvalue) != 1 or (
                    isinstance(dictvalue, ParseResults) and dictvalue.keys()
                ):
                    tokenlist[ikey] = _ParseResultsWithOffset(dictvalue, i)
                else:
                    tokenlist[ikey] = _ParseResultsWithOffset(dictvalue[0], i)

        if self.results_name:
            return [tokenlist]
        else:
            return tokenlist


class Suppress(TokenConverter):
    """Converter for ignoring the results of a parsed expression."""

    def post_parse(self, instring, loc, tokenlist):
        return []

    def suppress(self):
        return self


class OnlyOnce:
    """Wrapper for parse actions, to ensure they are only called once."""

    def __init__(self, method_call):
        self.callable = ParserElement._normalize_parse_action_args(method_call)
        self.called = False

    def __call__(self, s, l, t):
        if not self.called:
            results = self.callable(s, l, t)
            self.called = True
            return results
        raise ParseException(s, l, "")

    def reset(self):
        self.called = False


def trace_parse_action(f):
    """Decorator for debugging parse actions."""
    f = ParserElement._normalize_parse_action_args(f)

    def z(*pa_args):
        this_func = f.func_name
        s, l, t = pa_args[-3:]
        if len(pa_args) > 3:
            this_func = pa_args[0].__class__.__name__ + "." + this_func
        sys.stderr.write(
            ">>entering %s(line: '%s', %d, %s)\n" % (this_func, line(l, s), l, t)
        )
        try:
            ret = f(*pa_args)
        except ValueError as exc:
            sys.stderr.write(f"<<leaving {this_func} (exception: {exc})\n")
            raise
        sys.stderr.write(f"<<leaving {this_func} (ret: {ret})\n")
        return ret

    try:
        z.__name__ = f.__name__
    except AttributeError:
        pass
    return z


#
# global helpers
#
def delimited_list(expr, delim=",", combine=False):
    """Helper to define a delimited list of expressions - the delimiter defaults to ','.
    By default, the list elements and delimiters can have intervening whitespace, and
    comments, but this can be overridden by passing 'combine=True' in the constructor.
    If combine is set to True, the matching tokens are returned as a single token
    string, with the delimiters included; otherwise, the matching tokens are returned
    as a list of tokens, with the delimiters suppressed.
    """
    dl_name = str(expr) + " [" + str(delim) + " " + str(expr) + "]..."
    if combine:
        return Combine(expr + ZeroOrMore(delim + expr)).set_name(dl_name)
    else:
        return (expr + ZeroOrMore(Suppress(delim) + expr)).set_name(dl_name)


def counted_array(expr):
    """Helper to define a counted list of expressions.
    This helper defines a pattern of the form::
        integer expr expr expr...
    where the leading integer tells how many expr expressions follow.
    The matched tokens returns the array of expr tokens as a list - the leading count token is suppressed.
    """
    array_expr = Forward()

    def count_field_parse_action(s, l, t):
        n = int(t[0])
        array_expr << (n and Group(And([expr] * n)) or Group(empty))
        return []

    return (
        Word(nums)
        .set_name("arrayLen")
        .set_parse_action(count_field_parse_action, call_during_try=True)
        + array_expr
    )


def _flatten(l):
    if type(l) is not list:
        return [l]
    if l == []:
        return l
    return _flatten(l[0]) + _flatten(l[1:])


def match_previous_literal(expr):
    """Helper to define an expression that is indirectly defined from
    the tokens matched in a previous expression, that is, it looks
    for a 'repeat' of a previous expression.  For example::
        first = Word(nums)
        second = match_previous_literal(first)
        match_expr = first + ":" + second
    will match "1:1", but not "1:2".  Because this matches a
    previous literal, will also match the leading "1:1" in "1:10".
    If this is not desired, use match_previous_expr.
    Do *not* use with packrat parsing enabled.
    """
    rep = Forward()

    def copy_token_to_repeater(s, l, t):
        if t:
            if len(t) == 1:
                rep << t[0]
            else:
                # flatten t tokens
                tflat = _flatten(t.as_list())
                rep << And([Literal(tt) for tt in tflat])
        else:
            rep << Empty()

    expr.add_parse_action(copy_token_to_repeater, call_during_try=True)
    return rep


def match_previous_expr(expr):
    """Helper to define an expression that is indirectly defined from
    the tokens matched in a previous expression, that is, it looks
    for a 'repeat' of a previous expression.  For example::
        first = Word(nums)
        second = match_previous_expr(first)
        match_expr = first + ":" + second
    will match "1:1", but not "1:2".  Because this matches by
    expressions, will *not* match the leading "1:1" in "1:10";
    the expressions are evaluated first, and then compared, so
    "1" is compared with "10".
    Do *not* use with packrat parsing enabled.
    """
    rep = Forward()
    e2 = expr.copy()
    rep << e2

    def copy_token_to_repeater(s, l, t):
        match_tokens = _flatten(t.as_list())

        def must_match_these_tokens(s, l, t):
            these_tokens = _flatten(t.as_list())
            if these_tokens != match_tokens:
                raise ParseException("", 0, "")

        rep.set_parse_action(must_match_these_tokens, call_during_try=True)

    expr.add_parse_action(copy_token_to_repeater, call_during_try=True)
    return rep


def _escape_regex_range_chars(s):
    # ~  escape these chars: ^-]
    for c in r"\^-]":
        s = s.replace(c, _bslash + c)
    s = s.replace("\n", r"\n")
    s = s.replace("\t", r"\t")
    return str(s)


def one_of(strs, caseless=False, use_regex=True):
    """Helper to quickly define a set of alternative Literals, and makes sure to do
    longest-first testing when there is a conflict, regardless of the input order,
    but returns a MatchFirst for best performance.

    Parameters:
     - strs - a string of space-delimited literals, or a list of string literals
     - caseless - (default=False) - treat all literals as caseless
     - use_regex - (default=True) - as an optimization, will generate a Regex
       object; otherwise, will generate a MatchFirst object (if caseless=True, or
       if creating a Regex raises an exception)
    """
    if caseless:
        isequal = lambda a, b: a.upper() == b.upper()
        masks = lambda a, b: b.upper().startswith(a.upper())
        parse_element_class = CaselessLiteral
    else:
        isequal = lambda a, b: a == b
        masks = lambda a, b: b.startswith(a)
        parse_element_class = Literal

    if isinstance(strs, (list, tuple)):
        symbols = list(strs[:])
    elif isinstance(strs, str):
        symbols = strs.split()
    else:
        warnings.warn(
            "Invalid argument to one_of, expected string or list",
            SyntaxWarning,
            stacklevel=2,
        )

    i = 0
    while i < len(symbols) - 1:
        cur = symbols[i]
        for j, other in enumerate(symbols[i + 1 :]):
            if isequal(other, cur):
                del symbols[i + j + 1]
                break
            elif masks(cur, other):
                del symbols[i + j + 1]
                symbols.insert(i, other)
                cur = other
                break
        else:
            i += 1

    if not caseless and use_regex:
        # ~ print (strs,"->", "|".join( [ _escapeRegexChars(sym) for sym in symbols] ))
        try:
            if len(symbols) == len("".join(symbols)):
                return Regex(
                    f"[{''.join([_escape_regex_range_chars(sym) for sym in symbols])}]"
                )
            else:
                return Regex("|".join([re.escape(sym) for sym in symbols]))
        except:
            warnings.warn(
                "Exception creating Regex for one_of, building MatchFirst",
                SyntaxWarning,
                stacklevel=2,
            )

    # last resort, just use MatchFirst
    return MatchFirst([parse_element_class(sym) for sym in symbols])


def dict_of(key, value):
    """Helper to easily and clearly define a dictionary by specifying the respective patterns
    for the key and value.  Takes care of defining the Dict, ZeroOrMore, and Group tokens
    in the proper order.  The key pattern can include delimiting markers or punctuation,
    as long as they are suppressed, thereby leaving the significant key text.  The value
    pattern can include named results, so that the Dict results can include named token
    fields.
    """
    return Dict(ZeroOrMore(Group(key + value)))


def original_text_for(expr, as_string=True):
    """Helper to return the original, untokenized text for a given expression.  Useful to
    restore the parsed fields of an HTML start tag into the raw tag text itself, or to
    revert separate tokens with intervening whitespace back to the original matching
    input text. Simpler to use than the parse action keep_original_text, and does not
    require the inspect module to chase up the call stack.  By default, returns a
    string containing the original parsed text.

    If the optional as_string argument is passed as False, then the return value is a
    ParseResults containing any results names that were originally matched, and a
    single token containing the original matched text from the input string.  So if
    the expression passed to original_text_for contains expressions with defined
    results names, you must set as_string to False if you want to preserve those
    results name values."""
    loc_marker = Empty().set_parse_action(lambda s, loc, t: loc)
    match_expr = loc_marker("_original_start") + expr + loc_marker("_original_end")
    if as_string:
        extract_text = lambda s, l, t: s[t._original_start : t._original_end]
    else:

        def extract_text(s, l, t):
            del t[:]
            t.insert(0, s[t._original_start : t._original_end])
            del t["_original_start"]
            del t["_original_end"]

    match_expr.set_parse_action(extract_text)
    return match_expr


# convenience constants for positional expressions
empty = Empty().set_name("empty")
lineStart = LineStart().set_name("lineStart")
lineEnd = LineEnd().set_name("lineEnd")
stringStart = StringStart().set_name("stringStart")
stringEnd = StringEnd().set_name("stringEnd")

_escapedPunc = Word(_bslash, r"\[]-*.$+^?()~ ", exact=2).set_parse_action(
    lambda s, l, t: t[0][1]
)
_printables_less_backslash = "".join([c for c in printables if c not in r"\]"])
_escapedHexChar = Combine(Suppress(_bslash + "0x") + Word(hexnums)).set_parse_action(
    lambda s, l, t: chr(int(t[0], 16))
)
_escapedOctChar = Combine(Suppress(_bslash) + Word("0", "01234567")).set_parse_action(
    lambda s, l, t: chr(int(t[0], 8))
)
_singleChar = (
    _escapedPunc
    | _escapedHexChar
    | _escapedOctChar
    | Word(_printables_less_backslash, exact=1)
)
_charRange = Group(_singleChar + Suppress("-") + _singleChar)
_reBracketExpr = (
    Literal("[")
    + Optional("^").set_results_name("negate")
    + Group(OneOrMore(_charRange | _singleChar)).set_results_name("body")
    + "]"
)

_expanded = lambda p: (
    isinstance(p, ParseResults)
    and "".join([chr(c) for c in range(ord(p[0]), ord(p[1]) + 1)])
    or p
)


def srange(s):
    r"""Helper to easily define string ranges for use in Word construction.  Borrows
    syntax from regexp '[]' string range definitions::
       srange("[0-9]")   -> "0123456789"
       srange("[a-z]")   -> "abcdefghijklmnopqrstuvwxyz"
       srange("[a-z$_]") -> "abcdefghijklmnopqrstuvwxyz$_"
    The input string must be enclosed in []'s, and the returned string is the expanded
    character set joined into a single string.
    The values enclosed in the []'s may be::
       a single character
       an escaped character with a leading backslash (such as \- or \])
       an escaped hex character with a leading '\0x' (\0x21, which is a '!' character)
       an escaped octal character with a leading '\0' (\041, which is a '!' character)
       a range of any of the above, separated by a dash ('a-z', etc.)
       any combination of the above ('aeiouy', 'a-zA-Z0-9_$', etc.)
    """
    try:
        return "".join(
            [_expanded(part) for part in _reBracketExpr.parse_string(s).body]
        )
    except:
        return ""


def match_only_at_col(n):
    """Helper method for defining parse actions that require matching at a specific
    column in the input text.
    """

    def verify_col(strg, locn, toks):
        if col(locn, strg) != n:
            raise ParseException(strg, locn, "matched token not at column %d" % n)

    return verify_col


def replace_with(repl_str):
    """Helper method for common parse actions that simply return a literal value.  Especially
    useful when used with transform_string().
    """

    def _repl_func(*args):
        return [repl_str]

    return _repl_func


def remove_quotes(s, l, t):
    """Helper parse action for removing quotation marks from parsed quoted strings.
    To use, add this parse action to quoted string using::
      quotedString.set_parse_action( remove_quotes )
    """
    return t[0][1:-1]


def upcase_tokens(s, l, t):
    """Helper parse action to convert tokens to upper case."""
    return [tt.upper() for tt in map(str, t)]


def downcase_tokens(s, l, t):
    """Helper parse action to convert tokens to lower case."""
    return [tt.lower() for tt in map(str, t)]


def keep_original_text(s, start_loc, t):
    """Helper parse action to preserve original parsed text,
    overriding any nested parse actions."""
    try:
        endloc = get_tokens_end_loc()
    except ParseException:
        raise ParseFatalException(
            "incorrect usage of keep_original_text - may only be called as a parse action"
        )
    del t[:]
    t += ParseResults(s[start_loc:endloc])
    return t


def get_tokens_end_loc():
    """Method to be called from within a parse action to determine the end
    location of the parsed tokens."""
    import inspect

    fstack = inspect.stack()
    try:
        # search up the stack (through intervening argument normalizers) for correct calling routine
        for f in fstack[2:]:
            if f[3] == "_parse_no_cache":
                endloc = f[0].f_locals["loc"]
                return endloc
        else:
            raise ParseFatalException(
                "incorrect usage of get_tokens_end_loc - may only be called from within a parse action"
            )
    finally:
        del fstack


def _make_tags(tag_str, xml):
    """Internal helper to construct opening and closing tag expressions, given a tag name"""
    if isinstance(tag_str, str):
        resname = tag_str
        tag_str = Keyword(tag_str, caseless=not xml)
    else:
        resname = tag_str.name

    tag_attr_name = Word(alphas, alphanums + "_-:")
    if xml:
        tag_attr_value = dblQuotedString.copy().set_parse_action(remove_quotes)
        open_tag = (
            Suppress("<")
            + tag_str
            + Dict(ZeroOrMore(Group(tag_attr_name + Suppress("=") + tag_attr_value)))
            + Optional("/", default=[False])
            .set_results_name("empty")
            .set_parse_action(lambda s, l, t: t[0] == "/")
            + Suppress(">")
        )
    else:
        printables_less_rabrack = "".join([c for c in printables if c not in ">"])
        tag_attr_value = quotedString.copy().set_parse_action(remove_quotes) | Word(
            printables_less_rabrack
        )
        open_tag = (
            Suppress("<")
            + tag_str
            + Dict(
                ZeroOrMore(
                    Group(
                        tag_attr_name.set_parse_action(downcase_tokens)
                        + Optional(Suppress("=") + tag_attr_value)
                    )
                )
            )
            + Optional("/", default=[False])
            .set_results_name("empty")
            .set_parse_action(lambda s, l, t: t[0] == "/")
            + Suppress(">")
        )
    close_tag = Combine(_L("</") + tag_str + ">")

    open_tag = open_tag.set_results_name(
        "start" + "".join(resname.replace(":", " ").title().split())
    ).set_name(f"<{tag_str}>")
    close_tag = close_tag.set_results_name(
        "end" + "".join(resname.replace(":", " ").title().split())
    ).set_name(f"</{tag_str}>")

    return open_tag, close_tag


def make_html_tags(tag_str):
    """Helper to construct opening and closing tag expressions for HTML, given a tag name"""
    return _make_tags(tag_str, False)


def make_xml_tags(tag_str):
    """Helper to construct opening and closing tag expressions for XML, given a tag name"""
    return _make_tags(tag_str, True)


def with_attribute(*args, **attr_dict):
    """Helper to create a validating parse action to be used with start tags created
    with make_xml_tags or make_html_tags. Use with_attribute to qualify a starting tag
    with a required attribute value, to avoid false matches on common tags such as
    <TD> or <DIV>.

    Call with_attribute with a series of attribute names and values. Specify the list
    of filter attributes names and values as:
     - keyword arguments, as in (class="Customer",align="right"), or
     - a list of name-value tuples, as in ( ("ns1:class", "Customer"), ("ns2:align","right") )
    For attribute names with a namespace prefix, you must use the second form.  Attribute
    names are matched insensitive to upper/lower case.

    To verify that the attribute exists, but without specifying a value, pass
    with_attribute.ANY_VALUE as the value.
    """
    if args:
        attrs = args[:]
    else:
        attrs = attr_dict.items()
    attrs = list(attrs)

    def pa(s, l, tokens):
        for attr_name, attr_value in attrs:
            if attr_name not in tokens:
                raise ParseException(s, l, "no matching attribute " + attr_name)
            if (
                attr_value != with_attribute.ANY_VALUE
                and tokens[attr_name] != attr_value
            ):
                raise ParseException(
                    s,
                    l,
                    "attribute '%s' has value '%s', must be '%s'"
                    % (attr_name, tokens[attr_name], attr_value),
                )

    return pa


with_attribute.ANY_VALUE = object()

opAssoc = _Constants()
opAssoc.LEFT = object()
opAssoc.RIGHT = object()


def operator_precedence(base_expr, op_list):
    """Helper method for constructing grammars of expressions made up of
    operators working in a precedence hierarchy.  Operators may be unary or
    binary, left- or right-associative.  Parse actions can also be attached
    to operator expressions.

    Parameters:
     - base_expr - expression representing the most basic element for the nested
     - op_list - list of tuples, one for each operator precedence level in the
       expression grammar; each tuple is of the form
       (op_expr, numTerms, right_left_assoc, parse_action), where:
        - op_expr is the pyparsing expression for the operator;
           may also be a string, which will be converted to a Literal;
           if numTerms is 3, op_expr is a tuple of two expressions, for the
           two operators separating the 3 terms
        - numTerms is the number of terms for this operator (must
           be 1, 2, or 3)
        - right_left_assoc is the indicator whether the operator is
           right or left associative, using the pyparsing-defined
           constants opAssoc.RIGHT and opAssoc.LEFT.
        - parse_action is the parse action to be associated with
           expressions matching this operator expression (the
           parse action tuple member may be omitted)
    """
    ret = Forward()
    last_expr = base_expr | (Suppress("(") + ret + Suppress(")"))
    for i, oper_def in enumerate(op_list):
        op_expr, arity, right_left_assoc, pa = (oper_def + (None,))[:4]
        if arity == 3:
            if op_expr is None or len(op_expr) != 2:
                raise ValueError(
                    "if numterms=3, op_expr must be a tuple or list of two expressions"
                )
            op_expr1, op_expr2 = op_expr
        this_expr = Forward()  # .set_name("expr%d" % i)
        if right_left_assoc == opAssoc.LEFT:
            if arity == 1:
                match_expr = FollowedBy(last_expr + op_expr) + Group(
                    last_expr + OneOrMore(op_expr)
                )
            elif arity == 2:
                if op_expr is not None:
                    match_expr = FollowedBy(last_expr + op_expr + last_expr) + Group(
                        last_expr + OneOrMore(op_expr + last_expr)
                    )
                else:
                    match_expr = FollowedBy(last_expr + last_expr) + Group(
                        last_expr + OneOrMore(last_expr)
                    )
            elif arity == 3:
                match_expr = FollowedBy(
                    last_expr + op_expr1 + last_expr + op_expr2 + last_expr
                ) + Group(last_expr + op_expr1 + last_expr + op_expr2 + last_expr)
            else:
                raise ValueError(
                    "operator must be unary (1), binary (2), or ternary (3)"
                )
        elif right_left_assoc == opAssoc.RIGHT:
            if arity == 1:
                # try to avoid LR with this extra test
                if not isinstance(op_expr, Optional):
                    op_expr = Optional(op_expr)
                match_expr = FollowedBy(op_expr.expr + this_expr) + Group(
                    op_expr + this_expr
                )
            elif arity == 2:
                if op_expr is not None:
                    match_expr = FollowedBy(last_expr + op_expr + this_expr) + Group(
                        last_expr + OneOrMore(op_expr + this_expr)
                    )
                else:
                    match_expr = FollowedBy(last_expr + this_expr) + Group(
                        last_expr + OneOrMore(this_expr)
                    )
            elif arity == 3:
                match_expr = FollowedBy(
                    last_expr + op_expr1 + this_expr + op_expr2 + this_expr
                ) + Group(last_expr + op_expr1 + this_expr + op_expr2 + this_expr)
            else:
                raise ValueError(
                    "operator must be unary (1), binary (2), or ternary (3)"
                )
        else:
            raise ValueError("operator must indicate right or left associativity")
        if pa:
            match_expr.set_parse_action(pa)
        this_expr << (match_expr | last_expr)
        last_expr = this_expr
    ret << last_expr
    return ret


dblQuotedString = Regex(
    r'"(?:[^"\n\r\\]|(?:"")|(?:\\x[0-9a-fA-F]+)|(?:\\.))*"'
).set_name("string enclosed in double quotes")
sglQuotedString = Regex(
    r"'(?:[^'\n\r\\]|(?:'')|(?:\\x[0-9a-fA-F]+)|(?:\\.))*'"
).set_name("string enclosed in single quotes")
quotedString = Regex(
    r"""(?:"(?:[^"\n\r\\]|(?:"")|(?:\\x[0-9a-fA-F]+)|(?:\\.))*")|(?:'(?:[^'\n\r\\]|(?:'')|(?:\\x[0-9a-fA-F]+)|(?:\\.))*')"""
).set_name("quotedString using single or double quotes")
unicodeString = Combine(_L("u") + quotedString.copy())


def nested_expr(opener="(", closer=")", content=None, ignore_expr=quotedString):
    """Helper method for defining nested lists enclosed in opening and closing
    delimiters ("(" and ")" are the default).

    Parameters:
     - opener - opening character for a nested list (default="("); can also be a pyparsing expression
     - closer - closing character for a nested list (default=")"); can also be a pyparsing expression
     - content - expression for items within the nested lists (default=None)
     - ignore_expr - expression for ignoring opening and closing delimiters (default=quotedString)

    If an expression is not provided for the content argument, the nested
    expression will capture all whitespace-delimited content between delimiters
    as a list of separate values.

    Use the ignore_expr argument to define expressions that may contain
    opening or closing characters that should not be treated as opening
    or closing characters for nesting, such as quotedString or a comment
    expression.  Specify multiple expressions using an Or or MatchFirst.
    The default is quotedString, but if no expressions are to be ignored,
    then pass None for this argument.
    """
    if opener == closer:
        raise ValueError("opening and closing strings cannot be the same")
    if content is None:
        if isinstance(opener, str) and isinstance(closer, str):
            if len(opener) == 1 and len(closer) == 1:
                if ignore_expr is not None:
                    content = Combine(
                        OneOrMore(
                            ~ignore_expr
                            + CharsNotIn(
                                opener + closer + ParserElement.DEFAULT_WHITE_CHARS,
                                exact=1,
                            )
                        )
                    ).set_parse_action(lambda t: t[0].strip())
                else:
                    content = empty + CharsNotIn(
                        opener + closer + ParserElement.DEFAULT_WHITE_CHARS
                    ).set_parse_action(lambda t: t[0].strip())
            else:
                if ignore_expr is not None:
                    content = Combine(
                        OneOrMore(
                            ~ignore_expr
                            + ~Literal(opener)
                            + ~Literal(closer)
                            + CharsNotIn(ParserElement.DEFAULT_WHITE_CHARS, exact=1)
                        )
                    ).set_parse_action(lambda t: t[0].strip())
                else:
                    content = Combine(
                        OneOrMore(
                            ~Literal(opener)
                            + ~Literal(closer)
                            + CharsNotIn(ParserElement.DEFAULT_WHITE_CHARS, exact=1)
                        )
                    ).set_parse_action(lambda t: t[0].strip())
        else:
            raise ValueError(
                "opening and closing arguments must be strings if no content expression is given"
            )
    ret = Forward()
    if ignore_expr is not None:
        ret << Group(
            Suppress(opener)
            + ZeroOrMore(ignore_expr | ret | content)
            + Suppress(closer)
        )
    else:
        ret << Group(Suppress(opener) + ZeroOrMore(ret | content) + Suppress(closer))
    return ret


def indented_block(block_statement_expr, indent_stack, indent=True):
    """Helper method for defining space-delimited indentation blocks, such as
    those used to define block statements in Python source code.

    Parameters:
     - block_statement_expr - expression defining syntax of statement that
         is repeated within the indented block
     - indent_stack - list created by caller to manage indentation stack
         (multiple statementWithIndentedBlock expressions within a single grammar
         should share a common indent_stack)
     - indent - boolean indicating whether block must be indented beyond the
         the current level; set to False for block of left-most statements
         (default=True)

    A valid block must contain at least one blockStatement.
    """

    def check_peer_indent(s, l, t):
        if l >= len(s):
            return
        cur_col = col(l, s)
        if cur_col != indent_stack[-1]:
            if cur_col > indent_stack[-1]:
                raise ParseFatalException(s, l, "illegal nesting")
            raise ParseException(s, l, "not a peer entry")

    def check_sub_indent(s, l, t):
        cur_col = col(l, s)
        if cur_col > indent_stack[-1]:
            indent_stack.append(cur_col)
        else:
            raise ParseException(s, l, "not a subentry")

    def check_unindent(s, l, t):
        if l >= len(s):
            return
        cur_col = col(l, s)
        if not (
            indent_stack and cur_col < indent_stack[-1] and cur_col <= indent_stack[-2]
        ):
            raise ParseException(s, l, "not an unindent")
        indent_stack.pop()

    NL = OneOrMore(LineEnd().set_whitespace_chars("\t ").suppress())
    INDENT = Empty() + Empty().set_parse_action(check_sub_indent)
    PEER = Empty().set_parse_action(check_peer_indent)
    UNDENT = Empty().set_parse_action(check_unindent)
    if indent:
        sm_expr = Group(
            Optional(NL)
            + FollowedBy(block_statement_expr)
            + INDENT
            + (OneOrMore(PEER + Group(block_statement_expr) + Optional(NL)))
            + UNDENT
        )
    else:
        sm_expr = Group(
            Optional(NL)
            + (OneOrMore(PEER + Group(block_statement_expr) + Optional(NL)))
        )
    block_statement_expr.ignore(_bslash + LineEnd())
    return sm_expr


alphas8bit = srange(r"[\0xc0-\0xd6\0xd8-\0xf6\0xf8-\0xff]")
punc8bit = srange(r"[\0xa1-\0xbf\0xd7\0xf7]")

anyOpenTag, anyCloseTag = make_html_tags(Word(alphas, alphanums + "_:"))
commonHTMLEntity = Combine(
    _L("&") + one_of("gt lt amp nbsp quot").set_results_name("entity") + ";"
).streamline()
_htmlEntityMap = dict(zip("gt lt amp nbsp quot".split(), '><& "'))
replaceHTMLEntity = (
    lambda t: t.entity in _htmlEntityMap and _htmlEntityMap[t.entity] or None
)

# it's easy to get these comment structures wrong - they're very common, so may as well make them available
cStyleComment = Regex(r"/\*(?:[^*]*\*+)+?/").set_name("C style comment")

htmlComment = Regex(r"<!--[\s\S]*?-->")
restOfLine = Regex(r".*").leave_whitespace()
dblSlashComment = Regex(r"\/\/(\\\n|.)*").set_name("// comment")
cppStyleComment = Regex(
    r"/(?:\*(?:[^*]*\*+)+?/|/[^\n]*(?:\n[^\n]*)*?(?:(?<!\\)|\Z))"
).set_name("C++ style comment")

javaStyleComment = cppStyleComment
pythonStyleComment = Regex(r"#.*").set_name("Python style comment")
_noncomma = "".join([c for c in printables if c != ","])
_commasepitem = (
    Combine(
        OneOrMore(Word(_noncomma) + Optional(Word(" \t") + ~Literal(",") + ~LineEnd()))
    )
    .streamline()
    .set_name("commaItem")
)
commaSeparatedList = delimited_list(
    Optional(quotedString | _commasepitem, default="")
).set_name("commaSeparatedList")


if __name__ == "__main__":

    def test(teststring):
        try:
            tokens = simpleSQL.parse_string(teststring)
            tokenlist = tokens.as_list()
            print(teststring + "->" + str(tokenlist))
            print("tokens = " + str(tokens))
            print("tokens.columns = " + str(tokens.columns))
            print("tokens.tables = " + str(tokens.tables))
            print(tokens.as_xml("SQL", True))
        except ParseBaseException as err:
            print(teststring + "->")
            print(err.line)
            print(" " * (err.column - 1) + "^")
            print(err)
        print()

    selectToken = CaselessLiteral("select")
    fromToken = CaselessLiteral("from")

    ident = Word(alphas, alphanums + "_$")
    columnName = delimited_list(ident, ".", combine=True).set_parse_action(
        upcase_tokens
    )
    columnNameList = Group(delimited_list(columnName))  # .set_name("columns")
    tableName = delimited_list(ident, ".", combine=True).set_parse_action(upcase_tokens)
    tableNameList = Group(delimited_list(tableName))  # .set_name("tables")
    simpleSQL = (
        selectToken
        + ("*" | columnNameList).set_results_name("columns")
        + fromToken
        + tableNameList.set_results_name("tables")
    )

    test("SELECT * from XYZZY, ABC")
    test("select * from SYS.XYZZY")
    test("Select A from Sys.dual")
    test("Select AA,BB,CC from Sys.dual")
    test("Select A, B, C from Sys.dual")
    test("Select A, B, C from Sys.dual")
    test("Xelect A, B, C from Sys.dual")
    test("Select A, B, C frox Sys.dual")
    test("Select")
    test("Select ^^^ frox Sys.dual")
    test("Select A, B, C from Sys.dual, Table2   ")
