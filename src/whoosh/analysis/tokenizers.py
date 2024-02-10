# Copyright 2007 Matt Chaput. All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#    1. Redistributions of source code must retain the above copyright notice,
#       this list of conditions and the following disclaimer.
#
#    2. Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY MATT CHAPUT ``AS IS'' AND ANY EXPRESS OR
# IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
# EVENT SHALL MATT CHAPUT OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
# OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
# LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
# NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
# EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
# The views and conclusions contained in the software and documentation are
# those of the authors and should not be interpreted as representing official
# policies, either expressed or implied, of Matt Chaput.

from whoosh.analysis.acore import Composable, Token
from whoosh.util.text import rcompile

default_pattern = rcompile(r"[\w\*]+(\.?[\w\*]+)*")


# Tokenizers


class Tokenizer(Composable):
    """Base class for tokenizers.

    Tokenizers are responsible for breaking text into individual tokens. This base class
    provides the basic structure and behavior that all tokenizers should follow.

    Subclasses should override the `tokenize` method to implement the tokenization logic.

    Example usage:
        tokenizer = Tokenizer()
        tokens = tokenizer.tokenize("Hello, world!")
        for token in tokens:
            print(token)

    Attributes:
        None

    Methods:
        __eq__(self, other): Compare if two tokenizers are equal.

    """

    def __eq__(self, other):
        """Compare if two tokenizers are equal.

        Args:
            other (object): The other tokenizer object to compare.

        Returns:
            bool: True if the tokenizers are equal, False otherwise.

        """
        return other and self.__class__ is other.__class__


class IDTokenizer(Tokenizer):
    """Yields the entire input string as a single token. For use in indexed but
    untokenized fields, such as a document's path.

    Example:
        idt = IDTokenizer()
        [token.text for token in idt("/a/b 123 alpha")]
        Output: ["/a/b 123 alpha"]

    Args:
        positions (bool, optional): Whether to store token positions. Defaults to False.
        chars (bool, optional): Whether to store token character offsets. Defaults to False.
        keeporiginal (bool, optional): Whether to store the original token text. Defaults to False.
        removestops (bool, optional): Whether to remove stop words. Defaults to True.
        start_pos (int, optional): The starting position of the token. Defaults to 0.
        start_char (int, optional): The starting character offset of the token. Defaults to 0.
        mode (str, optional): The tokenization mode. Defaults to "".
        **kwargs: Additional keyword arguments.

    Yields:
        Token: The token object containing the token information.
    """

    def __call__(
        self,
        value,
        positions=False,
        chars=False,
        keeporiginal=False,
        removestops=True,
        start_pos=0,
        start_char=0,
        mode="",
        **kwargs,
    ):
        """
        Tokenizes the given value and yields a Token object.

        Args:
            value (str): The input string to be tokenized.
            positions (bool, optional): Whether to include position information in the Token object. Defaults to False.
            chars (bool, optional): Whether to include character information in the Token object. Defaults to False.
            keeporiginal (bool, optional): Whether to store the original value in the Token object. Defaults to False.
            removestops (bool, optional): Whether to remove stop words from the Token object. Defaults to True.
            start_pos (int, optional): The starting position of the Token object. Defaults to 0.
            start_char (int, optional): The starting character position of the Token object. Defaults to 0.
            mode (str, optional): The tokenization mode. Defaults to "".
            **kwargs: Additional keyword arguments to be passed to the Token object.

        Yields:
            Token: A Token object representing a tokenized value.

        Raises:
            AssertionError: If the input value is not a string.

        """
        assert isinstance(value, str), f"{value!r} is not unicode"
        t = Token(positions, chars, removestops=removestops, mode=mode, **kwargs)
        t.text = value
        t.boost = 1.0
        if keeporiginal:
            t.original = value
        if positions:
            t.pos = start_pos + 1
        if chars:
            t.startchar = start_char
            t.endchar = start_char + len(value)
        yield t


class RegexTokenizer(Tokenizer):
    """
    Uses a regular expression to extract tokens from text.

    Example:
    >>> rex = RegexTokenizer()
    >>> [token.text for token in rex("hi there 3.141 big-time under_score")]
    ["hi", "there", "3.141", "big", "time", "under_score"]

    Args:
        expression (Union[str, Pattern]): A regular expression object or string. Each match
            of the expression equals a token. Group 0 (the entire matched text)
            is used as the text of the token. If you require more complicated
            handling of the expression match, simply write your own tokenizer.
        gaps (bool): If True, the tokenizer *splits* on the expression, rather
            than matching on the expression.
    """

    def __init__(self, expression=default_pattern, gaps=False):
        """
        Initialize the RegexTokenizer.

        Args:
            expression (Union[str, Pattern]): A regular expression object or string. Each match
                of the expression equals a token. Group 0 (the entire matched text)
                is used as the text of the token. If you require more complicated
                handling of the expression match, simply write your own tokenizer.
            gaps (bool): If True, the tokenizer *splits* on the expression, rather
                than matching on the expression.
        """

        self.expression = rcompile(expression)
        self.gaps = gaps

    def __eq__(self, other):
        """
        Compare the RegexTokenizer with another object for equality.

        Args:
            other (object): The object to compare with.

        Returns:
            bool: True if the objects are equal, False otherwise.
        """

        if self.__class__ is other.__class__:
            if self.expression.pattern == other.expression.pattern:
                return True
        return False

    def __call__(
        self,
        value,
        positions=False,
        chars=False,
        keeporiginal=False,
        removestops=True,
        start_pos=0,
        start_char=0,
        tokenize=True,
        mode="",
        **kwargs,
    ):
        """
        Tokenize the input value using the RegexTokenizer.

        Args:
            value (str): The unicode string to tokenize.
            positions (bool): Whether to record token positions in the token.
            chars (bool): Whether to record character offsets in the token.
            keeporiginal (bool): Whether to keep the original text of the token.
            removestops (bool): Whether to remove stop words from the token.
            start_pos (int): The position number of the first token.
            start_char (int): The offset of the first character of the first token.
            tokenize (bool): If True, the text should be tokenized.
            mode (str): The tokenization mode.

        Yields:
            Token: The generated tokens.
        """

        assert isinstance(value, str), f"{repr(value)} is not unicode"

        t = Token(positions, chars, removestops=removestops, mode=mode, **kwargs)
        if not tokenize:
            t.original = t.text = value
            t.boost = 1.0
            if positions:
                t.pos = start_pos
            if chars:
                t.startchar = start_char
                t.endchar = start_char + len(value)
            yield t
        elif not self.gaps:
            # The default: expression matches are used as tokens
            for pos, match in enumerate(self.expression.finditer(value)):
                t.text = match.group(0)
                t.boost = 1.0
                if keeporiginal:
                    t.original = t.text
                t.stopped = False
                if positions:
                    t.pos = start_pos + pos
                if chars:
                    t.startchar = start_char + match.start()
                    t.endchar = start_char + match.end()
                yield t
        else:
            # When gaps=True, iterate through the matches and
            # yield the text between them.
            prevend = 0
            pos = start_pos
            for match in self.expression.finditer(value):
                start = prevend
                end = match.start()
                text = value[start:end]
                if text:
                    t.text = text
                    t.boost = 1.0
                    if keeporiginal:
                        t.original = t.text
                    t.stopped = False
                    if positions:
                        t.pos = pos
                        pos += 1
                    if chars:
                        t.startchar = start_char + start
                        t.endchar = start_char + end

                    yield t

                prevend = match.end()

            # If the last "gap" was before the end of the text,
            # yield the last bit of text as a final token.
            if prevend < len(value):
                t.text = value[prevend:]
                t.boost = 1.0
                if keeporiginal:
                    t.original = t.text
                t.stopped = False
                if positions:
                    t.pos = pos
                if chars:
                    t.startchar = prevend
                    t.endchar = len(value)
                yield t


class CharsetTokenizer(Tokenizer):
    """Tokenizes and translates text according to a character mapping object.
    Characters that map to None are considered token break characters. For all
    other characters the map is used to translate the character. This is useful
    for case and accent folding.

    This tokenizer loops character-by-character and so will likely be much
    slower than :class:`RegexTokenizer`.

    One way to get a character mapping object is to convert a Sphinx charset
    table file using :func:`whoosh.support.charset.charset_table_to_dict`.

    >>> from whoosh.support.charset import charset_table_to_dict
    >>> from whoosh.support.charset import default_charset
    >>> charmap = charset_table_to_dict(default_charset)
    >>> chtokenizer = CharsetTokenizer(charmap)
    >>> [t.text for t in chtokenizer(u'Stra\\xdfe ABC')]
    [u'strase', u'abc']

    The Sphinx charset table format is described at
    http://www.sphinxsearch.com/docs/current.html#conf-charset-table.
    """

    __inittype__ = {"charmap": str}

    def __init__(self, charmap):
        """
        Initialize the Tokenizer with a character map.

        :param charmap: A mapping from integer character numbers to Unicode
            characters, as used by the unicode.translate() method.
        :type charmap: dict
        """
        self.charmap = charmap

    def __eq__(self, other):
        """
        Compare this tokenizer with another tokenizer for equality.

        Parameters:
        - other: The other tokenizer to compare with.

        Returns:
        - True if the tokenizers are equal, False otherwise.
        """
        return (
            other
            and self.__class__ is other.__class__
            and self.charmap == other.charmap
        )

    def __call__(
        self,
        value,
        positions=False,
        chars=False,
        keeporiginal=False,
        removestops=True,
        start_pos=0,
        start_char=0,
        tokenize=True,
        mode="",
        **kwargs,
    ):
        """
        Tokenizes a given unicode string.

        :param value: The unicode string to tokenize.
        :param positions: Whether to record token positions in the token.
        :param chars: Whether to record character offsets in the token.
        :param keeporiginal: Whether to keep the original text in the token.
        :param removestops: Whether to remove stop words from the token.
        :param start_pos: The position number of the first token.
        :param start_char: The offset of the first character of the first token.
        :param tokenize: If True, the text should be tokenized.
        :param mode: The tokenization mode.
        :param kwargs: Additional keyword arguments.

        :return: A generator that yields Token objects.

        :raises AssertionError: If the value is not a unicode string.
        """

        assert isinstance(value, str), f"{value!r} is not unicode"

        t = Token(positions, chars, removestops=removestops, mode=mode, **kwargs)
        if not tokenize:
            t.original = t.text = value
            t.boost = 1.0
            if positions:
                t.pos = start_pos
            if chars:
                t.startchar = start_char
                t.endchar = start_char + len(value)
            yield t
        else:
            text = ""
            charmap = self.charmap
            pos = start_pos
            startchar = currentchar = start_char
            for char in value:
                tchar = charmap[ord(char)]
                if tchar:
                    text += tchar
                else:
                    if currentchar > startchar:
                        t.text = text
                        t.boost = 1.0
                        if keeporiginal:
                            t.original = t.text
                        if positions:
                            t.pos = pos
                            pos += 1
                        if chars:
                            t.startchar = startchar
                            t.endchar = currentchar
                        yield t
                    startchar = currentchar + 1
                    text = ""

                currentchar += 1

            if currentchar > startchar:
                t.text = value[startchar:currentchar]
                t.boost = 1.0
                if keeporiginal:
                    t.original = t.text
                if positions:
                    t.pos = pos
                if chars:
                    t.startchar = startchar
                    t.endchar = currentchar
                yield t


def SpaceSeparatedTokenizer():
    """
    Returns a RegexTokenizer that splits tokens by whitespace.

    This tokenizer splits input text into tokens based on whitespace characters (spaces, tabs, newlines).
    It uses a regular expression pattern to match and extract tokens.

    Example:
        sst = SpaceSeparatedTokenizer()
        tokens = [token.text for token in sst("hi there big-time, what's up")]
        print(tokens)
        # Output: ["hi", "there", "big-time,", "what's", "up"]

    Returns:
        A RegexTokenizer object that tokenizes input text based on whitespace.

    Note:
        The regular expression pattern used by this tokenizer is r"[^ \t\r\n]+",
        which matches one or more characters that are not whitespace.

    """
    return RegexTokenizer(r"[^ \t\r\n]+")


def CommaSeparatedTokenizer():
    """
    Tokenizes text by splitting tokens using commas.

    This tokenizer splits the input text into tokens by using commas as the delimiter.
    It also applies the `StripFilter` to remove leading and trailing whitespace from each token.

    Example:
        >>> cst = CommaSeparatedTokenizer()
        >>> [token.text for token in cst("hi there, what's , up")]
        ["hi there", "what's", "up"]

    Returns:
        A tokenizer object that can be used to tokenize text.

    Note:
        The tokenizer relies on the `RegexTokenizer` and `StripFilter` classes from the `whoosh.analysis` module.

    """
    from whoosh.analysis.filters import StripFilter

    return RegexTokenizer(r"[^,]+") | StripFilter()


class PathTokenizer(Tokenizer):
    """A simple tokenizer that given a string ``"/a/b/c"`` yields tokens
    ``["/a", "/a/b", "/a/b/c"]``.

    Args:
        expression (str, optional): The regular expression pattern used to tokenize the input string.
            Defaults to "[^/]+".

    Attributes:
        expr (Pattern): The compiled regular expression pattern.

    """

    def __init__(self, expression="[^/]+"):
        """
        Initialize the Tokenizer with the given regular expression pattern.

        Args:
            expression (str, optional): The regular expression pattern used for tokenization.
                Defaults to "[^/]+".

        Returns:
            None
        """
        self.expr = rcompile(expression)

    def __call__(self, value, positions=False, start_pos=0, **kwargs):
        """Tokenizes the input string.

        Args:
            value (str): The input string to be tokenized.
            positions (bool, optional): Whether to include token positions. Defaults to False.
            start_pos (int, optional): The starting position for token positions. Defaults to 0.
            **kwargs: Additional keyword arguments.

        Yields:
            Token: The generated tokens.

        Raises:
            AssertionError: If the input value is not a string.

        """
        assert isinstance(value, str), f"{value!r} is not unicode"
        token = Token(positions, **kwargs)
        pos = start_pos
        for match in self.expr.finditer(value):
            token.text = value[: match.end()]
            if positions:
                token.pos = pos
                pos += 1
            yield token
