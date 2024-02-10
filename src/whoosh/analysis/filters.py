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

from itertools import chain

from whoosh.analysis.acore import Composable
from whoosh.util.text import rcompile

# Default list of stop words (words so common it's usually wasteful to index
# them). This list is used by the StopFilter class, which allows you to supply
# an optional list to override this one.

STOP_WORDS = frozenset(
    (
        "a",
        "an",
        "and",
        "are",
        "as",
        "at",
        "be",
        "by",
        "can",
        "for",
        "from",
        "have",
        "if",
        "in",
        "is",
        "it",
        "may",
        "not",
        "of",
        "on",
        "or",
        "tbd",
        "that",
        "the",
        "this",
        "to",
        "us",
        "we",
        "when",
        "will",
        "with",
        "yet",
        "you",
        "your",
    )
)


# Simple pattern for filtering URLs, may be useful

url_pattern = rcompile(
    """
(
    [A-Za-z+]+://          # URL protocol
    \\S+?                  # URL body
    (?=\\s|[.]\\s|$|[.]$)  # Stop at space/end, or a dot followed by space/end
) | (                      # or...
    \\w+([:.]?\\w+)*         # word characters, with opt. internal colons/dots
)
""",
    verbose=True,
)


# Filters


class Filter(Composable):
    """Base class for Filter objects. A Filter subclass must implement a
    filter() method that takes a single argument, which is an iterator of Token
    objects, and yield a series of Token objects in return.

    Filters that do morphological transformation of tokens (e.g. stemming)
    should set their ``is_morph`` attribute to True.
    """

    def __eq__(self, other):
        """
        Compare this object with another object for equality.

        Args:
            other: The object to compare with.

        Returns:
            bool: True if the objects are equal, False otherwise.
        """
        return (
            other
            and self.__class__ is other.__class__
            and self.__dict__ == other.__dict__
        )

    def __ne__(self, other):
        """
        Check if the current object is not equal to another object.

        Parameters:
        - other: The object to compare with.

        Returns:
        - bool: True if the objects are not equal, False otherwise.
        """
        return self != other

    def __call__(self, tokens):
        """
        Applies the filter to the given list of tokens.

        Args:
            tokens (list): The list of tokens to be filtered.

        Returns:
            list: The filtered list of tokens.
        """
        raise NotImplementedError


class PassFilter(Filter):
    """An identity filter: passes the tokens through untouched."""

    def __call__(self, tokens):
        """
        Apply the pass filter to the given tokens.

        Parameters:
        tokens (list): The list of tokens to be filtered.

        Returns:
        list: The filtered list of tokens, which is the same as the input list.
        """
        return tokens


class LoggingFilter(Filter):
    """Prints the contents of every filter that passes through as a debug
    log entry.

    This filter is used to log the contents of each token that passes through it. It can be helpful for debugging purposes or for monitoring the tokenization process.

    Args:
        logger (Logger, optional): The logger to use for logging the token contents. If not provided, the "whoosh.analysis" logger is used.

    """

    def __init__(self, logger=None):
        """
        Initializes a new instance of the LoggingFilter class.

        Args:
            logger (Logger, optional): The logger to use. If omitted, the "whoosh.analysis" logger is used.
        """

        if logger is None:
            import logging

            logger = logging.getLogger("whoosh.analysis")
        self.logger = logger

    def __call__(self, tokens):
        """
        Applies the filter to the given tokens.

        Args:
            tokens (iterable): The tokens to filter.

        Yields:
            Token: The filtered tokens.

        """

        logger = self.logger
        for t in tokens:
            logger.debug(repr(t))
            yield t


class MultiFilter(Filter):
    """Chooses one of two or more sub-filters based on the 'mode' attribute
    of the token stream.

    This class is used to apply different filters to a token stream based on
    the value of the 'mode' attribute of each token. It allows you to associate
    different filters with different 'mode' attribute values and apply the
    appropriate filter to each token.

    Attributes:
        default_filter (Filter): The default filter to use when no matching
            'mode' attribute is found. Defaults to PassFilter().
        filters (dict): A dictionary that maps 'mode' attribute values to
            instantiated filters.

    Example:
        >>> iwf_for_index = IntraWordFilter(mergewords=True, mergenums=False)
        >>> iwf_for_query = IntraWordFilter(mergewords=False, mergenums=False)
        >>> mf = MultiFilter(index=iwf_for_index, query=iwf_for_query)
    """

    default_filter = PassFilter()

    def __init__(self, **kwargs):
        """Use keyword arguments to associate mode attribute values with
        instantiated filters.

        Args:
            **kwargs: Keyword arguments where the key is the 'mode' attribute
                value and the value is the instantiated filter.

        Note:
            This class expects that the value of the mode attribute is consistent
            among all tokens in a token stream.
        """
        self.filters = kwargs

    def __eq__(self, other):
        """Check if two MultiFilter instances are equal.

        Args:
            other (MultiFilter): The other MultiFilter instance to compare.

        Returns:
            bool: True if the two MultiFilter instances are equal, False otherwise.
        """
        return (
            other
            and self.__class__ is other.__class__
            and self.filters == other.filters
        )

    def __call__(self, tokens):
        """Apply the appropriate filter to each token in the token stream.

        Args:
            tokens (iterable): An iterable of tokens.

        Returns:
            iterable: An iterable of filtered tokens.

        Note:
            Only the first token is used to determine the appropriate filter to apply.
        """
        # Only selects on the first token
        t = next(tokens)
        selected_filter = self.filters.get(t.mode, self.default_filter)
        return selected_filter(chain([t], tokens))


class TeeFilter(Filter):
    r"""Interleaves the results of two or more filters (or filter chains).

    This filter takes the output of multiple filters or filter chains and interleaves them together.
    It is useful when you want to apply different transformations to the same input and combine the results.

    NOTE: This filter can be slow because it needs to create copies of each token for each sub-filter.

    Usage:
    >>> target = "ALFA BRAVO CHARLIE"
    >>> # In one branch, we'll lower-case the tokens
    >>> f1 = LowercaseFilter()
    >>> # In the other branch, we'll reverse the tokens
    >>> f2 = ReverseTextFilter()
    >>> ana = RegexTokenizer(r"\S+") | TeeFilter(f1, f2)
    >>> [token.text for token in ana(target)]
    ["alfa", "AFLA", "bravo", "OVARB", "charlie", "EILRAHC"]

    To combine the incoming token stream with the output of a filter chain, use
    ``TeeFilter`` and make one of the filters a :class:`PassFilter`.

    >>> f1 = PassFilter()
    >>> f2 = BiWordFilter()
    >>> ana = RegexTokenizer(r"\S+") | TeeFilter(f1, f2) | LowercaseFilter()
    >>> [token.text for token in ana(target)]
    ["alfa", "alfa-bravo", "bravo", "bravo-charlie", "charlie"]
    """

    def __init__(self, *filters):
        """
        Initialize the TeeFilter with the provided filters.

        Args:
            *filters: Variable number of filters or filter chains to be interleaved.

        Raises:
            ValueError: If less than two filters are provided.
        """
        if len(filters) < 2:
            raise ValueError("TeeFilter requires two or more filters")
        self.filters = filters

    def __eq__(self, other):
        """
        Check if two TeeFilter instances are equal.

        Args:
            other: Another TeeFilter instance.

        Returns:
            bool: True if the two instances are equal, False otherwise.
        """
        return self.__class__ is other.__class__ and self.filters == other.fitlers

    def __call__(self, tokens):
        """
        Apply the TeeFilter to the input tokens.

        Args:
            tokens: The input tokens to be filtered.

        Yields:
            Token: The interleaved tokens from the filters.
        """
        from itertools import tee

        count = len(self.filters)
        # Tee the token iterator and wrap each teed iterator with the
        # corresponding filter
        gens = [
            filter(t.copy() for t in gen)
            for filter, gen in zip(self.filters, tee(tokens, count))
        ]
        # Keep a count of the number of running iterators
        running = count
        while running:
            for i, gen in enumerate(gens):
                if gen is not None:
                    try:
                        yield next(gen)
                    except StopIteration:
                        gens[i] = None
                        running -= 1


class ReverseTextFilter(Filter):
    """Reverses the text of each token.

    This filter takes a stream of tokens and reverses the text of each token.
    It can be used as part of an analysis pipeline to modify the text of tokens.

    Example:
        >>> ana = RegexTokenizer() | ReverseTextFilter()
        >>> [token.text for token in ana("hello there")]
        ["olleh", "ereht"]

    """

    def __call__(self, tokens):
        """Apply the reverse text transformation to each token.

        Args:
            tokens (iterable): A stream of tokens.

        Yields:
            Token: A token with the reversed text.

        """
        for t in tokens:
            t.text = t.text[::-1]
            yield t


class LowercaseFilter(Filter):
    """A filter that uses unicode.lower() to lowercase token text.

    This filter converts the text of each token to lowercase using the unicode.lower() method.
    It is commonly used in text analysis pipelines to normalize the case of tokens.

    Example:
        >>> rext = RegexTokenizer()
        >>> stream = rext("This is a TEST")
        >>> [token.text for token in LowercaseFilter(stream)]
        ["this", "is", "a", "test"]

    Usage:
        1. Create an instance of the LowercaseFilter class.
        2. Pass a stream of tokens to the instance using the __call__ method.
        3. Iterate over the filtered tokens to access the lowercase text.

    Note:
        The LowercaseFilter modifies the text of each token in-place. It does not create new tokens.

    """

    def __call__(self, tokens):
        """Applies the lowercase transformation to each token in the stream.

        Args:
            tokens (iterable): A stream of tokens.

        Yields:
            Token: A token with its text converted to lowercase.

        """
        for t in tokens:
            t.text = t.text.lower()
            yield t


class StripFilter(Filter):
    """Calls unicode.strip() on the token text.

    This filter is used to remove leading and trailing whitespace from the token text.
    It is typically used in text analysis pipelines to clean up the tokenized text.

    Example usage:
    -------------
    from whoosh.analysis import Token, Tokenizer, TokenFilter

    class MyTokenizer(Tokenizer):
        def __call__(self, value, positions=False, chars=False, keeporiginal=False, removestops=True,
                     start_pos=0, start_char=0, mode='', **kwargs):
            # Tokenize the value
            tokens = self.tokenizer(value, positions=positions, chars=chars,
                                    keeporiginal=keeporiginal, removestops=removestops,
                                    start_pos=start_pos, start_char=start_char, mode=mode, **kwargs)

            # Apply the StripFilter to remove leading and trailing whitespace
            tokens = StripFilter()(tokens)

            return tokens

    # Create an instance of MyTokenizer
    tokenizer = MyTokenizer()

    # Tokenize a text
    text = "   Hello, World!   "
    tokens = tokenizer(text)

    # Print the tokens
    for token in tokens:
        print(token.text)

    Output:
    -------
    Hello,
    World!

    """

    def __call__(self, tokens):
        """Applies the strip() method to the token text.

        Args:
            tokens (iterable of whoosh.analysis.Token): The input tokens.

        Yields:
            whoosh.analysis.Token: The modified tokens with leading and trailing whitespace removed.

        """
        for t in tokens:
            t.text = t.text.strip()
            yield t


class StopFilter(Filter):
    """Marks "stop" words (words too common to index) in the stream (and by
    default removes them).

    Make sure you precede this filter with a :class:`LowercaseFilter`.

    Args:
        stoplist (collection, optional): A collection of words to remove from the stream.
            This is converted to a frozenset. The default is a list of
            common English stop words.
        minsize (int, optional): The minimum length of token texts. Tokens with
            text smaller than this will be stopped. The default is 2.
        maxsize (int, optional): The maximum length of token texts. Tokens with text
            larger than this will be stopped. Use None to allow any length.
        renumber (bool, optional): Change the 'pos' attribute of unstopped tokens
            to reflect their position with the stopped words removed.
        lang (str, optional): Automatically get a list of stop words for the given
            language.

    Attributes:
        stops (frozenset): The set of stop words.
        min (int): The minimum length of token texts.
        max (int): The maximum length of token texts.
        renumber (bool): Indicates whether the 'pos' attribute of unstopped tokens
            should be changed to reflect their position with the stopped words removed.

    Examples:
        >>> stopper = RegexTokenizer() | StopFilter()
        >>> [token.text for token in stopper(u"this is a test")]
        ["test"]
        >>> es_stopper = RegexTokenizer() | StopFilter(lang="es")
        >>> [token.text for token in es_stopper(u"el lapiz es en la mesa")]
        ["lapiz", "mesa"]

    Note:
        The list of available languages is in `whoosh.lang.languages`.
        You can use :func:`whoosh.lang.has_stopwords` to check if a given language
        has a stop word list available.
    """

    def __init__(
        self, stoplist=STOP_WORDS, minsize=2, maxsize=None, renumber=True, lang=None
    ):
        """
        Initialize the StopFilter.

        Args:
            stoplist (collection, optional): A collection of words to remove from the stream.
                This is converted to a frozenset. The default is a list of
                common English stop words.
            minsize (int, optional): The minimum length of token texts. Tokens with
                text smaller than this will be stopped. The default is 2.
            maxsize (int, optional): The maximum length of token texts. Tokens with text
                larger than this will be stopped. Use None to allow any length.
            renumber (bool, optional): Change the 'pos' attribute of unstopped tokens
                to reflect their position with the stopped words removed.
            lang (str, optional): Automatically get a list of stop words for the given
                language
        """

        stops = set()
        if stoplist:
            stops.update(stoplist)
        if lang:
            from whoosh.lang import stopwords_for_language

            stops.update(stopwords_for_language(lang))

        self.stops = frozenset(stops)
        self.min = minsize
        self.max = maxsize
        self.renumber = renumber

    def __eq__(self, other):
        """
        Compare the StopFilter with another object for equality.

        Args:
            other (object): The object to compare with.

        Returns:
            bool: True if the objects are equal, False otherwise.
        """
        return (
            other
            and self.__class__ is other.__class__
            and self.stops == other.stops
            and self.min == other.min
            and self.renumber == other.renumber
        )

    def __call__(self, tokens):
        """
        Apply the StopFilter to the tokens.

        Args:
            tokens (iterable): The input tokens.

        Yields:
            Token: The filtered tokens.
        """
        stoplist = self.stops
        minsize = self.min
        maxsize = self.max
        renumber = self.renumber

        pos = None
        for t in tokens:
            text = t.text
            if (
                len(text) >= minsize
                and (maxsize is None or len(text) <= maxsize)
                and text not in stoplist
            ):
                # This is not a stop word
                if renumber and t.positions:
                    if pos is None:
                        pos = t.pos
                    else:
                        pos += 1
                        t.pos = pos
                t.stopped = False
                yield t
            else:
                # This is a stop word
                if not t.removestops:
                    # This IS a stop word, but we're not removing them
                    t.stopped = True
                    yield t


class CharsetFilter(Filter):
    """
    Translates the text of tokens by calling unicode.translate() using the
    supplied character mapping object. This is useful for case and accent
    folding.

    The `whoosh.support.charset` module has a useful map for accent folding.

    Example usage:

    ```python
    from whoosh.support.charset import accent_map
    from whoosh.analysis import RegexTokenizer

    retokenizer = RegexTokenizer()
    chfilter = CharsetFilter(accent_map)
    tokens = chfilter(retokenizer(u'café'))
    [t.text for t in tokens]
    # Output: [u'cafe']
    ```

    Another way to get a character mapping object is to convert a Sphinx
    charset table file using `whoosh.support.charset.charset_table_to_dict`.

    Example usage:

    ```python
    from whoosh.support.charset import charset_table_to_dict, default_charset
    from whoosh.analysis import RegexTokenizer

    retokenizer = RegexTokenizer()
    charmap = charset_table_to_dict(default_charset)
    chfilter = CharsetFilter(charmap)
    tokens = chfilter(retokenizer(u'Stra\\xdfe'))
    [t.text for t in tokens]
    # Output: [u'strase']
    ```

    The Sphinx charset table format is described at
    https://www.sphinxsearch.com/docs/current.html#conf-charset-table.
    """

    __inittypes__ = {"charmap": dict}

    def __init__(self, charmap):
        """
        Initializes a CharsetFilter object.

        :param charmap: A dictionary mapping from integer character numbers to
            unicode characters, as required by the unicode.translate() method.
        """
        self.charmap = charmap

    def __eq__(self, other):
        """
        Checks if two CharsetFilter objects are equal.

        :param other: The other CharsetFilter object to compare.
        :return: True if the two objects are equal, False otherwise.
        """
        return (
            other
            and self.__class__ is other.__class__
            and self.charmap == other.charmap
        )

    def __call__(self, tokens):
        """
        Applies the CharsetFilter to a sequence of tokens.

        :param tokens: An iterable sequence of tokens.
        :return: A generator that yields the transformed tokens.
        """
        assert hasattr(tokens, "__iter__")
        charmap = self.charmap
        for t in tokens:
            t.text = t.text.translate(charmap)
            yield t


class DelimitedAttributeFilter(Filter):
    """Looks for delimiter characters in the text of each token and stores the
    data after the delimiter in a named attribute on the token.

    The defaults are set up to use the ``^`` character as a delimiter and store
    the value after the ``^`` as the boost for the token.

    Args:
        delimiter (str): A string that, when present in a token's text, separates
            the actual text from the "data" payload.
        attribute (str): The name of the attribute in which to store the data on
            the token.
        default (Any): The value to use for the attribute for tokens that don't have
            delimited data.
        type (type): The type of the data, for example ``str`` or ``float``. This is
            used to convert the string value of the data before storing it in the
            attribute.

    Example:
        >>> daf = DelimitedAttributeFilter(delimiter="^", attribute="boost")
        >>> ana = RegexTokenizer("\\\\S+") | DelimitedAttributeFilter()
        >>> for t in ana(u("image render^2 file^0.5")):
        ...    print("%r %f" % (t.text, t.boost))
        'image' 1.0
        'render' 2.0
        'file' 0.5

    Note:
        You need to make sure your tokenizer includes the delimiter and data as part
        of the token!
    """

    def __init__(self, delimiter="^", attribute="boost", default=1.0, type=float):
        """
        Initialize the DelimitedAttributeFilter.

        Args:
            delimiter (str): A string that, when present in a token's text, separates
                the actual text from the "data" payload.
            attribute (str): The name of the attribute in which to store the data on
                the token.
            default (Any): The value to use for the attribute for tokens that don't have
                delimited data.
            type (type): The type of the data, for example ``str`` or ``float``. This is
                used to convert the string value of the data before storing it in the
                attribute.
        """
        self.delim = delimiter
        self.attr = attribute
        self.default = default
        self.type = type

    def __eq__(self, other):
        """
        Compare the DelimitedAttributeFilter with another object for equality.

        Args:
            other (Any): The object to compare with.

        Returns:
            bool: True if the objects are equal, False otherwise.
        """
        return (
            other
            and self.__class__ is other.__class__
            and self.delim == other.delim
            and self.attr == other.attr
            and self.default == other.default
        )

    def __call__(self, tokens):
        """
        Apply the DelimitedAttributeFilter to a sequence of tokens.

        Args:
            tokens (Iterable[Token]): The sequence of tokens to filter.

        Yields:
            Token: The filtered tokens.
        """
        delim = self.delim
        attr = self.attr
        default = self.default
        type_ = self.type

        for t in tokens:
            text = t.text
            pos = text.find(delim)
            if pos > -1:
                setattr(t, attr, type_(text[pos + 1 :]))
                if t.chars:
                    t.endchar -= len(t.text) - pos
                t.text = text[:pos]
            else:
                setattr(t, attr, default)

            yield t


class SubstitutionFilter(Filter):
    """Performs a regular expression substitution on the token text.

    This filter applies a regular expression substitution to the text of each token.
    It is particularly useful for removing or replacing specific patterns of text within tokens.
    The filter utilizes the `re.sub()` method to perform the substitution.

    Example usage:
    --------------
    # Create an analyzer that removes hyphens from tokens
    tokenizer = RegexTokenizer(r"\\S+")
    substitution_filter = SubstitutionFilter("-", "")
    analyzer = tokenizer | substitution_filter

    Parameters:
    -----------
    pattern : str or Pattern
        A pattern string or compiled regular expression object describing the text to replace.
    replacement : str
        The substitution text.

    Methods:
    --------
    __call__(tokens)
        Applies the substitution filter to the given tokens.

    """

    def __init__(self, pattern, replacement):
        """
        Initializes a SubstitutionFilter object.

        Parameters:
        -----------
        pattern : str or Pattern
            A pattern string or compiled regular expression object describing the text to replace.
        replacement : str
            The substitution text.
        """
        self.pattern = rcompile(pattern)
        self.replacement = replacement

    def __eq__(self, other):
        """
        Checks if two SubstitutionFilter objects are equal.

        Parameters:
        -----------
        other : SubstitutionFilter
            The other SubstitutionFilter object to compare.

        Returns:
        --------
        bool
            True if the two SubstitutionFilter objects are equal, False otherwise.
        """
        return (
            other
            and self.__class__ is other.__class__
            and self.pattern == other.pattern
            and self.replacement == other.replacement
        )

    def __call__(self, tokens):
        """
        Applies the substitution filter to the given tokens.

        Parameters:
        -----------
        tokens : iterable
            An iterable of Token objects.

        Yields:
        -------
        Token
            The modified Token objects after applying the substitution filter.
        """
        pattern = self.pattern
        replacement = self.replacement

        for t in tokens:
            t.text = pattern.sub(replacement, t.text)
            yield t
