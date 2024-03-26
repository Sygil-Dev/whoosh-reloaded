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

from whoosh.analysis.acore import Composable, CompositionError
from whoosh.analysis.filters import STOP_WORDS, LowercaseFilter, StopFilter
from whoosh.analysis.intraword import IntraWordFilter
from whoosh.analysis.morph import StemFilter
from whoosh.analysis.tokenizers import (
    CommaSeparatedTokenizer,
    IDTokenizer,
    RegexTokenizer,
    SpaceSeparatedTokenizer,
    Tokenizer,
    default_pattern,
)
from whoosh.lang.porter import stem


# Analyzers
class Analyzer(Composable):
    """Abstract base class for analyzers.

    An analyzer is responsible for processing text data and producing a stream of tokens.
    Subclasses of Analyzer should implement the __call__ method to define the tokenization process.

    Attributes:
        None

    Methods:
        __repr__: Returns a string representation of the analyzer.
        __eq__: Checks if two analyzers are equal.
        __call__: Processes the input value and returns a stream of tokens.
        clean: Cleans up any resources used by the analyzer.

    """

    def __repr__(self):
        """Returns a string representation of the analyzer."""
        return f"{self.__class__.__name__}()"

    def __eq__(self, other):
        """Checks if two analyzers are equal."""
        return (
            other
            and self.__class__ is other.__class__
            and self.__dict__ == other.__dict__
        )

    def __call__(self, value, **kwargs):
        """Processes the input value and returns a stream of tokens.

        Args:
            value (str): The input value to be analyzed.
            **kwargs: Additional keyword arguments that may be required by specific analyzers.

        Returns:
            generator: A generator that yields the tokens produced by the analyzer.

        Raises:
            NotImplementedError: If the __call__ method is not implemented by a subclass.

        """
        raise NotImplementedError

    def clean(self):
        """Cleans up any resources used by the analyzer.

        This method is intentionally left empty.

        Args:
            None

        Returns:
            None

        """
        pass


class CompositeAnalyzer(Analyzer):
    """
    A composite analyzer that combines multiple analyzers and tokenizers into a single analyzer.

    Args:
        *composables: Variable number of analyzers and tokenizers to be combined.

    Raises:
        CompositionError: If more than one tokenizer is provided at the start of the analyzer.

    Example:
        analyzer = CompositeAnalyzer(standard_analyzer(), LowercaseFilter())
        tokens = analyzer("Hello World")
        for token in tokens:
            print(token)

    """

    def __init__(self, *composables):
        """
        Initializes the CompositeAnalyzer.

        Args:
            *composables: Variable number of analyzers and tokenizers to be combined.

        Raises:
            CompositionError: If more than one tokenizer is provided at the start of the analyzer.

        """
        self.items = []

        for comp in composables:
            if isinstance(comp, CompositeAnalyzer):
                self.items.extend(comp.items)
            else:
                self.items.append(comp)

        for item in self.items[1:]:
            if isinstance(item, Tokenizer):
                raise CompositionError(
                    f"Only one tokenizer allowed at the start of the analyzer: {self.items}"
                )

    def __repr__(self):
        """
        Returns a string representation of the CompositeAnalyzer.

        Returns:
            str: String representation of the CompositeAnalyzer.

        """
        return "{}({})".format(
            self.__class__.__name__,
            ", ".join(repr(item) for item in self.items),
        )

    def __call__(self, value, no_morph=False, **kwargs):
        """
        Applies the composite analyzer to the given value and returns a generator of tokens.

        Args:
            value (str): The input value to be analyzed.
            no_morph (bool, optional): Flag to skip morphological analysis. Defaults to False.
            **kwargs: Additional keyword arguments to be passed to the analyzers and tokenizers.

        Returns:
            generator: A generator of tokens.

        """
        items = self.items
        gen = items[0](value, **kwargs)
        for item in items[1:]:
            if not (no_morph and hasattr(item, "is_morph") and item.is_morph):
                gen = item(gen)
        return gen

    def __getitem__(self, item):
        """
        Returns the item at the specified index.

        Args:
            item (int): The index of the item to retrieve.

        Returns:
            object: The item at the specified index.

        """
        return self.items.__getitem__(item)

    def __len__(self):
        """
        Returns the number of items in the CompositeAnalyzer.

        Returns:
            int: The number of items in the CompositeAnalyzer.

        """
        return len(self.items)

    def __eq__(self, other):
        """
        Checks if the CompositeAnalyzer is equal to another object.

        Args:
            other (object): The object to compare with.

        Returns:
            bool: True if the CompositeAnalyzer is equal to the other object, False otherwise.

        """
        return other and self.__class__ is other.__class__ and self.items == other.items

    def clean(self):
        """
        Cleans up any resources used by the CompositeAnalyzer.

        """
        for item in self.items:
            if hasattr(item, "clean"):
                item.clean()

    def has_morph(self):
        """
        Checks if the CompositeAnalyzer has any morphological analysis.

        Returns:
            bool: True if the CompositeAnalyzer has morphological analysis, False otherwise.

        """
        return any(item.is_morph for item in self.items)


# Functions that return composed analyzers


def id_analyzer(lowercase=False):
    """
    Returns an analyzer that tokenizes input text into individual tokens using the IDTokenizer.
    If lowercase is set to True, it also applies the LowercaseFilter to convert tokens to lowercase.

    Parameters:
    - lowercase (bool): Whether to convert tokens to lowercase. Default is False.

    Returns:
    - tokenizer (Analyzer): The configured analyzer.

    Deprecated: This function is deprecated. It is recommended to use IDTokenizer directly, with a LowercaseFilter if desired.
    """
    tokenizer = IDTokenizer()
    if lowercase:
        tokenizer = tokenizer | LowercaseFilter()
    return tokenizer


def keyword_analyzer(lowercase=False, commas=False):
    """
    Parses whitespace- or comma-separated tokens.

    This analyzer is used to parse whitespace- or comma-separated tokens from a given text.
    It can be configured to lowercase the tokens and treat items separated by commas instead of whitespace.

    Example usage:
    >>> ana = keyword_analyzer()
    >>> [token.text for token in ana("Hello there, this is a TEST")]
    ["Hello", "there,", "this", "is", "a", "TEST"]

    :param lowercase: A boolean indicating whether to lowercase the tokens. Default is False.
    :param commas: A boolean indicating whether items are separated by commas instead of whitespace. Default is False.
    :return: A tokenizer object that can be used to tokenize the input text.
    """
    if commas:
        tokenizer = CommaSeparatedTokenizer()
    else:
        tokenizer = SpaceSeparatedTokenizer()
    if lowercase:
        tokenizer = tokenizer | LowercaseFilter()
    return tokenizer


def regex_analyzer(expression=r"\w+(\.?\w+)*", gaps=False):
    r"""
    Deprecated, just use a RegexTokenizer directly.

    Args:
        expression (str, optional): The regular expression pattern to match. Defaults to r"\w+(\.?\w+)*".
        gaps (bool, optional): Whether to split on gaps (non-matching substrings) or matches. Defaults to False.

    Returns:
        RegexTokenizer: A tokenizer that tokenizes text using a regular expression pattern.
    """

    return RegexTokenizer(expression=expression, gaps=gaps)


def simple_analyzer(expression=default_pattern, gaps=False):
    """
    Composes a RegexTokenizer with a LowercaseFilter.

    This function creates an analyzer that tokenizes text using a regular expression pattern and converts the tokens to lowercase.

    Example usage:
    >>> ana = simple_analyzer()
    >>> [token.text for token in ana("Hello there, this is a TEST")]
    ["hello", "there", "this", "is", "a", "test"]

    :param expression: The regular expression pattern to use for token extraction. Defaults to `default_pattern`.
    :param gaps: If True, the tokenizer *splits* on the expression, rather than matching on the expression. Defaults to False.
    :return: An analyzer object that tokenizes text using the specified regular expression pattern and converts the tokens to lowercase.
    """
    return RegexTokenizer(expression=expression, gaps=gaps) | LowercaseFilter()


def standard_analyzer(
    expression=default_pattern, stoplist=STOP_WORDS, minsize=2, maxsize=None, gaps=False
):
    """Composes a RegexTokenizer with a LowercaseFilter and optional
    StopFilter.

    This analyzer is used to tokenize and filter text into a stream of tokens.
    It applies a regular expression pattern to extract tokens, converts them to lowercase,
    and optionally removes stop words.

    Example usage:
    >>> ana = standard_analyzer()
    >>> [token.text for token in ana("Testing is testing and testing")]
    ["testing", "testing", "testing"]

    :param expression: The regular expression pattern to use to extract tokens.
    :param stoplist: A list of stop words. Set this to None to disable
        the stop word filter.
    :param minsize: Words smaller than this are removed from the stream.
    :param maxsize: Words longer than this are removed from the stream.
    :param gaps: If True, the tokenizer *splits* on the expression, rather
        than matching on the expression.
    :return: A chain of tokenizers and filters that can be used to analyze text.
    """
    ret = RegexTokenizer(expression=expression, gaps=gaps)
    chain = ret | LowercaseFilter()
    if stoplist is not None:
        chain = chain | StopFilter(stoplist=stoplist, minsize=minsize, maxsize=maxsize)
    return chain


def stemming_analyzer(
    expression=default_pattern,
    stoplist=STOP_WORDS,
    minsize=2,
    maxsize=None,
    gaps=False,
    stemfn=stem,
    ignore=None,
    cachesize=50000,
):
    r"""
    Composes a RegexTokenizer with a lower case filter, an optional stop
    filter, and a stemming filter.

    Args:
        expression (str, optional): The regular expression pattern to use to extract tokens.
        stoplist (list, optional): A list of stop words. Set this to None to disable the stop word filter.
        minsize (int, optional): Words smaller than this are removed from the stream.
        maxsize (int, optional): Words longer that this are removed from the stream.
        gaps (bool, optional): If True, the tokenizer *splits* on the expression, rather than matching on the expression.
        stemfn (function, optional): The stemming function to use. Defaults to the `stem` function.
        ignore (set, optional): A set of words to not stem.
        cachesize (int, optional): The maximum number of stemmed words to cache. The larger this number, the faster stemming will be but the more memory it will use. Use None for no cache, or -1 for an unbounded cache.

    Returns:
        Analyzer: The composed analyzer.

    Examples:
        >>> ana = stemming_analyzer()
        >>> [token.text for token in ana("Testing is testing and testing")]
        ["test", "test", "test"]

    This function composes an analyzer that tokenizes text using a regular expression pattern,
    converts tokens to lowercase, applies an optional stop word filter, and performs stemming
    on the tokens.

    The `expression` parameter specifies the regular expression pattern to use for token extraction.
    The `stoplist` parameter is a list of stop words to be filtered out. If set to None, the stop word
    filter is disabled. The `minsize` and `maxsize` parameters control the minimum and maximum word
    lengths to keep in the token stream. The `gaps` parameter determines whether the tokenizer splits
    on the expression or matches on it.

    The `stemfn` parameter specifies the stemming function to use. By default, it uses the `stem` function.
    The `ignore` parameter is a set of words that should not be stemmed. The `cachesize` parameter sets
    the maximum number of stemmed words to cache, improving performance at the cost of memory usage.

    The function returns the composed analyzer, which can be used to process text and extract tokens.

    Example usage:
    >>> analyzer = stemming_analyzer(expression=r'\w+', stoplist=['is', 'and'], minsize=3)
    >>> [token.text for token in analyzer("Testing is testing and testing")]
    ["test", "test", "test"]
    """
    ret = RegexTokenizer(expression=expression, gaps=gaps)
    chain = ret | LowercaseFilter()
    if stoplist is not None:
        chain = chain | StopFilter(stoplist=stoplist, minsize=minsize, maxsize=maxsize)
    return chain | StemFilter(stemfn=stemfn, ignore=ignore, cachesize=cachesize)


def fancy_analyzer(
    expression=r"\s+",
    stoplist=STOP_WORDS,
    minsize=2,
    gaps=True,
    splitwords=True,
    splitnums=True,
    mergewords=False,
    mergenums=False,
):
    """
    Composes a fancy_analyzer with a RegexTokenizer, IntraWordFilter, LowercaseFilter, and StopFilter.

    This analyzer tokenizes text using a regular expression pattern, applies intra-word filtering,
    converts tokens to lowercase, and removes stop words.

    Example usage:
    >>> ana = fancy_analyzer()
    >>> [token.text for token in ana("Should I call getInt or get_real?")]
    ["should", "call", "getInt", "get", "int", "get_real", "get", "real"]

    :param expression: The regular expression pattern to use for token extraction.
    :type expression: str, optional
    :param stoplist: A list of stop words. Set this to None to disable the stop word filter.
    :type stoplist: list or None, optional
    :param minsize: Words smaller than this are removed from the token stream.
    :type minsize: int, optional
    :param gaps: If True, the tokenizer splits on the expression, rather than matching on the expression.
    :type gaps: bool, optional
    :param splitwords: If True, intra-word filtering splits words.
    :type splitwords: bool, optional
    :param splitnums: If True, intra-word filtering splits numbers.
    :type splitnums: bool, optional
    :param mergewords: If True, intra-word filtering merges words.
    :type mergewords: bool, optional
    :param mergenums: If True, intra-word filtering merges numbers.
    :type mergenums: bool, optional
    :return: A composed analyzer.
    :rtype: Analyzer
    """
    return (
        RegexTokenizer(expression=expression, gaps=gaps)
        | IntraWordFilter(
            splitwords=splitwords,
            splitnums=splitnums,
            mergewords=mergewords,
            mergenums=mergenums,
        )
        | LowercaseFilter()
        | StopFilter(stoplist=stoplist, minsize=minsize)
    )


def language_analyzer(lang, expression=default_pattern, gaps=False, cachesize=50000):
    """
    Configures a simple analyzer for the given language, with a LowercaseFilter, StopFilter, and StemFilter.

    :param lang: The language code for the analyzer. The list of available languages is in `whoosh.lang.languages`.
    :param expression: The regular expression pattern to use to extract tokens.
    :param gaps: If True, the tokenizer *splits* on the expression, rather than matching on the expression.
    :param cachesize: The maximum number of stemmed words to cache. The larger this number, the faster stemming will be but the more memory it will use.
    :return: The configured analyzer chain.

    Example usage:
    >>> ana = language_analyzer("es")
    >>> [token.text for token in ana("Por el mar corren las liebres")]
    ['mar', 'corr', 'liebr']

    The list of available languages is in `whoosh.lang.languages`.
    You can use `whoosh.lang.has_stemmer` and `whoosh.lang.has_stopwords` to check if a given language has a stemming function and/or stop word list available.
    """
    from whoosh.lang import NoStemmer, NoStopWords

    # Make the start of the chain
    chain = RegexTokenizer(expression=expression, gaps=gaps) | LowercaseFilter()

    # Add a stop word filter
    try:
        chain = chain | StopFilter(lang=lang)
    except NoStopWords:
        pass

    # Add a stemming filter
    try:
        chain = chain | StemFilter(lang=lang, cachesize=cachesize)
    except NoStemmer:
        pass

    return chain
