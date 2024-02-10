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

from whoosh.analysis.acore import Token
from whoosh.analysis.filters import Filter, LowercaseFilter
from whoosh.analysis.tokenizers import RegexTokenizer, Tokenizer

# Tokenizer


class NgramTokenizer(Tokenizer):
    """Splits input text into N-grams instead of words.

    This tokenizer splits the input text into N-grams, where an N-gram is a
    contiguous sequence of N characters. The N-grams emitted by this tokenizer
    may contain whitespace, punctuation, and other characters. If you only want
    sub-word N-grams without whitespace, you can combine a RegexTokenizer with
    NgramFilter instead.

    Example:
        ngt = NgramTokenizer(4)
        tokens = [token.text for token in ngt("hi there")]
        # tokens = ["hi t", "i th", " the", "ther", "here"]

    Note:
        This tokenizer does not use a regular expression to extract words, so
        the N-grams emitted by it will contain whitespace, punctuation, etc.
        You may want to massage the input or add a custom filter to this
        tokenizer's output.

    Args:
        minsize (int): The minimum size of the N-grams.
        maxsize (int, optional): The maximum size of the N-grams. If not
            provided, maxsize will be set to minsize.

    Attributes:
        min (int): The minimum size of the N-grams.
        max (int): The maximum size of the N-grams.

    """

    __inittypes__ = {"minsize": int, "maxsize": int}

    def __init__(self, minsize, maxsize=None):
        """
        Initialize the NgramTokenizer.

        Args:
            minsize (int): The minimum size of the N-grams.
            maxsize (int, optional): The maximum size of the N-grams. If not
                provided, maxsize will be set to minsize.

        """
        self.min = minsize
        self.max = maxsize or minsize

    def __eq__(self, other):
        """
        Check if two ngram objects are equal.

        Args:
            other (Ngram): The other ngram object to compare with.

        Returns:
            bool: True if the ngram objects are equal, False otherwise.
        """
        if self.__class__ is other.__class__:
            if self.min == other.min and self.max == other.max:
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
        mode="",
        **kwargs,
    ):
        """
        Tokenizes the given value into n-grams.

        Args:
            value (str): The input string to be tokenized.
            positions (bool, optional): Whether to include position information in the tokens. Defaults to False.
            chars (bool, optional): Whether to include character offset information in the tokens. Defaults to False.
            keeporiginal (bool, optional): Whether to keep the original token text. Defaults to False.
            removestops (bool, optional): Whether to remove stop words from the tokens. Defaults to True.
            start_pos (int, optional): The starting position for position information. Defaults to 0.
            start_char (int, optional): The starting character offset. Defaults to 0.
            mode (str, optional): The tokenization mode. Defaults to "".

        Yields:
            Token: The generated tokens.

        Raises:
            AssertionError: If the input value is not a string.

        Note:
            This method tokenizes the input string into n-grams based on the specified parameters. It generates tokens
            by sliding a window of size `self.min` to `self.max` over the input string. The generated tokens can include
            position information, character offset information, and original token text depending on the specified
            parameters.

            If `mode` is set to "query", the method generates tokens by sliding a window of size `self.max` over the
            input string. This is typically used for query tokenization.

            If `mode` is not set to "query", the method generates tokens by sliding a window of size `self.min` to
            `self.max` over the input string. This is typically used for indexing tokenization.
        """
        assert isinstance(value, str), f"{value!r} is not unicode"

        inlen = len(value)
        t = Token(positions, chars, removestops=removestops, mode=mode)
        pos = start_pos

        if mode == "query":
            size = min(self.max, inlen)
            for start in range(0, inlen - size + 1):
                end = start + size
                if end > inlen:
                    continue
                t.text = value[start:end]
                if keeporiginal:
                    t.original = t.text
                t.stopped = False
                if positions:
                    t.pos = pos
                if chars:
                    t.startchar = start_char + start
                    t.endchar = start_char + end
                yield t
                pos += 1
        else:
            for start in range(0, inlen - self.min + 1):
                for size in range(self.min, self.max + 1):
                    end = start + size
                    if end > inlen:
                        continue
                    t.text = value[start:end]
                    if keeporiginal:
                        t.original = t.text
                    t.stopped = False
                    if positions:
                        t.pos = pos
                    if chars:
                        t.startchar = start_char + start
                        t.endchar = start_char + end

                    yield t
                pos += 1


# Filter
class NgramFilter(Filter):
    """Splits token text into N-grams.

    >>> rext = RegexTokenizer()
    >>> stream = rext("hello there")
    >>> ngf = NgramFilter(4)
    >>> [token.text for token in ngf(stream)]
    ["hell", "ello", "ther", "here"]
    """

    __inittypes__ = {"minsize": int, "maxsize": int}

    def __init__(self, minsize, maxsize=None, at=None):
        """
        :param minsize: The minimum size of the N-grams.
        :param maxsize: The maximum size of the N-grams. If you omit this
            parameter, maxsize == minsize.
        :param at: If 'start', only take N-grams from the start of each word.
            if 'end', only take N-grams from the end of each word. Otherwise,
            take all N-grams from the word (the default).
        """

        self.min = minsize
        self.max = maxsize or minsize
        self.at = 0
        if at == "start":
            self.at = -1
        elif at == "end":
            self.at = 1

    def __eq__(self, other):
        """
        Check if two ngrams objects are equal.

        Args:
            other (object): The object to compare with.

        Returns:
            bool: True if the two ngrams objects are equal, False otherwise.
        """
        return (
            other
            and self.__class__ is other.__class__
            and self.min == other.min
            and self.max == other.max
        )

    def __call__(self, tokens):
        """
        Tokenizes the given tokens into N-grams.

        Args:
            tokens (iterable): The input tokens to be tokenized.

        Yields:
            Token: The generated N-gram tokens.

        Raises:
            AssertionError: If the input tokens are not iterable.

        Note:
            This method tokenizes the input tokens into N-grams based on the specified parameters. It generates N-gram tokens by sliding a window of size `self.min` to `self.max` over the input tokens.

            If the token's text length is less than `self.min`, the token is skipped.

            If the token's mode is set to "query", the method generates N-gram tokens by sliding a window of size `self.max` over the token's text. This is typically used for query tokenization.

            If the token's mode is not set to "query", the method generates N-gram tokens by sliding a window of size `self.min` to `self.max` over the token's text. This is typically used for indexing tokenization.

            The generated N-gram tokens can include position information, character offset information, and original token text depending on the specified parameters.
        """
        assert hasattr(tokens, "__iter__")
        at = self.at
        for t in tokens:
            text = t.text
            if len(text) < self.min:
                continue

            chars = t.chars
            if chars:
                startchar = t.startchar
            # Token positions don't mean much for N-grams,
            # so we'll leave the token's original position
            # untouched.

            if t.mode == "query":
                size = min(self.max, len(t.text))
                if at == -1:
                    t.text = text[:size]
                    if chars:
                        t.endchar = startchar + size
                    yield t
                elif at == 1:
                    t.text = text[0 - size :]
                    if chars:
                        t.startchar = t.endchar - size
                    yield t
                else:
                    for start in range(0, len(text) - size + 1):
                        t.text = text[start : start + size]
                        if chars:
                            t.startchar = startchar + start
                            t.endchar = startchar + start + size
                        yield t
            else:
                if at == -1:
                    limit = min(self.max, len(text))
                    for size in range(self.min, limit + 1):
                        t.text = text[:size]
                        if chars:
                            t.endchar = startchar + size
                        yield t

                elif at == 1:
                    if chars:
                        original_startchar = t.startchar
                    start = max(0, len(text) - self.max)
                    for i in range(start, len(text) - self.min + 1):
                        t.text = text[i:]
                        if chars:
                            t.startchar = original_startchar + i
                        yield t
                else:
                    for start in range(0, len(text) - self.min + 1):
                        for size in range(self.min, self.max + 1):
                            end = start + size
                            if end > len(text):
                                continue

                            t.text = text[start:end]

                            if chars:
                                t.startchar = startchar + start
                                t.endchar = startchar + end

                            yield t


# Analyzers


def ngram_analyzer(minsize, maxsize=None):
    """
    Composes an NgramTokenizer and a LowercaseFilter.

    Args:
        minsize (int): The minimum size of the n-grams.
        maxsize (int, optional): The maximum size of the n-grams. Defaults to None.

    Returns:
        Analyzer: An analyzer that tokenizes text into n-grams and applies lowercase filtering.

    Examples:
        >>> ana = ngram_analyzer(4)
        >>> [token.text for token in ana("hi there")]
        ["hi t", "i th", " the", "ther", "here"]
    """
    return NgramTokenizer(minsize, maxsize=maxsize) | LowercaseFilter()


def ngram_word_analyzer(minsize, maxsize=None, tokenizer=None, at=None):
    """
    Creates an analyzer that tokenizes text into n-grams.

    Args:
        minsize (int): The minimum size of the n-grams.
        maxsize (int, optional): The maximum size of the n-grams. Defaults to None.
        tokenizer (Tokenizer, optional): The tokenizer to use. Defaults to None.
        at (str, optional): The position at which to split the n-grams. Defaults to None.

    Returns:
        Analyzer: The n-gram word analyzer.

    Example:
        >>> analyzer = ngram_word_analyzer(2, 3)
        >>> tokens = analyzer("Hello world")
        >>> list(tokens)
        ['he', 'el', 'll', 'lo', 'wo', 'or', 'rl', 'ld']
    """
    if not tokenizer:
        tokenizer = RegexTokenizer()
    return tokenizer | LowercaseFilter() | NgramFilter(minsize, maxsize, at=at)
