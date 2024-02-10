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

from whoosh.analysis.filters import Filter
from whoosh.lang.dmetaphone import double_metaphone
from whoosh.lang.porter import stem
from whoosh.util.cache import lfu_cache, unbound_cache


class StemFilter(Filter):
    """Stems (removes suffixes from) the text of tokens using the Porter
    stemming algorithm. Stemming attempts to reduce multiple forms of the same
    root word (for example, "rendering", "renders", "rendered", etc.) to a
    single word in the index.

    Args:
        stemfn (object): The function to use for stemming. Default is the Porter stemming algorithm for English.
        lang (str): If not None, overrides the stemfn with a language stemmer from the `whoosh.lang.snowball` package.
        ignore (list): A set/list of words that should not be stemmed. This is converted into a frozenset. If you omit this argument, all tokens are stemmed.
        cachesize (int): The maximum number of words to cache. Use -1 for an unbounded cache, or None for no caching.

    Attributes:
        is_morph (bool): Indicates if the filter is a morphological filter.

    Methods:
        __init__(self, stemfn=stem, lang=None, ignore=None, cachesize=50000): Initializes the StemFilter object.
        __getstate__(self): Returns the state of the object for pickling.
        __setstate__(self, state): Sets the state of the object after unpickling.
        clear(self): Clears the stem function and sets it based on the provided parameters.
        cache_info(self): Returns information about the cache used by the stem function.
        __eq__(self, other): Compares two StemFilter objects for equality.
        __call__(self, tokens): Applies stemming to the tokens.

    Examples:
        stemmer = RegexTokenizer() | StemFilter()
        [token.text for token in stemmer("fundamentally willows")]
        Output: ["fundament", "willow"]

        stemfilter = StemFilter(stem_function)
        stemfilter = StemFilter(lang="ru")
    """

    __inittypes__ = {"stemfn": object, "ignore": list}
    is_morph = True

    def __init__(self, stemfn=stem, lang=None, ignore=None, cachesize=50000):
        """
        Initializes the StemFilter object.

        Args:
            stemfn (object): The function to use for stemming. Default is the Porter stemming algorithm for English.
            lang (str): If not None, overrides the stemfn with a language stemmer from the `whoosh.lang.snowball` package.
            ignore (list): A set/list of words that should not be stemmed. This is converted into a frozenset. If you omit this argument, all tokens are stemmed.
            cachesize (int): The maximum number of words to cache. Use -1 for an unbounded cache, or None for no caching.

        Raises:
            TypeError: If the `stemfn` argument is not callable.
            ValueError: If the `cachesize` argument is not a positive integer or None.

        Notes:
            The StemFilter object is used to apply stemming to tokens during the analysis process. Stemming is the process of reducing words to their base or root form, which can help improve search accuracy by treating different forms of the same word as equivalent.

            The `stemfn` argument specifies the function to use for stemming. By default, the Porter stemming algorithm for English is used. You can provide your own custom stemming function if desired.

            The `lang` argument allows you to override the `stemfn` with a language stemmer from the `whoosh.lang.snowball` package. If `lang` is not None, the stemmer for the specified language will be used instead of the `stemfn`.

            The `ignore` argument is a set/list of words that should not be stemmed. If you omit this argument, all tokens will be stemmed. The `ignore` set/list is converted into a frozenset for efficient lookup.

            The `cachesize` argument specifies the maximum number of words to cache. Caching can improve performance by avoiding redundant stemming operations. Use -1 for an unbounded cache, or None for no caching.

        Example:
            # Initialize StemFilter with default settings
            stem_filter = StemFilter()

            # Initialize StemFilter with custom stemming function
            def custom_stemmer(word):
                # custom stemming logic
                return stemmed_word

            stem_filter = StemFilter(stemfn=custom_stemmer)

            # Initialize StemFilter with language stemmer
            stem_filter = StemFilter(lang='english')

            # Initialize StemFilter with ignored words
            stem_filter = StemFilter(ignore=['apple', 'banana', 'orange'])

            # Initialize StemFilter with caching disabled
            stem_filter = StemFilter(cachesize=None)
        """
        self.stemfn = stemfn
        self.lang = lang
        self.ignore = frozenset() if ignore is None else frozenset(ignore)
        self.cachesize = cachesize
        # clear() sets the _stem attr to a cached wrapper around self.stemfn
        self.clear()

    def __getstate__(self):
        """
        Get the state of the object for pickling.

        This method is called by the pickle module when pickling an object.
        It returns a dictionary representing the state of the object, excluding
        the '_stem' attribute.

        Returns:
            dict: The state of the object without the '_stem' attribute.

        Example:
            >>> obj = MyObject()
            >>> state = obj.__getstate__()
            >>> print(state)
            {'attr1': value1, 'attr2': value2, ...}

        Note:
            This method is automatically called by the pickle module and should
            not be called directly by user code.
        """
        # Can't pickle a dynamic function, so we have to remove the _stem
        # attribute from the state
        return {k: self.__dict__[k] for k in self.__dict__ if k != "_stem"}

    def __setstate__(self, state):
        """
        Set the state of the object during unpickling.

        This method is called by the pickle module when unpickling an object.
        It sets the state of the object based on the provided state dictionary.

        Parameters:
        - state (dict): The state dictionary containing the object's attributes.

        Notes:
        - This method is primarily used for backward compatibility with older versions
            of the StemFilter class.
        - It checks for old instances of StemFilter class and updates the state
            accordingly.
        - If the 'cachesize' attribute is not present in the state dictionary, it
            sets the 'cachesize' attribute to a default value of 50000.
        - If the 'ignores' attribute is present in the state dictionary, it sets the
            'ignore' attribute to the value of 'ignores'.
        - If the 'ignore' attribute is not present in the state dictionary, it sets
            the 'ignore' attribute to an empty frozenset.
        - If the 'lang' attribute is not present in the state dictionary, it sets the
            'lang' attribute to None.
        - If the 'cache' attribute is present in the state dictionary, it removes the
            'cache' attribute from the state dictionary.

        Returns:
        - None

        Example:
        >>> state = {
        ...     'cachesize': 10000,
        ...     'ignores': {'word1', 'word2'},
        ...     'lang': 'en',
        ...     'cache': {},
        ... }
        >>> obj = StemFilter()
        >>> obj.__setstate__(state)
        >>> obj.cachesize
        10000
        >>> obj.ignore
        {'word1', 'word2'}
        >>> obj.lang
        'en'
        >>> 'cache' in obj.__dict__
        False
        """
        if "cachesize" not in state:
            self.cachesize = 50000
        if "ignores" in state:
            self.ignore = state["ignores"]
        elif "ignore" not in state:
            self.ignore = frozenset()
        if "lang" not in state:
            self.lang = None
        if "cache" in state:
            del state["cache"]

        self.__dict__.update(state)
        self.clear()

    def clear(self):
        """
        Clears the stem function and sets it based on the provided parameters.

        This method clears the current stem function and sets it based on the provided parameters.
        If the language is specified, it retrieves the stemmer function for that language from the 'whoosh.lang' module.
        Otherwise, it uses the stem function that was previously set.

        If the 'cachesize' parameter is an integer and not equal to 0, it creates a cache for the stem function.
        If 'cachesize' is a negative integer, an unbound cache is created using the stem function.
        If 'cachesize' is a positive integer greater than 1, an LFU (Least Frequently Used) cache is created with the specified size.

        If 'cachesize' is not an integer or equal to 0, no cache is created and the stem function is used directly.

        Note: The stem function is responsible for transforming words into their base or root form.

        Usage:
        morph = MorphAnalyzer()
        morph.clear()
        """
        if self.lang:
            from whoosh.lang import stemmer_for_language

            stemfn = stemmer_for_language(self.lang)
        else:
            stemfn = self.stemfn

        if isinstance(self.cachesize, int) and self.cachesize != 0:
            if self.cachesize < 0:
                self._stem = unbound_cache(stemfn)
            elif self.cachesize > 1:
                self._stem = lfu_cache(self.cachesize)(stemfn)
        else:
            self._stem = stemfn

    def cache_info(self):
        """
        Returns information about the cache used by the stem function.

        The cache_info method provides information about the cache used by the stem function.
        It returns an object that contains details such as the number of cache hits, misses,
        and the current size of the cache.

        Returns:
            cache_info (object): An object containing information about the cache used by the stem function.
                The object has the following attributes:
                - hits (int): The number of cache hits.
                - misses (int): The number of cache misses.
                - maxsize (int): The maximum size of the cache.
                - currsize (int): The current size of the cache.

                Returns None if caching is disabled.
        """
        if self.cachesize <= 1:
            return None
        return self._stem.cache_info()

    def __eq__(self, other):
        """
        Compares two StemFilter objects for equality.

        This method compares the current StemFilter object with another StemFilter object
        to determine if they are equal. Two StemFilter objects are considered equal if they
        are of the same class and have the same stem function.

        Args:
            other (StemFilter): The other StemFilter object to compare.

        Returns:
            bool: True if the two StemFilter objects are equal, False otherwise.
        """
        return (
            other and self.__class__ is other.__class__ and self.stemfn == other.stemfn
        )

    def __call__(self, tokens):
        """
        Applies stemming to the tokens.

        This method applies stemming to the given tokens using the specified stemmer.
        It iterates over the tokens, checks if the token is not stopped, and if the token's text
        is not in the ignore list. If the conditions are met, the token's text is stemmed using
        the stemmer's stem function.

        Args:
            tokens (iterable): The tokens to apply stemming to.

        Yields:
            Token: The stemmed tokens.

        Example:
            >>> stemmer = Stemmer()
            >>> tokens = [Token("running"), Token("jumps"), Token("jumping")]
            >>> stemmed_tokens = stemmer(tokens)
            >>> list(stemmed_tokens)
            [Token("run"), Token("jump"), Token("jump")]
        """
        stemfn = self._stem
        ignore = self.ignore

        for t in tokens:
            if not t.stopped:
                text = t.text
                if text not in ignore:
                    t.text = stemfn(text)
            yield t


class PyStemmerFilter(StemFilter):
    """This is a simple subclass of StemFilter that works with the py-stemmer
    third-party library. You must have the py-stemmer library installed to use
    this filter.

    Args:
        lang (str, optional): A string identifying the stemming algorithm to use.
            You can get a list of available algorithms by using the `algorithms()`
            method. The identification strings are directly from the py-stemmer library.
            Defaults to "english".
        ignore (set or list, optional): A set or list of words that should not be stemmed.
            If provided, these words will be excluded from the stemming process.
            Defaults to None.
        cachesize (int, optional): The maximum number of words to cache. Defaults to 10000.

    Attributes:
        lang (str): The language identifier for the stemming algorithm.
        ignore (frozenset): The set of words to be ignored during stemming.
        cachesize (int): The maximum number of words to cache.
        _stem (function): The stemmer function used for stemming.

    Methods:
        algorithms(): Returns a list of stemming algorithms provided by the py-stemmer library.
        cache_info(): Returns information about the cache (not implemented).
        __getstate__(): Returns the state of the object for pickling (excluding _stem attribute).
        __setstate__(): Sets the state of the object after unpickling.

    Example:
        >>> filter = PyStemmerFilter("spanish")
    """

    def __init__(self, lang="english", ignore=None, cachesize=10000):
        """
        Initialize the PyStemmerFilter.

        Args:
            lang (str, optional): A string identifying the stemming algorithm to use.
                You can get a list of available algorithms by using the `algorithms()`
                method. The identification strings are directly from the py-stemmer library.
                Defaults to "english".
            ignore (set or list, optional): A set or list of words that should not be stemmed.
                If provided, these words will be excluded from the stemming process.
                Defaults to None.
            cachesize (int, optional): The maximum number of words to cache. Defaults to 10000.
        """

        self.lang = lang
        self.ignore = frozenset() if ignore is None else frozenset(ignore)
        self.cachesize = cachesize
        self._stem = self._get_stemmer_fn()

    def algorithms(self):
        """
        Returns a list of stemming algorithms provided by the py-stemmer library.

        This method uses the py-stemmer library to retrieve a list of available stemming algorithms.
        Stemming algorithms are used to reduce words to their base or root form, which can be useful
        in natural language processing tasks such as information retrieval, text mining, and language
        modeling.

        Returns:
            list: A list of strings representing the names of available stemming algorithms.

        Example:
            >>> analyzer = Analyzer()
            >>> algorithms = analyzer.algorithms()
            >>> print(algorithms)
            ['porter', 'snowball']
        """
        import Stemmer  # type: ignore @UnresolvedImport

        return Stemmer.algorithms()

    def cache_info(self):
        """Returns information about the cache.

        This method is not implemented and always returns None.

        Returns:
            None: This method does not provide any information about the cache.
        """
        return None

    def _get_stemmer_fn(self):
        """
        Returns a stemmer function for the specified language.

        This function imports the Stemmer module and initializes a stemmer object
        with the specified language. The stemmer object is then configured with
        the specified cache size. Finally, the stemWord method of the stemmer
        object is returned as the stemmer function.

        Returns:
            callable: A stemmer function that takes a word as input and returns its stem.

        Raises:
            ImportError: If the Stemmer module cannot be imported.
        """
        import Stemmer  # type: ignore @UnresolvedImport

        stemmer = Stemmer.Stemmer(self.lang)
        stemmer.maxCacheSize = self.cachesize
        return stemmer.stemWord

    def __getstate__(self):
        """
        Get the state of the object for pickling.

        This method is called by the pickle module when pickling an object.
        It returns a dictionary representing the object's state, excluding the
        '_stem' attribute.

        Returns:
            dict: A dictionary representing the object's state.

        Note:
            The '_stem' attribute is excluded from the state because dynamic
            functions cannot be pickled.

        """
        return {k: self.__dict__[k] for k in self.__dict__ if k != "_stem"}

    def __setstate__(self, state):
        """
        Set the state of the object during unpickling.

        This method is called by the pickle module when unpickling an object.
        It is responsible for setting the state of the object based on the
        provided `state` dictionary.

        Parameters:
            state (dict): The dictionary containing the state of the object.

        Returns:
            None

        Raises:
            None

        Notes:
            - This method is used to handle backward compatibility with old
              instances of the `StemFilter` class.
            - If the `state` dictionary does not contain the key "cachesize",
              the `cachesize` attribute is set to the default value of 10000.
            - If the `state` dictionary contains the key "ignores", the `ignore`
              attribute is set to the value of "ignores".
            - If the `state` dictionary does not contain the key "ignore", the
              `ignore` attribute is set to an empty frozenset.
            - The "cache" key is removed from the `state` dictionary.
            - The `state` dictionary is used to update the object's attributes.
            - The `_stem` attribute is set using the `_get_stemmer_fn` method.
        """
        if "cachesize" not in state:
            self.cachesize = 10000
        if "ignores" in state:
            self.ignore = state["ignores"]
        elif "ignore" not in state:
            self.ignore = frozenset()
        if "cache" in state:
            del state["cache"]

        self.__dict__.update(state)
        # Set the _stem attribute
        self._stem = self._get_stemmer_fn()


class DoubleMetaphoneFilter(Filter):
    """Transforms the text of the tokens using Lawrence Philips's Double
    Metaphone algorithm. This algorithm attempts to encode words in such a way
    that similar-sounding words reduce to the same code. This may be useful for
    fields containing the names of people and places, and other uses where
    tolerance of spelling differences is desirable.

    Args:
        primary_boost (float, optional): The boost to apply to the token containing the
            primary code. Defaults to 1.0.
        secondary_boost (float, optional): The boost to apply to the token containing the
            secondary code, if any. Defaults to 0.5.
        combine (bool, optional): If True, the original unencoded tokens are kept in the
            stream, preceding the encoded tokens. Defaults to False.
    """

    is_morph = True

    def __init__(self, primary_boost=1.0, secondary_boost=0.5, combine=False):
        """
        Initialize a MorphAnalyzer object.

        Args:
            primary_boost (float, optional): The boost factor for primary morphological analysis. Defaults to 1.0.
            secondary_boost (float, optional): The boost factor for secondary morphological analysis. Defaults to 0.5.
            combine (bool, optional): Whether to combine the results of primary and secondary analysis. Defaults to False.
        """
        self.primary_boost = primary_boost
        self.secondary_boost = secondary_boost
        self.combine = combine

    def __eq__(self, other):
        """
        Check if two objects are equal.

        This method compares the current object with another object to determine if they are equal.
        The comparison is based on the class type and the primary_boost attribute.

        Parameters:
        - other: The object to compare with.

        Returns:
        - bool: True if the objects are equal, False otherwise.
        """
        return (
            other
            and self.__class__ is other.__class__
            and self.primary_boost == other.primary_boost
        )

    def __call__(self, tokens):
        """
        Applies morphological analysis to a sequence of tokens.

        Args:
            tokens (iterable): The input tokens to be analyzed.

        Yields:
            Token: The analyzed tokens with modified text and boost.

        Notes:
            This method applies morphological analysis to each token in the input sequence.
            It uses the double metaphone algorithm to generate primary and secondary forms of the token's text.
            The token's text and boost are then modified based on the generated forms and yielded.

        Example:
            >>> analyzer = MorphAnalyzer()
            >>> tokens = [Token("running", boost=1.0), Token("swimming", boost=0.8)]
            >>> analyzed_tokens = list(analyzer(tokens))
            >>> for token in analyzed_tokens:
            ...     print(token.text, token.boost)
            ...
            run 1.0
            swim 0.8
        """
        primary_boost = self.primary_boost
        secondary_boost = self.secondary_boost
        combine = self.combine

        for t in tokens:
            if combine:
                yield t

            primary, secondary = double_metaphone(t.text)
            b = t.boost
            # Overwrite the token's text and boost and yield it
            if primary:
                t.text = primary
                t.boost = b * primary_boost
                yield t
            if secondary:
                t.text = secondary
                t.boost = b * secondary_boost
                yield t
