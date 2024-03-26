===================
``analysis`` module
===================

.. automodule:: whoosh.analysis

Analyzers
=========

.. autofunction:: id_analyzer
.. autofunction:: keyword_analyzer
.. autofunction:: regex_analyzer
.. autofunction:: simple_analyzer
.. autofunction:: standard_analyzer
.. autofunction:: stemming_analyzer
.. autofunction:: fancy_analyzer
.. autofunction:: ngram_analyzer
.. autofunction:: ngram_word_analyzer
.. autofunction:: language_analyzer


Tokenizers
==========

.. autoclass:: IDTokenizer
.. autoclass:: RegexTokenizer
.. autoclass:: CharsetTokenizer
.. autofunction:: SpaceSeparatedTokenizer
.. autofunction:: CommaSeparatedTokenizer
.. autoclass:: NgramTokenizer
.. autoclass:: PathTokenizer


Filters
=======

.. autoclass:: PassFilter
.. autoclass:: LoggingFilter
.. autoclass:: MultiFilter
.. autoclass:: TeeFilter
.. autoclass:: ReverseTextFilter
.. autoclass:: LowercaseFilter
.. autoclass:: StripFilter
.. autoclass:: StopFilter
.. autoclass:: StemFilter
.. autoclass:: CharsetFilter
.. autoclass:: NgramFilter
.. autoclass:: IntraWordFilter
.. autoclass:: CompoundWordFilter
.. autoclass:: BiWordFilter
.. autoclass:: ShingleFilter
.. autoclass:: DelimitedAttributeFilter
.. autoclass:: DoubleMetaphoneFilter
.. autoclass:: SubstitutionFilter


Token classes and functions
===========================

.. autoclass:: Token
.. autofunction:: unstopped
