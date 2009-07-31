==========================
The default query language
==========================

.. highlight:: none

Overview
========

A query consists of *terms* and *operators*. There are two types of terms: single terms and *phrases*. Multiple terms can be combined with operators such as *AND* and *OR*.

Whoosh supports indexing text in different *fields*. You must specify the *default field* when you create the :class:`whoosh.qparser.QueryParser` object. This is the field in which any terms the user does not explicitly specify a field for will be searched.


Individual terms and phrases
============================

Find documents containing the term ``render``::

    render

Find documents containing the phrase ``all was well``::

    "all was well"

Note that a field must store Position information for phrase searching to work in that field.


Boolean operators
=================

Find documents containing ``render`` *and* ``shading``::

    render AND shading
    
Find documents containing ``render``, *and* also either ``shading`` *or* ``modeling``::

    render AND shading OR modeling

Find documents containing ``render`` but *not* modeling::

    render NOT modeling
    
Find documents containing ``alpha`` but not either ``beta`` or ``gamma``::

    alpha NOT (beta OR gamma)

Note that when no boolean operator is specified between terms, the parser will insert one, by default AND. So this query::

    render shading modeling
    
is equivalent (by default) to::

    render AND shading AND modeling

See :doc:`customizing the default parser <parsing>` for information on how to change the default operator to OR.

Group operators together with parentheses. For example to find documents that contain both ``render`` and ``shading``, or contain ``modeling``::

    (render AND shading) OR modeling


Fields
======

Find the term ``ivan`` in the ``name`` field::

    name:ivan
    
The ``field:`` prefix only sets the field for the term it directly precedes, so the query::
    
    title:open sesame
        
Will search for ``open`` in the ``title`` field and ``sesame`` in the *default* field.

To apply a field prefix to multiple terms, group them with parentheses::

    title:(open sesame)
    
This is the same as::

    title:open title:sesame
    
Of course you can specify a field for phrases too::

    title:"open sesame"


Inexact terms
=============

Use "globs" (wildcard expressions using ``?`` to represent a single character and ``*`` to represent any number of characters) to match terms::

    te?t test* *b?g*``
    
Note that a wildcard starting with ``?`` or ``*`` is very slow. Note also that these wildcards only match *individual terms*. For example, the query::

    my*life
    
will **not** match an indexed phrase like::

    my so called life
    
because those are four separate terms.


Ranges
======

You can match a range of terms. For example, the following query will match documents containing terms in the lexical range from ``apple`` to ``bear`` *inclusive*. For example, it will match documents containing ``azores`` and ``be`` but not ``blur``::

    apple..bear

This is very useful when you've stored, for example, dates in a lexically sorted format (i.e. YYYYMMDD)::

    date:20050101..20090715
    
**TODO**: Lucene has a different syntax for the range operator, and ways to specify whether a range is inclusive or exclusive.


Boosting query elements
=======================

You can specify that certain parts of a query are more important for calculating the score of a matched document than others. For example, to specify that ``ninja`` is twice as important as other words, and ``bear`` is half as important::

    ninja^2 cowboy bear^0.5
    
You can apply a boost to several terms using grouping parentheses::

    (open sesame)^2.5 roc


Escaping special characters
===========================

Use backslashes to escape special characters that would otherwise be interpreted by the query parser::

    MacHD\:Documents
    term\ with\ spaces
    function\(\)
    
You can also place a single term inside quotes. The parser does not interpret special characters (other than spaces) inside quotes::

    oldmacpath:"MacHD:Documents"

