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


import random
import sys
import time
from bisect import insort
from functools import wraps

# These must be valid separate characters in CASE-INSENSTIVE filenames
IDCHARS = "0123456789abcdefghijklmnopqrstuvwxyz"


if hasattr(time, "perf_counter"):
    now = time.perf_counter
elif sys.platform == "win32":
    now = time.clock
else:
    now = time.time


def random_name(size=28):
    """
    Generates a random name consisting of alphanumeric characters.

    Parameters:
    - size (int): The length of the random name to generate. Default is 28.

    Returns:
    - str: The randomly generated name.
    """
    return "".join(random.choice(IDCHARS) for _ in range(size))


def random_bytes(size=28):
    """
    Generate a random byte string of the specified size.

    Parameters:
    - size (int): The size of the byte string to generate. Default is 28.

    Returns:
    - bytes: A random byte string of the specified size.

    Example:
    >>> random_bytes(16)
    b'\x8f\x9a\x0b\x1e\x9c\x8d\x8c\x9e\x1f\x9d\x9e\x0e\x1e\x9e\x1e\x9e'
    """
    return bytes(random.randint(0, 255) for _ in range(size))


def make_binary_tree(fn, args, **kwargs):
    """Takes a function/class that takes two positional arguments and a list of
    arguments and returns a binary tree of results/instances.

    Args:
        fn (callable): A function or class that takes two positional arguments.
        args (list): A list of arguments to be used to construct the binary tree.

    Keyword Args:
        **kwargs: Additional keyword arguments to be passed to the class initializer.

    Returns:
        object: The binary tree of results/instances.

    Raises:
        ValueError: If called with an empty list.

    Examples:
        >>> make_binary_tree(UnionMatcher, [matcher1, matcher2, matcher3])
        UnionMatcher(matcher1, UnionMatcher(matcher2, matcher3))

    This function takes a function or class `fn` that takes two positional arguments,
    and a list of arguments `args`. It constructs a binary tree of results/instances
    by recursively splitting the `args` list into two halves and calling `fn` with
    the left and right halves as arguments.

    If the `args` list contains only one element, that element is returned as is.

    Any additional keyword arguments given to this function are passed to the class
    initializer of `fn`.

    Note:
        The `fn` should be a function or class that can be called with two positional
        arguments and returns a result/instance.

    """
    count = len(args)
    if not count:
        raise ValueError("Called make_binary_tree with empty list")
    elif count == 1:
        return args[0]

    half = count // 2
    return fn(
        make_binary_tree(fn, args[:half], **kwargs),
        make_binary_tree(fn, args[half:], **kwargs),
        **kwargs,
    )


def make_weighted_tree(fn, ls, **kwargs):
    """
    Takes a function/class that takes two positional arguments and a list of
    (weight, argument) tuples and returns a huffman-like weighted tree of
    results/instances.

    Args:
        fn (function/class): The function or class that takes two positional arguments.
        ls (list): A list of (weight, argument) tuples.
        **kwargs: Additional keyword arguments that can be passed to the function/class.

    Returns:
        object: The huffman-like weighted tree of results/instances.

    Raises:
        ValueError: If the input list is empty.

    Example:
        >>> def combine(a, b):
        ...     return a + b
        ...
        >>> ls = [(1, 'a'), (2, 'b'), (3, 'c')]
        >>> make_weighted_tree(combine, ls)
        'abc'
    """
    if not ls:
        raise ValueError("Called make_weighted_tree with empty list")

    ls.sort()
    while len(ls) > 1:
        a = ls.pop(0)
        b = ls.pop(0)
        insort(ls, (a[0] + b[0], fn(a[1], b[1])))
    return ls[0][1]


# Fibonacci function

_fib_cache = {}


def fib(n):
    """
    Returns the nth value in the Fibonacci sequence.

    Parameters:
    - n (int): The position of the value in the Fibonacci sequence to be returned.

    Returns:
    - int: The nth value in the Fibonacci sequence.

    Notes:
    - The Fibonacci sequence starts with 0 and 1, and each subsequent value is the sum of the two preceding values.
    - The function uses memoization to improve performance by caching previously calculated values.
    """
    if n <= 2:
        return n
    if n in _fib_cache:
        return _fib_cache[n]
    result = fib(n - 1) + fib(n - 2)
    _fib_cache[n] = result
    return result


# Decorators


def synchronized(func):
    """Decorator for storage-access methods, which synchronizes on a threading
    lock. The parent object must have 'is_closed' and '_sync_lock' attributes.

    Args:
        func (callable): The function to be decorated.

    Returns:
        callable: The decorated function.

    Example:
        >>> class MyClass:
        ...     def __init__(self):
        ...         self._sync_lock = threading.Lock()
        ...
        ...     @synchronized
        ...     def my_method(self):
        ...         # Access shared storage here
        ...         pass

    """

    @wraps(func)
    def synchronized_wrapper(self, *args, **kwargs):
        with self._sync_lock:
            return func(self, *args, **kwargs)

    return synchronized_wrapper


def unclosed(method):
    """
    Decorator to check if the object is closed.

    This decorator can be used to wrap methods in a class to ensure that the object is not closed before executing the method.
    If the object is closed, a ValueError is raised.

    Parameters:
    - method: The method to be wrapped.

    Returns:
    - The wrapped method.

    Example usage:
    ```
    class MyClass:
        @unclosed
        def my_method(self):
            # Method implementation
    ```
    """

    @wraps(method)
    def unclosed_wrapper(self, *args, **kwargs):
        if self.closed:
            raise ValueError("Operation on a closed object")
        return method(self, *args, **kwargs)

    return unclosed_wrapper
