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


import functools
from collections import Counter
from collections.abc import Callable, Hashable
from heapq import nsmallest
from operator import itemgetter
from typing import Protocol, TypeVar, cast

R = TypeVar("R")
R_co = TypeVar("R_co", covariant=True)


class _CachedFunction(Protocol[R_co]):
    def __call__(self, *args: Hashable) -> R_co: ...

    def cache_info(self) -> tuple[int, int, int, int]: ...

    def cache_clear(self) -> None: ...


def unbound_cache(func: Callable[..., R]) -> Callable[..., R]:
    """Caching decorator with an unbounded cache size."""

    cache: dict[tuple[Hashable, ...], R] = {}

    @functools.wraps(func)
    def caching_wrapper(*args: Hashable) -> R:
        try:
            return cache[args]
        except KeyError:
            result = func(*args)
            cache[args] = result
            return result

    return caching_wrapper


def lfu_cache(maxsize: int = 100) -> Callable[[Callable[..., R]], _CachedFunction[R]]:
    """A simple cache that, when the cache is full, deletes the least frequently
    used 10% of the cached values.

    This function duplicates (more-or-less) the protocol of the
    ``functools.lru_cache`` decorator in the Python 3.2 standard library.

    Arguments to the cached function must be hashable.

    View the cache statistics tuple ``(hits, misses, maxsize, currsize)``
    with f.cache_info().  Clear the cache and statistics with f.cache_clear().
    Access the underlying function with f.__wrapped__.
    """

    def decorating_function(user_function: Callable[..., R]) -> _CachedFunction[R]:
        stats = [0, 0]  # Hits, misses
        data: dict[tuple[Hashable, ...], R] = {}
        usecount: Counter[tuple[Hashable, ...]] = Counter()

        @functools.wraps(user_function)
        def wrapper(*args: Hashable) -> R:
            try:
                result = data[args]
                stats[0] += 1  # Hit
            except KeyError:
                stats[1] += 1  # Miss
                if len(data) == maxsize:
                    for k, _ in nsmallest(
                        maxsize // 10 or 1, usecount.items(), key=itemgetter(1)
                    ):
                        del data[k]
                        del usecount[k]
                data[args] = user_function(*args)
                result = data[args]
            finally:
                usecount[args] += 1
            return result

        def cache_info() -> tuple[int, int, int, int]:
            return stats[0], stats[1], maxsize, len(data)

        def cache_clear() -> None:
            data.clear()
            usecount.clear()

        cached_wrapper = cast("_CachedFunction[R]", wrapper)
        cached_wrapper.cache_info = cache_info
        cached_wrapper.cache_clear = cache_clear
        return cached_wrapper

    return decorating_function
