# Copyright 2010 Matt Chaput. All rights reserved.
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
from __future__ import annotations

import calendar
import copy
from datetime import date, datetime, timedelta, timezone
from typing import TYPE_CHECKING, ClassVar, Literal

if TYPE_CHECKING:
    from collections.abc import Collection


class TimeError(Exception):
    pass


def relative_days(
    current_wday: Literal[0, 1, 2, 3, 4, 5, 6],
    wday: Literal[0, 1, 2, 3, 4, 5, 6],
    dir: Literal[-1, 1],
) -> Literal[-7, 7, 0, 6, 5, 4, 3, 2, 1, -6, -5, -4, -3, -2, -1]:
    """Returns the number of days (positive or negative) to the "next" or
    "last" of a certain weekday. ``current_wday`` and ``wday`` are numbers,
    i.e. 0 = monday, 1 = tuesday, 2 = wednesday, etc.

    >>> # Get the number of days to the next tuesday, if today is Sunday
    >>> relative_days(6, 1, 1)
    2

    :param current_wday: the number of the current weekday.
    :param wday: the target weekday.
    :param dir: -1 for the "last" (past) weekday, 1 for the "next" (future)
        weekday.
    """

    if current_wday == wday:
        return 7 * dir

    if dir == 1:
        return (wday + 7 - current_wday) % 7
    else:
        return (current_wday + 7 - wday) % 7 * -1


def timedelta_to_usecs(td: timedelta) -> int:
    total = td.days * 86400000000  # Microseconds in a day
    total += td.seconds * 1000000  # Microseconds in a second
    total += td.microseconds
    return total


def datetime_to_long(dt: datetime) -> int:
    """Converts a datetime object to a long integer representing the number
    of microseconds since ``datetime.min``.
    """

    return timedelta_to_usecs(dt.replace(tzinfo=None) - dt.min)


def long_to_datetime(x: int) -> datetime:
    """Converts a long integer representing the number of microseconds since
    ``datetime.min`` to a datetime object.
    """

    days = x // 86400000000  # Microseconds in a day
    x -= days * 86400000000

    seconds = x // 1000000  # Microseconds in a second
    x -= seconds * 1000000

    return datetime.min + timedelta(days=days, seconds=seconds, microseconds=x)


# Ambiguous datetime object


class adatetime:
    """An "ambiguous" datetime object. This object acts like a
    ``datetime.datetime`` object but can have any of its attributes set to
    None, meaning unspecified.
    """

    year: int | None
    month: int | None
    day: int | None
    hour: int | None
    minute: int | None
    second: int | None
    microsecond: int | None

    units: ClassVar[frozenset[str]] = frozenset(
        ("year", "month", "day", "hour", "minute", "second", "microsecond")
    )

    def __init__(
        self,
        year: datetime | int | None = None,
        month: int | None = None,
        day: int | None = None,
        hour: int | None = None,
        minute: int | None = None,
        second: int | None = None,
        microsecond: int | None = None,
    ) -> None:
        if isinstance(year, datetime):
            dt = year
            self.year, self.month, self.day = dt.year, dt.month, dt.day
            self.hour, self.minute, self.second = dt.hour, dt.minute, dt.second
            self.microsecond = dt.microsecond
        else:
            if month is not None and (month < 1 or month > 12):
                raise TimeError("month must be in 1..12")

            if day is not None and day < 1:
                raise TimeError("day must be greater than 1")
            if (
                year is not None
                and month is not None
                and day is not None
                and day > calendar.monthrange(year, month)[1]
            ):
                raise TimeError("day is out of range for month")

            if hour is not None and (hour < 0 or hour > 23):
                raise TimeError("hour must be in 0..23")
            if minute is not None and (minute < 0 or minute > 59):
                raise TimeError("minute must be in 0..59")
            if second is not None and (second < 0 or second > 59):
                raise TimeError("second must be in 0..59")
            if microsecond is not None and (microsecond < 0 or microsecond > 999999):
                raise TimeError("microsecond must be in 0..999999")

            self.year, self.month, self.day = year, month, day
            self.hour, self.minute, self.second = hour, minute, second
            self.microsecond = microsecond

    def __eq__(self, other: object) -> bool:
        if not other.__class__ is self.__class__:
            if not is_ambiguous(self) and isinstance(other, datetime):
                return fix(self) == other
            else:
                return False
        return all(getattr(self, unit) == getattr(other, unit) for unit in self.units)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}{self.tuple()!r}"

    def tuple(
        self,
    ) -> tuple[
        int | None,
        int | None,
        int | None,
        int | None,
        int | None,
        int | None,
        int | None,
    ]:
        """Returns the attributes of the ``adatetime`` object as a tuple of
        ``(year, month, day, hour, minute, second, microsecond)``.
        """

        return (
            self.year,
            self.month,
            self.day,
            self.hour,
            self.minute,
            self.second,
            self.microsecond,
        )

    def date(self) -> date:
        return self.floor().date()

    def copy(self) -> adatetime:
        return adatetime(
            year=self.year,
            month=self.month,
            day=self.day,
            hour=self.hour,
            minute=self.minute,
            second=self.second,
            microsecond=self.microsecond,
        )

    def replace(self, **kwargs: int | None) -> adatetime:
        """Returns a copy of this object with the attributes given as keyword
        arguments replaced.

        >>> adt = adatetime(year=2009, month=10, day=31)
        >>> adt.replace(year=2010)
        (2010, 10, 31, None, None, None, None)
        """

        newadatetime = self.copy()
        for key, value in kwargs.items():
            if key in self.units:
                setattr(newadatetime, key, value)
            else:
                raise KeyError(f"Unknown argument {key!r}")
        return newadatetime

    def floor(self) -> datetime:
        """Returns a ``datetime`` version of this object with all unspecified
        (None) attributes replaced by their lowest values.

        This method raises an error if the ``adatetime`` object has no year.

        >>> adt = adatetime(year=2009, month=5)
        >>> adt.floor()
        datetime.datetime(2009, 5, 1, 0, 0, 0, 0)
        """

        y, m, d, h, mn, s, ms = (
            self.year,
            self.month,
            self.day,
            self.hour,
            self.minute,
            self.second,
            self.microsecond,
        )

        if y is None:
            raise ValueError("Date has no year")

        if m is None:
            m = 1
        if d is None:
            d = 1
        if h is None:
            h = 0
        if mn is None:
            mn = 0
        if s is None:
            s = 0
        if ms is None:
            ms = 0
        return datetime(y, m, d, h, mn, s, ms, tzinfo=timezone.utc)

    def ceil(self) -> datetime:
        """Returns a ``datetime`` version of this object with all unspecified
        (None) attributes replaced by their highest values.

        This method raises an error if the ``adatetime`` object has no year.

        >>> adt = adatetime(year=2009, month=5)
        >>> adt.floor()
        datetime.datetime(2009, 5, 30, 23, 59, 59, 999999)
        """

        y, m, d, h, mn, s, ms = (
            self.year,
            self.month,
            self.day,
            self.hour,
            self.minute,
            self.second,
            self.microsecond,
        )

        if y is None:
            raise ValueError("Date has no year")

        if m is None:
            m = 12
        if d is None:
            d = calendar.monthrange(y, m)[1]
        if h is None:
            h = 23
        if mn is None:
            mn = 59
        if s is None:
            s = 59
        if ms is None:
            ms = 999999
        return datetime(y, m, d, h, mn, s, ms, tzinfo=timezone.utc)

    def disambiguated(
        self, basedate: adatetime | datetime | None = None
    ) -> datetime | timespan:
        """Returns either a ``datetime`` or unambiguous ``timespan`` version
        of this object.

        Unless this ``adatetime`` object is full specified down to the
        microsecond, this method will return a timespan built from the "floor"
        and "ceil" of this object.

        This method raises an error if the ``adatetime`` object has no year.

        >>> adt = adatetime(year=2009, month=10, day=31)
        >>> adt.disambiguated()
        timespan(datetime(2009, 10, 31, 0, 0, 0, 0), datetime(2009, 10, 31, 23, 59 ,59, 999999)
        """

        dt = self
        if not is_ambiguous(dt):
            fixed = fix(dt)
            assert isinstance(fixed, datetime)
            return fixed
        return timespan(dt, dt).disambiguated(basedate)


# Time span class


class timespan:
    """A span of time between two ``datetime`` or ``adatetime`` objects."""

    def __init__(self, start: adatetime | datetime, end: adatetime | datetime) -> None:
        """
        :param start: a ``datetime`` or ``adatetime`` object representing the
            start of the time span.
        :param end: a ``datetime`` or ``adatetime`` object representing the
            end of the time span.
        """

        if not isinstance(start, (datetime, adatetime)):
            raise TimeError(f"{start!r} is not a datetime object")
        if not isinstance(end, (datetime, adatetime)):
            raise TimeError(f"{end!r} is not a datetime object")

        self.start = copy.copy(start)
        self.end = copy.copy(end)

    def __eq__(self, other: object) -> bool:
        if not other.__class__ is self.__class__:
            return False
        assert isinstance(other, timespan)
        return self.start == other.start and self.end == other.end

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.start!r}, {self.end!r})"

    def disambiguated(
        self, basedate: adatetime | datetime | None = None, debug: int = 0
    ) -> timespan:
        """Returns an unambiguous version of this object.

        >>> start = adatetime(year=2009, month=2)
        >>> end = adatetime(year=2009, month=10)
        >>> ts = timespan(start, end)
        >>> ts
        timespan(adatetime(2009, 2, None, None, None, None, None), adatetime(2009, 10, None, None, None, None, None))
        >>> td.disambiguated(datetime.now())
        timespan(datetime(2009, 2, 28, 0, 0, 0, 0), datetime(2009, 10, 31, 23, 59 ,59, 999999)
        """

        # - If year is in start but not end, use basedate.year for end
        # -- If year is in start but not end, but startdate is > basedate,
        #   use "next <monthname>" to get end month/year
        # - If year is in end but not start, copy year from end to start
        # - Support "next february", "last april", etc.

        start, end = copy.copy(self.start), copy.copy(self.end)
        if basedate is None:
            basedate = datetime.now(tz=timezone.utc)

        basedate_year = basedate.year
        basedate_month = basedate.month
        basedate_day = basedate.day
        assert basedate_year is not None
        assert basedate_month is not None
        assert basedate_day is not None
        start_year_was_amb = start.year is None
        end_year_was_amb = end.year is None

        if has_no_date(start) and has_no_date(end):
            # The start and end points are just times, so use the basedate
            # for the date information.
            start = start.replace(
                year=basedate_year, month=basedate_month, day=basedate_day
            )
            end = end.replace(
                year=basedate_year, month=basedate_month, day=basedate_day
            )
        else:
            # If one side has a year and the other doesn't, the decision
            # of what year to assign to the ambiguous side is kind of
            # arbitrary. I've used a heuristic here based on how the range
            # "reads", but it may only be reasonable in English. And maybe
            # even just to me.

            if start.year is None and end.year is None:
                # No year on either side, use the basedate
                assert isinstance(start, adatetime)
                assert isinstance(end, adatetime)
                start.year = end.year = basedate_year
            elif start.year is None:
                # No year in the start, use the year from the end
                assert isinstance(start, adatetime)
                start.year = end.year
            elif end.year is None:
                assert isinstance(end, adatetime)
                end.year = max(start.year, basedate_year)

        if start.year == end.year:
            # Once again, if one side has a month and day but the other side
            # doesn't, the disambiguation is arbitrary. Does "3 am to 5 am
            # tomorrow" mean 3 AM today to 5 AM tomorrow, or 3am tomorrow to
            # 5 am tomorrow? What I picked is similar to the year: if the
            # end has a month+day and the start doesn't, copy the month+day
            # from the end to the start UNLESS that would make the end come
            # before the start on that day, in which case use the basedate
            # instead. If the start has a month+day and the end doesn't, use
            # the basedate.
            start_dm = not (start.month is None and start.day is None)
            end_dm = not (end.month is None and end.day is None)
            if end_dm and not start_dm:
                assert isinstance(start, adatetime)
                if floor(start).time() > ceil(end).time():
                    start.month = basedate_month
                    start.day = basedate_day
                else:
                    start.month = end.month
                    start.day = end.day
            elif start_dm and not end_dm:
                assert isinstance(end, adatetime)
                end.month = basedate_month
                end.day = basedate_day

        if floor(start).date() > ceil(end).date():
            # If the disambiguated dates are out of order:
            # - If no start year was given, reduce the start year to put the
            #   start before the end
            # - If no end year was given, increase the end year to put the end
            #   after the start
            # - If a year was specified for both, just swap the start and end
            if start_year_was_amb:
                assert isinstance(start, adatetime)
                assert end.year is not None
                start.year = end.year - 1
            elif end_year_was_amb:
                assert isinstance(end, adatetime)
                assert start.year is not None
                end.year = start.year + 1
            else:
                start, end = end, start

        start = floor(start)
        end = ceil(end)

        if start.date() == end.date() and start.time() > end.time():
            # If the start and end are on the same day, but the start time
            # is after the end time, move the end time to the next day
            end += timedelta(days=1)

        return timespan(start, end)


# Functions for working with datetime/adatetime objects


def floor(at: adatetime | datetime) -> datetime:
    if isinstance(at, datetime):
        return at
    return at.floor()


def ceil(at: adatetime | datetime) -> datetime:
    if isinstance(at, datetime):
        return at
    return at.ceil()


def fill_in(
    at: adatetime | datetime,
    basedate: adatetime | datetime,
    units: Collection[str] = adatetime.units,
) -> adatetime | datetime:
    """Returns a copy of ``at`` with any unspecified (None) units filled in
    with values from ``basedate``.
    """

    if isinstance(at, datetime):
        return at

    args: dict[str, int | None] = {}
    for unit in units:
        v = getattr(at, unit)
        if v is None:
            v = getattr(basedate, unit)
        args[unit] = v
    return fix(adatetime(**args))


def has_no_date(at: adatetime | datetime) -> bool:
    """Returns True if the given object is an ``adatetime`` where ``year``,
    ``month``, and ``day`` are all None.
    """

    if isinstance(at, datetime):
        return False
    return at.year is None and at.month is None and at.day is None


def has_no_time(at: adatetime | datetime) -> bool:
    """Returns True if the given object is an ``adatetime`` where ``hour``,
    ``minute``, ``second`` and ``microsecond`` are all None.
    """

    if isinstance(at, datetime):
        return False
    return (
        at.hour is None
        and at.minute is None
        and at.second is None
        and at.microsecond is None
    )


def is_ambiguous(at: adatetime | datetime) -> bool:
    """Returns True if the given object is an ``adatetime`` with any of its
    attributes equal to None.
    """

    if isinstance(at, datetime):
        return False
    return any((getattr(at, attr) is None) for attr in adatetime.units)


def is_void(at: adatetime | datetime) -> bool:
    """Returns True if the given object is an ``adatetime`` with all of its
    attributes equal to None.
    """

    if isinstance(at, datetime):
        return False
    return all((getattr(at, attr) is None) for attr in adatetime.units)


def fix(at: adatetime | datetime) -> adatetime | datetime:
    """If the given object is an ``adatetime`` that is unambiguous (because
    all its attributes are specified, that is, not equal to None), returns a
    ``datetime`` version of it. Otherwise returns the ``adatetime`` object
    unchanged.
    """

    if is_ambiguous(at) or isinstance(at, datetime):
        return at
    year = at.year
    month = at.month
    day = at.day
    hour = at.hour
    minute = at.minute
    second = at.second
    microsecond = at.microsecond
    assert year is not None
    assert month is not None
    assert day is not None
    assert hour is not None
    assert minute is not None
    assert second is not None
    assert microsecond is not None
    return datetime(
        year=year,
        month=month,
        day=day,
        hour=hour,
        minute=minute,
        second=second,
        microsecond=microsecond,
        tzinfo=timezone.utc,
    )
