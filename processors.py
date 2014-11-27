# -*- coding: utf-8 -*-

"""Parsers useful for Archives metadata."""

__authors__ = 'User:Jean-Frédéric'

import sys
import re

reload(sys)
sys.setdefaultencoding('utf-8')


def parse_format(field, old_field_value):
    """Parse a foramt and return the parsed."""
    new_value = parse_format_unwrapped(old_field_value)
    return {field: new_value}


def _clean_dim(dim):
    """Clean a dimension-like string"""
    return re.sub(r"\s?,\s?", '.', dim).strip()


def parse_format_unwrapped(text):

    def repl(m):
        """Convert the pattern matchd in {{Size}}."""
        unit = 'cm'
        elements = m.groupdict()
        l = filter(None, [elements[x] for x in sorted(elements.keys())])
        s = '|'.join([_clean_dim(dim) for dim in l])
        return " {{Size|%s|%s}}" % (unit, s)

    format_pattern = re.compile(r"""
        (?P<a>[\d,\.]+?)   # Digits, comma or dot, captured as group
        \s*x\s*            # Whitespace, x, whitespace
        (?P<b>[\d,\.]+?)   # Same
        \s*$               # Whitespace until the end
        """, re.X)

    new_value = re.sub(format_pattern, repl, text)
    return new_value


def look_for_date(field, old_field_value):
    """Wrapper around look_for_date_unwrapped.

    Retrieve the values found by look_for_date_unwrapped
    Re-add the old_field_value to the dictionary
    Add the date and the year if they were found

    """

    (date, year) = look_for_date_unwrapped(old_field_value)

    result = {field: old_field_value}
    if date:
        result['date'] = date
    if year:
        result['year'] = year
    return result


def look_for_date_unwrapped(text):
    """Look for a date in the given text.

    Search a given string for a date pattern, using regular expressions.
    Return the date (either using the ISO YYY-MM-DD format
    or the {{Other date}} template) and the year.

    """

    monthList = {
        u'janvier': 1, u'février': 2, u'mars': 3, u'avril': 4,
        u'mai': 5, u'juin': 6, 'juillet': 7, u'août': 8,
        u'septembre': 9, u'octobre': 10, u'novembre': 11, u'décembre': 12
        }

    fullDatePattern = re.compile("""
        (?P<day>\d+?)           # Some digits
        \s                      # Whitespace
        (?P<month>[\w]+?)       # Some letters, captured as 'month'
        \s                      # Whitespace
        (?P<year>\d\d\d\d)      # Four digits, captured
        """, re.UNICODE + re.X)
    monthDatePattern = re.compile("""
        (?P<month>\w\w\w[\w]+?)  # Some letters, captured as 'month'
        \s                       # Whitespace
        (?P<year>\d\d\d\d)       # Four digits, captured as 'year'
        """, re.UNICODE + re.X)
    circaYearPattern = re.compile("""
        Vers                    # The 'Vers' word
        \s*?                    # Maybe some whitespace
        (?P<year>\d\d\d\d)      # Four digits, captured as 'year'
        """, re.UNICODE + re.X)
    circaDatePattern = re.compile("""
        Vers                    # The 'Vers' word
        \s                      # Whitespace
        (?P<month>\w*?)         # Some letters, captured as 'month'
        \s                      # Whitespace
        (?P<year>\d\d\d\d)      # Four digits, captured as 'year'>
        """, re.UNICODE + re.X)
    betweenDatePattern = re.compile("""
        Entre                   # The 'Entre' word
        [\s\w]*?                # Whatever words and whitespace
        \s                      # Whitespace
        (?P<year1>\d\d\d\d)     # Four digits
        \s                      # Whitespace
        et                      # The 'Et' word
        \s                      # Whitespace
        (?P<year2>\d\d\d\d)     # Four digits
        """, re.UNICODE + re.X)
    orDatePattern = re.compile("""
        (?P<year1>\d\d\d\d)
        \sou\s
        (?P<year2>\d\d\d\d)
        """, re.UNICODE + re.X)
    decadeDatePattern = re.compile("""
        Ann\wes                 # The 'Années' word
        \s                      # Whitespace
        (?P<year>\d\d\d\d)      # Four digits
        """, re.UNICODE + re.X)
    centuryPattern = re.compile("""
        (?P<qualifier>Fin)?\s?  # The 'Fin' word, possibly
        (?P<century>\d\d)       # Two digits
        e                       # The 'e' letter
        \s                      # Whitespace
        si\wcle                 # The 'Siècle' word
        """, re.UNICODE + re.X)

    fullDateR = re.search(fullDatePattern, text)
    monthDateR = re.search(monthDatePattern, text)
    circaYearR = re.search(circaYearPattern, text)
    circaDateR = re.search(circaDatePattern, text)
    betweenDateR = re.search(betweenDatePattern, text)
    orDateR = re.search(orDatePattern, text)
    decadeDateR = re.search(decadeDatePattern, text)
    centuryR = re.search(centuryPattern, text)

    if betweenDateR:
        date = u'{{Other date|between|%s|%s}}' % (betweenDateR.group('year1'),
                                                  betweenDateR.group('year2'))
        return (date, None)

    elif orDateR:
        date = u'{{Other date|or|%s|%s}}' % (orDateR.group('year1'),
                                             orDateR.group('year2'))
        return (date, None)

    elif decadeDateR:
        date = u'{{Other date|decade|%s}}' % (decadeDateR.group('year'))
        return (date, None)

    elif fullDateR:
        month = fullDateR.group('month').lower()
        if month in monthList.keys():
            monthNum = monthList[month]
            year = fullDateR.group('year')
            date = u'%s-%s-%s' % (year,
                                  '%02d' % monthList[month],
                                  '%02d' % int(fullDateR.group('day')))
            dateCategory = u"%s in " % fullDateR.group('year')
            return (date, year)
        else:
            return (None, None)

    elif circaDateR:
        month = circaDateR.group('month').lower()
        if month in monthList.keys():
            year = circaDateR.group('year')
            date = u'{{Other date|circa|%s-%s}}' % (year,
                                                    '%02d' % monthList[month])
            return (date, year)

    elif circaYearR:
        circaYear = circaYearR.group('year')
        date = u'{{Other date|circa|%s}}' % (circaYear)
        return (date, circaYear)

    elif monthDateR:
        month = monthDateR.group('month').lower()
        if month in monthList.keys():
            year = monthDateR.group('year')
            date = u'%s-%s' % (year, '%02d' % monthList[month])
            dateCategory = u"%s in " % monthDateR.group('year')
            return (date, year)
        else:
            return (None, None)

    elif centuryR:
        century = centuryR.group('century')
        date = '{{Other date|century|%s}}' % (century)

        if centuryR.groupdict()['qualifier']:
            qualifier = centuryR.group('qualifier').lower()
            table = {'fin': 'end'}
            date = u'{{Other date|%s|%s}}' % (table[qualifier], date)
        return (date, None)

    else:
        return (None, None)
