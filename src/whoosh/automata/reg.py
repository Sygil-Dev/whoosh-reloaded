# Copyright 2014 Matt Chaput. All rights reserved.
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

from whoosh.automata.fsa import ANY, EPSILON, NFA

# Operator precedence
CHOICE = ("|",)
ops = ()


def parse(pattern):
    """
    Parses a regular expression pattern and returns a parsed representation.

    Args:
        pattern (str): The regular expression pattern to parse.

    Returns:
        list: A list representing the parsed regular expression pattern.

    Example:
        >>> parse("ab*c")
        ['a', ('b', '*'), 'c']
    """
    stack = []
    ops = []


class RegexBuilder:
    """
    A class for building regular expressions using a simplified NFA representation.

    This class provides methods for constructing various components of a regular expression,
    such as epsilon, character, charset, dot, choice, concatenation, star, plus, and question.

    Usage:
    rb = RegexBuilder()
    nfa = rb.char('a')  # Create an NFA for the character 'a'
    nfa2 = rb.concat(nfa, rb.char('b'))  # Concatenate two NFAs
    """

    def __init__(self):
        """
        Initialize the RegexBuilder object.
        """
        self.statenum = 1

    def new_state(self):
        """
        Generate a new state number.

        Returns:
        int: The new state number.
        """
        self.statenum += 1
        return self.statenum

    def epsilon(self):
        """
        Create an NFA for the epsilon transition.

        Returns:
        NFA: The NFA representing the epsilon transition.
        """
        s = self.new_state()
        e = self.new_state()
        nfa = NFA(s)
        nfa.add_transition(s, EPSILON, e)
        nfa.add_final_state(e)
        return nfa

    def char(self, label):
        """
        Create an NFA for a single character.

        Args:
        label (str): The character label.

        Returns:
        NFA: The NFA representing the character.
        """
        s = self.new_state()
        e = self.new_state()
        nfa = NFA(s)
        nfa.add_transition(s, label, e)
        nfa.add_final_state(e)
        return nfa

    def charset(self, chars):
        """
        Create an NFA for a character set.

        Args:
        chars (str): The characters in the set.

        Returns:
        NFA: The NFA representing the character set.
        """
        s = self.new_state()
        e = self.new_state()
        nfa = NFA(s)
        for char in chars:
            nfa.add_transition(s, char, e)
        nfa.add_final_state(e)
        return e

    def dot(self):
        """
        Create an NFA for the dot (matches any character).

        Returns:
        NFA: The NFA representing the dot.
        """
        s = self.new_state()
        e = self.new_state()
        nfa = NFA(s)
        nfa.add_transition(s, ANY, e)
        nfa.add_final_state(e)
        return nfa

    def choice(self, n1, n2):
        """
        Create an NFA for the choice (|) operator.

        Args:
        n1 (NFA): The first NFA.
        n2 (NFA): The second NFA.

        Returns:
        NFA: The NFA representing the choice operator.
        """
        s = self.new_state()
        s1 = self.new_state()
        s2 = self.new_state()
        e1 = self.new_state()
        e2 = self.new_state()
        e = self.new_state()
        nfa = NFA(s)
        nfa.add_transition(s, EPSILON, s1)
        nfa.add_transition(s, EPSILON, s2)
        nfa.insert(s1, n1, e1)
        nfa.insert(s2, n2, e2)
        nfa.add_transition(e1, EPSILON, e)
        nfa.add_transition(e2, EPSILON, e)
        nfa.add_final_state(e)
        return nfa

    def concat(self, n1, n2):
        """
        Create an NFA for the concatenation operator.

        Args:
        n1 (NFA): The first NFA.
        n2 (NFA): The second NFA.

        Returns:
        NFA: The NFA representing the concatenation operator.
        """
        s = self.new_state()
        m = self.new_state()
        e = self.new_state()
        nfa = NFA(s)
        nfa.insert(s, n1, m)
        nfa.insert(m, n2, e)
        nfa.add_final_state(e)
        return nfa

    def star(self, n):
        """
        Create an NFA for the Kleene star (*) operator.

        Args:
        n (NFA): The NFA to apply the star operator to.

        Returns:
        NFA: The NFA representing the star operator.
        """
        s = self.new_state()
        m1 = self.new_state()
        m2 = self.new_state()
        e = self.new_state()
        nfa = NFA(s)
        nfa.add_transition(s, EPSILON, m1)
        nfa.add_transition(s, EPSILON, e)
        nfa.insert(m1, n, m2)
        nfa.add_transition(m2, EPSILON, m1)
        nfa.add_transition(m2, EPSILON, e)
        nfa.add_final_state(e)
        return nfa

    def plus(self, n):
        """
        Create an NFA for the plus (+) operator.

        Args:
        n (NFA): The NFA to apply the plus operator to.

        Returns:
        NFA: The NFA representing the plus operator.
        """
        return self.concat(n, self.star(n))

    def question(self, n):
        """
        Create an NFA for the question mark (?) operator.

        Args:
        n (NFA): The NFA to apply the question mark operator to.

        Returns:
        NFA: The NFA representing the question mark operator.
        """
        return self.choice(n, self.epsilon())
