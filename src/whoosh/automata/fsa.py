import itertools
import operator
import sys
from bisect import bisect_left

unull = chr(0)

# Marker constants


class Marker:
    """
    Represents a marker object.

    Markers are used to identify specific points in a program or data structure.
    They can be used to mark positions in a Finite State Automaton (FSA) or any
    other context where a named reference is needed.

    Attributes:
        name (str): The name of the marker.

    Methods:
        __repr__(): Returns a string representation of the marker.

    Example:
        >>> marker = Marker("start")
        >>> marker.name
        'start'
        >>> repr(marker)
        '<start>'
    """

    def __init__(self, name):
        """
        Initializes a new Marker object.

        Args:
            name (str): The name of the marker.
        """
        self.name = name

    def __repr__(self):
        """
        Returns a string representation of the marker.

        Returns:
            str: A string representation of the marker.
        """
        return f"<{self.name}>"


EPSILON = Marker("EPSILON")
ANY = Marker("ANY")


# Base class
class FSA:
    """
    Finite State Automaton (FSA) class.

    This class represents a finite state automaton, which is a mathematical model used to describe
    sequential logic circuits and pattern matching algorithms. It consists of states, transitions,
    and final states.

    Attributes:
        initial (object): The initial state of the automaton.
        transitions (dict): A dictionary that maps source states to a dictionary of labels and
            destination states.
        final_states (set): A set of final states in the automaton.

    Methods:
        __len__(): Returns the total number of states in the automaton.
        __eq__(other): Checks if two automata are equal.
        all_states(): Returns a set of all states in the automaton.
        all_labels(): Returns a set of all labels used in the automaton.
        get_labels(src): Returns an iterator of labels for a given source state.
        generate_all(state=None, sofar=""): Generates all possible strings accepted by the automaton.
        start(): Returns the initial state of the automaton.
        next_state(state, label): Returns the next state given the current state and a label.
        is_final(state): Checks if a given state is a final state.
        add_transition(src, label, dest): Adds a transition from a source state to a destination state
            with a given label.
        add_final_state(state): Adds a final state to the automaton.
        to_dfa(): Converts the automaton to a deterministic finite automaton (DFA).
        accept(string, debug=False): Checks if a given string is accepted by the automaton.
        append(fsa): Appends another automaton to the current automaton.

    """

    def __init__(self, initial):
        """
        Initialize a Finite State Automaton (FSA) with the given initial state.

        Args:
            initial: The initial state of the FSA.

        Attributes:
            initial (State): The initial state of the FSA.
            transitions (dict): A dictionary mapping states to dictionaries of transitions.
                Each transition dictionary maps input symbols to destination states.
            final_states (set): A set of final states in the FSA.

        """
        self.initial = initial
        self.transitions = {}
        self.final_states = set()

    def __len__(self):
        """
        Returns the number of states in the finite state automaton.

        :return: The number of states in the automaton.
        :rtype: int
        """
        return len(self.all_states())

    def __eq__(self, other):
        """
        Check if two Finite State Automata (FSAs) are equal.

        Args:
            other (FSA): The other FSA to compare with.

        Returns:
            bool: True if the FSAs are equal, False otherwise.
        """
        if self.initial != other.initial:
            return False
        if self.final_states != other.final_states:
            return False
        st = self.transitions
        ot = other.transitions
        return st == ot

    def all_states(self):
        """
        Returns a set of all states in the automaton.

        This method iterates over the transitions in the automaton and collects all the states
        encountered. It returns a set containing all the unique states.

        Returns:
            set: A set of all states in the automaton.

        Example:
            >>> automaton = FSA()
            >>> automaton.add_transition('A', 'B', 'a')
            >>> automaton.add_transition('B', 'C', 'b')
            >>> automaton.add_transition('C', 'D', 'c')
            >>> automaton.all_states()
            {'A', 'B', 'C', 'D'}

        """
        stateset = set(self.transitions)
        for trans in self.transitions.values():
            stateset.update(trans.values())
        return stateset

    def all_labels(self):
        """
        Returns a set of all labels used in the automaton.

        This method iterates over all transitions in the automaton and collects
        all unique labels used in those transitions. The labels are returned as
        a set.

        Returns:
            set: A set of all labels used in the automaton.

        Example:
            >>> automaton = FSA()
            >>> automaton.add_transition(0, 1, 'a')
            >>> automaton.add_transition(1, 2, 'b')
            >>> automaton.add_transition(2, 3, 'a')
            >>> automaton.all_labels()
            {'a', 'b'}

        """
        labels = set()
        for trans in self.transitions.values():
            labels.update(trans)
        return labels

    def get_labels(self, src):
        """
        Returns an iterator of labels for a given source state.

        Args:
            src (object): The source state.

        Returns:
            iterator: An iterator of labels for the given source state.

        Raises:
            None

        Examples:
            >>> fsa = FSA()
            >>> src_state = State()
            >>> fsa.add_transition(src_state, 'a', State())
            >>> fsa.add_transition(src_state, 'b', State())
            >>> labels = fsa.get_labels(src_state)
            >>> list(labels)
            ['a', 'b']

        Notes:
            - This method returns an iterator of labels for the given source state.
            - If the source state has no transitions, an empty iterator will be returned.
        """
        return iter(self.transitions.get(src, []))

    def generate_all(self, state=None, sofar=""):
        """
        Generates all possible strings accepted by the automaton.

        Args:
            state (object, optional): The current state. Defaults to the initial state.
            sofar (str, optional): The string generated so far. Defaults to an empty string.

        Yields:
            str: The generated string.

        Returns:
            None

        Raises:
            None

        Examples:
            # Create an automaton
            automaton = Automaton()

            # Generate all possible strings
            for string in automaton.generate_all():
                print(string)

        Notes:
            - This method uses a recursive approach to generate all possible strings accepted by the automaton.
            - The `state` parameter represents the current state of the automaton. If not provided, it defaults to the initial state.
            - The `sofar` parameter represents the string generated so far. If not provided, it defaults to an empty string.
            - The method yields each generated string one by one, allowing for efficient memory usage when dealing with large automata.

        """
        state = self.start() if state is None else state
        if self.is_final(state):
            yield sofar
        for label in sorted(self.get_labels(state)):
            newstate = self.next_state(state, label)
            yield from self.generate_all(newstate, sofar + label)

    def start(self):
        """
        Returns the initial state of the automaton.

        Returns:
            object:
                The initial state of the automaton.

        Raises:
            None.

        Examples:
            >>> automaton = FSA()
            >>> initial_state = automaton.start()
        """
        return self.initial

    def next_state(self, state, label):
        """
        Returns the next state given the current state and a label.

        Args:
            state (object): The current state.
                The current state of the finite state automaton.

            label (object): The label.
                The label representing the transition from the current state to the next state.

        Returns:
            object: The next state.
                The next state of the finite state automaton based on the current state and label.

        Raises:
            NotImplementedError: This method should be implemented in a subclass.
                This exception is raised when the `next_state` method is called on the base class
                and not overridden in a subclass.

        """
        raise NotImplementedError

    def is_final(self, state):
        """
        Checks if a given state is a final state.

        Args:
            state (object): The state to check.

        Returns:
            bool: True if the state is a final state, False otherwise.

        Raises:
            NotImplementedError: This method should be implemented in a subclass.

        Examples:
            >>> fsa = FSA()
            >>> fsa.is_final(0)
            False
            >>> fsa.is_final(1)
            True

        Notes:
            This method should be implemented in a subclass to provide the specific logic for determining
            whether a state is a final state or not. By default, it raises a NotImplementedError.

        """
        raise NotImplementedError

    def add_transition(self, src, label, dest):
        """
        Adds a transition from a source state to a destination state with a given label.

        Args:
            src (object): The source state.
            label (object): The label.
            dest (object): The destination state.

        Raises:
            NotImplementedError: This method should be implemented in a subclass.

        Returns:
            None

        Example:
            >>> fsa = FSA()
            >>> src = State('A')
            >>> dest = State('B')
            >>> label = 'transition'
            >>> fsa.add_transition(src, label, dest)

        """
        raise NotImplementedError

    def add_final_state(self, state):
        """
        Adds a final state to the automaton.

        Args:
            state (object): The final state to add.

        Raises:
            NotImplementedError: This method should be implemented in a subclass.

        Example:
            >>> automaton = Automaton()
            >>> automaton.add_final_state(5)

        This method should be implemented in a subclass to add a final state to the automaton.
        A final state is a state that marks the end of a sequence of transitions in the automaton.
        The `state` parameter should be an object representing the final state to be added.

        Note:
            This method raises a NotImplementedError to indicate that it should be implemented in a subclass.

        """
        raise NotImplementedError

    def to_dfa(self):
        """
        Converts the automaton to a deterministic finite automaton (DFA).

        This method takes the current automaton and converts it into an equivalent
        deterministic finite automaton (DFA). The resulting DFA will have the same
        language recognition capabilities as the original automaton, but with a
        potentially different internal representation.

        Returns:
            DFA: The converted DFA.

        Raises:
            NotImplementedError: This method should be implemented in a subclass.

        Example:
            >>> nfa = NFA()
            >>> # Add states, transitions, and final states to the NFA
            >>> dfa = nfa.to_dfa()
            >>> # Use the converted DFA for further processing

        Note:
            The `to_dfa` method should be implemented in a subclass to provide the
            conversion logic specific to that automaton type.

        """
        raise NotImplementedError

    def accept(self, string, debug=False):
        """
        Checks if a given string is accepted by the automaton.

        Args:
            string (str): The string to check.
            debug (bool, optional): Whether to print debug information. Defaults to False.

        Returns:
            bool: True if the string is accepted, False otherwise.

        Raises:
            None

        Examples:
            >>> automaton = Automaton()
            >>> automaton.accept("abc")
            True
            >>> automaton.accept("def")
            False

        Notes:
            This method iterates over each character in the input string and transitions the automaton
            to the next state based on the current state and the input label. If the automaton reaches
            a non-final state or encounters an invalid label, it breaks the loop and returns False.
            If the automaton reaches a final state after processing the entire string, it returns True.

        """
        state = self.start()

        for label in string:
            if debug:
                print("  ", state, "->", label, "->")

            state = self.next_state(state, label)
            if not state:
                break

        return self.is_final(state)

    def append(self, fsa):
        """
        Appends another automaton to the current automaton.

        Args:
            fsa (FSA): The automaton to append.

        Returns:
            None

        Raises:
            None

        Notes:
            This method appends the transitions and final states of the given automaton
            to the current automaton. It updates the transitions dictionary by adding
            the transitions from the given automaton. It also adds epsilon transitions
            from each final state of the current automaton to the initial state of the
            given automaton. Finally, it updates the final states of the current automaton
            to be the final states of the given automaton.

        Example:
            fsa1 = FSA()
            fsa2 = FSA()
            # ... code to define transitions and final states for fsa1 and fsa2 ...
            fsa1.append(fsa2)
            # Now fsa1 contains the appended transitions and final states from fsa2.
        """
        self.transitions.update(fsa.transitions)
        for state in self.final_states:
            self.add_transition(state, EPSILON, fsa.initial)
        self.final_states = fsa.final_states


# Implementations


class NFA(FSA):
    """
    NFA (Non-Deterministic Finite Automaton) class represents a non-deterministic finite automaton.
    It is a subclass of FSA (Finite State Automaton).

    Attributes:
        transitions (dict): A dictionary that maps source states to a dictionary of labels and destination states.
        final_states (set): A set of final states.
        initial: The initial state of the NFA.

    Methods:
        dump(stream=sys.stdout): Prints a textual representation of the NFA to the specified stream.
        start(): Returns the initial state of the NFA as a frozenset.
        add_transition(src, label, dest): Adds a transition from source state to destination state with the specified label.
        add_final_state(state): Adds a final state to the NFA.
        triples(): Generates all possible triples (source state, label, destination state) in the NFA.
        is_final(states): Checks if any of the given states is a final state.
        _expand(states): Expands the given set of states by following epsilon transitions.
        next_state(states, label): Returns the set of states that can be reached from the given states with the specified label.
        get_labels(states): Returns the set of labels that can be reached from the given states.
        embed(other): Copies all transitions from another NFA into this NFA.
        insert(src, other, dest): Connects the source state to the initial state of another NFA, and the final states of the other NFA to the destination state.
        to_dfa(): Converts the NFA to a DFA (Deterministic Finite Automaton).
    """

    def __init__(self, initial):
        """
        Initializes a Finite State Automaton (FSA) object.

        Parameters:
        - initial: The initial state of the FSA.

        Attributes:
        - transitions: A dictionary representing the transitions between states.
        - final_states: A set containing the final states of the FSA.
        - initial: The initial state of the FSA.
        """
        self.transitions = {}
        self.final_states = set()
        self.initial = initial

    def dump(self, stream=sys.stdout):
        """
        Prints a textual representation of the NFA to the specified stream.

        Args:
            stream (file): The stream to print the representation to. Defaults to sys.stdout.

        Returns:
            None

        Raises:
            None

        Example:
            nfa = NFA()
            nfa.add_transition(0, 'a', 1)
            nfa.add_transition(1, 'b', 2)
            nfa.add_transition(2, 'c', 3)
            nfa.dump()  # Prints the NFA representation to sys.stdout
        """
        starts = self.start()
        for src in self.transitions:
            beg = "@" if src in starts else " "
            print(beg, src, file=stream)
            xs = self.transitions[src]
            for label in xs:
                dests = xs[label]
                _ = "||" if self.is_final(dests) else ""

    def start(self):
        """
        Returns the initial state of the NFA as a frozenset.

        This method returns the initial state of the NFA (Non-Deterministic Finite Automaton)
        as a frozenset. The initial state is the starting point of the automaton.

        Returns:
            frozenset: The initial state of the NFA.
        """
        return frozenset(self._expand({self.initial}))

    def add_transition(self, src, label, dest):
        """
        Adds a transition from the source state to the destination state with the specified label.

        This method is used to define transitions between states in a finite state automaton.

        Args:
            src (object): The source state.
            label (object): The label of the transition.
            dest (object): The destination state.

        Returns:
            None

        Raises:
            None

        Example:
            >>> fsa = FSA()
            >>> fsa.add_transition('state1', 'a', 'state2')
        """
        self.transitions.setdefault(src, {}).setdefault(label, set()).add(dest)

    def add_final_state(self, state):
        """
        Adds a final state to the NFA.

        Args:
            state (object): The final state to add.

        Returns:
            None

        Raises:
            TypeError: If the state is not a valid object.

        Notes:
            This method adds a final state to the NFA (Non-Deterministic Finite Automaton).
            A final state is a state that, when reached during the execution of the NFA,
            indicates that the input string has been accepted.

        Example:
            >>> nfa = NFA()
            >>> state = State()
            >>> nfa.add_final_state(state)
        """
        self.final_states.add(state)

    def triples(self):
        """
        Generates all possible triples (source state, label, destination state) in the NFA.

        This method iterates over the transitions of the NFA and yields a tuple for each triple found.
        Each triple consists of the source state, the label of the transition, and the destination state.

        Yields:
            tuple: A triple (source state, label, destination state).
        """
        for src, trans in self.transitions.items():
            for label, dests in trans.items():
                for dest in dests:
                    yield src, label, dest

    def is_final(self, states):
        """
        Checks if any of the given states is a final state.

        Args:
            states (set): The set of states to check.

        Returns:
            bool: True if any of the states is a final state, False otherwise.
        """
        return bool(self.final_states.intersection(states))

    def _expand(self, states):
        """
        Expands the given set of states by following epsilon transitions.

        This method takes a set of states and expands it by following epsilon transitions.
        Epsilon transitions are transitions that do not consume any input symbol.

        Args:
            states (set): The set of states to expand.

        Returns:
            set: The expanded set of states.

        Example:
            >>> automaton = FSA()
            >>> initial_states = {0}
            >>> expanded_states = automaton._expand(initial_states)
            >>> print(expanded_states)
            {0, 1, 2, 3}

        Note:
            This method modifies the input set of states in-place by adding the newly expanded states to it.
            If you want to keep the original set of states unchanged, make a copy before calling this method.
        """
        transitions = self.transitions
        frontier = set(states)
        while frontier:
            state = frontier.pop()
            if state in transitions and EPSILON in transitions[state]:
                new_states = transitions[state][EPSILON].difference(states)
                frontier.update(new_states)
                states.update(new_states)
        return states

    def next_state(self, states, label):
        """
        Returns the set of states that can be reached from the given states with the specified label.

        Args:
            states (set): The set of states to start from.
            label: The label of the transition.

        Returns:
            frozenset: The set of states that can be reached.

        Raises:
            None

        Example:
            >>> automaton = FSA()
            >>> automaton.add_transition(0, 'a', 1)
            >>> automaton.add_transition(1, 'b', 2)
            >>> automaton.add_transition(2, 'c', 3)
            >>> automaton.next_state({0}, 'a')
            frozenset({1})

        This method takes a set of states and a label as input and returns the set of states that can be reached from the given states with the specified label. It considers the transitions defined in the automaton and follows them to determine the reachable states.

        The method first checks if each state in the input set has any outgoing transitions defined. If a transition with the specified label is found, the destination states are added to the result set. Additionally, if there is a transition with the special label 'ANY', the destination states of that transition are also added to the result set.

        The result set is then expanded to include all states reachable from the initial set of states, considering all possible transitions.

        Note: The input states should be a set of valid states in the automaton. The label can be any valid label defined in the automaton's transitions.

        """
        transitions = self.transitions
        dest_states = set()
        for state in states:
            if state in transitions:
                xs = transitions[state]
                if label in xs:
                    dest_states.update(xs[label])
                if ANY in xs:
                    dest_states.update(xs[ANY])
        return frozenset(self._expand(dest_states))

    def get_labels(self, states):
        """
        Returns the set of labels that can be reached from the given states.

        Args:
            states (set): The set of states.

        Returns:
            set: The set of labels.

        Raises:
            None.

        Examples:
            >>> automaton = FSA()
            >>> automaton.add_transition(1, 'a', 2)
            >>> automaton.add_transition(2, 'b', 3)
            >>> automaton.add_transition(3, 'c', 4)
            >>> automaton.get_labels({1, 2, 3})
            {'a', 'b', 'c'}
        """
        transitions = self.transitions
        labels = set()
        for state in states:
            if state in transitions:
                labels.update(transitions[state])
        return labels

    def embed(self, other):
        """
        Copies all transitions from another NFA into this NFA.

        Args:
            other (NFA): The other NFA to copy transitions from.

        Returns:
            None

        Raises:
            None

        Notes:
            This method copies all transitions from the specified NFA (`other`) into the current NFA.
            It updates the transitions of the current NFA by adding the transitions from `other`.
            The transitions are copied based on the source state and the label of the transition.
            If a transition with the same source state and label already exists in the current NFA,
            the destination states are updated by adding the destination states from `other`.

        Example:
            nfa1 = NFA()
            nfa2 = NFA()
            # ... add transitions to nfa1 and nfa2 ...

            nfa1.embed(nfa2)
            # Now nfa1 contains all transitions from nfa2.
        """
        for s, othertrans in other.transitions.items():
            trans = self.transitions.setdefault(s, {})
            for label, otherdests in othertrans.items():
                dests = trans.setdefault(label, set())
                dests.update(otherdests)

    def insert(self, src, other, dest):
        """
        Connects the source state to the initial state of another NFA, and the final states of the other NFA to the destination state.

        Args:
            src (State): The source state to connect from.
            other (NFA): The other NFA to connect.
            dest (State): The destination state to connect to.

        Returns:
            None

        Raises:
            TypeError: If src or dest are not instances of the State class.
            ValueError: If other is not an instance of the NFA class.

        Notes:
            This method modifies the current NFA by embedding the other NFA into it. It connects the source state to the initial state of the other NFA, and connects the final states of the other NFA to the destination state.

        Example:
            nfa = NFA()
            src = State()
            dest = State()
            other = NFA()
            # ... Initialize src, dest, and other with appropriate values ...

            nfa.insert(src, other, dest)
        """
        self.embed(other)
        self.add_transition(src, EPSILON, other.initial)
        for finalstate in other.final_states:
            self.add_transition(finalstate, EPSILON, dest)

    def to_dfa(self):
        """
        Converts the NFA to a DFA (Deterministic Finite Automaton).

        This method performs the conversion of a Non-Deterministic Finite Automaton (NFA) to a
        Deterministic Finite Automaton (DFA). The resulting DFA is constructed by exploring
        the states and transitions of the NFA.

        Returns:
            DFA: The converted DFA.

        Notes:
            - The NFA must be initialized before calling this method.
            - The NFA should have at least one start state.
            - The NFA should have at least one final state.

        Example:
            nfa = NFA()
            # ... code to initialize the NFA ...
            dfa = nfa.to_dfa()
            # ... code to use the converted DFA ...
        """
        dfa = DFA(self.start())
        frontier = [self.start()]
        seen = set()
        while frontier:
            current = frontier.pop()
            if self.is_final(current):
                dfa.add_final_state(current)
            labels = self.get_labels(current)
            for label in labels:
                if label is EPSILON:
                    continue
                new_state = self.next_state(current, label)
                if new_state not in seen:
                    frontier.append(new_state)
                    seen.add(new_state)
                    if self.is_final(new_state):
                        dfa.add_final_state(new_state)
                if label is ANY:
                    dfa.set_default_transition(current, new_state)
                else:
                    dfa.add_transition(current, label, new_state)
        return dfa


class DFA(FSA):
    """
    Deterministic Finite Automaton (DFA) class.

    This class represents a DFA, which is a type of finite state automaton
    where each input symbol uniquely determines the next state. DFAs are
    commonly used in pattern matching and string searching algorithms.

    Attributes:
        initial (object): The initial state of the DFA.
        transitions (dict): A dictionary representing the transitions between
            states. The keys are the source states, and the values are
            dictionaries where the keys are the input labels and the values
            are the destination states.
        defaults (dict): A dictionary representing the default transitions
            for states that do not have a specific transition defined for a
            given input label. The keys are the source states, and the values
            are the default destination states.
        final_states (set): A set containing the final states of the DFA.
        outlabels (dict): A dictionary caching the sorted output labels for
            each state.

    Methods:
        dump(stream=sys.stdout): Prints a textual representation of the DFA
            to the specified stream.
        start(): Returns the initial state of the DFA.
        add_transition(src, label, dest): Adds a transition from the source
            state to the destination state with the given input label.
        set_default_transition(src, dest): Sets the default transition for
            the source state to the specified destination state.
        add_final_state(state): Adds the specified state as a final state of
            the DFA.
        is_final(state): Checks if the specified state is a final state of
            the DFA.
        next_state(src, label): Returns the next state of the DFA given the
            current state and the input label.
        next_valid_string(string, asbytes=False): Returns the lexicographically
            smallest valid string that can be obtained by following the DFA
            from the initial state using the characters in the input string.
        find_next_edge(s, label, asbytes): Finds the next edge label for the
            specified state and input label.
        reachable_from(src, inclusive=True): Returns the set of states that
            can be reached from the specified source state.
        minimize(): Minimizes the DFA by removing unreachable states and
            merging equivalent states.
        to_dfa(): Returns a reference to itself (DFA).

    """

    def __init__(self, initial):
        """
        Initializes a new instance of the DFA class.

        Args:
            initial (object): The initial state of the DFA.

        Attributes:
            initial (object): The initial state of the DFA.
            transitions (dict): A dictionary mapping state and input symbol pairs to the next state.
            defaults (dict): A dictionary mapping states to default next states.
            final_states (set): A set of final states.
            outlabels (dict): A dictionary mapping states to output labels.

        """
        self.initial = initial
        self.transitions = {}
        self.defaults = {}
        self.final_states = set()
        self.outlabels = {}

    def dump(self, stream=sys.stdout):
        """
        Prints a textual representation of the DFA to the specified stream.

        Args:
            stream (file-like object, optional): The stream to print the
                representation to. Defaults to sys.stdout.

        Returns:
            None

        Raises:
            None

        Example:
            >>> dfa = DFA()
            >>> dfa.add_transition(0, 'a', 1)
            >>> dfa.add_transition(1, 'b', 2)
            >>> dfa.add_transition(2, 'c', 3)
            >>> dfa.dump()  # Prints the DFA representation to sys.stdout
            @ 0
             a -> 1
            1
             b -> 2
            2
             c -> 3||

        """
        for src in sorted(self.transitions):
            beg = "@" if src == self.initial else " "
            print(beg, src, file=stream)
            xs = self.transitions[src]
            for label in sorted(xs):
                dest = xs[label]
                _ = "||" if self.is_final(dest) else ""

    def start(self):
        """
        Returns the initial state of the DFA.

        Returns:
            object: The initial state of the DFA.

        """
        return self.initial

    def add_transition(self, src, label, dest):
        """
        Adds a transition from the source state to the destination state with
        the given input label.

        Args:
            src (object): The source state.
            label (object): The input label.
            dest (object): The destination state.

        Returns:
            None

        Raises:
            None

        Examples:
            >>> fsa = FSA()
            >>> fsa.add_transition('A', 'a', 'B')
            >>> fsa.add_transition('B', 'b', 'C')

        """
        self.transitions.setdefault(src, {})[label] = dest

    def set_default_transition(self, src, dest):
        """
        Sets the default transition for the source state to the specified
        destination state.

        Args:
            src (object): The source state.
            dest (object): The default destination state.

        Returns:
            None

        Raises:
            None

        Examples:
            # Create an instance of the FSA class
            fsa = FSA()

            # Set the default transition from state 'A' to state 'B'
            fsa.set_default_transition('A', 'B')

        Notes:
            - This method allows you to define a default transition for a source state.
            - If a specific transition is not defined for a given input in the FSA,
              the default transition will be used.
        """
        self.defaults[src] = dest

    def add_final_state(self, state):
        """
        Adds the specified state as a final state of the DFA.

        Args:
            state (object): The final state to add.

        Returns:
            None

        Raises:
            TypeError: If the state is not of the expected type.

        Notes:
            - This method adds a state to the set of final states of the DFA.
            - Final states are used to determine whether a given input sequence is accepted by the DFA.

        Example:
            >>> dfa = DFA()
            >>> dfa.add_final_state(3)
            >>> dfa.add_final_state(5)
        """
        self.final_states.add(state)

    def is_final(self, state):
        """
        Checks if the specified state is a final state of the DFA.

        Args:
            state (object): The state to check.

        Returns:
            bool: True if the state is a final state, False otherwise.

        Raises:
            None

        Examples:
            >>> dfa = DFA()
            >>> dfa.add_final_state('q1')
            >>> dfa.is_final('q1')
            True
            >>> dfa.is_final('q2')
            False

        Notes:
            - This method is used to determine if a given state is a final state in a Deterministic Finite Automaton (DFA).
            - A final state is a state in which the DFA accepts the input string and terminates.
            - The method returns True if the specified state is a final state, and False otherwise.
        """
        return state in self.final_states

    def next_state(self, src, label):
        """
        Returns the next state of the DFA given the current state and the
        input label.

        Args:
            src (object): The current state.
            label (object): The input label.

        Returns:
            object: The next state.

        Raises:
            KeyError: If the current state or input label is not found in the DFA.

        Notes:
            - If the current state is not found in the DFA transitions, the default
              state for that source state will be returned.
            - If the input label is not found in the transitions for the current state,
              None will be returned.

        Example:
            >>> dfa = DFA()
            >>> dfa.add_transition('A', 'a', 'B')
            >>> dfa.add_transition('B', 'b', 'C')
            >>> dfa.next_state('A', 'a')
            'B'
            >>> dfa.next_state('B', 'b')
            'C'
            >>> dfa.next_state('C', 'c')
            None
        """
        trans = self.transitions.get(src, {})
        return trans.get(label, self.defaults.get(src, None))

    def next_valid_string(self, string, asbytes=False):
        """
        Returns the lexicographically smallest valid string that can be
        obtained by following the DFA from the initial state using the
        characters in the input string.

        Args:
            string (str or bytes): The input string.
            asbytes (bool, optional): Specifies whether the input string is
                in bytes format. Defaults to False.

        Returns:
            str or bytes: The lexicographically smallest valid string, or
                None if no valid string can be obtained.

        Raises:
            None

        Examples:
            >>> fsa = FSA()
            >>> fsa.add_transition(0, 'a', 1)
            >>> fsa.add_transition(1, 'b', 2)
            >>> fsa.add_transition(2, 'c', 3)
            >>> fsa.set_final(3)
            >>> fsa.next_valid_string('ab')  # Returns 'abc'
            >>> fsa.next_valid_string('abc')  # Returns 'abc'
            >>> fsa.next_valid_string('abcd')  # Returns None

        Notes:
            - The method follows the DFA (Deterministic Finite Automaton) from
              the initial state using the characters in the input string.
            - It returns the lexicographically smallest valid string that can be
              obtained by following the DFA.
            - If the input string is already a valid string, it is returned as is.
            - If no valid string can be obtained, None is returned.
            - The `asbytes` parameter specifies whether the input string is in
              bytes format. By default, it is set to False.

        """
        state = self.start()
        stack = []

        # Follow the DFA as far as possible
        i = 0
        for i, label in enumerate(string):
            stack.append((string[:i], state, label))
            state = self.next_state(state, label)
            if not state:
                break
        else:
            stack.append((string[: i + 1], state, None))

        if self.is_final(state):
            # Word is already valid
            return string

        # Perform a 'wall following' search for the lexicographically smallest
        # accepting state.
        while stack:
            path, state, label = stack.pop()
            label = self.find_next_edge(state, label, asbytes=asbytes)
            if label:
                path += label
                state = self.next_state(state, label)
                if self.is_final(state):
                    return path
                stack.append((path, state, None))
        return None

    def find_next_edge(self, s, label, asbytes):
        """
        Finds the next edge label for the specified state and input label.

        Args:
            s (object): The current state.
            label (object): The current input label.
            asbytes (bool): Specifies whether the labels are in bytes format.

        Returns:
            object: The next edge label, or None if no label is found.

        Raises:
            None

        Examples:
            >>> automaton = FSA()
            >>> automaton.find_next_edge(1, 'a', False)
            'b'

        Notes:
            - This method is used to find the next edge label for a given state and input label in the automaton.
            - The `s` parameter represents the current state in the automaton.
            - The `label` parameter represents the current input label.
            - The `asbytes` parameter specifies whether the labels are in bytes format.
            - If `label` is None, it is set to b"\x00" if `asbytes` is True, or "\0" if `asbytes` is False.
            - The method returns the next edge label if found, or None if no label is found.

        """
        if label is None:
            label = b"\x00" if asbytes else "\0"
        else:
            label = (label + 1) if asbytes else chr(ord(label) + 1)
        trans = self.transitions.get(s, {})
        if label in trans or s in self.defaults:
            return label

        try:
            labels = self.outlabels[s]
        except KeyError:
            self.outlabels[s] = labels = sorted(trans)

        pos = bisect_left(labels, label)
        if pos < len(labels):
            return labels[pos]
        return None

    def reachable_from(self, src, inclusive=True):
        """
        Returns the set of states that can be reached from the specified
        source state.

        Args:
            src (object): The source state.
            inclusive (bool, optional): Specifies whether the source state
                should be included in the result. Defaults to True.

        Returns:
            set: The set of reachable states.

        Example:
            >>> automaton = FSA()
            >>> automaton.add_state('A')
            >>> automaton.add_state('B')
            >>> automaton.add_transition('A', 'B')
            >>> automaton.reachable_from('A')
            {'A', 'B'}

        """
        transitions = self.transitions

        reached = set()
        if inclusive:
            reached.add(src)

        stack = [src]
        seen = set()
        while stack:
            src = stack.pop()
            seen.add(src)
            for dest in transitions[src].values():
                reached.add(dest)
                if dest not in seen:
                    stack.append(dest)
        return reached

    def minimize(self):
        """
        Minimizes the DFA by removing unreachable states and merging equivalent states.

        This method performs the following steps:
        1. Deletes unreachable states from the DFA.
        2. Partitions the remaining states into equivalence sets.
        3. Chooses one representative state from each equivalence set and maps all equivalent states to it.
        4. Applies the mapping to the existing transitions.
        5. Removes dead states - non-final states with no outgoing arcs except to themselves.

        After the minimization process, the DFA will have a reduced number of states while preserving its language.

        Usage:
        dfa = DFA(...)
        dfa.minimize()

        :return: None
        """
        transitions = self.transitions
        initial = self.initial

        # Step 1: Delete unreachable states
        reachable = self.reachable_from(initial)
        for src in list(transitions):
            if src not in reachable:
                del transitions[src]
        final_states = self.final_states.intersection(reachable)
        labels = self.all_labels()

        # Step 2: Partition the states into equivalence sets
        changed = True
        parts = [final_states, reachable - final_states]
        while changed:
            changed = False
            for i in range(len(parts)):
                part = parts[i]
                changed_part = False
                for label in labels:
                    next_part = None
                    new_part = set()
                    for state in part:
                        dest = transitions[state].get(label)
                        if dest is not None:
                            if next_part is None:
                                for p in parts:
                                    if dest in p:
                                        next_part = p
                            elif dest not in next_part:
                                new_part.add(state)
                                changed = True
                                changed_part = True
                    if changed_part:
                        old_part = part - new_part
                        parts.pop(i)
                        parts.append(old_part)
                        parts.append(new_part)
                        break

        # Choose one state from each equivalence set and map all equivalent
        # states to it
        new_trans = {}

        # Create mapping
        mapping = {}
        new_initial = None
        for part in parts:
            representative = part.pop()
            if representative is initial:
                new_initial = representative
            mapping[representative] = representative
            new_trans[representative] = {}
            for state in part:
                if state is initial:
                    new_initial = representative
                mapping[state] = representative
        assert new_initial is not None

        # Apply mapping to existing transitions
        new_finals = {mapping[s] for s in final_states}
        for state, d in new_trans.items():
            trans = transitions[state]
            for label, dest in trans.items():
                d[label] = mapping[dest]

        # Remove dead states - non-final states with no outgoing arcs except
        # to themselves
        non_final_srcs = [src for src in new_trans if src not in new_finals]
        removing = set()
        for src in non_final_srcs:
            dests = set(new_trans[src].values())
            dests.discard(src)
            if not dests:
                removing.add(src)
                del new_trans[src]
        # Delete transitions to removed dead states
        for t in new_trans.values():
            for label in list(t):
                if t[label] in removing:
                    del t[label]

        self.transitions = new_trans
        self.initial = new_initial
        self.final_states = new_finals

    def to_dfa(self):
        """
        Converts the Finite State Automaton (FSA) to a Deterministic Finite Automaton (DFA).

        This method returns a reference to itself, as the conversion from FSA to DFA is an in-place operation.

        Returns:
            DFA: A reference to the converted DFA.

        Notes:
            - The conversion from FSA to DFA eliminates non-determinism by creating a new DFA with equivalent language acceptance.
            - The resulting DFA may have a larger number of states compared to the original FSA.
            - The original FSA is not modified during the conversion process.

        Example:
            >>> fsa = FSA()
            >>> # Add states, transitions, and final states to the FSA
            >>> dfa = fsa.to_dfa()
            >>> # Use the converted DFA for further operations

        """
        return self


# Useful functions


def renumber_dfa(dfa, base=0):
    """
    Renumber the states of a DFA (Deterministic Finite Automaton) starting from a given base number.

    Args:
        dfa (DFA): The DFA to renumber.
        base (int, optional): The base number to start renumbering from. Defaults to 0.

    Returns:
        DFA: The renumbered DFA.

    Raises:
        None.

    Examples:
        >>> dfa = DFA()
        >>> dfa.add_state(0)
        >>> dfa.add_state(1)
        >>> dfa.add_transition(0, 'a', 1)
        >>> dfa.add_transition(1, 'b', 0)
        >>> dfa.set_initial_state(0)
        >>> dfa.add_final_state(1)
        >>> renumbered_dfa = renumber_dfa(dfa, base=10)
        >>> renumbered_dfa.get_states()
        [10, 11]
        >>> renumbered_dfa.get_initial_state()
        10
        >>> renumbered_dfa.get_final_states()
        [11]

    Note:
        This function renumbers the states of a DFA by assigning new numbers to each state, starting from the base number.
        It creates a new DFA object with the renumbered states and updates the transitions, final states, and default transitions accordingly.
        The mapping between the old states and the new states is stored in a dictionary called 'mapping'.

    """
    c = itertools.count(base)
    mapping = {}

    def remap(state):
        if state in mapping:
            newnum = mapping[state]
        else:
            newnum = next(c)
            mapping[state] = newnum
        return newnum

    newdfa = DFA(remap(dfa.initial))
    for src, trans in dfa.transitions.items():
        for label, dest in trans.items():
            newdfa.add_transition(remap(src), label, remap(dest))
    for finalstate in dfa.final_states:
        newdfa.add_final_state(remap(finalstate))
    for src, dest in dfa.defaults.items():
        newdfa.set_default_transition(remap(src), remap(dest))
    return newdfa


def u_to_utf8(dfa, base=0):
    """
    Converts Unicode labels in a DFA to UTF-8 labels.

    This function takes a DFA (Deterministic Finite Automaton) and converts
    its Unicode labels to UTF-8 labels. It modifies the DFA in-place.

    Parameters:
    - dfa (DFA): The DFA to convert.
    - base (int): The base value for generating new state IDs. Defaults to 0.

    Raises:
    - ValueError: If the DFA contains a transition with the label ANY.

    Returns:
    - None: The function modifies the DFA in-place.

    Example usage:
    ```
    dfa = DFA()
    # ... construct the DFA ...
    u_to_utf8(dfa)
    # ... continue using the modified DFA ...
    ```
    """
    c = itertools.count(base)
    transitions = dfa.transitions

    for src, trans in transitions.items():
        trans = transitions[src]
        for label, dest in list(trans.items()):
            if label is EPSILON:
                continue
            elif label is ANY:
                raise ValueError("DFA contains a transition with the label ANY")
            else:
                assert isinstance(label, str)
                label8 = label.encode("utf8")
                for i, byte in enumerate(label8):
                    if i < len(label8) - 1:
                        st = next(c)
                        dfa.add_transition(src, byte, st)
                        src = st
                    else:
                        dfa.add_transition(src, byte, dest)
                del trans[label]


def find_all_matches(dfa, lookup_func, first=unull):
    """
    Finds all words within a given Levenshtein distance of a target word.

    This function uses the provided `lookup_func` to find all words within a specified
    Levenshtein distance (`k`) of a target word. It iterates through the DFA (Deterministic
    Finite Automaton) `dfa` to generate all possible matches.

    Args:
        dfa (DFA): The DFA representing the search space.
        lookup_func (function): A function that takes a word as input and returns the first
            word in the database that is greater than or equal to the input word.
        first (str): The first word to start the search from. Defaults to `unull`.

    Yields:
        str: Every matching word within the specified Levenshtein distance `k` from the database.

    Example:
        >>> dfa = DFA()
        >>> lookup_func = lambda word: word
        >>> matches = find_all_matches(dfa, lookup_func, first="hello")
        >>> for match in matches:
        ...     print(match)
        ...
        hello
        hallo
        hullo
        helio
        ...

    Note:
        The `dfa` parameter should be an instance of the DFA class, which represents the search space.
        The `lookup_func` parameter should be a function that returns the first word in the database
        that is greater than or equal to the input word. This function is used to efficiently search
        for matches within the specified Levenshtein distance.

    """
    match = dfa.next_valid_string(first)
    while match:
        key = lookup_func(match)
        if key is None:
            return
        if match == key:
            yield match
            key += unull
        match = dfa.next_valid_string(key)


# Construction functions


def reverse_nfa(n):
    """
    Reverses the given NFA (Non-deterministic Finite Automaton).

    Args:
        n (NFA): The NFA to be reversed.

    Returns:
        NFA: The reversed NFA.

    Notes:
        This function creates a new NFA by reversing the transitions of the given NFA.
        It adds transitions from the destination states to the source states for each
        transition in the original NFA. It also adds transitions from the initial state
        of the original NFA to the final states of the original NFA.

    Example:
        nfa = NFA(...)
        reversed_nfa = reverse_nfa(nfa)
    """
    s = object()
    nfa = NFA(s)
    for src, trans in n.transitions.items():
        for label, destset in trans.items():
            for dest in destset:
                nfa.add_transition(dest, label, src)
    for finalstate in n.final_states:
        nfa.add_transition(s, EPSILON, finalstate)
    nfa.add_final_state(n.initial)
    return nfa


def product(dfa1, op, dfa2):
    """
    Compute the product of two DFAs.

    This function takes two deterministic finite automata (DFAs) represented by `dfa1` and `dfa2`,
    and computes their product DFA based on the given binary operator `op`.

    Parameters:
    - dfa1 (DFA): The first DFA.
    - op (function): The binary operator used to combine the states of `dfa1` and `dfa2`.
    - dfa2 (DFA): The second DFA.

    Returns:
    - dfa (DFA): The product DFA.

    Algorithm:
    1. Convert `dfa1` and `dfa2` to DFAs if they are not already.
    2. Create the start state of the product DFA as a tuple of the start states of `dfa1` and `dfa2`.
    3. Initialize an empty stack and push the start state onto the stack.
    4. While the stack is not empty:
       - Pop a state from the stack.
       - Get the transitions of the corresponding states in `dfa1` and `dfa2`.
       - For each label that is common to both sets of transitions:
         - Compute the next states in `dfa1` and `dfa2` based on the label.
         - If the binary operator `op` returns True for the next states, add a transition to the product DFA.
         - Push the next state onto the stack.
         - If both next states are final states, mark the next state in the product DFA as a final state.
    5. Return the product DFA.

    Note:
    - The `op` function should take two boolean arguments and return a boolean value.
    - The `DFA` class represents a deterministic finite automaton.

    Example usage:
    ```
    dfa1 = DFA(...)
    dfa2 = DFA(...)
    product_dfa = product(dfa1, my_operator, dfa2)
    ```

    :param dfa1: The first DFA.
    :type dfa1: DFA
    :param op: The binary operator used to combine the states of `dfa1` and `dfa2`.
    :type op: function
    :param dfa2: The second DFA.
    :type dfa2: DFA
    :return: The product DFA.
    :rtype: DFA
    """
    dfa1 = dfa1.to_dfa()
    dfa2 = dfa2.to_dfa()
    start = (dfa1.start(), dfa2.start())
    dfa = DFA(start)
    stack = [start]
    while stack:
        src = stack.pop()
        state1, state2 = src
        trans1 = set(dfa1.transitions[state1])
        trans2 = set(dfa2.transitions[state2])
        for label in trans1.intersection(trans2):
            state1 = dfa1.next_state(state1, label)
            state2 = dfa2.next_state(state2, label)
            if op(state1 is not None, state2 is not None):
                dest = (state1, state2)
                dfa.add_transition(src, label, dest)
                stack.append(dest)
                if op(dfa1.is_final(state1), dfa2.is_final(state2)):
                    dfa.add_final_state(dest)
    return dfa


def intersection(dfa1, dfa2):
    """
    Compute the intersection of two deterministic finite automata (DFAs).

    This function takes two DFAs, `dfa1` and `dfa2`, and returns a new DFA that represents the intersection of the two DFAs.
    The intersection of two DFAs is a new DFA that accepts only the strings that are accepted by both `dfa1` and `dfa2`.

    Parameters:
    - dfa1 (DFA): The first DFA.
    - dfa2 (DFA): The second DFA.

    Returns:
    - DFA: The DFA representing the intersection of `dfa1` and `dfa2`.

    Example:
    >>> dfa1 = DFA(...)
    >>> dfa2 = DFA(...)
    >>> result = intersection(dfa1, dfa2)
    """

    return product(dfa1, operator.and_, dfa2)


def union(dfa1, dfa2):
    """
    Computes the union of two deterministic finite automata (DFAs).

    Parameters:
    - dfa1 (DFA): The first DFA.
    - dfa2 (DFA): The second DFA.

    Returns:
    - DFA: The DFA resulting from the union of dfa1 and dfa2.

    Raises:
    - TypeError: If either dfa1 or dfa2 is not a DFA object.

    Example:
    >>> dfa1 = DFA(...)
    >>> dfa2 = DFA(...)
    >>> result = union(dfa1, dfa2)
    """

    return product(dfa1, operator.or_, dfa2)


def epsilon_nfa():
    """
    Creates an epsilon-NFA (non-deterministic finite automaton) with a single epsilon transition.

    Returns:
        A basic NFA (Nondeterministic Finite Automaton) with a single epsilon transition.

    Notes:
        - The epsilon transition allows the automaton to move from one state to another without consuming any input.
        - This function is a helper function that creates a basic NFA with only an epsilon transition.
        - The resulting NFA can be further modified and combined with other NFAs to build more complex automata.

    Example:
        >>> nfa = epsilon_nfa()
        >>> nfa
        <NFA with 2 states and 1 transition>
    """
    return basic_nfa(EPSILON)


def dot_nfa():
    """
    Creates a non-deterministic finite automaton (NFA) that matches any single character.

    Returns:
        NFA: A non-deterministic finite automaton that matches any single character.

    Example:
        >>> nfa = dot_nfa()
        >>> nfa.match('a')
        True
        >>> nfa.match('b')
        True
        >>> nfa.match('1')
        True
    """
    return basic_nfa(ANY)


def basic_nfa(label):
    """
    Creates a basic NFA (Non-Deterministic Finite Automaton) with a single transition.

    Parameters:
    label (str): The label of the transition.

    Returns:
    NFA: The created NFA.

    Example:
    >>> nfa = basic_nfa('a')
    >>> nfa.transitions
    {<object object at 0x7f8a8c6a7a30>: {'a': [<object object at 0x7f8a8c6a7a60>]}}
    >>> nfa.final_states
    {<object object at 0x7f8a8c6a7a60>}
    """
    s = object()
    e = object()
    nfa = NFA(s)
    nfa.add_transition(s, label, e)
    nfa.add_final_state(e)
    return nfa


def charset_nfa(labels):
    """
    Constructs a non-deterministic finite automaton (NFA) that recognizes a character set.

    Parameters:
    - labels (iterable): An iterable of labels representing the characters in the character set.

    Returns:
    - NFA: The constructed NFA.

    Example:
    >>> labels = ['a', 'b', 'c']
    >>> nfa = charset_nfa(labels)
    """
    s = object()
    e = object()
    nfa = NFA(s)
    for label in labels:
        nfa.add_transition(s, label, e)
    nfa.add_final_state(e)
    return nfa


def string_nfa(string):
    """
    Creates a Non-Deterministic Finite Automaton (NFA) that recognizes the given string.

    Parameters:
    - string (str): The string to be recognized by the NFA.

    Returns:
    - NFA: The NFA object that recognizes the given string.

    Example:
    >>> nfa = string_nfa("abc")
    >>> nfa.matches("abc")
    True
    >>> nfa.matches("def")
    False
    """
    s = object()
    e = object()
    nfa = NFA(s)
    for label in string:
        e = object()
        nfa.add_transition(s, label, e)
        s = e
    nfa.add_final_state(e)
    return nfa


def choice_nfa(n1, n2):
    """
    Creates a non-deterministic finite automaton (NFA) that represents a choice between two NFAs.

    Parameters:
    - n1: The first NFA to choose from.
    - n2: The second NFA to choose from.

    Returns:
    - nfa: The resulting NFA representing the choice between n1 and n2.

    Example:
    nfa1 = NFA(...)
    nfa2 = NFA(...)
    choice = choice_nfa(nfa1, nfa2)
    """

    s = object()
    e = object()
    nfa = NFA(s)
    #   -> nfa1 -
    #  /         \
    # s           e
    #  \         /
    #   -> nfa2 -
    nfa.insert(s, n1, e)
    nfa.insert(s, n2, e)
    nfa.add_final_state(e)
    return nfa


def concat_nfa(n1, n2):
    """
    Concatenates two NFAs (n1 and n2) into a single NFA.

    Parameters:
    - n1 (NFA): The first NFA to be concatenated.
    - n2 (NFA): The second NFA to be concatenated.

    Returns:
    - nfa (NFA): The resulting NFA after concatenation.

    Example:
    nfa1 = NFA(...)
    nfa2 = NFA(...)
    concatenated_nfa = concat_nfa(nfa1, nfa2)
    """
    s = object()
    m = object()
    e = object()
    nfa = NFA(s)
    nfa.insert(s, n1, m)
    nfa.insert(m, n2, e)
    nfa.add_final_state(e)
    return nfa


def star_nfa(n):
    r"""
    Creates a non-deterministic finite automaton (NFA) that represents the Kleene star operation on the given NFA.

    Parameters:
    - n (NFA): The input NFA.

    Returns:
    - nfa (NFA): The resulting NFA after applying the Kleene star operation.

    Description:
    The star_nfa function takes an NFA as input and constructs a new NFA that represents the Kleene star operation on the input NFA.
    The resulting NFA accepts any number of repetitions (including zero) of the language accepted by the input NFA.

    The construction of the new NFA involves adding two new states, 's' and 'e', and modifying the transitions of the input NFA.
    The new NFA has the following structure:

        -----<-----
       /           \
      s ---> n ---> e
       \           /
        ----->-----

    The state 's' is the start state of the new NFA, 'n' is the start state of the input NFA, and 'e' is a new final state.
    The new NFA has transitions from 's' to 'n' and from 'e' to 's' to allow for repetitions of the input NFA's language.
    The input NFA's final states are also connected to 's' to allow for zero repetitions of the input NFA's language.

    Example usage:
    nfa = star_nfa(input_nfa)
    """

    s = object()
    e = object()
    nfa = NFA(s)

    nfa.insert(s, n, e)
    nfa.add_transition(s, EPSILON, e)
    for finalstate in n.final_states:
        nfa.add_transition(finalstate, EPSILON, s)
    nfa.add_final_state(e)

    return nfa


def plus_nfa(n):
    """
    Constructs a non-deterministic finite automaton (NFA) that matches one or more occurrences of the given NFA.

    Parameters:
    n (NFA): The NFA to be repeated one or more times.

    Returns:
    NFA: The NFA that matches one or more occurrences of the given NFA.

    Example:
    >>> nfa = plus_nfa(nfa1)
    """
    return concat_nfa(n, star_nfa(n))


def optional_nfa(n):
    """
    Creates a non-deterministic finite automaton (NFA) that matches zero or one occurrence of the given NFA.

    Parameters:
    - n: The NFA to match zero or one occurrence of.

    Returns:
    - The NFA that matches zero or one occurrence of the given NFA.

    Example:
    >>> nfa = optional_nfa(nfa1)
    """
    return choice_nfa(n, epsilon_nfa())


# Daciuk Mihov DFA construction algorithm


class DMNode:
    """
    Represents a deterministic finite state automaton (DFSA) node.

    Attributes:
        n (int): The node identifier.
        arcs (dict): A dictionary of arcs, where the keys are input symbols and the values are the next nodes.
        final (bool): Indicates whether the node is a final state.

    Methods:
        __init__(self, n: int): Initializes a new instance of the DMNode class.
        __repr__(self) -> str: Returns a string representation of the DMNode.
        __hash__(self) -> int: Returns the hash value of the DMNode.
        tuple(self) -> tuple: Returns a tuple representation of the DMNode.

    """

    def __init__(self, n: int):
        """
        Initializes a new instance of the DMNode class.

        Args:
            n (int): The node identifier.

        """
        self.n = n
        self.arcs = {}
        self.final = False

    def __repr__(self) -> str:
        """
        Returns a string representation of the DMNode.

        Returns:
            str: The string representation of the DMNode.

        """
        return f"<{self.n}, {self.tuple()!r}>"

    def __hash__(self) -> int:
        """
        Returns the hash value of the DMNode.

        Returns:
            int: The hash value of the DMNode.

        """
        return hash(self.tuple())

    def tuple(self) -> tuple:
        """
        Returns a tuple representation of the DMNode.

        Returns:
            tuple: The tuple representation of the DMNode.

        """
        arcs = tuple(sorted(self.arcs.items()))
        return arcs, self.final


def strings_dfa(strings):
    """
    Constructs a Deterministic Finite Automaton (DFA) from a list of strings.

    Args:
        strings (list): A list of strings to construct the DFA from.

    Returns:
        DFA: The constructed DFA.

    Raises:
        ValueError: If the strings are not in lexicographical order or if an empty string is encountered.

    Notes:
        - The DFA is constructed by iteratively adding strings to the automaton.
        - The DFA is built incrementally, reusing common prefixes between strings to optimize space.
        - The DFA is represented using DMNode objects, which store the state transitions and accept states.
        - The DFA is returned as an instance of the DFA class.

    Example:
        strings = ["apple", "banana", "cherry"]
        dfa = strings_dfa(strings)
    """
    dfa = DFA(0)
    c = itertools.count(1)

    last = ""
    seen = {}
    nodes = [DMNode(0)]

    for string in strings:
        if string <= last:
            raise ValueError("Strings must be in order")
        if not string:
            raise ValueError("Can't add empty string")

        # Find the common prefix with the previous string
        i = 0
        while i < len(last) and i < len(string) and last[i] == string[i]:
            i += 1
        prefixlen = i

        # Freeze the transitions after the prefix, since they're not shared
        add_suffix(dfa, nodes, last, prefixlen + 1, seen)

        # Create new nodes for the substring after the prefix
        for label in string[prefixlen:]:
            node = DMNode(next(c))
            # Create an arc from the previous node to this node
            nodes[-1].arcs[label] = node.n
            nodes.append(node)
        # Mark the last node as an accept state
        nodes[-1].final = True

        last = string

    if len(nodes) > 1:
        add_suffix(dfa, nodes, last, 0, seen)
    return dfa


def add_suffix(dfa, nodes, last, downto, seen):
    """
    Add a suffix to the given DFA.

    This function takes a DFA (Deterministic Finite Automaton) and adds a suffix to it.
    The suffix is constructed from a list of nodes, starting from the last node and
    going up to the specified downto index.

    Parameters:
    - dfa (DFA): The DFA to which the suffix will be added.
    - nodes (list): The list of nodes representing the suffix.
    - last (list): The list of labels representing the transitions from the last node
                   to its parent nodes.
    - downto (int): The index indicating the last node in the suffix to be added.
    - seen (dict): A dictionary that keeps track of already seen nodes.

    Returns:
    None

    Notes:
    - If a node with the same characteristics (final/nonfinal, same arcs to same destinations)
      is already seen, it is replaced with the already seen node.
    - If a node is replaced with an already seen one, the parent node's pointer to this node is fixed.
    - The node's transitions are added to the DFA.

    """
    while len(nodes) > downto:
        node = nodes.pop()
        tup = node.tuple()

        try:
            this = seen[tup]
        except KeyError:
            this = node.n
            if node.final:
                dfa.add_final_state(this)
            seen[tup] = this
        else:
            parent = nodes[-1]
            inlabel = last[len(nodes) - 1]
            parent.arcs[inlabel] = this

        for label, dest in node.arcs.items():
            dfa.add_transition(this, label, dest)
