from whoosh.automata.fsa import ANY, EPSILON, NFA


def levenshtein_automaton(term, k, prefix=0):
    """
    Generate a Levenshtein automaton for a given term and maximum edit distance.

    Args:
        term (str): The term to generate the automaton for.
        k (int): The maximum edit distance allowed.
        prefix (int, optional): The length of the prefix to match exactly. Defaults to 0.

    Returns:
        NFA: The generated Levenshtein automaton.

    """
    nfa = NFA((0, 0))
    if prefix:
        for i in range(prefix):
            c = term[i]
            nfa.add_transition((i, 0), c, (i + 1, 0))

    for i in range(prefix, len(term)):
        c = term[i]
        for e in range(k + 1):
            # Correct character
            nfa.add_transition((i, e), c, (i + 1, e))
            if e < k:
                # Deletion
                nfa.add_transition((i, e), ANY, (i, e + 1))
                # Insertion
                nfa.add_transition((i, e), EPSILON, (i + 1, e + 1))
                # Substitution
                nfa.add_transition((i, e), ANY, (i + 1, e + 1))
    for e in range(k + 1):
        if e < k:
            nfa.add_transition((len(term), e), ANY, (len(term), e + 1))
        nfa.add_final_state((len(term), e))
    return nfa
