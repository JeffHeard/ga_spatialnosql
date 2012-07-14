__author__ = 'jeff'

def file_contents(filename):
    with open(filename) as f:
        contents = f.read()

    return contents


def aif(test, then_clause, else_clause):
    k = test()
    if k:
        then_clause(k)
    else:
        else_clause()