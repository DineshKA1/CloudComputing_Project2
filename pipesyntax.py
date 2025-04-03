import re

def parse_qep(qep):
    """
    parse the QEP output -> extract key operations in order
    :param qep: query execution plan as a string
    :return: list of parsed execution steps
    """
    steps = []
    for line in qep.split('\n'):
        match = re.search(r'->\s+([A-Za-z ]+)', line)
        if match:
            steps.append(match.group(1).strip())
    return steps

def convert_to_pipe_syntax(qep):
    """
    convert parsed QEP into pipe-syntax format
    :param qep: query execution plan as a string
    :return: equivalent pipe-syntax SQL query
    """
    steps = parse_qep(qep)
    
    if not steps:
        return "Error: Unable to parse QEP."
    
    pipe_syntax_query = "\n|> ".join(steps)
    return f"FROM base_table\n|> {pipe_syntax_query};"

#test usage
if __name__ == "__main__":
    sample_qep = """
    -> Seq Scan on customer
    -> Hash Join
    -> Aggregate
    -> Sort
    """
    print(convert_to_pipe_syntax(sample_qep))
