import re

from typing import List

def parse_qep(qep_text: str) -> List[str]:
    """
    Parse a textual EXPLAIN plan and emit pipe‑syntax steps.
    :param qep_text: the raw output of `EXPLAIN` (text format)
    :return: list of steps like
             ["FROM customer",
              "RIGHT HASH JOIN ON orders.o_custkey = customer.c_custkey -- cost: 3822.69..39395.28",
              "AGGREGATE GROUP BY customer.c_custkey -- cost: 96991.22..134993.41",
              "AGGREGATE GROUP BY count(orders.o_orderkey) -- cost: 137243.40..137245.40",
              "ORDER BY (count(*)) DESC, (count(orders.o_orderkey)) DESC -- cost: 137253.04..137253.54"]
    """
    steps = []
    lines = qep_text.splitlines()

    for i, line in enumerate(lines):
        indent = len(line) - len(line.lstrip())

        if 'Partial HashAggregate' in line \
           or 'Gather Merge' in line \
           or 'Worker' in line:
            continue

        cost_m = re.search(r'\(cost=([0-9.]+)\.\.([0-9.]+)', line)
        cost_str = f"-- cost: {cost_m.group(1)}..{cost_m.group(2)}" if cost_m else ""

        if 'Scan on' in line and '->' in line:
            tbl_m = re.search(r'on\s+(\w+)', line)
            if tbl_m:
                tbl = tbl_m.group(1)
                op = f"SCAN {tbl}"
                if i+1 < len(lines) and 'Filter:' in lines[i+1]:
                    f_m = re.search(r'Filter:\s*(.+)', lines[i+1])
                    if f_m:
                        op += f" WHERE {f_m.group(1)}"
                steps.append(f"{op} {cost_str}".strip())
                continue

        if 'Join' in line and '->' in line:
            jt_m = re.search(r'->\s+(?:Parallel\s+)?(.+Join)', line)
            if jt_m:
                join_type = jt_m.group(1).upper()
                cond = None
                if i+1 < len(lines):
                    for tag in ('Hash Cond:', 'Merge Cond:', 'Join Filter:'):
                        if tag in lines[i+1]:
                            cond = re.search(rf'{tag}\s*(.+)', lines[i+1]).group(1)
                            break
                op = f"{join_type} ON {cond}" if cond else join_type
                steps.append(f"{op} {cost_str}".strip())
                continue

        if 'Finalize GroupAggregate' in line:
            gk = None
            for j in range(i+1, min(i+4, len(lines))):
                if 'Group Key:' in lines[j]:
                    gk = re.search(r'Group Key:\s*(.+)', lines[j]).group(1)
                    break
            op = "AGGREGATE"
            if gk:
                op += f" GROUP BY {gk}"
            steps.append(f"{op} {cost_str}".strip())
            continue

        if re.match(r'\s*->\s*HashAggregate', line):
            gk = None
            for j in range(i+1, min(i+4, len(lines))):
                if 'Group Key:' in lines[j]:
                    gk = re.search(r'Group Key:\s*(.+)', lines[j]).group(1)
                    break
            op = "AGGREGATE"
            if gk:
                op += f" GROUP BY {gk}"
            steps.append(f"{op} {cost_str}".strip())
            continue

        if 'Sort' in line and 'Sort Key:' in (lines[i+1] if i+1 < len(lines) else ""):
            sk = re.search(r'Sort Key:\s*(.+)', lines[i+1]).group(1)
            op = f"ORDER BY {sk}"
            steps.append(f"{op} {cost_str}".strip())
            continue
    steps.reverse()

    if steps and steps[0].startswith("SCAN"):
        tbl = steps[0].split()[1]
        steps[0] = f"FROM {tbl}"

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
    return pipe_syntax_query

#test usage
if __name__ == "__main__":
    sample_qep = """
    ->  Seq Scan on customer  (cost=0.00..1.00)
    ->  Hash Join on customer.c_custkey = orders.o_custkey  (cost=1.00..2.00)
    ->  Aggregate  (cost=2.00..3.00)
    ->  Sort  (cost=3.00..4.00)
    """
    print(convert_to_pipe_syntax(sample_qep))
