import sqlglot
from sqlglot import parse_one, exp

def sql_to_pipe(query: str, is_subquery=False) -> str:
    tree = parse_one(query)
    lines = []

    def extract(expr):
        if isinstance(expr, exp.From):
            table_expr = expr.this
            if isinstance(table_expr, exp.Subquery):
                # Flatten and inline the subquery only
                return sql_to_pipe(table_expr.unnest().sql(), is_subquery=True)
            return f"FROM {table_expr.sql()}"

        elif isinstance(expr, exp.Join):
            table_expr = expr.args["this"]
            on_expr = expr.args.get("on")
            join_type = expr.args.get("kind", "INNER").upper()
            # If the join type is OUTER, check for left or right
            if join_type == "OUTER":
                if "LEFT" in expr.sql():
                   join_type = "LEFT"
            elif "RIGHT" in expr.sql():
                   join_type = "RIGHT"
            condition = f" ON {on_expr.sql()}" if on_expr else ""

            if isinstance(table_expr, exp.Subquery):
                # Flatten subquery only if it contains relational logic
                sub_lines = sql_to_pipe(table_expr.unnest().sql(), is_subquery=True).splitlines()
                return "\n".join(sub_lines + [f"|> {join_type} JOIN subplan{condition}"])
            else:
                return f"|> {join_type} JOIN {table_expr.sql()}{condition}"

        elif isinstance(expr, exp.Where):
            return f"|> WHERE {expr.this.sql()}"

        elif isinstance(expr, exp.Group):
            return f"|> AGGREGATE {', '.join([e.sql() for e in expr.expressions])} GROUP BY {', '.join([e.sql() for e in expr.expressions])}"

        elif isinstance(expr, exp.Having):
            return f"|> HAVING {expr.this.sql()}"

        elif isinstance(expr, exp.Order):
            return f"|> ORDER BY {expr.sql().replace('ORDER BY', '').strip()}"

        elif isinstance(expr, exp.Limit):
            limit_value = expr.args.get("expression")
            if limit_value:
                return f"|> LIMIT {limit_value.sql()}"

        return None

    # Handle SELECT
    aggregates = []
    if isinstance(tree, exp.Select):
        if tree.args.get("from"):
            from_clause = extract(tree.args["from"])
            if from_clause:
                lines.append(from_clause)

        for join in tree.find_all(exp.Join):
            lines.append(extract(join))

        if tree.args.get("where"):
            lines.append(extract(tree.args["where"]))

        if tree.args.get("group"):
            group = tree.args["group"]
            aggregates = tree.expressions
            lines.append(extract(group))
        else:
            lines.append(f"|> SELECT {', '.join([e.sql() for e in tree.expressions])}")

        if tree.args.get("having"):
            lines.append(extract(tree.args["having"]))

        if tree.args.get("order"):
            lines.append(extract(tree.args["order"]))

        if tree.args.get("limit"):
            lines.append(extract(tree.args["limit"]))

    # Only return lines if not from a subquery that's being inlined
    if is_subquery:
        return "\n".join(lines)
    else:
        return "\n".join(lines)
    
if __name__ == "__main__":
    query = """
SELECT 
    c_custkey,
    c_name,
    COUNT(DISTINCT o_orderkey) as order_count,
    SUM(l_quantity) as total_quantity,
    SUM(l_extendedprice) as total_price
FROM 
    customer
    JOIN orders ON c_custkey = o_custkey
    JOIN lineitem ON o_orderkey = l_orderkey
WHERE 
    o_orderdate >= '1995-01-01'
    AND o_orderdate < '1996-01-01'
GROUP BY 
    c_custkey, c_name
HAVING 
    COUNT(DISTINCT o_orderkey) > 5
ORDER BY 
    total_price DESC
LIMIT 10;
    """

    result = sql_to_pipe(query)
    print(result)