import re
from typing import List
import sqlglot
from sqlglot import parse_one, exp

class QEPNode:
    def __init__(self, operation, startup_cost=None, total_cost=None, table=None):
        self.operation = operation    
        self.startup_cost = startup_cost  
        self.total_cost = total_cost          
        self.table = table           
        self.children = []           
        self.properties = {
            'condition': None,        # join条件
            'group_key': None,        # 分组键
            'sort_key': None,         # 排序键
            'sort_order': None,       # 排序顺序
            'filter': None,           # 过滤条件
            'method': None,           # 操作方法（如Hash/Merge/Nested Loop）
        }

def parse_qep(qep):
    root = None
    nodes_stack = []
    qep = "\n-> " + qep
    
    for line in qep.split('\n'):
        if "->" not in line:
            # 处理附加信息行
            if "Sort Key:" in line:
                if nodes_stack:
                    sort_info = re.search(r'Sort Key: (.+)', line)
                    if sort_info:
                        current = nodes_stack[-1][1]
                        current.properties['sort_key'] = sort_info.group(1)
                        # 提取排序顺序（DESC/ASC）
                        if "DESC" in line:
                            current.properties['sort_order'] = "DESC"
                        else:
                            current.properties['sort_order'] = "ASC"
            elif "Group Key:" in line:
                if nodes_stack:
                    group_info = re.search(r'Group Key: (.+)', line)
                    if group_info:
                        nodes_stack[-1][1].properties['group_key'] = group_info.group(1)
            elif "Hash Cond:" in line:
                if nodes_stack:
                    cond_info = re.search(r'Hash Cond: (.+)', line)
                    if cond_info:
                        nodes_stack[-1][1].properties['condition'] = cond_info.group(1)
            elif "Filter:" in line:
                if "Rows Removed by Filter:" in line:
                    continue
                if nodes_stack:
                    filter_info = re.search(r'Filter: (.+)', line)
                    if filter_info:
                        nodes_stack[-1][1].properties['filter'] = filter_info.group(1)
            continue
        
        # 忽略所有并行相关的操作
        if any(op in line for op in [
            'Partial', 
            'Finalize',
            'Gather',
            'Worker'
        ]):
            continue
        
        indent = len(line) - len(line.lstrip())
        cost_match = re.search(r'\(cost=(\d+\.\d+)\.\.(\d+\.\d+)', line)
        cost = f"{cost_match.group(1)}..{cost_match.group(2)}" if cost_match else None
        
        op_match = re.search(r'->\s+([A-Za-z ]+)', line)
        if not op_match:
            continue
            
        operation = op_match.group(1).strip()
        node = QEPNode(operation, cost)
        
        # 处理Scan操作
        if "Scan" in operation:
            table_match = re.search(r'on\s+(\w+)', line)
            if table_match:
                node.table = table_match.group(1)
                node.operation = "SCAN"
                # 捕获扫描方法
                if "Index" in operation:
                    node.properties['method'] = "INDEX"
                elif "Seq" in operation:
                    node.properties['method'] = "SEQUENTIAL"
        
        # 处理Join操作
        elif "Join" in operation:
            node.operation = "JOIN"
            if "Hash" in operation:
                node.properties['method'] = "HASH"
            elif "Merge" in operation:
                node.properties['method'] = "MERGE"
            elif "Nested" in operation:
                node.properties['method'] = "NESTED LOOP"
        elif "Aggregate" in operation:
            node.operation = "AGGREGATE"
            if "Partial" in operation:
                node.properties['method'] = "PARTIAL"
            elif "Final" in operation:
                node.properties['method'] = "FINAL"

        # 根据缩进级别构建树
        while nodes_stack and nodes_stack[-1][0] >= indent:
            nodes_stack.pop()
            
        if nodes_stack:
            nodes_stack[-1][1].children.append(node)
        else:
            root = node
        nodes_stack.append((indent, node))
    
    return root

def parse_qep_json(qep_json):
    """Parse JSON format query execution plan"""
    def process_node(plan_dict):
        node_type = plan_dict.get('Node Type', '')
        node = QEPNode(node_type)
        
        startup_cost = plan_dict.get('Startup Cost')
        total_cost = plan_dict.get('Total Cost')
        if startup_cost is not None and total_cost is not None:
            node.startup_cost = startup_cost
            node.total_cost = total_cost
        
        if 'Filter' in plan_dict:
            node.properties['filter'] = plan_dict['Filter']

        if 'Plans' in plan_dict:
            for child_plan in plan_dict['Plans']:
                child_node = process_node(child_plan)
                if child_node:
                    node.children.append(child_node)

        
        if 'Scan' in node_type:
            node.table = plan_dict.get('Relation Name')
            node.operation = node_type
            
            #if 'Filter' in plan_dict:
                #node.properties['filter'] = plan_dict['Filter']
        
        # Process Join operations
        elif 'Join' in node_type:
            node.operation = node_type
            if 'Hash' in node_type:
                node.properties['method'] = "HASH"
            elif 'Merge' in node_type:
                node.properties['method'] = "MERGE"
            elif 'Nested' in node_type:
                node.properties['method'] = "NESTED LOOP"
            elif 'Loop' in node_type:
                node.properties['method'] = "LOOP"
            
            if 'Hash Cond' in plan_dict:
                node.properties['condition'] = plan_dict['Hash Cond']
            elif 'Merge Cond' in plan_dict:
                node.properties['condition'] = plan_dict['Merge Cond']
            elif 'Join Filter' in plan_dict:
                node.properties['condition'] = plan_dict['Join Filter']
        
        # Process Aggregate operations
        elif 'Aggregate' in node_type:
            node.operation = "AGGREGATE"
            if 'Group Key' in plan_dict:
                node.properties['group_key'] = plan_dict['Group Key']
            if 'Strategy' in plan_dict:
                node.properties['strategy'] = plan_dict['Strategy']
            if 'Output' in plan_dict:
                node.properties['output'] = plan_dict['Output']
        
        # Process Sort operations
        elif 'Sort' in node_type:
            node.operation = "SORT"
            if 'Sort Key' in plan_dict:
                node.properties['sort_key'] = plan_dict['Sort Key']
        
        # Process Limit operations
        elif 'Limit' in node_type:
             node.operation = "LIMIT"

        
        return node
    
    # Processroot node
    if not qep_json or not isinstance(qep_json, list) or not qep_json[0].get('Plan'):
        return None
    
    return process_node(qep_json[0]['Plan'])

def sql_to_pipe(query: str, qep_root=None, is_subquery=False) -> str:
    tree = parse_one(query)
    lines = []
    qep_nodes = []

     # Flatten the QEP tree into a list (pre-order traversal)
    def flatten_qep(node):
        if not node:
            return []
        nodes = [node]
        for child in node.children:
            nodes.extend(flatten_qep(child))
        return nodes

    if qep_root:
        qep_nodes = flatten_qep(qep_root)

    # Help to find a matching node and consume it
    def pop_qep_node(operation_type=None):
        if not qep_nodes:
            return None
        if operation_type is None:
            return qep_nodes.pop(0)
        for i, node in enumerate(qep_nodes):
            if operation_type.upper() in node.operation.upper():
                return qep_nodes.pop(i)
        return qep_nodes.pop(0)  # fallback

    def cost_comment(node):
        if node and node.startup_cost is not None and node.total_cost is not None:
            return f"  -- cost={node.startup_cost:.2f}..{node.total_cost:.2f}"
        return ""


    def extract(expr):
        if isinstance(expr, exp.From):
            node = pop_qep_node("Scan")
            table_expr = expr.this
            if isinstance(table_expr, exp.Subquery):
                return sql_to_pipe(table_expr.unnest().sql(), qep_root=None, is_subquery=True)
            return f"FROM {table_expr.sql()}{cost_comment(node)}"


        elif isinstance(expr, exp.Join):
            table_expr = expr.args["this"]
            on_expr = expr.args.get("on")
            join_type = expr.args.get("kind", "INNER").upper()
            if join_type == "OUTER":
                if "LEFT" in expr.sql():
                    join_type = "LEFT"
                elif "RIGHT" in expr.sql():
                    join_type = "RIGHT"
            condition = f" ON {on_expr.sql()}" if on_expr else ""
            node = pop_qep_node("Join")

            if isinstance(table_expr, exp.Subquery):
                sub_lines = sql_to_pipe(table_expr.unnest().sql(), qep_root=None, is_subquery=True).splitlines()
                return "\n".join(sub_lines + [f"|> {join_type} JOIN subplan{condition}{cost_comment(node)}"])
            else:
                return f"|> {join_type} JOIN {table_expr.sql()}{condition}{cost_comment(node)}"

        elif isinstance(expr, exp.Where):
            node = pop_qep_node("Filter")
            return f"|> WHERE {expr.this.sql()}{cost_comment(node)}"

        elif isinstance(expr, exp.Group):
            node = pop_qep_node("Aggregate")
            return f"|> AGGREGATE {', '.join([e.sql() for e in expr.expressions])} GROUP BY {', '.join([e.sql() for e in expr.expressions])}{cost_comment(node)}"

        elif isinstance(expr, exp.Having):
            return f"|> HAVING {expr.this.sql()}" 

        elif isinstance(expr, exp.Order):
            node = pop_qep_node("Sort")
            return f"|> ORDER BY {expr.sql().replace('ORDER BY', '').strip()}{cost_comment(node)}"

        elif isinstance(expr, exp.Limit):
            node = pop_qep_node("Limit")
            limit_value = expr.args.get("expression")
            if limit_value:
                return f"|> LIMIT {limit_value.sql()}{cost_comment(node)}"

        return None

    # Handle SELECT
    if isinstance(tree, exp.Select):
        if tree.args.get("from"):
            from_clause = extract(tree.args["from"])
            if from_clause:
                lines.append(from_clause)

        joins = tree.args.get("joins") or []
        for join in joins:
            join_clause = extract(join)
            if join_clause:
                lines.append(join_clause)

        if tree.args.get("where"):
            lines.append(extract(tree.args["where"]))

        if tree.args.get("group"):
            group = tree.args["group"]
            lines.append(extract(group))
        else:
            lines.append(f"|> SELECT {', '.join([e.sql() for e in tree.expressions])}")

        if tree.args.get("having"):
            lines.append(extract(tree.args["having"]))

        if tree.args.get("order"):
            lines.append(extract(tree.args["order"]))

        if tree.args.get("limit"):
            lines.append(extract(tree.args["limit"]))

    return "\n".join(lines)