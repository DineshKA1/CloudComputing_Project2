import re
from typing import List

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
        
        if 'Scan' in node_type:
            node.table = plan_dict.get('Relation Name')
            node.operation = node_type
            
            if 'Filter' in plan_dict:
                node.properties['filter'] = plan_dict['Filter']
        
        # Process Join operations
        elif 'Join' in node_type:
            node.operation = node_type
            if 'Hash' in node_type:
                node.properties['method'] = "HASH"
            elif 'Merge' in node_type:
                node.properties['method'] = "MERGE"
            elif 'Nested' in node_type:
                node.properties['method'] = "NESTED LOOP"
            
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
        
        # Process Sort operations
        elif 'Sort' in node_type:
            node.operation = "SORT"
            if 'Sort Key' in plan_dict:
                node.properties['sort_key'] = plan_dict['Sort Key']
        
        # Process Limit operations
        elif 'Limit' in node_type:
             node.operation = "LIMIT"
        
        if 'Plans' in plan_dict:
            for child_plan in plan_dict['Plans']:
                child_node = process_node(child_plan)
                if child_node:
                    node.children.append(child_node)
        
        return node
    
    # Processroot node
    if not qep_json or not isinstance(qep_json, list) or not qep_json[0].get('Plan'):
        return None
    
    return process_node(qep_json[0]['Plan'])

def node_to_syntax(node, is_root=True):
    if not node:
        return ""
        
    result = []
    for child in node.children:
        result.append(node_to_syntax(child, is_root=False))

    op_type = node.operation.upper()
    line = ""

    # FROM clause
    if op_type == "SCAN" and node.table:
        line = f"FROM {node.table}"

    # JOINs
    elif op_type == "JOIN":
        method = node.properties.get("method", "")
        condition = node.properties.get("condition", "")
        line = f"{method} JOIN on {condition}" if method else f"JOIN on {condition}"
    
    # Filter
    if node.properties.get("filter"):
        line += f" WHERE {node.properties['filter']}"

    # Aggregation
    if op_type == "AGGREGATE":
        group_by = node.properties.get("group_key")
        if group_by:
            line = f"AGGREGATE GROUP BY {group_by}"
        else:
            line = "AGGREGATE"


    # Sort
    if op_type == "SORT":
        sort_key = node.properties.get("sort_key", "")
        sort_order = node.properties.get("sort_order", "")
        line = f"ORDER BY {sort_key} {sort_order}".strip()

    # Limit   
    if op_type == "LIMIT":
        limit_val = node.properties.get("limit")
        if limit_val:
            line = f"LIMIT {limit_val}"

    
    # Cost
    if node.startup_cost and node.total_cost:
        line += f" -- startup cost: {node.startup_cost}, total cost: {node.total_cost}"
    
    result.append(line)
    return "\n|> ".join(filter(None, result))


def convert_to_pipe_syntax(qep_json):
    root = parse_qep_json(qep_json)
    return node_to_syntax(root)