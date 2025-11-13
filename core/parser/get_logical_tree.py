#!/Library/Frameworks/Python.framework/Versions/3.11/bin/python3

import argparse
from pglast import parse_sql
from pprint import pprint
from anytree import Node, RenderTree
import json
from pathlib import Path
#from pglast.stream import IndentedStream, RawStream  # can be used to get query out of AST

class myNode:
    def __init__(self, name=None, parent=None, children=None, numlinks=0):
        self.name = name
        self.parent = parent
        self.children = children
        self.numlinks = numlinks
        self.isfilter = False
        self.filterop = ""
        self.filterval = 0
        self.input1 = ""
        self.input2 = ""
        self.colref1 = ""
        self.colref2 = ""
        
    def add_child(self, child_node):
        if self.children == None:
            self.children = []
        #if child_node not in self.children:
        self.children.append(child_node)
        self.numlinks += 1
    def remove_child(self, child_node):
        self.children.remove(child_node)
    def get_name(self):
        return self.name
    def get_parent(self):
        return self.parent
    def set_parent(self, new_parent):
        self.parent = new_parent
    def get_children(self):
        return self.children
    def get_numlinks(self):
        return self.numlinks
    def get_isfilter(self):
        return self.isfilter
    def set_isfilter(self, filter_cond_exists):
        self.isfilter = filter_cond_exists
    def set_filtercond(self, filter_cond):
        temp_lst = filter_cond.split(" ")
        rel_and_col = temp_lst[0].split(".")
        self.input1 = rel_and_col[0]
        self.colref1 = int(rel_and_col[1][-1])
        self.filterop = temp_lst[1]
        self.filterval = int(temp_lst[2])
                
    def get_node_details(self):
        details_dict = {}
        details_dict["name"] = self.name
        if self.name == "join":
            details_dict["input1"] = self.input1
            details_dict["input2"] = self.input2
            details_dict["colref1"] = self.colref1
            details_dict["colref2"] = self.colref2
        if self.name == "filter":
            details_dict["input1"] = self.input1
            details_dict["colref1"] = self.colref1
            details_dict["filterval"] = self.filterval
            details_dict["filterop"] = self.filterop
        return details_dict
            
    def set_filter_input(self, relname, colref):
        self.input1 = relname
        self.colref1 = colref
    def set_join_input(self, relname1, relname2, colref1, colref2):
        self.input1 = relname1
        self.input2 = relname2
        self.colref1 = colref1
        self.colref2 = colref2
#_________________________________________________________________________________
def get_rels(sd):
    """
    Takes a SQL query's statement dictionary (sd) as input.
    Returns a list of base relations in the statement.
    """
    rels = []
    n1 = sd['@']
    if n1 == "SelectStmt":
        from_tup = sd["fromClause"]
        #print(from_dict)
        for i in range(len(from_tup)):
            from_dict = from_tup[i]
            n2 = from_dict["@"]
            if n2 == "RangeVar":
                rels.append(from_dict["relname"])
            elif n2 == "JoinExpr":
                arg_dict = from_dict["larg"]
                rels.append(arg_dict["relname"])
                arg_dict = from_dict["rarg"]
                rels.append(arg_dict["relname"]) 
    return rels

         
def get_subtree_nodes(parent, dict, alias_dict, rel_dict, nodes_lst):
    node_type = dict["@"]
    
    # base case 1 : RangeVar
    if node_type=="RangeVar":
        alias_dict, rel_dict, nodes_lst = create_rel_node(parent, dict, alias_dict, rel_dict, nodes_lst)
    
    # recursive case 1 : JoinExpr 
    elif node_type=="JoinExpr":
        new_node = myNode("join", parent, [], 1)
        populate_join_details(new_node, dict)
        parent.add_child(new_node)
        nodes_lst.append(new_node)
        alias_dict, rel_dict, nodes_lst = get_subtree_nodes(new_node, dict["larg"], alias_dict, rel_dict, nodes_lst)
        alias_dict, rel_dict, nodes_lst = get_subtree_nodes(new_node, dict["rarg"], alias_dict, rel_dict, nodes_lst)

        
    # base case 2(a) : BoolExpr (involves loop though)
    elif node_type=="BoolExpr":
        args_tuple = dict["args"]
        next_parent = parent
        for dict2 in args_tuple:
            #("filter", end = ": ")
            new_node = myNode("filter", next_parent, [], 1)
            next_parent.add_child(new_node)
            nodes_lst.append(new_node)
            populate_filter_details(new_node, dict2)
            next_parent = new_node
            
    # base case 2(a) : BoolExpr (involves loop though)
    elif node_type=="A_Expr":
        new_node = myNode("filter", parent, [], 1)
        parent.add_child(new_node)
        nodes_lst.append(new_node)
        populate_filter_details(new_node, dict)

            
    
    # recursive case 2: SelectStmt
    elif node_type=="SelectStmt":
        new_node = myNode("select", parent, [], 1)
        parent.add_child(new_node)
        nodes_lst.append(new_node)
        
        # make nodes from the WHERE clause and make them parent of select node
        if "whereClause" in dict:
            where_dict = dict["whereClause"]
            alias_dict, rel_dict, nodes_lst = get_subtree_nodes(new_node, where_dict, alias_dict, rel_dict, nodes_lst)
            # swap positions of select node and filters on current tree
            bring_filters_to_the_top(parent, new_node)


        # get base relations or join 
        from_tuple = dict["fromClause"]
        for dict2 in from_tuple:
            alias_dict, rel_dict, nodes_lst = get_subtree_nodes(new_node, dict2, alias_dict, rel_dict, nodes_lst)
            
    else:
        print("Unknown node type encountered.")
    return alias_dict, rel_dict, nodes_lst

def populate_filter_details(filternode, dict):
    filter_cond = get_filter_cond(dict["lexpr"], dict["name"], dict["rexpr"])
    filternode.set_isfilter(True)
    filternode.set_filtercond(filter_cond)



def populate_join_details(node, dict):
    rel1, rel2, colref1, colref2 = "", "", "", ""
    
    #1. Get the left and right arguments of the join if they are relaiton names
    left_arg_dict = dict["larg"]
    if left_arg_dict["@"]=="RangeVar":
        rel1 = left_arg_dict["relname"]
    right_arg_dict = dict["rarg"]
    if right_arg_dict["@"]=="RangeVar":
        rel2 = right_arg_dict["relname"]
        
    #2. Get the join qualification / column references from each table
    quals_dict = dict["quals"]
    left_expr = quals_dict["lexpr"]
    if left_expr["@"]=="ColumnRef":
        colref1 = int(left_expr["fields"][1]["sval"][-1])
    right_expr = quals_dict["rexpr"]
    if right_expr["@"]=="ColumnRef":
        colref2 = int(right_expr["fields"][1]["sval"][-1])
        
    node.set_join_input(rel1, rel2, colref1, colref2)

def create_rel_node(parent, dict, alias_dict, rel_dict, nodes_lst):
    relname = dict["relname"]
    relname_alias = None
    if "alias" in dict:
        relname_alias = dict["alias"]
    if relname_alias!=None:
        alias_dict[relname_alias] = relname
        relname = relname_alias

    # if a relation of this name already exists, then make its parent the current parent's child
    if relname in rel_dict:
        existing_parent = rel_dict[relname].get_parent()
        existing_grand_parent = existing_parent.get_parent()
        current_grand_parent = parent.get_parent()
        
        # 1. make the current parent the existing parent's parent
        existing_parent.set_parent(parent)
        parent.add_child(existing_parent)
        
        # 2. make existing grandparent the current parent's parent
        parent.set_parent(existing_grand_parent)
        existing_grand_parent.add_child(parent)
        
        # 3. remove connection between existing grandparent and exisitng parent
        existing_grand_parent.remove_child(existing_parent)
        
        #4. remove connection between current grandparent and current parent
        current_grand_parent.remove_child(parent)
    # else make a new node and insert it in the right spot on the tree
    else:
        new_node = myNode(relname, parent, numlinks=1)
        parent.add_child(new_node)
        nodes_lst.append(new_node)
        rel_dict[relname] = new_node
            
    return alias_dict, rel_dict, nodes_lst

def bring_filters_to_the_top(parent, curr_node):
    child_list = curr_node.get_children()
    if child_list==[]:
        return 
    # 1. Get child_node and also leaf node on the path of filters
    child_node = child_list[0]
    leaf_node = child_node
    while leaf_node.get_children()!=[]:
        leaf_node = leaf_node.get_children()[0]
        
    # 2. Remove edges: (a) parent to curr_node (b) from curr_node to child_node
    parent.remove_child(curr_node)
    curr_node.remove_child(child_node)
    
    
    # 3. Make parent point to child_node
    parent.add_child(child_node)
    child_node.set_parent(parent)
    
    # 4. Make leaf_node parent of curr_node
    leaf_node.add_child(curr_node)
    curr_node.set_parent(parent)


def get_filter_cond(left_dict, operator_tuple, right_dict):
    left_expr = ""
    right_expr = ""
    
    left_dict_content = left_dict["@"]
    if left_dict_content=="ColumnRef":
        left_expr = get_colref(left_dict)
    
    right_dict_content = right_dict["@"]
    if right_dict_content=="A_Const":
        right_expr = right_dict["val"]["ival"]
        
    operator_string = ""
    for operator_details in operator_tuple:
        operator_string += operator_details["sval"]
        
    return left_expr + " " + operator_string + " " + str(right_expr)
        
def get_colref(dict):
    relname = dict["fields"][0]["sval"]
    colname = dict["fields"][1]["sval"]
    return (relname + "." + colname)
        
def construct_tree(parent, child_lst):
    # 1. Regardless of case, create nodes and link to parent
    for my_node in child_lst:
        tree_node = Node(my_node.get_name(), parent = parent)
        off_shoot = my_node.get_children()
        # 2. Recursive case
        if off_shoot != None:
            construct_tree(tree_node, off_shoot)
    return parent

def construct_tree_dict(parent, op_counts, node_dict):
    #parent_name = parent.get_name()
    plan_dict = {}
    # 1. create the nodes and put them in the parent's list of children in the plan_dict
    child_lst = parent.get_children()
    for child in child_lst:
        key_name = child.get_name()
        #print(key_name, end=": ")
        if key_name in op_counts:
            count = op_counts[key_name]
            op_counts[key_name] = count + 1
            key_name = key_name + "_" + str(count)
        else:
            op_counts[key_name] = 1
            key_name = key_name + "_" + str(0)
        #print(key_name)
        plan_dict[key_name] = {}
        node_dict[key_name] = child.get_node_details()
        
        # 2. Recursive case
        off_shoot = child.get_children()
        if off_shoot != None and off_shoot != []:
            plan_dict[key_name], op_counts, node_dict = construct_tree_dict(child, op_counts, node_dict)
            
        else:
            # do not want the _i next to relation names
            plan_dict[child.get_name()] = plan_dict[key_name]
            del plan_dict[key_name]

    return plan_dict, op_counts, node_dict


def plan_json(query_name, my_root):
    #1. get the plan in a dictionary format
    plan_dict = {"root":{}}
    plan_dict["root"], _ , node_dict= construct_tree_dict(my_root, {}, {})

    # 2. write plan_dict to json file
    # Step 2a: Get current directory
    current_dir = Path.cwd()
    
    # Step 2b: Specify a sibling subdirectory (e.g., "data")
    target_file = current_dir / "test" / "parsed" / (query_name+"_logictree.json") #current_dir / "test" / "parsed" / (query_name+"_logictree.json") #current_dir / (query_name+"_logictree.json")
    
    with open(target_file, "w") as f:
        f.write(json.dumps(plan_dict, indent=1))
        
    # Step 2c: Print node details in another json file
    target_file =  current_dir / "test" / "parsed" / (query_name+"_nodes.json") #current_dir / "test" / "parsed" / (query_name+"_nodes.json") #current_dir / (query_name+"_node_details.json")

    with open(target_file, "w") as f:
        f.write(json.dumps(node_dict, indent=1))
        
    # 3. return string version
    return json.dumps(plan_dict, indent=1)


def main():

    #1. Get user input for SQL query and epsilon (to be used later)
    parser = argparse.ArgumentParser()
    parser.add_argument('query_name', type=str)
    parser.add_argument('query', type=str)
    args = parser.parse_args()
    
    #2. Use pg_last to get an AST for the query
    q = args.query
    root = parse_sql(q)
    
    for i in range(len(root)):
        stmt = root[i]
        #pprint(stmt())
    
        #3. Get the names of the tables/relations
        stmt_dict = stmt.stmt(skip_none=True)
        rels = get_rels(stmt_dict)
        #print(rels)
        # print(compute_cost(stmt_dict))
        
    #3. Get tree nodes
    rootNode = myNode("root", None, [], 0)
    alias_dict, rel_dict, nodes_lst = get_subtree_nodes(rootNode, stmt_dict, {}, {}, [rootNode])
    '''for node in nodes_lst:
        print(node, end=", ")
        print(node.get_name(), end=": ")
        print(node.get_children())'''
    #print(nodes_lst)
    
    #4. Draw tree
    # 4(a) find root in list of nodes
    my_root, root = None, None
    for my_node in nodes_lst:
        if my_node.get_name()=="root":
            my_root = my_node
            root = Node("root")
    
    # 4(b) recursively construct tree
    root = construct_tree(root, my_root.get_children())
    json_obj = plan_json(args.query_name, my_root)
    
    # 5 draw the tree
    for pre, fill, node in RenderTree(root):
        print(f"{pre}{node.name}")
    
    
 

if __name__=="__main__":
    main()
    
# test_query: 
# "select c1 from r1 join r2 on c1 where r1.c2 < 2 and r2.c3 > 3;"
# "select c1 from r1 where r1.c1 in (select c1 from r2);"

# Decisions that might look dumb but were made due to valid reasons:
# 1. The construct_tree function takes the list of children but
## construct_tree_dict does not. Why? Because construct_tree takes parent
## in the form of a Node object provided by the python libary used to draw trees
## but construct_tree_dict takes parent in the form of the myNode object I wrote
## which has links to the myNode objects represeting its children.

# QUERY PLAN COST COMPUTATION
'''
def compute_cost(dict):
    # hard-coded dict of cardinalities
    base_cards = {"company_name": 234997, "company_type": 4, "keyword": 134170, "link_type": 18, 
                  "movie_companies": 2609129, "movie_keyword":4523930, "movie_link": 29997, "title": 2528312,  
                  "char_name":3140339, "cast_info":36244344, "role_type":12, "info_type":113, "movie_info_idx":1380035, "name":4167491,
                  "movie_info":14835720}
    node_type = dict["@"]
    # base case 1 : RangeVar
    if node_type=="RangeVar":
        relname = dict["relname"]
        if relname not in base_cards:
            return 1
        return base_cards[relname]
    
    # recursive case 1 : JoinExpr 
    elif node_type=="JoinExpr":
        return ( compute_cost(dict["larg"]) * compute_cost(dict["rarg"]) )
    
    # recursive case 2: SelectStmt
    elif node_type=="SelectStmt":
        cost = 0
        from_tuple = dict["fromClause"]
        for dict2 in from_tuple:
            cost += compute_cost(dict2)
        return cost
    
    return -1
'''