#include <iostream>
#include <fstream>
#include <memory>
#include <string>
#include <nlohmann/json.hpp>
#include <cstdlib>
#include "plan_node.hpp"
#include "op_filter_syscat.hpp"
#include "op_equijoin_syscat.hpp"
#include "relation.hpp"
#include "stats.hpp"

using json = nlohmann::json;

class Planner{
public:
    std::map<string, SecureRelation*> rels_dict;
    std::map<string, Stats*> stats_dict;
    string plan_json_file_name;
    std::string nodes_json_file_name;

    Planner(std::string file1_name, std::string file2_name, std::map<string, SecureRelation*> rels_dict);

    json get_json_tree();
    json get_json_nodes();

    planNode * build_plan();

    planNode * make_node(planNode &parent, string nodeName, json child_dict, json node_details);

    string get_operator_expr(string op);
    void get_value_given_key(json dict, string key);
};

Planner::Planner(std::string file1_name, std::string file2_name, std::map<string, SecureRelation*> rels_dict):
    plan_json_file_name(file1_name), nodes_json_file_name(file2_name), rels_dict(rels_dict){}

json Planner::get_json_tree(){
    //std::cout << plan_json_file_name << "\n";
    std::fstream file_stream_tree(plan_json_file_name);
    json j1 = json::parse(file_stream_tree);
    return j1;
}

json Planner::get_json_nodes(){
    std::fstream file_stream_nodes(nodes_json_file_name);
    json j2 = json::parse(file_stream_nodes);
    return j2;
}


// Recursively builds the planNode from JSON
planNode * Planner::build_plan()
{
    json tree = get_json_tree();
    json node_details = get_json_nodes();
    json top_dict = tree["root"];
    string nodeName;
   
    for(auto &[key,items]: top_dict.items()){
        nodeName = key;
    }
    planNode* rootNode = new planNode();
    rootNode->set_name("root");
    planNode* firstOperatorNode = make_node(*rootNode, nodeName, top_dict[nodeName], node_details);
    
    return rootNode;
    //return nullptr;
}

planNode * Planner::make_node(planNode &parent, string nodeName, json child_dict, json node_details){
    json node_info = node_details[nodeName];
    // edge case because of relation nodes
    if (node_info==nullptr){
        if (nodeName.substr(0,3) == "rel"){
            node_info = node_details[nodeName+"_0"];
        }
    }
    
    string node_type = node_info["name"];
    planNode* to_return = nullptr;

    
    //1. Base case 1: relation node
    if (node_info.size() == 1 && node_type != "select" ){
        SecureRelation *rel = rels_dict[nodeName];
        planNode* relNode = new planNode(rel);
        relNode->set_next(parent);
        relNode->set_name(nodeName);
        return relNode;
    }
    //2. Recursive case 1: select
    else if (node_type == "select"){
        // get the child node name
        string child_name;
        for(auto &[key,items]: child_dict.items()){
            child_name = key;
        }
        // get the child dict
        to_return = make_node(parent, child_name, child_dict[child_name], node_details);
        parent.set_previous(*to_return, 1);
    }
    
    
    //2. Recursive case 1: filter
    else if (node_type == "filter"){
        // need to get the column number, target and condition out of the dictionary
        string relname = node_info["input1"];
        int colnum = node_info["colref1"];
        int filterval = node_info["filterval"];
        string filter_cond_op = get_operator_expr(node_info["filterop"]);
        //string input_name = node_details["input1"];
        
        // need to create filter operator object
        FilterOperatorSyscat filter_op(colnum, Integer(32, filterval, PUBLIC), filter_cond_op);
      
        //filter_op.set_stats(stats_dict[relname]);
        
        planNode* new_node = new planNode(&filter_op);
        new_node->set_next(parent);
        new_node->set_name(nodeName);
        new_node->input1 = rels_dict[relname];
        int pos = 1;
        if (parent.previous1 != nullptr){ pos = 2;}
        parent.set_previous(*new_node, pos);
        string childName;
        // need to know what the input is
        for(auto &[key,items]: child_dict.items()){
            childName = key;
            planNode* child_node = make_node(*new_node, childName, child_dict[childName], node_details);
            new_node->set_previous(*child_node, 1);
        }
        to_return = new_node;
    }
    //3. Recursive case: join
    else if (node_type == "join"){
        int colnum1 = node_info["colref1"];
        int colnum2 = node_info["colref2"];
        string input1_name = node_info["input1"];
        string input2_name = node_info["input2"];
        EquiJoinOperatorSyscat join_op(colnum1, colnum2);
        //join_op.set_stats(stats_dict[input1_name], stats_dict[input2_name]);
        planNode *new_node = new planNode(&join_op);
        new_node->set_name(nodeName);
        new_node->set_next(parent);
        int pos = 1;
        if (parent.previous1 != nullptr){ pos = 2;}
        parent.set_previous(*new_node, pos);

        int child_num = 1;
        string childName;
        for(auto &[key,items]: child_dict.items()){
            childName = key;
            planNode* child_node = make_node(*new_node, childName, child_dict[childName], node_details);
            new_node->set_previous(*child_node, child_num);
            child_node->set_next(*new_node);
            child_num += 1;
        }
        to_return = new_node;
    }
    
    return to_return;
}

string Planner::get_operator_expr(string op){
    if (op == "<"){
        return "lt";
    }else if (op == ">"){
        return "gt";
    }else{
        return "eq";
    }
}

void get_value_given_key(json dict, string key){

}

/*if (node.is_object())
    {
        for (auto &[key, val] : node.items())
        {
            if (key.find("select") != std::string::npos)
            {
                // Unary operator
                planNode *child = build_plan(val);
                //UnaryOperator *uop = new UnaryOperator(key); //does not work because UnaryOperator is abstract
                //need the specific kind of Unary operator and call constructor accordingly
                return NULL; //new planNode(uop, child);
            }
            else if (key.find("join") != std::string::npos)
            {
                // Binary operator
                auto it = val.items();
                auto left = build_plan(val[it.begin().key()]);
                auto right = build_plan(val[std::next(it.begin()).key()]);
                //BinaryOperator *bop = new BinaryOperator(key); //does not work because BinaryOperator is abstract
                //need the specific kind of Binary operator and call constructor accordingly
                return NULL; //new planNode(bop, left, right);
            }
            else if (key.find("r") != std::string::npos)
            {
                //return new planNode(nullptr, createRelation(key)); //
            }
        }
    }*/
