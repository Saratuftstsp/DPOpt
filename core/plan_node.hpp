#pragma once

#include "_op_unary.hpp"
#include "_op_binary.hpp"


class planNode{
public:
    //1. What kind of operator is it?
    UnaryOperator* up = nullptr;
    BinaryOperator* bp = nullptr;

    //2. What are its inputs?
    SecureRelation* input1 = nullptr;
    SecureRelation* input2 = nullptr;

    //3. Does it have an intermediate operator before it?
    planNode* previous1 = nullptr;
    planNode* previous2 = nullptr;

    //4. Does it have an intermediate operator after it?
    planNode* next = nullptr;

    //5. What is its output?
    SecureRelation* output = nullptr;

    //6. What is its selectivity?
    float selectivity = 1;

    //7. node name, needed by planner
    std::string node_name = "";

    planNode();
    planNode(SecureRelation* rel);
    planNode(UnaryOperator* u_op, float sel);
    planNode(BinaryOperator* b_op, float sel);

    //a) Constructor: unary operator, input SecureRelation
    planNode(UnaryOperator* u_op, SecureRelation* input_rel, float sel);

    //b) Constructor: unary operator, input from another operator
    planNode(UnaryOperator* u_op, planNode* child, float sel);

    //c) Constructor: binary operator, both inputs SecureRelations
    planNode(BinaryOperator* bin_op, SecureRelation* input_rel1, SecureRelation* input_rel2, float sel);

    //d) Constructor: binary operator, both inputs from other operators
    planNode(BinaryOperator* bin_op, planNode* left_child, planNode* right_child, float sel);

    //e) Constructor: binary operator, left_child intermediate operator
    //   right_child SecureRelation
    planNode(BinaryOperator* bin_op, planNode* left_child, SecureRelation* input_rel2, float sel);

    //f) Constructor: binary operator, left_child SecureRelation
    //   right_child another operator
    planNode(BinaryOperator* bin_op, SecureRelation* input_rel1, planNode* right_child, float sel);

    SecureRelation* get_output();

    float get_cost();

    void set_name(string name);

    void set_previous(planNode &prev, int pos);
    void set_next(planNode &nxt);



};

planNode::planNode(){}
planNode::planNode(SecureRelation* rel): input1(rel){}

planNode::planNode(UnaryOperator* u_op, float sel): up(u_op), selectivity(sel){}

planNode::planNode(BinaryOperator* b_op, float sel): bp(b_op), selectivity(sel) {}

//a) Constructor: unary operator, input SecureRelation
planNode::planNode(UnaryOperator* u_op, SecureRelation* input_rel, float sel)
    : up(u_op), input1(input_rel), selectivity(sel) {}

//b) Constructor: unary operator, input from another operator
planNode::planNode(UnaryOperator* u_op, planNode* child, float sel)
    : up(u_op), previous1(child), selectivity(sel) {}

//c) Constructor: binary operator, both inputs SecureRelations
planNode::planNode(BinaryOperator* bin_op, SecureRelation* input_rel1, SecureRelation* input_rel2, float sel)
    : bp(bin_op), input1(input_rel1), input2(input_rel2), selectivity(sel) {}

//d) Constructor: binary operator, both inputs from other operators
planNode::planNode(BinaryOperator* bin_op, planNode* left_child, planNode* right_child, float sel)
    : bp(bin_op), previous1(left_child), previous2(right_child), selectivity(sel) {}

//e) Constructor: binary operator, left_child intermediate operator
//   right_child SecureRelation
planNode::planNode(BinaryOperator* bin_op, planNode* left_child, SecureRelation* input_rel2, float sel)
    : bp(bin_op), previous1(left_child), input2(input_rel2), selectivity(sel) {}

//f) Constructor: binary operator, left_child SecureRelation
//   right_child another operator
planNode::planNode(BinaryOperator* bin_op, SecureRelation* input_rel1, planNode* right_child, float sel)
    : bp(bin_op), input1(input_rel1), previous2(right_child), selectivity(sel) {}

// Running the operator in the planNode and connecting to its parent
SecureRelation* planNode::get_output(){
    std::cout << node_name << endl;
    auto start_time = std::chrono::high_resolution_clock::now();
    auto end_time = std::chrono::high_resolution_clock::now();
    SecureRelation* output = new SecureRelation();
    // Case 1: Unary operator
    if (up != nullptr){
        // Case 1a) input is a SecureRelation
        if (input1 != nullptr){
            start_time = std::chrono::high_resolution_clock::now();
            *output = up->execute(*input1);
            end_time = std::chrono::high_resolution_clock::now();
        }else{
            SecureRelation* input = previous1 -> get_output();
            start_time = std::chrono::high_resolution_clock::now();
            *output = up->execute(*input);
            end_time = std::chrono::high_resolution_clock::now();
        }
    }
    // Case 2: Binary operator
    else if (bp != nullptr){
        if (input1 != nullptr){
            //Case 2a) both left child and right child is also a secure relation
            if (input2 != nullptr){
                start_time = std::chrono::high_resolution_clock::now();
                *output = bp -> execute(*input1, *input2);
                end_time = std::chrono::high_resolution_clock::now();
            //Case 2b) left child is a secure relation and right child is another operator
            }else{
                SecureRelation* right_input = previous2 -> get_output();
                start_time = std::chrono::high_resolution_clock::now();
                *output = bp -> execute(*input1, *right_input);
                end_time = std::chrono::high_resolution_clock::now();
            }
        }else{
            SecureRelation* left_input = previous1 -> get_output();
            //(*left_input).print_relation("Filter1 output: \n");
            //Case 2c) left child is an operator and right child is a secure relation
            if (input2 != nullptr){
                start_time = std::chrono::high_resolution_clock::now();
                *output = bp -> execute(*left_input, *input2);
                end_time = std::chrono::high_resolution_clock::now();
            //Case 2d) left child is a secure relation and right child is another operator
            }else{
                SecureRelation* right_input = previous2 -> get_output();
                start_time = std::chrono::high_resolution_clock::now();
                *output = bp -> execute(*left_input, *right_input);
                end_time = std::chrono::high_resolution_clock::now();
            }
        }
    }
    else if (node_name.substr(0,3)=="rel") {
        start_time = std::chrono::high_resolution_clock::now();
        *output = *input1;
        end_time = std::chrono::high_resolution_clock::now();
    }
    else{ //rootNode case
        std::cout << "rootNode check" << endl;
        *output = *(previous1 -> get_output());
    }

    auto duration_filter_by_fixed_value = std::chrono::duration_cast<std::chrono::milliseconds>((end_time - start_time)).count();
    auto duration_us = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
    std::cout << "Total runtime for individual operator "<< node_name << " " << duration_us << " micro_sec" << std::endl;
    std::cout << "Output size for operator " << node_name << " " << output->flags.size() << endl;
    return output;
}

float planNode::get_cost(){
    float cost;
    // Case 1: Unary operator
    if (up != nullptr){
        // Case 1a) input is a SecureRelation
        if (input1 != nullptr){
            cost = input1->flags.size() * selectivity;
        }else{
            float input_cardinality = previous1 -> get_cost();
            cost = input_cardinality * selectivity;
        }
    }
    // Case 2: Binary operator
    else if (bp != nullptr){
        if (input1 != nullptr){
            //Case 2a) both left child and right child is also a secure relation
            if (input2 != nullptr){
                cost = input1->flags.size() * input2->flags.size() * selectivity;
            //Case 2b) left child is a secure relation and right child is another operator
            }else{
                float right_input_cost = previous2 -> get_cost();
                //selectivity =* previous2->selectivity;
                cost = input1->flags.size() * right_input_cost * selectivity;
            }
        }else{
            float left_input_cost = previous1 -> get_cost();
            //left_input.print_relation("Filter1 output: \n");
            //Case 2c) left child is an operator and right child is a secure relation
            if (input2 != nullptr){
                cost = left_input_cost * input2->flags.size() * selectivity;
            //Case 2d) left child is a secure relation and right child is another operator
            }else{
                float right_input_cost = previous2 -> get_cost();
                //right_input.print_relation("Filter2 output: \n");
                cost = left_input_cost * right_input_cost * selectivity;
            }
        }
    }

    return cost;
}

void planNode::set_name(string name){
    node_name = name;
}

void planNode::set_previous(planNode &prev, int pos){
    if (pos == 1){
        previous1 = &prev;
    }else{
        previous2 = &prev;
    }
}

void planNode::set_next(planNode &nxt){
    next = &nxt;
}