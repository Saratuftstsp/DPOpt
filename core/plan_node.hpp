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

    //5. What is its selectivity?
    float selectivity = 1;

    //a) Constructor: unary operator, input SecureRelation
    planNode(UnaryOperator* u_op, SecureRelation* input_rel);

    //b) Constructor: unary operator, input from another operator
    planNode(UnaryOperator* u_op, planNode* child);

    //c) Constructor: binary operator, both inputs SecureRelations
    planNode(BinaryOperator* bin_op, SecureRelation* input_rel1, SecureRelation* input_rel2);

    //d) Constructor: binary operator, both inputs from other operators
    planNode(BinaryOperator* bin_op, planNode* left_child, planNode* right_child);

    //e) Constructor: binary operator, left_child intermediate operator
    //   right_child SecureRelation
    planNode(BinaryOperator* bin_op, planNode* left_child, SecureRelation* input_rel2);

    //f) Constructor: binary operator, left_child SecureRelation
    //   right_child another operator
    planNode(BinaryOperator* bin_op, SecureRelation* input_rel1, planNode* left_child);

    SecureRelation get_output();

    float get_cost();

};


//a) Constructor: unary operator, input SecureRelation
planNode::planNode(UnaryOperator* u_op, SecureRelation* input_rel)
    : up(u_op), input1(input_rel) {}

//b) Constructor: unary operator, input from another operator
planNode::planNode(UnaryOperator* u_op, planNode* child)
    : up(u_op), previous1(child) {}

//c) Constructor: binary operator, both inputs SecureRelations
planNode::planNode(BinaryOperator* bin_op, SecureRelation* input_rel1, SecureRelation* input_rel2)
    : bp(bin_op), input1(input_rel1), input2(input_rel2) {}

//d) Constructor: binary operator, both inputs from other operators
planNode::planNode(BinaryOperator* bin_op, planNode* left_child, planNode* right_child)
    : bp(bin_op), previous1(left_child), previous2(right_child) {}

//e) Constructor: binary operator, left_child intermediate operator
//   right_child SecureRelation
planNode::planNode(BinaryOperator* bin_op, planNode* left_child, SecureRelation* input_rel2)
    : bp(bin_op), previous1(left_child), input2(input_rel2) {}

//f) Constructor: binary operator, left_child SecureRelation
//   right_child another operator
planNode::planNode(BinaryOperator* bin_op, SecureRelation* input_rel1, planNode* left_child)
    : bp(bin_op), input1(input_rel1), previous2(left_child) {}

// Running the operator in the planNode and connecting to its parent
SecureRelation planNode::get_output(){
    SecureRelation output;
    // Case 1: Unary operator
    if (up != nullptr){
        // Case 1a) input is a SecureRelation
        if (input1 != nullptr){
            std::cout << "Found right execution scenario.\n";
            output = up->execute(*input1);
            std::cout << "Completed execution.\n";
        }else{
            SecureRelation input = previous1 -> get_output();
            output = up->execute(input);
        }
    }
    // Case 2: Binary operator
    else if (bp != nullptr){
        if (input1 != nullptr){
            //Case 2a) both left child and right child is also a secure relation
            if (input2 != nullptr){
                output = bp -> execute(*input1, *input2);
            //Case 2b) left child is a secure relation and right child is another operator
            }else{
                SecureRelation right_input = previous2 -> get_output();
                //selectivity =* previous2->selectivity;
                output = bp -> execute(*input1, right_input);
            }
        }else{
            SecureRelation left_input = previous1 -> get_output();
            left_input.print_relation("Filter1 output: \n");
            //Case 2c) left child is an operator and right child is a secure relation
            if (input2 != nullptr){
                output = bp -> execute(left_input, *input2);
            //Case 2d) left child is a secure relation and right child is another operator
            }else{
                SecureRelation right_input = previous2 -> get_output();
                right_input.print_relation("Filter2 output: \n");
                output = bp -> execute(left_input, right_input);
            }
        }
    }

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