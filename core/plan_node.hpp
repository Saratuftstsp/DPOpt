#pragma once

#include "_op_unary.hpp"
#include "_op_binary.hpp"


class planNode{
public:
    //1. What kind of operator is it?
    UnaryOperator* up;
    BinaryOperator* bp;

    //2. What are its inputs?
    SecureRelation* input1;
    SecureRelation* input2;

    //3. Does it have an intermediate operator before it?
    planNode* previous1;
    planNode* previous2;

    //4. Does it have an intermediate operator after it?
    planNode* next;

    //5. What is its output?
    SecureRelation* output;

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

    SecureRelation get_output();

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

// Running the operator in the planNode and connecting to its parent
SecureRelation planNode::get_output(){
    SecureRelation output;
    // Case 1: Unary operator
    if (up != nullptr){
        // Case 1a) input is a SecureRelation
        if (input1 != nullptr){
            output = up->execute(*input1);
        }
    }

    return output;
}