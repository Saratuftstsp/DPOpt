#pragma once

#include "_op_unary.hpp"
#include "_op_binary.hpp"
#include "plan_node.hpp"
#include <iostream>
#include "core/stats.hpp"
#include <boost/random.hpp>
#include <boost/random/exponential_distribution.hpp>
#include <cmath>
#include "emp-sh2pc/emp-sh2pc.h"

class CostModel{
public:
    //Constructor
    CostModel(){};

    float get_cost(planNode *node);
    //void dpanalyze_col(int col_idx, std::vector<emp::Integer> column, Stats &col_stats, int party);
    void get_counts(int col_idx, std::vector<emp::Integer> column, int party, Stats &s);
};

float CostModel::get_cost(planNode *node){
    //implement like the get_cost() funtion of filter_syscat
    //1. add cost for sorting and copying
    // Case 1: Unary operator
    if (node->up != nullptr){
        // Case 1a) input is a SecureRelation ---> does not get executed, need to fix
        if (node->input1 != nullptr){
            std::cout << "Check-null" << endl;
            return node->input1->flags.size() * node->selectivity;
        }else{
            float input_cardinality = get_cost(node->previous1);
            std::cout << node->selectivity << endl;
            return input_cardinality * node->selectivity;
        }
    }
    // Case 2: Binary operator
    else if (node->bp != nullptr){
        if (node->input1 != nullptr){
            //Case 2a) both left child and right child is also a secure relation
            if (node->input2 != nullptr){
                return node->input1->flags.size() * node->input2->flags.size() * node->selectivity;
            //Case 2b) left child is a secure relation and right child is another operator
            }else{
                float right_input_cost = get_cost(node->previous2);
                //selectivity =* previous2->selectivity;
                return node->input1->flags.size() * right_input_cost * node->selectivity;
            }
        }else{
            float left_input_cost = get_cost(node->previous1);
            //Case 2c) left child is an operator and right child is a secure relation
            if (node->input2 != nullptr){
                return left_input_cost * node->input2->flags.size() * node->selectivity;
            //Case 2d) both children operators
            }else{
                float right_input_cost =  get_cost(node->previous2);
                return left_input_cost * right_input_cost * node->selectivity;
            }
        }
    }else{
         if (node->input1 != nullptr){
            return node->input1->flags.size() * node->selectivity;
        }
    }
}



void CostModel::get_counts(int col_idx, std::vector<emp::Integer> column, int party, Stats &s){
    //1. Make array of all domain values
    //.  and corresponding array of counts initialized to emp Integer 0
    std::vector<Integer> vals_emp;
    for(int i = 0; i < 11; i++){
        s.mcv.push_back(i);
        vals_emp.push_back(Integer(32, i, PUBLIC));
        Integer alice_integer(32, 0, ALICE);
        s.mcf_priv.push_back(alice_integer); // don't know if using just one party is okay or not
    }

    //2. For each data value, evaluate predicate checking equality with each domain value
    //   and add to count
    for(int i = 0; i < column.size(); i++){
       for(int j = 0; j < vals_emp.size(); j++){
            std::vector<Bit> bit_bool;
            bit_bool.push_back(column[i].equal(vals_emp[j]));
            s.mcf_priv[j] = emp::If((column[i]==vals_emp[j]), s.mcf_priv[j]+Integer(32, 1, ALICE), s.mcf_priv[j]);
       }
    }
    //3. populate stats object partially
    s.column_index = col_idx; //the index of the column for which these statistics are collected
    s.num_rows = column.size();
    s.ndistinct = -1; //leave as -1 for now, will assign after adding noise
    bool diffp = 0;

}
