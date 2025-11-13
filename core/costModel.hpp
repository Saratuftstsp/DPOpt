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
#include <tuple>

class CostModel{
public:
    //Constructor
    CostModel(){};

    std::tuple<float,float> get_cost(planNode *node, int cost, int prune);
    //void dpanalyze_col(int col_idx, std::vector<emp::Integer> column, Stats &col_stats, int party);
    void get_counts(int col_idx, std::vector<emp::Integer> column, int party, Stats &s);
};

std::tuple<float,float> CostModel::get_cost(planNode *node, int cost, int prune){
    //implement like the get_cost() funtion of filter_syscat
    //1. add cost for sorting and copying
    // Case 1: Unary operator
    if (node->up != nullptr){
        int input_size = 0;
        // get input_size
        if (node->input1 != nullptr){
            std::cout << "Check-null" << endl;
            input_size = node->input1->flags.size();
        }else{
            std::tuple<float,float> cost_and_input_size = get_cost(node->previous1,cost, prune);
            float prev_op_cost = std::get<0>(cost_and_input_size);
            input_size = std::get<1>(cost_and_input_size);
        }

        int next_op_insize = input_size;
        int op_cost = input_size; //O(n) to apply filter
        cost = cost + op_cost;

        if ((prune==1)){ //extra cost in the DP Syscat case: sort and prune
            int sort_cost = std::ceil(input_size * pow(std::log10(input_size),2)); //sort cost of O(n log^2(n))
            cost = cost + sort_cost;
            if(0 < node->selectivity < 1){
                int prune_cost = input_size; // O(n) cost for pruning
                cost = cost + prune_cost;
                next_op_insize = node->selectivity * next_op_insize;
            }
        }
        
        return std::make_tuple(cost, next_op_insize);
    }
    // Case 2: Binary operator
    else if (node->bp != nullptr){

        int input_size1 = 0;
        int input_size2 = 0;
        int next_op_insize = 0;

        // get left_input cost and size
        if (node->input1 != nullptr){
            input_size1 = node->input1->flags.size();
        }else{
            std::tuple<float,float> costModel_output = get_cost(node->previous1, cost, prune);
            cost = std::get<0>(costModel_output);
            input_size1 = std::get<1>(costModel_output);
        }

        // get right_input cost and size
        if (node->input2 != nullptr){
            input_size2 = node->input2->flags.size();
        }else{
            std::tuple<float,float> costModel_output = get_cost(node->previous2, cost, prune);
            cost = std::get<0>(costModel_output);
            input_size2 = std::get<1>(costModel_output);
        }

        //compute join cost and join output size
        next_op_insize = input_size1 * input_size2;
        int op_cost = next_op_insize;
        cost = cost + op_cost;
        if (prune==1){
            int sort_cost = std::ceil(input_size1 * input_size2 * pow(std::log10(input_size1 * input_size2),2)); //sort cost of O(n log^2(n))
            cost = cost + sort_cost;
            if(node->selectivity < 1){
                int prune_cost = input_size1 * input_size2;
                cost = cost + prune_cost;
                next_op_insize = node->selectivity * next_op_insize;
            }
        }

        return std::make_tuple(cost,next_op_insize);
    }else{
         if (node->input1 != nullptr){
            return std::make_tuple(cost, node->input1->flags.size() * node->selectivity);
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
