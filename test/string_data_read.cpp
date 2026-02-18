#pragma once

#include "emp-sh2pc/emp-sh2pc.h"
#include "core/relation.hpp"
#include "core/op_filter.hpp"
#include "core/op_filter_syscat.hpp"
#include <iostream>
#include <string>
#include <chrono>

#include "core/op_equijoin_syscat.hpp"
#include "core/op_equijoin.hpp"
#include "core/op_idx_equijoin.hpp"  // Including the new header file
#include "core/stats.hpp"
#include <map>
#include "core/plan_node.hpp"
#include "core/op_scanner.hpp"
#include "core/dpoptimizer.hpp"
#include "core/parser/parser.hpp"
//#include "core/planner.hpp"
#include "core/costModel.hpp"
#include "core/global_string_encoder.hpp"
#include <tuple>

using namespace emp;


void get_relations(int party, int num_cols, int alice_rows, int bob_rows, std::map<string, SecureRelation*> &rels_dict, GlobalStringEncoder &encoder){
    auto start_time = std::chrono::high_resolution_clock::now();
    string relname = "drt";
    std::string fname = party==BOB ? relname+"_bob.csv":relname+"_alice.csv";  
    ScanOperator s = ScanOperator(fname, num_cols, alice_rows, bob_rows, encoder);
    s.execute(*rels_dict[relname], party); // get histogram also, non-emp
    rels_dict[relname]->print_relation("Testing Scanner: \n");
    
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration_scan = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    std::cout << "Time taken to scan relations: " << duration_scan << " ms" << std::endl;
}

void get_noisy_col_stats(int col_idx, std::vector<emp::Integer> column, int party, Stats &s){
    //1. Make array of all domain values
    //.  and corresponding array of counts initialized to emp Integer 0
    std::vector<int> vals;
    std::vector<Integer> vals_emp;
    std::vector<Integer> counts; // need to make it a predetermined size
    for(int i = 0; i < 11; i++){
        vals.push_back(i);
        vals_emp.push_back(Integer(32, i, PUBLIC));
        counts.push_back(Integer(32, 0, ALICE)); // don't know if using just one party is okay or not
    }


    //2. For each data value, evaluate predicate checking equality with each domain value
    //   and add to count
    for(int i = 0; i < column.size(); i++){
       for(int j = 0; j < vals.size(); j++){
            std::vector<Bit> bit_bool;
            bit_bool.push_back(column[i].equal(vals_emp[i]));
            Integer increment = Integer(bit_bool);
            counts[j] = counts[j] + increment;
       }
    }

    //3. Add noise and find top-k

    //4. populate stats object
    s.column_index = col_idx; //the index of the column for which these statistics are collected
    s.num_rows = column.size();
    s.ndistinct = -1; //leave as -1 for now, will assign after adding noise
    bool diffp = 0;

}

void get_noisy_relation_statistics(std::map<string, SecureRelation*> &rels_dict, int num_cols, int party, std::map<string, std::vector<Stats>> &noisy_rel_stats){
    DPOptimizer dpopt;
    for (const auto& pair : rels_dict) {
        const std::string& relname = pair.first;   // the key (string)
        SecureRelation* rel = pair.second;   // the value (pointer)
        //Idea: for each relation, pass in the columns and Stats objects to be populated
        dpopt.dpanalyze(num_cols, rel->columns, noisy_rel_stats[relname], party);
    }
}



void q1_prev_ops_cost(std::map<string, SecureRelation*> rels_dict){
    planNode *testNode = new planNode(rels_dict["rel1"]);
    testNode->node_name = "rel1";

    planNode *testNode2 = new planNode(rels_dict["rel2"]);
    testNode2->node_name = "rel2";

    
    FilterOperator* filter_op1 = new FilterOperator(1, Integer(32, 5, PUBLIC), "eq");
    planNode *filterNode1 = new planNode(filter_op1, 1);
    filterNode1->node_name = "filter_0";
    filterNode1->set_previous(*testNode, 1);

    FilterOperator* filter_op2 = new FilterOperator(1, Integer(32, 5, PUBLIC), "eq");
    planNode *filterNode2 = new planNode(filter_op2, 1);
    filterNode2->node_name = "filter_1";
    filterNode2->set_previous(*testNode2, 1);

    EquiJoinOperator* jOp = new EquiJoinOperator(1, 1);
    planNode* jNode = new planNode(jOp, filterNode1, filterNode2, 1);


    float input_size1 = rels_dict["rel1"]->flags.size();
    float input_size2 = rels_dict["rel2"]->flags.size();
    //Manual cost computation
    float manual_cost = input_size1 + input_size2 + input_size1*input_size2;
    //Cost model output
    CostModel cm;
    float cost = 0;
    std::tuple<float,float> costModel_output = cm.get_cost(jNode, cost, 0);
    float costModel_cost = std::get<0>(costModel_output);
    std::cout << "Manual cost oblivious case: " << manual_cost << endl;
    std::cout << "CostModel cost oblivious case: " << costModel_cost << endl;

    //Record runtime
    auto start_time = std::chrono::high_resolution_clock::now();
    SecureRelation* query_output;
    for(int i= 0; i < 1; i++){
        query_output = jNode->get_output();
    }
    //(*query_output).print_relation("Testing query output: ");
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration_filter_by_fixed_value = std::chrono::duration_cast<std::chrono::milliseconds>((end_time - start_time)).count();
    auto duration_us = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
    std::cout << "Q1 runtime oblivious case: " << duration_us << " micro_sec" << std::endl;

}






int main(int argc, char** argv) {

    // 1. Get port and party information from user
    // do not completely understand this part but using it as black box for now
    int port, party;
    parse_party_and_port(argv, &party, &port);

    NetIO* io = new NetIO(party == ALICE ? nullptr : "127.0.0.1", port);
    setup_semi_honest(io, party);


    //2. Create and initialize a large relation
    const int num_cols = 3;  // 4 columns
    const int num_rows = 5;
    int alice_rows = 3;
    int bob_rows = num_rows - alice_rows;
    //std::cout << alice_rows << ", " << bob_rows << endl;
    //const int num_rows = 1 << 12;  // Around a million rows


    //3. Get relations
    std::map<string, SecureRelation*> rels_dict;
    rels_dict["drt"] = new SecureRelation(num_cols, 0);
    GlobalStringEncoder encoder;
    get_relations(party, num_cols, alice_rows, bob_rows, rels_dict, encoder);
    

    //4. Build DP System Catalog (Use DPOptimizer to add noise to counts)
    /*std::map<string, std::vector<Stats>> rel_stats_dict;
    for(int i = 0; i < 4; i++){
        string relname = "rel" + std::to_string(i);
        rel_stats_dict[relname] = std::vector<Stats>(num_cols);
    }
    get_noisy_relation_statistics(rels_dict, 4, party, rel_stats_dict);*/

    //7. run query

    // previous ops
    //std::cout << "Q1 Oblivious run: " << endl;
    //q1_prev_ops(rels_dict);
  
    io->flush();




    delete io;
    return 0;
}



