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

using namespace emp;


void get_relations(int party, int num_cols, int alice_rows, int bob_rows, std::map<string, SecureRelation*> &rels_dict){
    auto start_time = std::chrono::high_resolution_clock::now();
    for(int i = 0; i < 3; i++){
        string relname = "rel" + std::to_string(i);
        std::string fname = party==BOB ? relname+"_bob.csv":relname+"_alice.csv";
        //std::cout << "Party: " << party << "\n";
        //std::cout << fname << "\n";

        ScanOperator s = ScanOperator(fname, num_cols, alice_rows, bob_rows);
        s.execute(*rels_dict[relname], party); // get histogram also, non-emp
        //input_rel.print_relation("Testing Scanner: \n");
    }
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

void q3_dp_ops(std::map<string, SecureRelation*> rels_dict,std::map<string, std::vector<Stats>> rel_stats_dict){

    planNode *testNode = new planNode(rels_dict["rel1"]);
    testNode->node_name = "rel1";

    planNode *testNode2 = new planNode(rels_dict["rel2"]);
    testNode2->node_name = "rel2";

    planNode *testNode3 = new planNode(rels_dict["rel3"]);
    testNode3->node_name = "rel3";

    planNode *testNode4 = new planNode(rels_dict["rel4"]);
    testNode4->node_name = "rel4";

    FilterOperatorSyscat* filter_op1 = new FilterOperatorSyscat(1, Integer(32, 5, PUBLIC), "eq", &rel_stats_dict["rel1"][1]);
    planNode* filterNode1 = new planNode(filter_op1, filter_op1->selectivity);
    filterNode1->node_name = "filter_0";
    filterNode1->set_previous(*testNode, 1);

    FilterOperatorSyscat* filter_op2 = new FilterOperatorSyscat(1, Integer(32, 5, PUBLIC), "eq", &rel_stats_dict["rel2"][1]);
    planNode *filterNode2 = new planNode(filter_op2, filter_op2->selectivity);
    filterNode2->node_name = "filter_1";
    filterNode2->set_previous(*testNode2, 1);

    EquiJoinOperatorSyscat* jOp = new EquiJoinOperatorSyscat(1, 1, nullptr, nullptr);
    planNode* jNode = new planNode(jOp, filterNode1, filterNode2, jOp->selectivity);
    jNode->node_name = "join_0";

    EquiJoinOperatorSyscat* jOp2 = new EquiJoinOperatorSyscat(1, 1, nullptr, nullptr);
    planNode* jNode2 = new planNode(jOp, jNode, testNode3, jOp->selectivity);
    jNode2->node_name = "join_1";
    jNode2->set_previous(*jNode,1);
    jNode2->set_previous(*testNode3,2);

    EquiJoinOperatorSyscat* jOp3 = new EquiJoinOperatorSyscat(1, 1, nullptr, nullptr);
    planNode* jNode3 = new planNode(jOp, jNode2, testNode4, jOp->selectivity);
    jNode3->node_name = "join_2";
    jNode3->set_previous(*jNode2,1);
    jNode3->set_previous(*testNode4,2);

    auto start_time = std::chrono::high_resolution_clock::now();
    SecureRelation* query_output;
    for(int i= 0; i < 2; i++){
        query_output = jNode2->get_output();
    }
    //(*query_output).print_relation("Testing query output: ");
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration_filter_by_fixed_value = std::chrono::duration_cast<std::chrono::milliseconds>((end_time - start_time)).count();
    std::cout << "Runtime with DP Syscat: " << duration_filter_by_fixed_value/2 << " ms" << std::endl;
    //std::cout << "Output size for DP Syscat case: " << query_output->flags.size() << endl;
    CostModel cm;
    float cost = cm.get_cost(jNode,1);
    std::cout << "Cost DP Syscat case: " << cost << endl;
    //query_output->print_relation("Result of DP-Syscat-optimized execution.");
}

void q3_prev_ops(std::map<string, SecureRelation*> rels_dict){
    planNode *testNode = new planNode(rels_dict["rel1"]);
    testNode->node_name = "rel1";

    planNode *testNode2 = new planNode(rels_dict["rel2"]);
    testNode2->node_name = "rel2";

    planNode *testNode3 = new planNode(rels_dict["rel3"]);
    testNode3->node_name = "rel3";

    planNode *testNode4 = new planNode(rels_dict["rel4"]);
    testNode4->node_name = "rel4";

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
    

    EquiJoinOperator* jOp2 = new EquiJoinOperator(1, 1);
    planNode* jNode2 = new planNode(jOp2, filterNode2, testNode3, 1);
    jNode2->set_previous(*jNode,1);
    jNode2->set_previous(*testNode3,2);

    EquiJoinOperator* jOp3 = new EquiJoinOperator(1, 1);
    planNode* jNode3 = new planNode(jOp3, jNode2, testNode4, 1);
    jNode3->set_previous(*jNode2,1);
    jNode3->set_previous(*testNode4,2);

    auto start_time = std::chrono::high_resolution_clock::now();
    SecureRelation* query_output;
    for(int i= 0; i < 2; i++){
        query_output = jNode3->get_output();
    }
    //(*query_output).print_relation("Testing query output: ");
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration_filter_by_fixed_value = std::chrono::duration_cast<std::chrono::milliseconds>((end_time - start_time)).count();
    std::cout << "Q3 runtime in the completely oblivious case: " << duration_filter_by_fixed_value/2 << " ms" << std::endl;
    //std::cout << "Output size for oblivious case: " << query_output->flags.size() << endl;
    float input_size1 = rels_dict["rel1"]->flags.size();
    float input_size2 = rels_dict["rel2"]->flags.size();
    float input_size3 = rels_dict["rel3"]->flags.size();
    float input_size4 = rels_dict["rel4"]->flags.size();
    float cost = input_size1 + input_size2 + (input_size1 * input_size2) + input_size3 + (input_size1 * input_size2 * input_size1) + input_size4 + (input_size1 * input_size2 * input_size1 * input_size4);
    std::cout << "Cost oblivious case: " << input_size1 + input_size2 + (input_size1 * input_size2) << endl;
    //query_output->sort_by_flag();
    //query_output->print_relation("Result of oblivious execution.");
}

void q2_dp_ops(std::map<string, SecureRelation*> rels_dict,std::map<string, std::vector<Stats>> rel_stats_dict){

    planNode *testNode = new planNode(rels_dict["rel1"]);
    testNode->node_name = "rel1";

    planNode *testNode2 = new planNode(rels_dict["rel2"]);
    testNode2->node_name = "rel2";

    planNode *testNode3 = new planNode(rels_dict["rel3"]);
    testNode3->node_name = "rel3";

    FilterOperatorSyscat* filter_op1 = new FilterOperatorSyscat(1, Integer(32, 5, PUBLIC), "eq", &rel_stats_dict["rel1"][1]);
    planNode* filterNode1 = new planNode(filter_op1, filter_op1->selectivity);
    filterNode1->node_name = "filter_0";
    filterNode1->set_previous(*testNode, 1);

    FilterOperatorSyscat* filter_op2 = new FilterOperatorSyscat(1, Integer(32, 5, PUBLIC), "eq", &rel_stats_dict["rel2"][1]);
    planNode *filterNode2 = new planNode(filter_op2, filter_op2->selectivity);
    filterNode2->node_name = "filter_1";
    filterNode2->set_previous(*testNode2, 1);

    EquiJoinOperatorSyscat* jOp = new EquiJoinOperatorSyscat(1, 1, nullptr, nullptr);
    planNode* jNode = new planNode(jOp, filterNode1, filterNode2, jOp->selectivity);
    jNode->node_name = "join_0";

    EquiJoinOperatorSyscat* jOp2 = new EquiJoinOperatorSyscat(1, 1, nullptr, nullptr);
    planNode* jNode2 = new planNode(jOp, jNode, testNode3, jOp->selectivity);
    jNode2->node_name = "join_1";
    jNode2->set_previous(*jNode,1);
    jNode2->set_previous(*testNode3,2);

    auto start_time = std::chrono::high_resolution_clock::now();
    SecureRelation* query_output;
    for(int i= 0; i < 2; i++){
        query_output = jNode2->get_output();
    }
    //(*query_output).print_relation("Testing query output: ");
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration_filter_by_fixed_value = std::chrono::duration_cast<std::chrono::milliseconds>((end_time - start_time)).count();
    std::cout << "Runtime with DP Syscat: " << duration_filter_by_fixed_value/2 << " ms" << std::endl;
    //std::cout << "Output size for DP Syscat case: " << query_output->flags.size() << endl;
    CostModel cm;
    float cost = cm.get_cost(jNode,1);
    std::cout << "Cost DP Syscat case: " << cost << endl;
    //query_output->print_relation("Result of DP-Syscat-optimized execution.");
}

void q2_prev_ops(std::map<string, SecureRelation*> rels_dict){
    planNode *testNode = new planNode(rels_dict["rel1"]);
    testNode->node_name = "rel1";

    planNode *testNode2 = new planNode(rels_dict["rel2"]);
    testNode2->node_name = "rel2";

    planNode *testNode3 = new planNode(rels_dict["rel3"]);
    testNode2->node_name = "rel3";

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
    

    EquiJoinOperator* jOp2 = new EquiJoinOperator(1, 1);
    planNode* jNode2 = new planNode(jOp2, filterNode2, testNode3, 1);
    jNode2->set_previous(*jNode,1);
    jNode2->set_previous(*testNode3,2);

    auto start_time = std::chrono::high_resolution_clock::now();
    SecureRelation* query_output;
    for(int i= 0; i < 2; i++){
        query_output = jNode->get_output();
    }
    //(*query_output).print_relation("Testing query output: ");
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration_filter_by_fixed_value = std::chrono::duration_cast<std::chrono::milliseconds>((end_time - start_time)).count();
    std::cout << "Q2 runtime in the completely oblivious case: " << duration_filter_by_fixed_value/2 << " ms" << std::endl;
    //std::cout << "Output size for oblivious case: " << query_output->flags.size() << endl;
    float input_size1 = rels_dict["rel1"]->flags.size();
    float input_size2 = rels_dict["rel2"]->flags.size();
    float input_size3 = rels_dict["rel3"]->flags.size();
    float cost = input_size1 + input_size2 + (input_size1 * input_size2) + input_size3 + (input_size1 * input_size2 * input_size1);
    std::cout << "Cost oblivious case: " << input_size1 + input_size2 + (input_size1 * input_size2) << endl;
    //query_output->sort_by_flag();
    //query_output->print_relation("Result of oblivious execution.");
}


void q1_dp_ops(std::map<string, SecureRelation*> rels_dict,std::map<string, std::vector<Stats>> rel_stats_dict){
        auto start_time = std::chrono::high_resolution_clock::now();
        planNode *testNode = new planNode(rels_dict["rel1"]);
        testNode->node_name = "rel1";

        planNode *testNode2 = new planNode(rels_dict["rel2"]);
        testNode2->node_name = "rel2";

        FilterOperatorSyscat* filter_op1 = new FilterOperatorSyscat(1, Integer(32, 5, PUBLIC), "eq", &rel_stats_dict["rel1"][1]);
        planNode* filterNode1 = new planNode(filter_op1, filter_op1->selectivity);
        filterNode1->node_name = "filter_0";
        filterNode1->set_previous(*testNode, 1);

        FilterOperatorSyscat* filter_op2 = new FilterOperatorSyscat(1, Integer(32, 5, PUBLIC), "eq", &rel_stats_dict["rel2"][1]);
        planNode *filterNode2 = new planNode(filter_op2, filter_op2->selectivity);
        filterNode2->node_name = "filter_1";
        filterNode2->set_previous(*testNode2, 1);

        EquiJoinOperatorSyscat* jOp = new EquiJoinOperatorSyscat(1, 1, nullptr, nullptr);
        planNode* jNode = new planNode(jOp, filterNode1, filterNode2, jOp->selectivity);
        jNode->node_name = "join_0";
    start_time = std::chrono::high_resolution_clock::now();
    SecureRelation* query_output;
    for(int i= 0; i < 1; i++){
        query_output = jNode->get_output();
    }
    //(*query_output).print_relation("Testing query output: ");
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration_filter_by_fixed_value = std::chrono::duration_cast<std::chrono::milliseconds>((end_time - start_time)).count();
    //std::cout << "Runtime with DP Syscat: " << duration_filter_by_fixed_value/2 << " ms" << std::endl;
    //std::cout << "Output size for DP Syscat case: " << query_output->flags.size() << endl;
    CostModel cm;
    float cost = cm.get_cost(jNode,1);
    //std::cout << "Cost DP Syscat case: " << cost << endl;
    //query_output->print_relation("Result of DP-Syscat-optimized execution.");
}

void q1_prev_ops(std::map<string, SecureRelation*> rels_dict){
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
    auto start_time = std::chrono::high_resolution_clock::now();
    SecureRelation* query_output;
    for(int i= 0; i < 1; i++){
        query_output = jNode->get_output();
    }
    //(*query_output).print_relation("Testing query output: ");
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration_filter_by_fixed_value = std::chrono::duration_cast<std::chrono::milliseconds>((end_time - start_time)).count();
    //std::cout << "Runtime in the completely oblivious case: " << duration_filter_by_fixed_value/2 << " ms" << std::endl;
    //std::cout << "Output size for oblivious case: " << query_output->flags.size() << endl;
    float input_size1 = rels_dict["rel1"]->flags.size();
    float input_size2 = rels_dict["rel2"]->flags.size();
    float cost = input_size1 + input_size2 + (input_size1 * input_size2);
    //std::cout << "Cost oblivious case: " << input_size1 + input_size2 + (input_size1 * input_size2) << endl;
    //query_output->sort_by_flag();
    //query_output->print_relation("Result of oblivious execution.");
}

/*planNode* parse_and_plan(std::map<string, SecureRelation*> rels_dict){
    //5. Parse query
    std::string qname = "q2";
    //std::string query = "select * from rel1 join rel2 on rel1.col1 = rel2.col1 where rel1.col0 = 4 and rel2.col0 = 5;";
    std::string query = "select * from rel1 where rel1.col1=1";

    auto start_time = std::chrono::high_resolution_clock::now();
    Parser p(qname, query);
    p.get_json();
    
    
   
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration_filter_by_fixed_value = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    std::cout << "Time to parse query: " << duration_filter_by_fixed_value << " ms" << std::endl;
    
    //6. plan query
    start_time = std::chrono::high_resolution_clock::now();
    fs::path current_path = std::filesystem::current_path(); //current_dir / "core" / "parser" / "get_logical_tree.py";
    std::string plan_file_name = (current_path / "test" / "parsed" / (qname + "_logictree.json")).string();
    std::string node_details_file = (current_path / "test" / "parsed" / (qname + "_nodes.json")).string();

    Planner plnr(plan_file_name, node_details_file, rels_dict);
    planNode *rootNode = plnr.build_plan();
    plnr.rels_dict = rels_dict;
    end_time = std::chrono::high_resolution_clock::now();
    duration_filter_by_fixed_value = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    std::cout << "Time to build plan: " << duration_filter_by_fixed_value << " ms" << std::endl;



    return rootNode;
}*/



int main(int argc, char** argv) {

    // 1. Get port and party information from user
    // do not completely understand this part but using it as black box for now
    int port, party;
    parse_party_and_port(argv, &party, &port);

    NetIO* io = new NetIO(party == ALICE ? nullptr : "127.0.0.1", port);
    setup_semi_honest(io, party);


    //2. Create and initialize a large relation
    const int num_cols = 4;  // 4 columns
    const int num_rows = 100;
    int alice_rows = num_rows/2;
    int bob_rows = num_rows - alice_rows;
    //std::cout << alice_rows << ", " << bob_rows << endl;
    //const int num_rows = 1 << 12;  // Around a million rows


    //3. Get relations
    std::map<string, SecureRelation*> rels_dict;
    for(int i = 0; i < 4; i++){
        string relname = "rel" + std::to_string(i);
        rels_dict[relname] = new SecureRelation(num_cols, 0);
    }
    get_relations(party, num_cols, alice_rows, bob_rows, rels_dict);
    

    //4. Build DP System Catalog (Use DPOptimizer to add noise to counts)
    std::map<string, std::vector<Stats>> rel_stats_dict;
    for(int i = 0; i < 4; i++){
        string relname = "rel" + std::to_string(i);
        rel_stats_dict[relname] = std::vector<Stats>(num_cols);
    }
    get_noisy_relation_statistics(rels_dict, 4, party, rel_stats_dict);


    //7. run query
    //planNode* node = parse_and_plan(rels_dict);

    // previous ops
    //std::cout << "Q1 Oblivious run: " << endl;
    //q1_prev_ops(rels_dict);
    //std::cout << "Q2 Oblivious run: " << endl;
    //q2_prev_ops(rels_dict);
    std::cout << "Q3 Oblivious run: " << endl;
    q3_prev_ops(rels_dict);
   
    // current ops
    //std::cout << "Q1 DP run: " << endl;
    //q1_dp_ops(rels_dict, rel_stats_dict);
    //std::cout << "Q2 DP run: " << endl;
    //q2_dp_ops(rels_dict, rel_stats_dict);
    //std::cout << "Q3 DP run: " << endl;
    //q3_dp_ops(rels_dict, rel_stats_dict);


    
    
    io->flush();




    delete io;
    return 0;
}



