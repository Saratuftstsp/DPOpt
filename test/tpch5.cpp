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
#include "core/tpch_preproc.hpp"
#include <tuple>

using namespace emp;

#define NUM_OF_ROWS 20

void get_public_string_encoding(GlobalStringEncoder &encoder){
    // The name of your CSV file
    std::string file_name = "string_mapping.csv"; 

    // Open the CSV file for reading
    std::ifstream file(file_name);

    // Check if the file was opened successfully
    if (!file.is_open()) {
        std::cerr << "Error opening file: " << file_name << std::endl;
        return;
    }

    std::string line;
    // Read each line of the file
    while (std::getline(file, line)) {
        std::stringstream ss(line);
        std::string int_str, string_val;

        // Extract the integer part (before the first comma)
        if (std::getline(ss, int_str, ',')) {
            // Extract the string part (rest of the line after the comma)
            if (std::getline(ss, string_val)) {
                encoder.encode(string_val);
            }
        }
    }

    // Close the file
    file.close();
}


void get_relations(int party, std::vector<int> num_cols, std::vector<int> alice_rows, std::vector<int> bob_rows, std::map<string, SecureRelation*> &rels_dict, GlobalStringEncoder &encoder, std::map<string,std::vector<int>> &schema_dict){
    std::vector<std::string> tpch_tables = {
    "customer",
    "orders",
    "lineitem",
    "supplier",
    "nation",
    "region",
    "part",
    "partsupp"
    };  
    auto start_time = std::chrono::high_resolution_clock::now();
    string relname;
    for(int i = 0; i < 5; i++){
        relname = tpch_tables[i];
        std::string fname = party==BOB ? "tpch_data/bob_" +relname +".csv": "tpch_data/alice_" + relname +".csv";  
        ScanOperator s = ScanOperator(fname, num_cols[i], alice_rows[i], bob_rows[i], encoder);
        s.delimiter = '|';
        s.execute(*rels_dict[relname], party); // get histogram also, non-emp
        schema_dict[relname] = rels_dict[relname]->col_types;
        //rels_dict[relname]->print_relation("Testing Scanner: \n");
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

double decode_double(Integer encrypted_int64){
    long long unencrypted = encrypted_int64.reveal<int64_t>();
    double true_float;
    memcpy(&true_float, &unencrypted, sizeof(double));
    return true_float;
}


void decode_and_print_relation(SecureRelation rel, GlobalStringEncoder encoder, std::map<string,std::vector<int>> schema_dict, std::vector<string> relnames){
    std::vector<int> new_schema;
    for (string relname: relnames){
        std::vector<int> rel_schema = schema_dict[relname];
        for(int i = 0; i < rel_schema.size(); i++){
            //std::cout << rel_schema[i] << "\n";
            new_schema.push_back(rel_schema[i]);
        }
    }
    std::cout << "TPC-H query 5 result:" << "\n";
    int true_size = 0;
    for (size_t row = 0; row < rel.columns[0].size(); row++) {
        int flag = rel.flags[row].reveal<int>();
        if (flag){
            true_size++;
            for (size_t col = 0; col < rel.columns.size(); col++) {
                //std::cout << rel.columns[col][row].reveal<int>() << "\n";
                switch(new_schema[col]){
                    case 0:
                        std::cout << rel.columns[col][row].reveal<int>() << "\t";
                        break;
                    case 1:
                        std::cout << rel.columns[col][row].reveal<int>() << "\t";
                        break;
                    case 2:
                        std::cout << decode_double(rel.columns[col][row]) << "\t";
                        break;
                    case 3:
                        std::cout << encoder.decode(rel.columns[col][row].reveal<int>()) << "\t";
                        break;
                }
            
            } std::cout << "| Flag: " << rel.flags[row].reveal<int>() << "\n";
        }
    
    } std::cout << "True size of the result: " << true_size << std::endl;
        
}



void tpch_q5_prev_ops(std::map<string, SecureRelation*> rels_dict, GlobalStringEncoder encoder, std::map<string,std::vector<int>> schema_dict){
    
    planNode *testNode1 = new planNode(rels_dict["customer"]);
    testNode1->node_name = "rel1";
    

    planNode *testNode2 = new planNode(rels_dict["orders"]);
    testNode2->node_name = "rel2";

    planNode *testNode3 = new planNode(rels_dict["lineitem"]);
    testNode3->node_name = "rel3";

    planNode *testNode4 = new planNode(rels_dict["nation"]);
    testNode4->node_name = "rel4";

    string filter_val = "JORDAN";
    uint32_t id = encoder.encode(filter_val);
    FilterOperator* filter_op1 = new FilterOperator(1, Integer(32, id, PUBLIC), "eq");
    planNode *filterNode1 = new planNode(filter_op1, 1);
    filterNode1->node_name = "filter_1";
    filterNode1->set_previous(*testNode4, 1);

    EquiJoinOperator* jOp1 = new EquiJoinOperator(0, 1);
    planNode* jNode1 = new planNode(jOp1, testNode1, testNode2, 1);
    jNode1->node_name = "join_1";
    jNode1->set_previous(*testNode1, 1);
    jNode1->set_previous(*testNode2, 2);

    

    EquiJoinOperator* jOp2 = new EquiJoinOperator(0, 0);
    planNode* jNode2 = new planNode(jOp2, testNode2, testNode3, 1);
    jNode2->node_name = "join_2";
    jNode2->set_previous(*testNode2, 1);
    jNode2->set_previous(*testNode3, 2);

    EquiJoinOperator* jOp3 = new EquiJoinOperator(3, 0);
    planNode* jNode3 = new planNode(jOp3, jNode2, filterNode1, 1);
    jNode3->node_name = "join_3";
    jNode3->set_previous(*jNode2, 1);
    jNode3->set_previous(*filterNode1, 2);



    //Record runtime
    auto start_time = std::chrono::high_resolution_clock::now();
    SecureRelation* query_output;
    for(int i= 0; i < 10; i++){
        query_output = jNode2->get_output();
    }
    //(*query_output).print_relation("Testing query output: ");
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration_filter_by_fixed_value = std::chrono::duration_cast<std::chrono::milliseconds>((end_time - start_time)).count();
    auto duration_us = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count()/10;
    std::vector<string> relnames = {"orders", "lineitem"};//{"customer", "orders", "lineitem", "nation"};
    decode_and_print_relation(*query_output, encoder, schema_dict, relnames);
    std::cout << "String Join query runtime oblivious case: " << duration_us << " micro_sec" << std::endl;
}


int main(int argc, char** argv) {

    // 1. Get port and party information from user
    // do not completely understand this part but using it as black box for now
    int port, party;
    parse_party_and_port(argv, &party, &port);

    //only run preprocessing code for one party
    if (party==ALICE){
        PreProc preprocessor;
        preprocessor.create_public_mapping();
    }

    NetIO* io = new NetIO(party == ALICE ? nullptr : "127.0.0.1", port);
    setup_semi_honest(io, party);


    //2. Create and initialize a large relation
    //const int num_cols = 3;  // 4 columns
    //const int num_rows = 5;
    //int alice_rows = 3;
    //int bob_rows = num_rows - alice_rows;
    //std::cout << alice_rows << ", " << bob_rows << endl;
    //const int num_rows = 1 << 12;  // Around a million rows

    std::vector<std::string> tpch_tables = {
    "customer",
    "orders",
    "lineitem",
    "supplier",
    "nation",
    "region",
    "part",
    "partsupp"
    };  

    std::vector<int> tpch_numcols = {
    8,
    9,
    16,
    7,
    4,
    3,
    9,
    5
    }; 

    // how many rows does Alice contribute
    std::vector<int> alice_rows = {
    NUM_OF_ROWS,
    NUM_OF_ROWS,
    NUM_OF_ROWS,
    NUM_OF_ROWS,
    13,
    5,
    NUM_OF_ROWS,
    NUM_OF_ROWS
    };
    
    // how many rows does Bob contribute
    std::vector<int> bob_rows = {
    NUM_OF_ROWS,
    NUM_OF_ROWS,
    NUM_OF_ROWS,
    NUM_OF_ROWS,
    12,
    5,
    NUM_OF_ROWS,
    NUM_OF_ROWS
    };

    
    //3. Get relations
    std::map<string, SecureRelation*> rels_dict;
    std::map<string,std::vector<int>> schema_dict;
    for(int i =0; i < 8; i++){
        rels_dict[tpch_tables[i]] = new SecureRelation(tpch_numcols[i], 0);
        std::vector<int> schema;
        schema_dict[tpch_tables[i]] = schema;
    }
        
    
    GlobalStringEncoder encoder;
    get_public_string_encoding(encoder);
    get_relations(party, tpch_numcols, alice_rows, bob_rows, rels_dict, encoder, schema_dict);
    

    //4. Build DP System Catalog (Use DPOptimizer to add noise to counts)
    *std::map<string, std::vector<Stats>> rel_stats_dict;
    for(int i = 0; i < 8; i++){
        string relname = tpch_tables[i];
        rel_stats_dict[relname] = std::vector<Stats>(tpch_numcols[i]);
    }
    get_noisy_relation_statistics(rels_dict, 4, party, rel_stats_dict);

    //7. run query

    // previous ops
    std::cout << "TPCH Q5 Oblivious run: " << endl;
    tpch_q5_prev_ops(rels_dict, encoder, schema_dict);

    std::cout << "TPCH Q5 DP run: " << endl;
    tpch_q5_prev_ops(rels_dict, encoder, schema_dict);
  
    io->flush();




    delete io;
    return 0;
}



// example code for decoding a double value
//rels_dict["customer"]->print_relation("Customer relation: ");
    /*Integer encrypted_int64 = rels_dict["customer"]->columns[5][0];
    long long unencrypted = encrypted_int64.reveal<int64_t>();
    double true_float;
    memcpy(&true_float, &unencrypted, sizeof(double));
    std::cout << true_float << "\n";*/

// Single join operator used to sanity check 
//whether query responses remain correct after converting data to all integers
/*EquiJoinOperator* jOp2 = new EquiJoinOperator(8, 2);
    planNode* jNode2 = new planNode(jOp2, jNode1, testNode3, 1);
    jNode2->node_name = "join_2";
    jNode2->set_previous(*jNode1, 1);
    jNode2->set_previous(*testNode3, 2);*/
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

#define NUM_OF_ROWS 20
void get_relations(int party, std::vector<int> num_cols, std::vector<int> alice_rows, std::vector<int> bob_rows, std::map<string, SecureRelation*> &rels_dict, GlobalStringEncoder &encoder){
    std::vector<std::string> tpch_tables = {
    "customer",
    "orders",
    "lineitem",
    "supplier",
    "nation",
    "region",
    "part",
    "partsupp"
    };  
    auto start_time = std::chrono::high_resolution_clock::now();
    string relname;
    for(int i = 0; i < 8; i++){
        relname = tpch_tables[i];
        std::string fname = party==BOB ? "tpch_data/bob_" +relname +".csv": "tpch_data/alice_" + relname +".csv";
        ScanOperator s = ScanOperator(fname, num_cols[i], alice_rows[i], bob_rows[i], encoder);
        s.delimiter = '|';
        s.execute(*rels_dict[relname], party); // get histogram also, non-emp
        rels_dict[relname]->print_relation("Testing Scanner: \n");
    }
   
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration_scan = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    std::cout << "Time taken to scan relations: " << duration_scan << " ms" << std::endl;
}

void get_relations2(int party, std::vector<int> num_cols, std::vector<int> alice_rows, std::vector<int> bob_rows, std::map<string, SecureRelation*> &rels_dict, GlobalStringEncoder &encoder){
    std::vector<std::string> tpch_tables = {
    "customer",
    "orders",
    "lineitem",
    "supplier",
    "nation",
    "region",
    "part",
    "partsupp"
    };  
    auto start_time = std::chrono::high_resolution_clock::now();
    string relname;
    for(int i = 0; i < 5; i++){
        relname = tpch_tables[i];
        std::string fname = party==BOB ? "tpch_data/bob_" +relname +".csv": "tpch_data/alice_" + relname +".csv";  
        ScanOperator s = ScanOperator(fname, num_cols[i], alice_rows[i], bob_rows[i], encoder);
        s.delimiter = '|';
        s.execute(*rels_dict[relname], party); // get histogram also, non-emp
        //rels_dict[relname]->print_relation("Testing Scanner: \n");
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



void q1_prev_ops(std::map<string, SecureRelation*> rels_dict, GlobalStringEncoder encoder){
    planNode *testNode1 = new planNode(rels_dict["customer"]);
    testNode1->node_name = "rel1";
    rels_dict["customer"]->print_relation("Customer relation: ");
    Integer encrypted_int64 = rels_dict["customer"]->columns[5][0];
    long long unencrypted = encrypted_int64.reveal<int64_t>();
    double true_float;
    memcpy(&true_float, &unencrypted, sizeof(double));
    std::cout << true_float << "\n";
    

    planNode *testNode2 = new planNode(rels_dict["orders"]);
    testNode2->node_name = "rel2";
    rels_dict["orders"]->print_relation("Orders relation: ");

    planNode *testNode3 = new planNode(rels_dict["lineitem"]);
    testNode3->node_name = "rel3";

    planNode *testNode4 = new planNode(rels_dict["nation"]);
    testNode4->node_name = "rel4";

    string filter_val = "JORDAN";
    uint32_t id = encoder.encode(filter_val);
    FilterOperator* filter_op1 = new FilterOperator(1, Integer(32, id, PUBLIC), "eq");
    planNode *filterNode1 = new planNode(filter_op1, 1);
    filterNode1->node_name = "filter_1";
    filterNode1->set_previous(*testNode4, 1);

    EquiJoinOperator* jOp1 = new EquiJoinOperator(0, 1);
    planNode* jNode1 = new planNode(jOp1, testNode1, testNode2, 1);
    jNode1->node_name = "join_1";
    jNode1->set_previous(*testNode1, 1);
    jNode1->set_previous(*testNode2, 2);



    //Record runtime
    auto start_time = std::chrono::high_resolution_clock::now();
    SecureRelation* query_output;
    for(int i= 0; i < 1; i++){
        query_output = jNode1->get_output();
    }
    //(*query_output).print_relation("Testing query output: ");
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration_filter_by_fixed_value = std::chrono::duration_cast<std::chrono::milliseconds>((end_time - start_time)).count();
    auto duration_us = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
    query_output->print_relation("TPC-H q5 c-o join query output:");
    std::cout << "String Join query runtime oblivious case: " << duration_us << " micro_sec" << std::endl;

}






int main(int argc, char** argv) {

    // 1. Get port and party information from user
    // do not completely understand this part but using it as black box for now
    int port, party;
    parse_party_and_port(argv, &party, &port);

    NetIO* io = new NetIO(party == ALICE ? nullptr : "127.0.0.1", port);
    setup_semi_honest(io, party);


    //2. Create and initialize a large relation
    //const int num_cols = 3;  // 4 columns
    //const int num_rows = 5;
    //int alice_rows = 3;
    //int bob_rows = num_rows - alice_rows;
    //std::cout << alice_rows << ", " << bob_rows << endl;
    //const int num_rows = 1 << 12;  // Around a million rows

    std::vector<std::string> tpch_tables = {
    "customer",
    "orders",
    "lineitem",
    "supplier",
    "nation",
    "region",
    "part",
    "partsupp"
    };  

    std::vector<int> tpch_numcols = {
    8,
    9,
    16,
    7,
    4,
    3,
    9,
    5
    }; 

    // how many rows does Alice contribute
    std::vector<int> alice_rows = {
    NUM_OF_ROWS,
    NUM_OF_ROWS,
    NUM_OF_ROWS,
    NUM_OF_ROWS,
    13,
    5,
    NUM_OF_ROWS,
    NUM_OF_ROWS
    };
    
    // how many rows does Bob contribute
    std::vector<int> bob_rows = {
    NUM_OF_ROWS,
    NUM_OF_ROWS,
    NUM_OF_ROWS,
    NUM_OF_ROWS,
    12,
    5,
    NUM_OF_ROWS,
    NUM_OF_ROWS
    };


    //3. Get relations
    std::map<string, SecureRelation*> rels_dict;
    for(int i =0; i < 8; i++){
        rels_dict[tpch_tables[i]] = new SecureRelation(tpch_numcols[i], 0);
    }
        
    GlobalStringEncoder encoder;
    get_relations2(party, tpch_numcols, alice_rows, bob_rows, rels_dict, encoder);
    

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
    q1_prev_ops(rels_dict, encoder);
  
    io->flush();




    delete io;
    return 0;
}



