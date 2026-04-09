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

#define NUM_OF_ROWS 100

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
    for(int i = 0; i < tpch_tables.size(); i++){
        relname = tpch_tables[i];
        std::string fname = party==BOB ? "tpch_data/bob_" +relname +".csv": "tpch_data/alice_" + relname +".csv";  
        ScanOperator s = ScanOperator(fname, num_cols[i], alice_rows[i], bob_rows[i], encoder);
        s.delimiter = '|';
        s.execute(*rels_dict[relname], party); // get histogram also, non-emp
        schema_dict[relname] = rels_dict[relname]->col_types;
    }
   
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration_scan = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    std::cout << "Time taken to scan relations: " << duration_scan << " ms" << std::endl;
}

void load_domains(std::map<std::string, std::vector<Stats>>& rel_stats_dict,const std::vector<std::string>& tpch_tables){
    for (const std::string& relname : tpch_tables) {
        std::vector<Stats> rel_stats_vec = rel_stats_dict[relname];
        int num_cols = rel_stats_vec.size();

        for (size_t col = 0; col < num_cols; col++) {
            std::vector<int> col_domain = rel_stats_vec[col].domain;
            std::string fname = "./tpch_data/domains/" +  relname + "_col_" + std::to_string(col) + ".txt";

            std::ifstream file(fname);

            if (!file) {
                std::cerr << "Error opening domain file: " << fname << std::endl;
                continue;
            }

            std::string line;

            while (std::getline(file, line)) {
                if(line.empty()) continue;
                int value = std::stoi(line);
                rel_stats_dict[relname][col].domain.push_back(value);
            }

            file.close();
        }
    }
}

void get_noisy_relation_statistics(std::map<string, SecureRelation*> &rels_dict, std::map<string, int> num_cols_dict, int party, std::map<string, std::vector<Stats>> &noisy_rel_stats){
    DPOptimizer dpopt;
    for (const auto& pair : rels_dict) {
        const std::string& relname = pair.first;
        SecureRelation* rel = pair.second;
        dpopt.dpanalyze(num_cols_dict[relname], rel->columns, noisy_rel_stats[relname], party);
        std::cout << relname << endl;
        std::cout << noisy_rel_stats[relname][0].mcf_priv.size() << endl;
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
            new_schema.push_back(rel_schema[i]);
        }
    }
    std::cout << "Custom Query Result:" << "\n";
    int true_size = 0;
    for (size_t row = 0; row < rel.columns[0].size(); row++) {
        int flag = rel.flags[row].reveal<int>();
        if (flag){
            true_size++;
            for (size_t col = 0; col < rel.columns.size(); col++) {
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
                        std::cout << encoder.decode(rel.columns[col][row].reveal<uint32_t>()) << "\t";
                        break;
                }
            
            } std::cout << "| Flag: " << rel.flags[row].reveal<int>() << "\n";
        }
    
    } std::cout << "True size of the result: " << true_size << std::endl;
}
// SELECT *
// FROM customer c
// JOIN orders o   ON c.c_custkey = o.o_custkey
// JOIN lineitem l ON o.o_orderkey = l.l_orderkey
// JOIN supplier s ON l.l_suppkey = s.s_suppkey
// JOIN nation n   ON s.s_nationkey = n.n_nationkey
// JOIN region r   ON n.n_regionkey = r.r_regionkey
// WHERE r.r_name = 'ASIA'
//   AND o.o_orderdate >= DATE '1996-01-01'
//   AND l.l_returnflag = 'A'
void query3_ops(std::map<string, SecureRelation*> rels_dict, GlobalStringEncoder encoder, std::map<string, std::vector<int>> schema_dict){
    // 1. Initialize base relation nodes
    planNode *customerNode = new planNode(rels_dict["customer"]);
    customerNode->node_name = "rel_customer";

    planNode *ordersNode = new planNode(rels_dict["orders"]);
    ordersNode->node_name = "rel_orders";

    planNode *lineitemNode = new planNode(rels_dict["lineitem"]);
    lineitemNode->node_name = "rel_lineitem";

    planNode *supplierNode = new planNode(rels_dict["supplier"]);
    supplierNode->node_name = "rel_supplier";

    planNode *nationNode = new planNode(rels_dict["nation"]);
    nationNode->node_name = "rel_nation";

    planNode *regionNode = new planNode(rels_dict["region"]);
    regionNode->node_name = "rel_region";

    // 2. Filters
    // Region filter: r_name = 'ASIA' (Column index 1)
    FilterOperator* filter_op_region = new FilterOperator(1, Integer(32, encoder.encode("ASIA"), PUBLIC), "eq");
    planNode *filterNode_region = new planNode(filter_op_region, 1);
    filterNode_region->node_name = "filter_r_name";
    filterNode_region->set_previous(*regionNode, 1);

    // Orders filter: o_orderdate >= '1996-01-01' (Column index 4)
    FilterOperator* filter_op_orders = new FilterOperator(4, Integer(32, encoder.encode("1996-01-01"), PUBLIC), "geq");
    planNode *filterNode_orders = new planNode(filter_op_orders, 1);
    filterNode_orders->node_name = "filter_o_orderdate";
    filterNode_orders->set_previous(*ordersNode, 1);

    // Lineitem filter: l_returnflag = 'A' (Column index 8)
    FilterOperator* filter_op_lineitem = new FilterOperator(8, Integer(32, encoder.encode("A"), PUBLIC), "eq");
    planNode *filterNode_lineitem = new planNode(filter_op_lineitem, 1);
    filterNode_lineitem->node_name = "filter_l_returnflag";
    filterNode_lineitem->set_previous(*lineitemNode, 1);

    // 3. Joins
    // Join 1: customer JOIN filtered orders on c_custkey (idx 0) == o_custkey (idx 1)
    EquiJoinOperator* jOp1 = new EquiJoinOperator(0, 1);
    planNode* jNode1 = new planNode(jOp1, customerNode, filterNode_orders, 1);
    jNode1->node_name = "join_cust_orders";
    jNode1->set_previous(*customerNode, 1);
    jNode1->set_previous(*filterNode_orders, 2);

    // Join 2: Output1 JOIN filtered lineitem on o_orderkey == l_orderkey
    // Left side (jNode1) schema: 8 (cust) + 9 (orders) = 17 cols.
    // o_orderkey was index 0 in orders -> shifts to 8 + 0 = 8.
    // Right side (lineitem): l_orderkey is index 0.
    EquiJoinOperator* jOp2 = new EquiJoinOperator(8, 0);
    planNode* jNode2 = new planNode(jOp2, jNode1, filterNode_lineitem, 1);
    jNode2->node_name = "join_c_o_lineitem";
    jNode2->set_previous(*jNode1, 1);
    jNode2->set_previous(*filterNode_lineitem, 2);

    // Join 3: Output2 JOIN supplier on l_suppkey == s_suppkey
    // Left side (jNode2) schema: 17 + 16 (lineitem) = 33 cols.
    // l_suppkey was index 2 in lineitem -> shifts to 17 + 2 = 19.
    // Right side (supplier): s_suppkey is index 0.
    EquiJoinOperator* jOp3 = new EquiJoinOperator(19, 0);
    planNode* jNode3 = new planNode(jOp3, jNode2, supplierNode, 1);
    jNode3->node_name = "join_c_o_l_supplier";
    jNode3->set_previous(*jNode2, 1);
    jNode3->set_previous(*supplierNode, 2);

    // Join 4: Output3 JOIN nation on s_nationkey == n_nationkey
    // Left side (jNode3) schema: 33 + 7 (supplier) = 40 cols.
    // s_nationkey was index 3 in supplier -> shifts to 33 + 3 = 36.
    // Right side (nation): n_nationkey is index 0.
    EquiJoinOperator* jOp4 = new EquiJoinOperator(36, 0);
    planNode* jNode4 = new planNode(jOp4, jNode3, nationNode, 1);
    jNode4->node_name = "join_all_nation";
    jNode4->set_previous(*jNode3, 1);
    jNode4->set_previous(*nationNode, 2);

    // Join 5: Output4 JOIN filtered region on n_regionkey == r_regionkey
    // Left side (jNode4) schema: 40 + 4 (nation) = 44 cols.
    // n_regionkey was index 2 in nation -> shifts to 40 + 2 = 42.
    // Right side (region): r_regionkey is index 0.
    EquiJoinOperator* jOp5 = new EquiJoinOperator(42, 0);
    planNode* jNode5 = new planNode(jOp5, jNode4, filterNode_region, 1);
    jNode5->node_name = "join_all_region";
    jNode5->set_previous(*jNode4, 1);
    jNode5->set_previous(*filterNode_region, 2);

    // 8. Execute query and record runtime
    auto start_time = std::chrono::high_resolution_clock::now();
    SecureRelation* query_output = jNode5->get_output();
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration_us = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();

    // 9. Decode and print result (ordered exactly as joined)
    std::vector<string> relnames = {"customer", "orders", "lineitem", "supplier", "nation", "region"};
    decode_and_print_relation(*query_output, encoder, schema_dict, relnames);
    std::cout << "Custom 6-Way Join Query runtime: " << duration_us << " micro_sec" << std::endl;
}

void query3_dp_ops(std::map<string, SecureRelation*> rels_dict, GlobalStringEncoder encoder, std::map<string, std::vector<int>> schema_dict, std::map<string, std::vector<Stats>>& rel_stats_dict) {
    planNode *customerNode = new planNode(rels_dict["customer"]);
    planNode *ordersNode = new planNode(rels_dict["orders"]);
    planNode *lineitemNode = new planNode(rels_dict["lineitem"]);
    planNode *supplierNode = new planNode(rels_dict["supplier"]);
    planNode *nationNode = new planNode(rels_dict["nation"]);
    planNode *regionNode = new planNode(rels_dict["region"]);

    // DP Filters
    FilterOperatorSyscat* filter_op_region = new FilterOperatorSyscat(1, Integer(32, encoder.encode("ASIA"), PUBLIC), "eq", &rel_stats_dict["region"][1]);
    planNode *filterNode_region = new planNode(filter_op_region, filter_op_region->selectivity);
    filterNode_region->set_previous(*regionNode, 1);

    FilterOperatorSyscat* filter_op_orders = new FilterOperatorSyscat(4, Integer(32, encoder.encode("1996-01-01"), PUBLIC), "geq", &rel_stats_dict["orders"][4]);
    planNode *filterNode_orders = new planNode(filter_op_orders, filter_op_orders->selectivity);
    filterNode_orders->set_previous(*ordersNode, 1);

    FilterOperatorSyscat* filter_op_lineitem = new FilterOperatorSyscat(8, Integer(32, encoder.encode("A"), PUBLIC), "eq", &rel_stats_dict["lineitem"][8]);
    planNode *filterNode_lineitem = new planNode(filter_op_lineitem, filter_op_lineitem->selectivity);
    filterNode_lineitem->set_previous(*lineitemNode, 1);

    // DP Joins
    EquiJoinOperatorSyscat* jOp1 = new EquiJoinOperatorSyscat(0, 1, &rel_stats_dict["customer"][0], &rel_stats_dict["orders"][1]);
    std::cout << "First join selectivity: " << jOp1->selectivity << endl;
    planNode* jNode1 = new planNode(jOp1, customerNode, filterNode_orders, jOp1->selectivity);
    jNode1->set_previous(*customerNode, 1);
    jNode1->set_previous(*filterNode_orders, 2);

    EquiJoinOperatorSyscat* jOp2 = new EquiJoinOperatorSyscat(2, 0, &rel_stats_dict["lineitem"][2], &rel_stats_dict["supplier"][0]);
    std::cout << "Second join selectivity: " << jOp2->selectivity << endl;
    planNode* jNode2 = new planNode(jOp2, filterNode_lineitem, supplierNode, jOp2->selectivity);
    jNode2->set_previous(*filterNode_lineitem, 1);
    jNode2->set_previous(*supplierNode, 2);
    
    EquiJoinOperatorSyscat* jOp3 = new EquiJoinOperatorSyscat(8, 0, &rel_stats_dict["orders"][0], &rel_stats_dict["lineitem"][0]);
    std::cout << "Third join selectivity: " << jOp3->selectivity << endl;
    planNode* jNode3 = new planNode(jOp3, jNode1, jNode2, jOp3->selectivity);
    jNode3->set_previous(*jNode1, 1);
    jNode3->set_previous(*jNode2, 2);
    
    EquiJoinOperatorSyscat* jOp4 = new EquiJoinOperatorSyscat(36, 0, &rel_stats_dict["supplier"][3], &rel_stats_dict["nation"][0]);
    std::cout << "Fourth join selectivity: " << jOp4->selectivity << endl;
    planNode* jNode4 = new planNode(jOp4, jNode3, nationNode, jOp4->selectivity);
    jNode4->set_previous(*jNode3, 1);
    jNode4->set_previous(*nationNode, 2);

    EquiJoinOperatorSyscat* jOp5 = new EquiJoinOperatorSyscat(42, 0, &rel_stats_dict["nation"][2], &rel_stats_dict["region"][0]);
    std::cout << "Fifth join selectivity: " << jOp5->selectivity << endl;
    planNode* jNode5 = new planNode(jOp5, jNode4, filterNode_region, jOp5->selectivity);
    jNode5->set_previous(*jNode4, 1);
    jNode5->set_previous(*filterNode_region, 2);

    // Uncomment below to execute actual output computation 
    // auto start_time = std::chrono::high_resolution_clock::now();
    // SecureRelation* query_output = jNode5->get_output();
    // auto end_time = std::chrono::high_resolution_clock::now();
    // auto duration_us = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();

    // std::vector<string> relnames = {"customer", "orders", "lineitem", "supplier", "nation", "region"};
    // decode_and_print_relation(*query_output, encoder, schema_dict, relnames);
    // std::cout << "Custom Query DP runtime: " << duration_us << " micro_sec" << std::endl;
}

void get_public_string_encoding(GlobalStringEncoder &encoder){
    std::string file_name = "string_mapping.csv"; 
    std::ifstream file(file_name);

    if (!file.is_open()) {
        std::cerr << "Error opening file: " << file_name << std::endl;
        return;
    }

    std::string line;
    while (std::getline(file, line)) {
        std::stringstream ss(line);
        std::string int_str, string_val;

        if (std::getline(ss, int_str, '|')) {
            if (std::getline(ss, string_val)) {
                encoder.encode(string_val);
            }
        }
    }
    file.close();
}


int main(int argc, char** argv) {

    // 1. Get port and party information from user
    int port, party;
    parse_party_and_port(argv, &party, &port);

    NetIO* io = new NetIO(party == ALICE ? nullptr : "127.0.0.1", port);
    setup_semi_honest(io, party);

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

    std::vector<int> alice_rows = {
    NUM_OF_ROWS,
    NUM_OF_ROWS,
    NUM_OF_ROWS,
    NUM_OF_ROWS,
    13,
    3,
    NUM_OF_ROWS,
    NUM_OF_ROWS
    };
    
    std::vector<int> bob_rows = {
    NUM_OF_ROWS,
    NUM_OF_ROWS,
    NUM_OF_ROWS,
    NUM_OF_ROWS,
    12,
    2,
    NUM_OF_ROWS,
    NUM_OF_ROWS
    };

    //3. Get relations
    std::map<string, SecureRelation*> rels_dict;
    std::map<string, std::vector<Stats>> rel_stats_dict;
    std::map<string, std::vector<int>> schema_dict;
    std::map<string, int> tpch_numcols_dict;

    for(int i =0; i < 8; i++){
        string relname = tpch_tables[i];
        int numcols = tpch_numcols[i];
        tpch_numcols_dict[relname] = numcols;
        rels_dict[relname] = new SecureRelation(numcols, 0);
        rel_stats_dict[relname] = std::vector<Stats>(numcols);
        std::vector<int> schema;
        schema_dict[tpch_tables[i]] = schema;
    }
        
    GlobalStringEncoder encoder;
    get_public_string_encoding(encoder);
    get_relations(party, tpch_numcols, alice_rows, bob_rows, rels_dict, encoder, schema_dict);
    
    load_domains(rel_stats_dict, tpch_tables);
    
    //7. run query
    // To execute without DP, you can call: 
    // query3_ops(rels_dict, encoder, schema_dict);
    for(int i = 0; i < 10; i++){
        std::cout << "\nNoise addition iteration " << i << ": " << endl;
        // Inject privacy noise into catalog
        get_noisy_relation_statistics(rels_dict, tpch_numcols_dict, party, rel_stats_dict);
        
        std::cout << "DP run " << i << ": "<< endl;
        
        query3_dp_ops(rels_dict, encoder, schema_dict, rel_stats_dict); 
    }
  
    io->flush();

    delete io;
    return 0;
}