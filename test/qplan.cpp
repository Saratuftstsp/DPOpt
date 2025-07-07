#include "emp-sh2pc/emp-sh2pc.h"
#include "core/relation.hpp"
#include "core/op_filter.hpp"
//#include "core/op_filter_syscat.hpp"
#include <iostream>
#include <chrono>

#include "core/op_equijoin.hpp"
#include "core/op_idx_equijoin.hpp"  // Including the new header file
#include "core/stats.hpp"
#include <map>
#include "core/plan_node.hpp"


using namespace emp;



// Utility function to initialize a relation with random values
void init_relation(SecureRelation& relation, std::vector<Stats>& rel_stats, int num_cols, int num_rows) {
    // for each column, 
    // generate row number of values
    // and track frequencies of unique vals
    for (int col = 0; col < num_cols; ++col) {
        std::map<int, int> myMap;
        for (int row = 0; row < num_rows; ++row) {
            int val = rand() % 10;
            if (myMap.count(val) == 0) {myMap[val] = 0;}
            myMap[val] += 1;
            relation.columns[col][row] = Integer(32, val, ALICE);  // Random values
        }
        // construct Stats struct for this column, using map of unqieu values and their frequencies
        Stats s;
        s.column_index = col;
        std::vector<float> mcv;
        std::vector<float> mcf;
        for (const auto& [key, value] : myMap) {
            mcv.push_back(key);
            mcf.push_back(value);
        }
        s.mcv = mcv;
        s.mcf = mcf;
        s.num_rows = num_rows;
        rel_stats.push_back(s);
    }

    for (int row = 0; row < num_rows; ++row) {
        relation.flags[row] = Integer(1, 1, ALICE);  // Set all flags to 1 initially
    }
}

int main(int argc, char** argv) {

    // 1. Get port and party information from user
    // do not completely understand this part but using it as black box for now
    int port, party;
    parse_party_and_port(argv, &party, &port);

    NetIO* io = new NetIO(party == ALICE ? nullptr : "127.0.0.1", port);
    setup_semi_honest(io, party);

    //2. Create and initialize a large relation
    const int num_cols = 3;  // 3 columns
    const int num_rows = 1 << 4;
    //const int num_rows = 1 << 12;  // Around a million rows

    SecureRelation rel1(num_cols, num_rows);
    std::vector<Stats> rel1_stats;
    init_relation(rel1, rel1_stats, num_cols, num_rows);
    std::vector<float> mcv = rel1_stats.at(0).mcv;
    std::vector<float> mcf = rel1_stats.at(0).mcf;
    float total = 0;
    for (int i = 0; i < mcv.size(); i++){
        total += mcf.at(i);
    } //std::cout << total << "\n";

    SecureRelation rel2(num_cols, num_rows);
    std::vector<Stats> rel2_stats;
    init_relation(rel2, rel2_stats, num_cols, num_rows);

    //auto start_time = std::chrono::high_resolution_clock::now();

    //A. Parse query - get Tree object of operators
    std::string query = "select * from rel1 join rel2 on rel1.col1 = rel2.col1 where rel1.col0 = 4 and rel2.col0 = 5;";
    std::map<std::string, std::string> queryElements; // don't know how to get this or what format it will be
    
    //A.1) previous implementation
    FilterOperator prev_f1(0, Integer(32,4,ALICE), "eq");
    SecureRelation prev_output = prev_f1.execute(rel1);
    prev_output.print_relation("Previous result:");

    //FilterOperatorSyscat f1(0,Integer(32,4,ALICE),"eq", &rel2_stats.at(0));
    //planNode select1(&f1, &rel1);

    
    //B. Pass tree into some function that reorders operator to find
    //   other trees, use heuristics, compute cost
    //   returns best plan

    
    //C. Executor recursively calls operate
    //SecureRelation output = select1.get_output();
    //output.print_relation("Executing filter operator as a planNode:");


    //3. Filter(s) based on a fixed value
    /*FilterOperatorSyscat filter_by_fixed_value1(0, Integer(32, 4, ALICE), "eq");
    //std::cout << "Before: " << filter_by_fixed_value1.selectivity << "\n";
    filter_by_fixed_value1.get_stat(rel1_stats.at(0));
    //std::cout << "After: " << filter_by_fixed_value1.selectivity << "\n";
    SecureRelation filtered_relation1 = filter_by_fixed_value1.execute(rel1);

    FilterOperatorSyscat filter_by_fixed_value2(0, Integer(32, 6, ALICE), "gt");
    filter_by_fixed_value1.get_stat(rel2_stats.at(0));
    SecureRelation filtered_relation2 = filter_by_fixed_value2.execute(rel2);*/

    //4. Join
    /*EquiJoinOperator join(1,1);
    SecureRelation join_res = join.execute(filtered_relation1, filtered_relation2);
    join_res.print_relation("Two filters on two tables, followed by a join, result:");

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration_filter_by_fixed_value = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    std::cout << "Filter by Fixed Value Time: " << duration_filter_by_fixed_value << " ms" << std::endl;*/
    io->flush();



    // Filter based on an input column (comparing first and second columns)
    /* FilterOperator filter_by_column(0, relation2.columns[1], "gt");
    SecureRelation filtered_relation2 = filter_by_column.execute(relation2);*/


    delete io;
    return 0;
}


/*SQL query:
select * from rel1, rel2 where rel1.col1<300 and rel1.col2=rel2.col2*/