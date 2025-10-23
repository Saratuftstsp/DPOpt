// equijoin.hpp

#ifndef EQUIJOIN_SYSCAT_OPERATOR_HPP
#define EQUIJOIN_SYSCAT_OPERATOR_HPP

#include "core/_op_binary.hpp"
#include <vector>
#include <iostream>
#include <map>
#include <algorithm>
#include "core/stats.hpp"

using namespace std;


class EquiJoinOperatorSyscat : public BinaryOperator {
public:
    int column_index1;  // The join column index for the first relation
    int column_index2;  // The join column index for the second relation
    float selectivity = 1;
    Stats* stats1 = nullptr;
    Stats* stats2 = nullptr;

    EquiJoinOperatorSyscat(int col_idx1, int col_idx2);

    // Construction changed to also take in a Statistics object pointer
    EquiJoinOperatorSyscat(int col_idx1, int col_idx2, Stats* statistics1, Stats* statistics2);

    void set_stats(Stats* stats1, Stats* stats2);

    
protected: //Why was this made protected? ---> Need to ask Prof Wang
    SecureRelation operation(const SecureRelation& rel1, const SecureRelation& rel2) override;

    void get_stat();

    SecureRelation prune(SecureRelation rel);


};

// Definitions
EquiJoinOperatorSyscat::EquiJoinOperatorSyscat(int col_idx1, int col_idx2): column_index1(col_idx1), column_index2(col_idx2){}
EquiJoinOperatorSyscat::EquiJoinOperatorSyscat(int col_idx1, int col_idx2, Stats* statistics1, Stats* statistics2) 
    : column_index1(col_idx1), column_index2(col_idx2), stats1(statistics1), stats2(statistics2) { if (stats1!=nullptr && stats2!=nullptr) {this-> get_stat();}}

SecureRelation EquiJoinOperatorSyscat::operation(const SecureRelation& rel1, const SecureRelation& rel2) {
    //std::cout << "Calling prune for join.\n";
    int result_rows = rel1.columns[0].size() * rel2.columns[0].size();
    int result_cols = rel1.columns.size() + rel2.columns.size();

    SecureRelation result(result_cols, result_rows);

    for (int i = 0; i < rel1.columns[0].size(); i++) {
        for (int j = 0; j < rel2.columns[0].size(); j++) {
            int result_row_index = i * rel2.columns[0].size() + j;
            
            // Copy the columns from the first relation
            for (int k = 0; k < rel1.columns.size(); k++) {
                result.columns[k][result_row_index] = rel1.columns[k][i];
            }

            // Copy the columns from the second relation
            for (int k = 0; k < rel2.columns.size(); k++) {
                result.columns[rel1.columns.size() + k][result_row_index] = rel2.columns[k][j];
            }

            // Set the join flag. 1 if the join condition is satisfied, 0 otherwise.
            emp::Bit join_condition = (rel1.columns[column_index1][i] == rel2.columns[column_index2][j]) 
                                      & (rel1.flags[i] == emp::Integer(1, 1, ALICE)) 
                                      & (rel2.flags[j] == emp::Integer(1, 1, ALICE));
                                      
            result.flags[result_row_index] = emp::If(join_condition, emp::Integer(1, 1, ALICE), emp::Integer(1, 0, ALICE));
        }
    }


    return this->prune(result);
}

void EquiJoinOperatorSyscat::get_stat(){
    // 1. Check if n_distinct1 and n_distinct2 are different from the cardinalities of rel1 and rel2
    int n_dist1 = stats1->ndistinct;
    int num_rows1 = stats1->num_rows;
    int n_dist2 = stats2->ndistinct;
    int num_rows2 = stats2->num_rows;

    //2. If the n_distincts are significantly lower than cardinalities, 
    // then go through and find fraction of cross product 
    // for each distinct value and add it to selectivity
    if ((n_dist1 < 0.5*num_rows1) || (n_dist2 < 0.5*num_rows2)){
        // sample "enough" times to get bar chart / histogram. ---> look up "sample complexity" (look in the probability Alice book)
        // for now, get the exact bar-chart/histogram
        float new_selectivity_num_rows = 0;
        auto mcv1 = stats1->mcv;
        auto mcv2 = stats2->mcv;
        auto mcf1 = stats1->mcf;
        auto mcf2 = stats2->mcf;
        auto rel1_num_common_vals = mcv1.size();
        auto rel2_num_common_vals = mcv2.size();
        auto rel1_total_rows_counted = 0;
        auto rel2_total_rows_counted = 0;
        /*for(int i = 0; i < rel1_num_common_vals; i++){
            std::cout << mcv1.at(i) << ", ";
        }std::cout << "\n";
        for(int i = 0; i < rel2_num_common_vals; i++){
            std::cout << mcv2.at(i) << ", ";
        }std::cout << "\n";*/
        //3. Combine the two histograms to find the total selectivity as a fraction of the cross product cardinality
        for(int i = 0; i < rel1_num_common_vals; i++){
            auto key = stats1->mcv.at(i);
            auto idx = find(mcv2.begin(), mcv2.end(), key);
            if (idx != mcv2.end()){
                rel1_total_rows_counted += mcf1.at(i);
                //std::cout << rel1_total_rows_counted << ", ";
                rel2_total_rows_counted += mcf2.at(idx - mcv2.begin());
                //std::cout << rel2_total_rows_counted << "\n";
                new_selectivity_num_rows += mcf1.at(i) * mcf2.at(idx - mcv2.begin());
            }
        }
        float new_selectivity = new_selectivity_num_rows/(num_rows1 * num_rows2);
        std::cout << new_selectivity << "\n";
        if (0 < new_selectivity){
            selectivity = new_selectivity;
        }
    }
}

SecureRelation EquiJoinOperatorSyscat::prune(SecureRelation rel){
    //std::cout << "Calling prune for join.\n";
    rel.sort_by_flag();
    //rel.print_relation("After sorting but before pruning: \n");
    //this->get_stat();
    cout << selectivity << "\n";
    if (selectivity < 1){
        int num_rows1 = stats1->num_rows;
        int num_rows2 = stats2->num_rows;
        int num_cols = rel.columns.size();
        float cross_product_size = (num_rows1 * num_rows2);
        float start_of_prune = selectivity * cross_product_size + 1 ;
        if (cross_product_size > rel.flags.size()){ start_of_prune = ceil(selectivity * rel.flags.size());}
        SecureRelation pruned_rel(num_cols, start_of_prune);
        //std::cout << "Selectivity: " << selectivity << "\n";
        //std::cout << "Start of Prune: " << start_of_prune << "\n";
        for(int i = 0; i < num_cols; i++){
            for(int j=0; j < start_of_prune; j++){
                pruned_rel.columns[i][j] = rel.columns[i][j];
                //std::cout << i << ", " << j << "\n";
            }
        }
        for(int j=0; j < start_of_prune; j++){
                pruned_rel.flags[j] = rel.flags[j];
        }
        //std::cout << "Returning from prune for join.\n";
        return pruned_rel;
    }
    //std::cout << "Returning from prune for join.\n";
    return rel;
}

void EquiJoinOperatorSyscat::set_stats(Stats* s1, Stats* s2){
    stats1 = s1;
    stats2 = s2;
}


#endif // EQUIJOIN_HPP
