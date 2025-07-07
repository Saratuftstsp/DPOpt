// equijoin.hpp

#ifndef EQUIJOIN_OPERATOR_HPP
#define EQUIJOIN_OPERATOR_HPP

#include "core/_op_binary.hpp"
#include <vector>

class EquiJoinOperatorSyscat : public BinaryOperator {
public:
    int column_index1;  // The join column index for the first relation
    int column_index2;  // The join column index for the second relation
    float selectivity = 1;
    Stats* stats1;
    Stats* stats2;

    // Construction changed to also take in a Statistics object pointer
    EquiJoinOperator(int col_idx1, int col_idx2, Stats* statistics1, Stats* statistics2);

protected: //Why was this made protected? ---> Need to ask Prof Wang
    SecureRelation operation(const SecureRelation& rel1, const SecureRelation& rel2) override;

    void get_stat();

    SecureRelation prune(SecureRelation rel);

};

// Definitions

EquiJoinOperatorSyscat::EquiJoinOperator(int col_idx1, int col_idx2, Stats* statistics1, Stats* statistics2) 
    : column_index1(col_idx1), column_index2(col_idx2), stats1(statistics1), stats2(statistics2) {}

SecureRelation EquiJoinOperatorSyscat::operation(const SecureRelation& rel1, const SecureRelation& rel2) {
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

    return result;
}

void EquiJoinOperatorSyscat::get_stat(){
    // 1. Check if n_distinct1 and n_distinct2 are different from the cardinalities of rel1 and rel2
    int n_dist1 = stats1->ndistinct;
    int num_rows1 = stats1->num_rows;
    int n_dist2 = stats2->ndistinct;
    int num_rows2 = stats2->num_rows2;
    if (n_dist1 < num_rows1){
        // sample "enough" times to get bar chart / histogram. ---> look up "sample complexity" (look in the probability Alice book)
        // for now, get the exact bar-chart/histogram

    }
    //2. If the n_distincts are lower than cardinalities, then go through the make a histogram for each list of distinct vals

    //3. Combine the two histograms to find the total selectivity as a fraction of the cross product cardinality
    
    
    std::vector<float> mcv1 = stats1->mcv;
    std::vector<float> mcf1 = stats1->mcf;

    for (int i = 0; i < 10; i++){
        int val = mcv.at(i);
        emp::Integer secure_val = Integer(32, val, ALICE);
        int cond_val = compare(secure_val, target_value, "eq").reveal<bool>();
        if (cond_val == 1){
            std::cout << mcf.at(i) << "\n";
            selectivity = mcf.at(i)/stats->num_rows; i = 11; // change selectivity and break out of loop
        }
    }
}


#endif // EQUIJOIN_HPP
