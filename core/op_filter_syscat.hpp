#ifndef FILTER_OPERATOR_SYSCAT_HPP
#define FILTER_OPERATOR_SYSCAT_HPP

#include "core/_op_unary.hpp"
#include "stats.hpp"
#include <string>
#include <iostream>

class FilterOperatorSyscat : public UnaryOperator {
public:
    int column_index;  // The index of the column on which the filter is applied
    emp::Integer target_value;  // A target value for comparison if it's not a column
    std::vector<emp::Integer> target_column; // A column for comparison, if applicable
    std::string condition;  // One of the conditions: "gt, geq, lt, leq, eq, neq"
    float selectivity = 1;
    Stats* stats;
    int party;


    emp::Bit compare(const emp::Integer& a, const emp::Integer& b, const std::string& condition);

    // Constructor when target is a single value
    FilterOperatorSyscat(int col_idx, const emp::Integer& target, const std::string& cnd);

    // Constructor when target is a column
    FilterOperatorSyscat(int col_idx, const std::vector<emp::Integer>& target_col, const std::string& cnd);
    
    // Constructor when target column is single value and there is Stats
    // Constructor when target is a column
    FilterOperatorSyscat(int col_idx, const emp::Integer& target, const std::string& cnd, Stats* stats);


    SecureRelation operation(const SecureRelation& input) override;

    void get_stat();

    SecureRelation prune(SecureRelation rel);

    void set_stats(Stats* stats);

};


FilterOperatorSyscat::FilterOperatorSyscat(int col_idx, const emp::Integer& target, const std::string& cnd) 
    : column_index(col_idx), target_value(target), condition(cnd) {}

FilterOperatorSyscat::FilterOperatorSyscat(int col_idx, const std::vector<emp::Integer>& target_col, const std::string& cnd) 
    : column_index(col_idx), target_column(target_col), condition(cnd) {}

FilterOperatorSyscat::FilterOperatorSyscat(int col_idx, const emp::Integer& target, const std::string& cnd, Stats* statistics)
    : column_index(col_idx), target_value(target), condition(cnd), stats(statistics) {
        this-> get_stat();
    }
    
emp::Bit FilterOperatorSyscat::compare(const emp::Integer& a, const emp::Integer& b, const std::string& condition) {
    if (condition == "gt") return a > b;
    if (condition == "geq") return a >= b;
    if (condition == "lt") return a < b;
    if (condition == "leq") return a <= b;
    if (condition == "eq") return a == b;
    if (condition == "neq") return a != b;
    return emp::Bit(false); // Default to false for unsupported conditions
}

SecureRelation FilterOperatorSyscat::operation(const SecureRelation& input) {
    auto start_time = std::chrono::high_resolution_clock::now();
    SecureRelation output = input; // Make a copy of the input relation
    
    if (target_column.empty()) { // If the target is a single value
        for (int i = 0; i < input.columns[0].size(); i++) {
            output.flags[i] = Integer(1, compare(input.columns[column_index][i], target_value, condition).reveal<bool>(), party);
        }
    } else { // If the target is a column
        for (int i = 0; i < input.columns[0].size(); i++) {
            output.flags[i] = Integer(1, compare(input.columns[column_index][i], target_column[i], condition).reveal<bool>(), party);
        }
    }
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration_filter_by_fixed_value = std::chrono::duration_cast<std::chrono::milliseconds>((end_time - start_time)).count();
    std::cout << "Runtime for individual operator "<< duration_filter_by_fixed_value/2 << " ms" << std::endl;
    // resize that uses selectivity
    return this->prune(output);
}

void FilterOperatorSyscat::get_stat(){
    //std::cout << "Calling get_stat for filter.\n";
    // 1. Check if there are any statistics related to the specific filter ---> not done yet
    // 2. Make a secure Integer out of all the keys in mcv list and compare to target_value
    // 3. Modify the operator's selectivity field
    std::vector<float> mcv = stats->mcv;
    std::vector<float> mcf = stats->mcf;

    for (int i = 0; i < stats->mcf.size(); i++){
        int val = mcv.at(i);
        // this is public because no party provides it
        // and it is part of the query itself
        emp::Integer secure_val = Integer(32, val, PUBLIC);
        int cond_val = compare(secure_val, target_value, "eq").reveal<bool>();
        if (cond_val == 1){ //if filter value found
            // change selectivity and break out of loop
            selectivity = (mcf.at(i)*1.0)/(stats->num_rows); 
            i = stats->mcf.size() + 1;
        }
    }
    //std::cout << "Number of rows: " << stats->num_rows << endl;
    std::cout << "Selectivity after get_stat: " << selectivity << endl;
}

SecureRelation FilterOperatorSyscat::prune(SecureRelation rel){
    auto start_time = std::chrono::high_resolution_clock::now();
    std::cout << "Filter selectivity: " << selectivity << endl;
    rel.sort_by_flag();

    if (selectivity < 1){ // this ensures noisy selectivity of more than 1 is not considered
        int num_rows = rel.flags.size();
        int num_cols = rel.columns.size();
        float start_of_prune = selectivity * num_rows + 1 ;
        SecureRelation pruned_rel(num_cols, start_of_prune);
        //std::cout << "Selectivity: " << selectivity << "\n";
        //std::cout << "Start of Prune: " << start_of_prune << "\n";
        for(int i = 0; i < num_cols; i++){
            for(int j=0; j < start_of_prune; j++){
                pruned_rel.columns[i][j] = rel.columns[i][j];
            }
        }
        for(int j=0; j < start_of_prune; j++){
                pruned_rel.flags[j] = rel.flags[j];
        }
        
        return pruned_rel;
    }
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration_filter_by_fixed_value = std::chrono::duration_cast<std::chrono::milliseconds>((end_time - start_time)).count();
    std::cout << "Prune time for individual operator "<< duration_filter_by_fixed_value/2 << " ms" << std::endl;
    return rel;
}


void FilterOperatorSyscat::set_stats(Stats* s){
    stats = s;
}

#endif // FILTER_OPERATOR_SYSCAT_HPP

