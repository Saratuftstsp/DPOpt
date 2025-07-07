#ifndef FILTER_OPERATOR_HPP
#define FILTER_OPERATOR_HPP

#include "core/_op_unary.hpp"
#include "stats.hpp"
#include <string>

class FilterOperatorSyscat : public UnaryOperator {
public:
    int column_index;  // The index of the column on which the filter is applied
    emp::Integer target_value;  // A target value for comparison if it's not a column
    std::vector<emp::Integer> target_column; // A column for comparison, if applicable
    std::string condition;  // One of the conditions: "gt, geq, lt, leq, eq, neq"
    float selectivity = 1;
    Stats* stats;


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
};


FilterOperatorSyscat::FilterOperatorSyscat(int col_idx, const emp::Integer& target, const std::string& cnd) 
    : column_index(col_idx), target_value(target), condition(cnd) {}

FilterOperatorSyscat::FilterOperatorSyscat(int col_idx, const std::vector<emp::Integer>& target_col, const std::string& cnd) 
    : column_index(col_idx), target_column(target_col), condition(cnd) {}

FilterOperatorSyscat::FilterOperatorSyscat(int col_idx, const emp::Integer& target, const std::string& cnd, Stats* statistics)
    : column_index(col_idx), target_value(target), condition(cnd), stats(statistics) {}
    
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
    SecureRelation output = input; // Make a copy of the input relation

    if (target_column.empty()) { // If the target is a single value
        for (int i = 0; i < input.columns[0].size(); i++) {
            output.flags[i] = Integer(1, compare(input.columns[column_index][i], target_value, condition).reveal<bool>(), ALICE);
        }
    } else { // If the target is a column
        for (int i = 0; i < input.columns[0].size(); i++) {
            output.flags[i] = Integer(1, compare(input.columns[column_index][i], target_column[i], condition).reveal<bool>(), ALICE);
        }
    }
    // resize that uses selectivity ----> Future plan, look at ShrinkWrap for inspo

    return this->prune(output);
}

void FilterOperatorSyscat::get_stat(){
    // 1. Check if there are any statistics related to the specific filter ---> not done yet
    // 2. Make a secure Integer out of all the keys in mcv list and compare to target_value
    // 3. Modify the operator's selectivity field
    std::vector<float> mcv = stats->mcv;
    std::vector<float> mcf = stats->mcf;

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

SecureRelation FilterOperatorSyscat::prune(SecureRelation rel){
    rel.sort_by_flag();
    this->get_stat();
    if (selectivity < 1){
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
    return rel;
}

#endif // FILTER_OPERATOR_HPP

