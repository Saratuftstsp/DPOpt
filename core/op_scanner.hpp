#ifndef SCAN_OPERATOR_HPP
#define SCAN_OPERATOR_HPP

#include "relation.hpp"  // Assuming relation.hpp is in the same directory or include path
#include <vector>
#include <iostream>
#include <string>

class ScanOperator : public UnaryOperator {
public:
    string* csvFileName = nullptr;
    string* postgresDbName = nullptr;

    // 1. Constructor for scanning csv file
    ScanOperator(string* filename) : csvFileName(filename) {}

    // 2. Constructor for scanning a relation from a postgres database
    ScanOperator(string* pgresDb): postgresDbName(pgresDb) {}

protected:
    // Operation method
    SecureRelation operation(const SecureRelation& input) override {
        // implement

        //placeholder return statement
        return input;
    }
};

#endif // SCAN_OPERATOR_HPP
