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
        //1. open the csv file with the name provided
        // optional: add field in SecureRelation object to maintain 
        // mapping between column number and column name

        //2. read contents

        //3. place each row of data 
        // into the SecureRelation object passed in as parameter

        //placeholder return statement
        return input;
    }
};

#endif // SCAN_OPERATOR_HPP
