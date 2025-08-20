#ifndef SCAN_OPERATOR_HPP
#define SCAN_OPERATOR_HPP

#include "relation.hpp"
#include <vector>
#include <iostream>
#include <string>
#include <fstream>
#include <sstream>
#include "_op_unary.hpp"

#include <pqxx/pqxx>

using namespace pqxx;
using namespace std;


class ScanOperator
{
public:
    // Store filename as a string, not pointer
    std::string csvFileName;
    int num_cols;
    int alice_size;
    int bob_size;
    

    // Constructor takes string by const reference
    ScanOperator(const std::string &filename, int num_cols, int alice_rows, int bob_rows) : csvFileName(filename), num_cols(num_cols), alice_size(alice_rows), bob_size(bob_rows) {}
    void execute(SecureRelation &input, const int party)
    {
        operation(input, party);
    }
    std::vector<string> make_dummy_row(){
        std::vector<string> dummy_row;
        for(int i = 0; i < num_cols; i++){
            dummy_row.push_back("0");
        }
        return dummy_row;
    }

protected:
    void operation(SecureRelation &combinedData, const int party) //add another parameter for the stats
    {   
        //SecureRelation fileData(col_num,0);

        if (csvFileName.empty())
        {
            std::cerr << "Error: CSV file name not provided.\n";
            return;
        }

        std::ifstream file(csvFileName);
        if (!file.is_open())
        {
            std::cerr << "Error: Failed to open CSV file: " << csvFileName << std::endl;
            return;
        }

        // if party is Bob then, fill first alice_size rows with dummies
        if(BOB==party){
            std::vector<string> dummy_row = make_dummy_row();
            for(int i = 0; i < alice_size; i++){
                combinedData.addRow(dummy_row, party);
            }
        }std::cout << "Vector length after dummies inserted: " << combinedData.columns[0].size() << endl;
        

        std::string line;
        bool isHeader = false;
        // read from file
        while (std::getline(file, line))

        {
            std::stringstream ss(line);
            std::string cell;
            std::vector<std::string> rowData;

            while (std::getline(ss, cell, ','))
            {
                //std::cout << cell << "\n";
                rowData.push_back(cell);
            }

            if (isHeader)
            {
                combinedData.setColumnNames(rowData); // ensure this exists & is safe
                isHeader = false;
            }
            else
            {
                combinedData.addRow(rowData,alice_size); // ensure this exists & is safe
            }
        }

        file.close();
        if(ALICE==party){
            std::vector<string> dummy_row = make_dummy_row();
            for(int i = alice_size; i < alice_size+bob_size; i++){
                combinedData.addRow(dummy_row, party);
            }
        } 
        
    }
    
};

#endif // SCAN_OPERATOR_HPP

//2 possible approaches:
// 1. pass the true values into the relation object
// 2. create the Integers and sum them in the Scanner object itself

/*Integer alice_integer;
        Integer bob_integer;
        // whenever something is pushed into the result, we must create something that is shared across Alice and Bob
        for (int i = 0; i < alice_size; i++){
            //std::cout << "True cols val: " << fileData.true_cols[0][i] << endl;
            alice_integer = Integer(32,fileData.true_cols[0][i], ALICE);
            bob_integer = Integer(32,0,BOB);
            Integer sum = alice_integer + bob_integer;
            //std::cout << sum.reveal<int>()<<endl;
            res.push_back(sum);
        }

        for (int i = alice_size; i < alice_size+bob_size; i++){
            //std::cout << "True cols val: " << fileData.true_cols[0][i-alice_size] << endl;
            alice_integer = Integer(32,0,ALICE);
            bob_integer = Integer(32,fileData.true_cols[0][i-alice_size], BOB);
            Integer sum = alice_integer + bob_integer;
            //std::cout << sum.reveal<int>()<<endl;
            res.push_back(sum);
        }
*/