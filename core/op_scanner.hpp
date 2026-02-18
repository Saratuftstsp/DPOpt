#ifndef SCAN_OPERATOR_HPP
#define SCAN_OPERATOR_HPP

#include "relation.hpp"
#include <vector>
#include <iostream>
#include <string>
#include <fstream>
#include <sstream>
#include <bit>
#include <cstdint>
#include "_op_unary.hpp"
#include "global_string_encoder.hpp"

#include <pqxx/pqxx>

using namespace pqxx;
using namespace std;

enum class DataType{
    BOOLEAN,
    INT,
    FLOAT,
    STRING
};

class ScanOperator
{
public:
    // Store filename as a string, not pointer
    std::string csvFileName;
    int num_cols;
    int alice_size;
    int bob_size;
    GlobalStringEncoder encoder;
    

    // Constructor takes string by const reference
    ScanOperator(const std::string &filename, int num_cols, int alice_rows, int bob_rows, GlobalStringEncoder &encoder) : csvFileName(filename), num_cols(num_cols), alice_size(alice_rows), bob_size(bob_rows) {}
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

    DataType detectType(const std::string& token)
    {
        std::istringstream iss(token);

        int intVal;
        if (iss >> intVal && iss.eof())
            return DataType::INT;

        iss.clear();
        iss.str(token);

        double doubleVal;
        if (iss >> doubleVal && iss.eof())
            return DataType::FLOAT;

        return DataType::STRING;
    }

    std::vector<int> inferSchema(const std::string& line) {
        std::vector<int> schema;

        std::stringstream ss(line);
        std::string token;

        while (std::getline(ss, token, ','))
        {   
            DataType token_type = detectType(token);
            switch (token_type){
                case DataType::BOOLEAN:
                    std::cout << "BOOLEAN\n";
                    schema.push_back(0);
                    break;
                case DataType::INT:
                    std::cout << "INT\n";
                    schema.push_back(1);
                    break;
                case DataType::FLOAT:
                    std::cout << "FLOAT\n";
                    schema.push_back(2);
                    break;
                case DataType::STRING:
                    std::cout << "STRING\n";
                    schema.push_back(3);
                    break;
            }
            
        }

        return schema;
    }

    void get_schema(SecureRelation &combinedData){
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
        std::string line;
        // ----- Read and parse column headers -----
        if (std::getline(file, line))
        {
            std::stringstream ss(line);
            std::string token;

            while (std::getline(ss, token, ','))
            {
                combinedData.col_names.push_back(token);
            }

            //sanity check - print out column names extracted from csv
            std::cout << "Column names:\n";
            for (const auto& col : combinedData.col_names)
                std::cout << col << std::endl;

            //detect data types from first 2 (can replace this with some n later, depending on the data size) rows
            int num_rows_to_infer_types_from = 2;
            int i = 0;
            std::vector<int> schema;
            while (std::getline(file, line) && i < num_rows_to_infer_types_from){
                schema = inferSchema(line);
                i++;
            }
            combinedData.col_types = schema;

        }
        file.close();
    }

    double parseDouble(const std::string& token){
        size_t pos;
        double value = std::stod(token, &pos);

        if (pos != token.size())
            throw std::runtime_error("Invalid floating-point value: " + token);

        return value;
    }

    Integer doubleToInteger(double value, int party){
        uint64_t bits;
        std::memcpy(&bits, &value, sizeof(double));

        // reinterpret as signed for EMP constructor
        int64_t signed_bits = static_cast<int64_t>(bits);

        return Integer(64, signed_bits, party);
    }

    Integer convert_cell_to_int(string cell, int col_type, int party){
        
        switch (col_type){
            case 1:
                //int conversion
                //read string "cell" as an int and then create emp Integer with it
                return Integer(32,std::stoi(cell),party);
                // return Integer(32,std::stoi(cell),party);
                case 2:
                //float conversion
                return doubleToInteger(parseDouble(cell), party);
                case 3:
                //string conversion
                std::cout << "Here for string\n";
                std::cout << cell << "\n";
                int mapping = encoder.encode(cell);
                std::cout << mapping << "\n";
                return Integer(32, mapping, party);
        }

    }

protected:
    void operation(SecureRelation &combinedData, const int party) //add another parameter for the stats
    {  
        get_schema(combinedData);
     
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
 
        
        std::string line;
        // if party is Bob then, fill first alice_size rows with dummies
        if(BOB==party){
            std::vector<string> dummy_row = make_dummy_row();
            for(int i = 0; i < alice_size; i++){
                combinedData.addRow(dummy_row, party);
            }
        }//std::cout << "Vector length after dummies inserted: " << combinedData.columns[0].size() << endl;
        

        // ignore header line
        std::getline(file,line);
        while (std::getline(file, line)){
            std::stringstream ss(line);
            std::string cell;


            int col_idx = 0;
            Integer alice_integer;
            Integer bob_integer;
            while (std::getline(ss, cell, ','))
            {  
                Integer converted_cell = convert_cell_to_int(cell, combinedData.col_types[col_idx],party);
                if (party==ALICE){ alice_integer = converted_cell; bob_integer = Integer(32,0,BOB);}
                else{ alice_integer = Integer(32, 0, ALICE); bob_integer = converted_cell;}
                Integer ss_converted_cell = alice_integer + bob_integer;
                combinedData.columns[col_idx].push_back(ss_converted_cell);
                col_idx++;
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