#pragma once

#include <iostream>
#include <string>
#include <fstream>
#include <sstream>
#include <bit>
#include <cstdint>
#include <fstream>
#include <string>
#include <unordered_map>
#include <vector>
#include <stdexcept>
#include "core/global_string_encoder.hpp"

using namespace std;

//#define NUM_OF_ROWS 10

enum class DataType{
    BOOLEAN,
    INT,
    FLOAT,
    STRING
};

class PreProc {
public:
    GlobalStringEncoder global_encoder;
    std::string encoded_filename = "string_mapping.csv";
    char delimiter='|';
    int num_of_rows = 10;

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

    PreProc(int num_rows){num_of_rows=num_rows;}

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

    void inferSchema(const std::string& line, std::vector<int> &schema) {
        std::stringstream ss(line);
        std::string token;

        while (std::getline(ss, token, delimiter))
        {   //std::cout << token << "\n";
            DataType token_type = detectType(token);
            switch (token_type){
                case DataType::BOOLEAN:
                    //std::cout << "BOOLEAN\n";
                    schema.push_back(0);
                    break;
                case DataType::INT:
                    //std::cout << "INT\n";
                    schema.push_back(1);
                    break;
                case DataType::FLOAT:
                    //std::cout << "FLOAT\n";
                    schema.push_back(2);
                    break;
                case DataType::STRING:
                    //std::cout << "STRING\n";
                    schema.push_back(3);
                    break;
            }
            
        }
    }

    void get_schema(string csvFileName, std::vector<int> &schema){
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
        
        //detect data types from first n rows
        // n = num_rows_to_infer_types_from
        int num_rows_to_infer_types_from = 1;
        int i = 0;
        while (std::getline(file, line) && i < num_rows_to_infer_types_from){
            //std::cout << line << std::endl;
            inferSchema(line, schema);
            i++;
        }

        file.close();

    }

    void read_csv(string fname){
        //1. Get the schema
        std::vector<int> schema;
        get_schema(fname, schema);
        //std::cout << schema.size() << std::endl;

        //2. Get the column values
        std::ifstream file(fname);
        if (!file.is_open())
        {
            std::cerr << "Error: Failed to open CSV file: " << fname << std::endl;
            return;
        }
        std::vector<std::vector<std::string>> local_data;
        std::string line;
        int num_rows_to_read = 2; //alice_size == bob_size so this is okay
        int num_rows_read = 0;
        while (std::getline(file, line) && num_rows_read < num_rows_to_read)
        {   //std::cout << "Raw line: \n" << line << "\n";
            std::stringstream ss(line);
            std::string cell;
            std::vector<std::string> parsed_row;
            while (std::getline(ss, cell, delimiter))
            {  
                //std::cout << cell << "\n";
                parsed_row.push_back(cell);
            }
            local_data.push_back(parsed_row);
            num_rows_read+=1;
        }
        file.close();

        //3. For each column, if schema specifies type string, add to mapping
        std::cout << "Schema size: "; std::cout << schema.size() << std::endl;
        std::cout << "Number of columns found: "; std::cout << local_data[0].size() << std::endl;
        for(int i = 0; i < schema.size(); i++){
            if (schema[i]==3){
                for(int j = 0; j < local_data.size(); j++){
                    std::string data = local_data[j][i];
                    uint32_t encoding = global_encoder.encode(data);
                }
            } std::cout << "Encoded column "; std::cout<< i << std::endl;
        }
    }

    void create_public_mapping(){
        //for each table in tpch data
        //make the mapping and store it in the GlobalStringEncoder object
        for(int i = 0; i < 8; i++){
            std::string relname = tpch_tables[i];
            std::cout << "Relation name: " << relname << std::endl;
            std::string fname1 = "./tpch_data/bob_" +relname +".csv";
            read_csv(fname1);
            std::string fname2 = "./tpch_data/alice_" + relname +".csv";
            read_csv(fname2);
        }

        // write the mapping to another csv
        // std::ofstream creates the file if it doesn't exist by default.
        // std::ios::app ensures new data is added to the end.
        std::ofstream file(encoded_filename);
        // std::ofstream file(encoded_filename, std::ios::app);
        // Check if the file was opened successfully
        if (!file) {
            std::cerr << "Error opening file: " << encoded_filename << std::endl;
            return;
        }
        for(int i = 0; i < global_encoder.reverse_.size(); i++){
            // Append the encoding to the file
            //std::cout << i;
            //std::cout << "," << global_encoder.reverse_[i] << std::endl;
            file << i;
            file << "|" << global_encoder.reverse_[i] << std::endl;
        }

        // Close the file stream
        file.close();
    }

};