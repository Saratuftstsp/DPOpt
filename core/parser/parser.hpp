#include <iostream>
#include <fstream>
#include <memory>
#include <string>
#include <nlohmann/json.hpp>
#include <cstdlib>

#include <filesystem>

using json = nlohmann::json;
namespace fs = std::filesystem;

class Parser{
public:
    std::string query_name;
    std::string query;

    Parser(std::string query_name, std::string query_string);

    void get_json();

};

Parser::Parser(std::string query_name, std::string query_string)
    :query_name(query_name), query(query_string){}

void Parser::get_json(){

    // Step 1: get current directory
    fs::path current_dir = fs::current_path();

    // Step 3: add sibling directory + file name
    fs::path file_path = current_dir / "core" / "parser" / "get_logical_tree.py";

    //std::cout << "Current dir: " << current_dir << "\n";
    //std::cout << "File path: " << file_path << "\n";
    std::string command = "python3 " + file_path.string() + " " + query_name + " \"" + query + "\"";
    int status = system(command.c_str());

    std::cout << "Parsed successfully.\n";
}

