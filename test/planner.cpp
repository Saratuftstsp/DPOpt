#include <iostream>
#include <chrono>
#include <filesystem>


#include "core/planner.hpp"

using json = nlohmann::json;
namespace fs = std::filesystem;


int main(int argc, char** argv) {

    auto start_time = std::chrono::high_resolution_clock::now();
    
    //A. Parse query - get Tree object of operators
    std::string qname = argv[1];
    std::cout << qname << "\n";
    fs::path current_path = std::filesystem::current_path(); //current_dir / "core" / "parser" / "get_logical_tree.py";
    std::string plan_file_name = (current_path / "test" / "parsed" / (qname + "_logictree.json")).string();
    std::string node_details_file = (current_path / "test" / "parsed" / (qname + "_nodes.json")).string();

    std::map<string, SecureRelation*> rels_dict;
    Planner p(plan_file_name, node_details_file, rels_dict);
    p.build_plan();

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration_parse_query = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

    return 0;
}
