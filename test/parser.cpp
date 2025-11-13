#include <iostream>
#include <chrono>


#include "core/parser/parser.hpp"


int main(int argc, char** argv) {

    auto start_time = std::chrono::high_resolution_clock::now();

    //A. Parse query - get Tree object of operators
    std::string qname = argv[1];
    std::string query = argv[2]; //"select * from rel1 join rel2 on rel1.col1 = rel2.col1 where rel1.col0 = 4 and rel2.col0 = 5;";

    //std::cout << qname << "\n";
    //std::cout << query << "\n";
    Parser p(qname, query);
    p.get_json();

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration_parse_query = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

    return 0;
}


