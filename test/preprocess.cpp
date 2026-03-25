#include "tpch_preproc.hpp"

int main(int argc, char** argv){
    if(argc < 2){
        std::cerr << "Usage: " << argv[0] << " <num_rows>\n";
        return 1;
    }

    int num_rows_to_preprocess = atoi(argv[1]);
    //int num_rows_to_preprocess = atoi(argv[0]);
    PreProc preprocessor(num_rows_to_preprocess);
    preprocessor.create_public_mapping_and_domains();
}

