#include "tpch_preproc.hpp"

int main(int argc, char** argv){
    int num_rows_to_preprocess = atoi(argv[0]);
    PreProc preprocessor(num_rows_to_preprocess);
    preprocessor.create_public_mapping();
}

