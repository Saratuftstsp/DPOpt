
#pragma once

struct Stats{
    int column_index; //the index of the column for which these statistics are collected
    int num_rows;
    std::vector<float> mcv;
    std::vector<float> mcf;
    int ndistinct = 0;
    bool diffp = 0;

    //Integer n_distinct_priv;
    std::vector<Integer> mcf_priv;
    std::vector<int> mcf_noisy;
};