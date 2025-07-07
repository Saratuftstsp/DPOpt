
#pragma once

struct Stats{
    int column_index; //the index of the column for which these statistics are collected
    int num_rows;
    std::vector<float> mcv;
    std::vector<float> mcf;
    int ndistinct;
};