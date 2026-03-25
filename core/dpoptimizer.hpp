#pragma once

#include "_op_unary.hpp"
#include "_op_binary.hpp"
#include "plan_node.hpp"
#include <iostream>
#include "core/stats.hpp"
#include <boost/random.hpp>
#include <boost/random/exponential_distribution.hpp>
#include <cmath>
#include "emp-sh2pc/emp-sh2pc.h"

class DPOptimizer{
public:
    //Constructor
    DPOptimizer(){};

    float get_cost(planNode &root);
    void dpanalyze(int col_num, std::vector<std::vector<emp::Integer>> columns ,std::vector<Stats> &rel_stats, int party);
    int round(int a);
    //void dpanalyze_col(int col_idx, std::vector<emp::Integer> column, Stats &col_stats, int party);
    void get_counts(int col_idx, std::vector<emp::Integer> column, int party, Stats &s);
};



float DPOptimizer::get_cost(planNode &root){
    //implement like the get_cost() funtion of filter_syscat
}

void DPOptimizer::dpanalyze(int num_cols, std::vector<std::vector<emp::Integer>> columns, std::vector<Stats> &rel_stats, int party){
    for(int col_num = 0; col_num < num_cols; col_num++ ){ //replace 3 with number of columns
        if (rel_stats.at(col_num).diffp == 1){
            //std::cout << "Flag check\n";
            return;
        }

        //get the right column's stats
        Stats stats_to_noise = rel_stats.at(col_num);
        get_counts(col_num, columns[col_num], party, rel_stats.at(col_num));
        //std::cout << "Check after count: " << rel_stats[col_num].mcf.size() << endl;
    

        //set up the Laplace distribution
        static boost::mt19937 rng(static_cast<unsigned int>(std::time(0)));
        float sensitivity = 1;
        float epsilon = 1;
        float scale = sensitivity/epsilon;
        boost::random::exponential_distribution<> exp_dist(1.0 / scale);
        boost::variate_generator<boost::mt19937&, boost::random::exponential_distribution<> > gen(rng, exp_dist);

        //add noise to the mcf values
        rel_stats[col_num].diffp = 1; // flip diffp flag
        for(int i = 0; i < rel_stats[col_num].domain.size(); i++){
            Integer old_row_count = rel_stats.at(col_num).mcf_priv[i]; // get number of rows
            float noise = gen();   if (noise < 0){ noise = 0;} // need to set it to 0
            emp::Integer noise_from_alice(32, (party == ALICE ? noise*1000 : 0), ALICE);
            emp::Integer noise_from_bob(32, (party == BOB ? noise*1000 : 0), BOB);
            Integer new_row_count = old_row_count + noise_from_alice + noise_from_bob; // add noise to number of rows
            rel_stats[col_num].mcf_noisy.push_back(round(new_row_count.reveal<int>())); //update stats object
        }
        //std::cout << "Check after noise: " << rel_stats.at(col_num).mcf_noisy.size() << endl;
        for(int i = 0; i < rel_stats.at(col_num).mcf_priv.size(); i++){
            rel_stats.at(col_num).mcf_priv[i] = rel_stats.at(col_num).mcf_priv[i] / Integer(32, 1000, PUBLIC);
        }
    } 
}

// 9999 + 3000 = 12999 / 1000 = 12
// 9999 + 3000 = 13000 / 1000 = 12
// 9 + 3 = 12 = 12
// 12999
int DPOptimizer::round(int a) {
    if (a % 1000 >= 500) return (a / 1000 + 1);
    else return (a / 1000);
}

/*void DPOptimizer::dpanalyze_col(int col_idx, std::vector<emp::Integer> column, Stats &col_stats, int party){

    //set up the Laplace distribution
    static boost::mt19937 rng(static_cast<unsigned int>(std::time(0)));
    float sensitivity = 1;
    float epsilon = 0.001;
    float scale = sensitivity/epsilon;
    boost::random::exponential_distribution<> exp_dist(1.0 / scale);
    boost::variate_generator<boost::mt19937&, boost::random::exponential_distribution<> > gen(rng, exp_dist);

    //add noise to the mcf values
    col_stats.diffp = 1; // flip diffp flag
    for(int i = 0; i < col_stats.ndistinct; i++){
        int old_row_count = col_stats.mcf.at(i); // get number of rows
        //std::cout << "True row count: " << old_row_count << "\n";
        float noise = gen();   if (noise < 0){ noise = - noise;}
        emp::Integer noise_from_alice(32, (party == ALICE ? noise : 0), ALICE);
        emp::Integer noise_from_bob(32, (party == BOB ? noise : 0), BOB);

        emp::Integer new_row_count = Integer(32, old_row_count, party ) + Integer(32, noise, party ); // add noise to number of rows
        col_stats.mcf.at(i) = new_row_count.reveal<int>(); //update stats object
        //reveal data values AFTER counts have been noised
        col_stats.mcv.push_back(col_stats.mcv_priv.at(i).reveal<int>());
    }
}*/

void DPOptimizer::get_counts(int col_idx, std::vector<emp::Integer> column, int party, Stats &s){
    //1. Make array of all domain values
    //.  and corresponding array of counts initialized to emp Integer 0
    std::vector<Integer> vals_emp;
    for(int i = 0; i < s.domain.size(); i++){
        int domain_value = s.domain[i];
        //s.mcv.push_back(i);
        vals_emp.push_back(Integer(32, domain_value, PUBLIC));
        s.mcf_priv.push_back(Integer(32, 0, PUBLIC)); // don't know if using just one party is okay or not ----> It's not bro
    }

    //2. For each data value, evaluate predicate checking equality with each domain value
    //   and add to count
    
    for(int i = 0; i < column.size(); i++){
       //std::cout << "Iteration " << i << endl;
       for(int j = 0; j < vals_emp.size(); j++){
            s.mcf_priv[j] = s.mcf_priv[j] + emp::If((column[i]==vals_emp[j]), Integer(32, 1, PUBLIC), Integer(32, 0, PUBLIC));
       }
    }

    for(int i = 0; i < s.mcf_priv.size(); i++){
        s.mcf_priv[i] = s.mcf_priv[i]*Integer(32, 1000, PUBLIC);
    }
    //std::cout << "Check after count: " << s.mcf_priv.size() << endl;
    //3. populate stats object partially
    s.column_index = col_idx; //the index of the column for which these statistics are collected
    s.num_rows = column.size();
    s.ndistinct = -1; //leave as -1 for now, will assign after adding noise
    bool diffp = 0;

}

// Resources to help implement Laplace Mechanism:
// https://beta.boost.org/doc/libs/1_42_0/libs/math/doc/sf_and_dist/html/math_toolkit/dist/dist_ref/dists/laplace_dist.html
// The Mersenne Twister is a widely used pseudo-random number generator (PRNG) known for its speed and high-quality statistical properties.
// Library required to connect to Postgres:
// https://pqxx.org/libpqxx/

/*std::cout << "Data value after reveal: " << column[i].reveal<int>() << endl;
std::cout << "Domain value to compare data value with : " << vals_emp[j].reveal<int>() << endl;
std::cout << "Result of comparison (boolean): " << column[i].equal(vals_emp[j]).reveal<bool>() << endl;*/

/*Old code for Bit to Integer conversion I might use later
//std::vector<Bit> bit_bool;
            //bit_bool.push_back(column[i].equal(vals_emp[j]));
            //s.mcf_priv[j] = s.mcf_priv[j] + Integer(32, bit_bool, party);
*/