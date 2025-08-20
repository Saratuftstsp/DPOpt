#pragma once

#include "_op_unary.hpp"
#include "_op_binary.hpp"
#include "plan_node.hpp"
#include <iostream>
#include "core/stats.hpp"
#include <boost/random.hpp>
#include <boost/random/exponential_distribution.hpp>

class DPOptimizer{
public:
    //Constructor
    DPOptimizer(){};

    float get_cost(planNode &root);
    void dpanalyze(std::vector<Stats> &rel_stats);
};



float DPOptimizer::get_cost(planNode &root){

}

void DPOptimizer::dpanalyze(std::vector<Stats> &rel_stats){
    for(int col_num = 0; col_num < 3; col_num++ ){ //replace 3 with number of columns
            if (rel_stats.at(col_num).diffp == 1){
            std::cout << "Flag check\n";
            return;
        }

        //get the right column's stats
        Stats stats_to_noise = rel_stats.at(col_num);

        //set up the Laplace distribution
        static boost::mt19937 rng(static_cast<unsigned int>(std::time(0)));
        float sensitivity = 1;
        float epsilon = 0.1;
        float scale = sensitivity/epsilon;
        boost::random::exponential_distribution<> exp_dist(1.0 / scale);
        boost::variate_generator<boost::mt19937&, boost::random::exponential_distribution<> > gen(rng, exp_dist);

        //add noise to the mcf values
        rel_stats.at(col_num).diffp = 1; // flip diffp flag
        for(int i = 0; i < stats_to_noise.ndistinct; i++){
            int old_row_count = stats_to_noise.mcf.at(i); // get number of rows
            std::cout << "True row count: " << old_row_count << "\n";
            float new_row_count = old_row_count + gen(); // add noise to number of rows
            std::cout << "Noisy row count: " << new_row_count << "\n";
            float new_row_frac = new_row_count / stats_to_noise.num_rows; // get new noisy frequency
            std::cout << "Noisy frequency: " << new_row_frac << "\n";
            rel_stats.at(col_num).mcf.at(i) = new_row_frac; //update stats object
        }

    } 
    
}

// Resources to help implement Laplace Mechanism:
// https://beta.boost.org/doc/libs/1_42_0/libs/math/doc/sf_and_dist/html/math_toolkit/dist/dist_ref/dists/laplace_dist.html
// The Mersenne Twister is a widely used pseudo-random number generator (PRNG) known for its speed and high-quality statistical properties.
// Library required to connect to Postgres:
// https://pqxx.org/libpqxx/