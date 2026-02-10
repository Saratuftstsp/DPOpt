import argparse
import random

def generate(num_cols, num_rows, domain_starting_at_0, num_rels = 0):
    k = num_rels
    # generate random integer data and put it in an array of arrays
    rand_dat = []
    for i in range(num_cols):
        rand_dat.append([])
        for j in range(num_rows):
            rand_dat[i].append(random.randint(0, domain_starting_at_0))
        #print(rand_dat[i])
    
    # print the data to two files, one from Alice and one for Bob
    num_alice_rows = int(num_rows/2)
    num_bob_rows = num_rows - num_alice_rows
    with open("rel"+str(k)+"_alice.csv", "w") as f_alice:
        for j in range(num_alice_rows):
            line = ""
            for i in range(num_cols):
                line += str(rand_dat[i][j])
                if (i != num_cols - 1): line += ","
            line+="\n"
            f_alice.write(line)
    with open("rel"+str(k)+"_bob.csv", "w") as f_bob:
        for j in range(num_bob_rows):
            line = ""
            for i in range(num_cols):
                line += str(rand_dat[i][j+num_alice_rows])
                if (i != num_cols - 1): line += ","
            line+="\n"
            f_bob.write(line)
    
        


def main():
    parser = argparse.ArgumentParser(description="Columns Number and Row Number for random integer data.")
    parser.add_argument('num_cols', type=int)
    parser.add_argument('num_rows', type=int)
    parser.add_argument('domain_starting_at_0', type=int)
    parser.add_argument('num_rels', type=int)
    args = parser.parse_args()
    for k in range(args.num_rels):
        generate(args.num_cols, args.num_rows, args.domain_starting_at_0, k)

if __name__=='__main__':
    main()