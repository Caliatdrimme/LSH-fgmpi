#run python3 experiment.py exp_num


#free() invalid next size (fast)
#malloc() top size corrupted
#exit code 134 
#aborted signal 6

#this script runs experiment with the code in lsh.c 
#across the files in processed/ folder
#with various parameter values

import csv
import numpy as np
import os
import math
import time
import sys

#example mpiexec commands
#ABC
#os.system("mpiexec -nfg 1 -n 12 ./lsh 1 0 0 6 3 3 1 2 1 2.0 0.1 0.1 3 data.txt")

#SAX
#os.system("mpiexec -nfg 1 -n 12 ./lsh 2 1 0 9 3 2 1 3 2 4.5 0.1 0.1 3 data.txt")

#SSH
#os.system("mpiexec -nfg 1 -n 12 ./lsh 3 2 0 9 3 3 1 2 1 2.0 0.1 0.1 3 data.txt")

#TODO
#add timing to exp1
#test argument switch
#loop over all files in processed folder - create exp3 as copy of exp2 but over all files
#commit

def main():
    print("started main")
    args = sys.argv[1:]
    exp = int(args[0])
    print("exp is " + str(exp))
    if (exp == 1):
        print("running exp1")
        exp1()
    elif exp == 2:
        exp2()
    elif exp == 3:
        exp3()       


def exp1():
    print("running exp1")
    nfg = 2
    num = 20

    trial = [1, 2, 3]
    flag = [0, 1, 2]
    start = [0, 0, 0]
    elements = [6, 9, 9]
    num_hash = [3, 3, 3]
    size_hash = [3, 2, 3]
    step_hash = [1, 1, 1]
    num_symbols = [2, 3, 2]
    word_length = [1, 2, 1]
    average = [2.0, 4.5, 2.0]
    sd = [0.1, 0.1, 0.1]
    sim = [0.1, 0.1, 0.1]
    n = [3, 3, 3]

    times = [0, 0, 0]
    s = time.time_ns() // 1_000_000 

    for i in range(0,3):
        print(f"TRIAL {trial[i]}")
        fname = "data.txt"
        command = f"mpiexec -nfg {nfg} -n {num} ./lsh {trial[i]} {flag[i]} {start[i]} {elements[i]} {num_hash[i]} {size_hash[i]} {step_hash[i]} {num_symbols[i]} {word_length[i]} {average[i]} {sd[i]} {sim[i]} {n[i]} {fname}"
        #print(command)
        t0 = time.time_ns() // 1_000_000 
        os.system(command)
        t1 = time.time_ns() // 1_000_000 
        print("dt: "+str(t1-t0))
        times[i] = t1-t0

    e = time.time_ns() // 1_000_000 

    dif = e-s

    print("total time " + str(dif))

    for i in range(0,3):
        print(times[i])

    #valgrind --leak-check=full --show-leak-kinds=all

def exp2():
    nfg = 2
    num = 20

    trial = [1, 2, 3]
    flag = [0, 1, 2]
    start = [0, 0, 0]
    elements = [800, 800, 800]
    num_hash = [5, 5, 5]
    size_hash = [5, 5, 5]
    step_hash = [5, 5, 5]
    num_symbols = [2, 5, 3]
    word_length = [10, 10, 10]
    average = [0.5, 0, 2.0]
    sd = [0.1, 1, 0.1]
    sim = [0.01, 0.1, 0.1]
    n = [28, 28, 28]

    times = [0, 0, 0]

    """
    1 receiver, manager, writer, similarity, hash and storage = 6 nodes
    num_hash hashtables
    rest are workers

    need at least 6 + num_hash + 1 total threads (n_subprocessXn_process)
    start should be between 0 and total number of elements 
    num_hash, size_hash, step_hash, word_length, 3=<num_symbols=<15 - all much smaller than n

    average and sd come from data 
    """

    s = time.time_ns() // 1_000_000 

    for i in range(1, 2):
        print(f"TRIAL {trial[i]}")
        if (flag[i] == 0): 
            fname = "processed/0-flight-binary.txt"
        if (flag[i] == 1): 
            fname = "processed/0-flight-standard.txt"
        if (flag[i] == 2): 
            fname = "processed/0-flight.txt"
        
        command = f"mpiexec -nfg {nfg} -n {num} ./lsh {trial[i]} {flag[i]} {start[i]} {elements[i]} {num_hash[i]} {size_hash[i]} {step_hash[i]} {num_symbols[i]} {word_length[i]} {average[i]} {sd[i]} {sim[i]} {n[i]} {fname}"
        print(command)

        t0 = time.time_ns() // 1_000_000 
        os.system(command)
        #os.system("sleep 10")
        t1 = time.time_ns() // 1_000_000 
        print("dt: "+str(t1-t0))
        times[i] = t1-t0



    e = time.time_ns() // 1_000_000 

    dif = e-s

    print("total time " + str(dif))

    for i in range(0,3):
        print(times[i])

def exp3():
    print("LAST ONE")
    nfg = 2
    num = 20

    trial = [1, 2, 3]
    flag = [0, 1, 2]
    start = [0, 0, 0]
    elements = [800, 800, 800]
    num_hash = [5, 5, 5]
    size_hash = [5, 5, 5]
    step_hash = [5, 5, 5]
    num_symbols = [2, 5, 3]
    word_length = [10, 10, 10]
    average = [0.5, 0, 2.0]
    sd = [0.1, 1, 0.1]
    sim = [0.01, 0.1, 0.1]
    n = [28, 28, 28]

    times = []

    s = time.time_ns() // 1_000_000 

    t = 0

    for j in range(0,29):
        for i in range(0,3):
            t = t +1
            print(f"TRIAL {t}")
            if (flag[i] == 0): 
                fname = "processed/" + str(j) + "-flight-binary.txt"
            if (flag[i] == 1): 
                fname = "processed/" + str(j) + "-flight-standard.txt"
            if (flag[i] == 2): 
                fname = "processed/" + str(j) + "-flight.txt"
            
            command = f"mpiexec -nfg {nfg} -n {num} ./lsh {t} {flag[i]} {start[i]} {elements[i]} {num_hash[i]} {size_hash[i]} {step_hash[i]} {num_symbols[i]} {word_length[i]} {average[i]} {sd[i]} {sim[i]} {n[i]} {fname}"
            print(command)

            t0 = time.time_ns() // 1_000_000 
            os.system(command)
            t1 = time.time_ns() // 1_000_000 
            print("dt: " + str(t1-t0))
            times.append(t1-t0)



    e = time.time_ns() // 1_000_000 

    dif = e-s

    print("total time " + str(dif))

    for i in range(0,len(times)):
        print(times[i])


if __name__ == "__main__":
    main()
