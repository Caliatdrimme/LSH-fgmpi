#run

#this script runs experiment with the code in lsh.c 
#across the files in processed/ folder
#with various parameter values


import csv
import numpy as np
import os
import math
import datetime

#ABC
#os.system("mpiexec -nfg 1 -n 12 ./lsh 1 0 0 6 3 3 1 2 1 2.0 0.1 0.1 3 data.txt")

#SAX
#os.system("mpiexec -nfg 1 -n 12 ./lsh 2 1 0 9 3 2 1 3 2 4.5 0.1 0.1 3 data.txt")

#SSH
#os.system("mpiexec -nfg 1 -n 12 ./lsh 3 2 0 9 3 3 1 2 1 2.0 0.1 0.1 3 data.txt")

#TODO
#loop all three methods on a flight file
#loop over all files in processed folder

"""
nfg = 1
num = 12

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


for i in range(0,1):
    print(f"TRIAL {trial[i]}")
    fname = "data.txt"
    command = f"mpiexec -nfg {nfg} -n {num} ./lsh {trial[i]} {flag[i]} {start[i]} {elements[i]} {num_hash[i]} {size_hash[i]} {step_hash[i]} {num_symbols[i]} {word_length[i]} {average[i]} {sd[i]} {sim[i]} {n[i]} {fname}"
    #print(command)
    os.system(command)

#valgrind --leak-check=full --show-leak-kinds=all
"""

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

#s = datetime.datetime.now()

for i in range(0,3):
    print(f"TRIAL {trial[i]}")
    if (flag[i] == 0): 
        fname = "processed/0-flight-binary.txt"
    if (flag[i] == 1): 
        fname = "processed/0-flight-standard.txt"
    if (flag[i] == 2): 
        fname = "processed/0-flight.txt"
    
    command = f"mpiexec -nfg {nfg} -n {num} ./lsh {trial[i]} {flag[i]} {start[i]} {elements[i]} {num_hash[i]} {size_hash[i]} {step_hash[i]} {num_symbols[i]} {word_length[i]} {average[i]} {sd[i]} {sim[i]} {n[i]} {fname}"
    print(command)
    #start timer
    #start = datetime.datetime.now()
    os.system(command)
    #end timer
    #end = datetime.datetime.now()
    #dif = end - start
    #times[i] = dif.total_seconds()

"""
e = datetime.datetime.now()

dif = s - e

print("total time " + str(dif.total_seconds()))

for i in range(0,3):
    print(time[i])

"""

