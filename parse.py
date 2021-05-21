#run as python3 parse.py > res-stats.txt

#takes all the .evt files from the data/ folder
#processes them to be in the format ready for lsh.c -flight.txt
#makes a binarized version for ABC (0 if below average value for each var, 1 if above) - flight-binary.txt
#and a standarized version for SSH (val - mean/sd) -flight-standard.txt

import csv
import numpy as np
import os
import math

#gather all file names needed 
files = os.listdir("data/")

#initialize necessary data structures to hold stats across all files for the vars
means = []
sds = []
starts = []
ends = []

#gather stats for every var
for j in range(0, 29):
    print("variable " + str(j))

    #initialize
    s = 0
    mean = 0
    sd = 0
    count = 0
    sq = 0
    mini = float("inf")
    maxi = float("-inf")
    
    #open file for gathering all vals for this var (across all flights)
    fname = str(j) + ".txt"
    f = open(fname, "a+")

    for filename in files:
        name = 'data/' + filename 
        with open(name, newline='') as csvfile:
            data = list(csv.reader(csvfile))
    
        row = np.array(data).T[j][1:]
        if count == 0:
            label = np.array(data).T[j][0]
            print(label)

        count = count + len(row)

        if j == 0:
            starts.append(row[0])
            ends.append(row[-1])
            print(filename + " flight start is " + str(row[0]) + " flight end is " + str(row[-1]) + " number of timesteps is " + str(len(row)))

        for item in row:

            f.write(item)
            f.write(" ")

            s = s + float(item)
            if float(item) > maxi:
                maxi = float(item)

            if float(item) < mini:
                mini = float(item)

        f.write("\n")
        
    #calculate the stats and report
    mean = s/count
    print(" mean " + str(mean))
    means.append(mean)
    print(" minimum " + str(mini))
    print(" maximum " + str(maxi))
     
    for filename in files:
        name = 'data/' + filename 
        with open(name, newline='') as csvfile:
            data = list(csv.reader(csvfile))
    
        row = np.array(data).T[j][1:]

        for item in row:
            st = float(item) - mean
            sq = sq +st**2
        
    sqm = sq/count
    sd = math.sqrt(sqm)  
    print(" sd " + str(sd) + "\n")
    sds.append(sd)
        
    f.close()

flight = 0

for filename in files:
    #raw data
    fname = str(flight) + "-flight.txt"
    fn = open(fname, "a+")

    #standarized data
    fname = str(flight) + "-flight-standard.txt"
    fs = open(fname, "a+")

    #binarized data 
    fname = str(flight) + "-flight-binary.txt"
    fb = open(fname, "a+")

    name = 'data/' + filename 
    with open(name, newline='') as csvfile:
        data = list(csv.reader(csvfile))

        for j in range(1, 29):
            row = np.array(data).T[j][1:]

            for item in row:
                fn.write(item)
                fn.write(" ")

                new_item = (float(item) - means[j])/sds[j]

                fs.write(str(new_item))
                fs.write(" ")

                if float(item) >= means[j]:
                    fb.write(str(1))
                else: 
                    fb.write(str(0))

                fb.write(" ")


            fn.write("\n")
            fs.write("\n")
            fb.write("\n")

    fn.close()
    fs.close()
    fb.close()
    flight = flight +1

