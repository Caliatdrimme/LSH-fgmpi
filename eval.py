#run as python3 eval.py

#creates distance matrixes from the resulting files from the lsh.c 
#pairs of the same var have value 1
#pairs that did not get their distance calculated get distance 0 
#all calculated values get normalized to be between 0 and 1 (exclusive)(val - min/(max - min))

import csv
import numpy as np
import os
import math

files = os.listdir("results/")

#for each file that was created in the results folder by lsh.c
for filename in files:
    name = 'results/' + filename 
    f = open(name, "r")

    fname = "final/" + "matrix-" + filename

    mat = np.zeros((28, 28))
    line = f.readline()

    #gather the data into the matrix
    while(line):

        if line.startswith("sim"):
            x = line.split()
            ind1 = int(x[1])
            ind2 = int(x[2])
            val = float(x[3])

            mat[ind1][ind2] = val
            print("value at " + str(ind1) + " " + str(ind2) + " is " + str(val))

        line = f.readline()

#make the values be scaled between 0 and 1 - but none of the distances become 0 or 1
#this assumes that the similarity was calculated only between non-equal time series and
#since they clash at least on one hash the distance is not 0
    for i in range (0, 28):
        for j in range(0,28):
            if mat[i][j] != 0:
                print("old value is " + str(mat[i][j]))
                mat[i][j] = mat[i][j] - mat.min()
                mat[i][j] = mat[i][j] / ((mat.max()+0.001) - mat.min())
                print("new value is " + str(mat[i][j]))

            if i == j :
                mat[i][j] = 1

    
    #write the matrix to file
    np.savetxt(fname, mat)

    #fn.close()
    f.close()

    

    












