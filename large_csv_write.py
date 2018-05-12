## what we want to do is to simulate a very large csv file and save it to hdfs

for x in range(10**12):
        print("1,2")
 
If you were to do:
 
# python3 dgc.py > /hadoop/mnt/wsc/song273/whatever.csv

import numpy as np
import uuid
import csv
import os
outfile = '/hadoop/mnt/wsc/song273/try.csv'
''m: subtset size; r:subset numbers''
m = 6
r = 10
p = 20
def generate(line,m=m,p=p):
    p = p
    m = 2**m
    np.random.seed(line)
    x = np.random.normal(0, 1, [m, p])
    np.random.seed(line)
    #y = np.random.choice([0, 1], [m, 1])
    y = np.random.binomial(1,0.5,[m,1]) 
    dataframe = np.hstack((x, y,np.full((m,1),line)))
    return np.ndarray.tolist(dataframe) 

with open(outfile, 'a') as csvfile:
        for i in range(r):
                #data = [
                #[uuid.uuid4() for i in range(chunksize)],
                #        np.random.random(chunksize)*50,
                #        np.random.random(chunksize)*50,
                #        np.random.randint(1000, size=(chunksize,))]
                #csvfile.writelines(['%s,%.6f,%.6f,%i\n' % row for row in zip(*data)])
                data = generate(i,m,p)
                csvfile.writelines(['%.6f,'*p+'%i\n'*2 % row for row in zip(*data)])   

