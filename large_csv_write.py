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
m = 
r = 
with open(outfile, 'a') as csvfile:
        for i in range(r):
                data = [
                #[uuid.uuid4() for i in range(chunksize)],
                        np.random.random(chunksize)*50,
                        np.random.random(chunksize)*50,
                        np.random.randint(1000, size=(chunksize,))]
                #csvfile.writelines(['%s,%.6f,%.6f,%i\n' % row for row in zip(*data)])   
                csvfile.writelines(['%.6f,%.6f,%i\n' % row for row in zip(*data)])   

