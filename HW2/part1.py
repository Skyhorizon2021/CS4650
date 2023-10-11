import re
import json

from mrjob.job import MRJob

QUALITY_RE = re.compile(r"[01459]")

class Part1(MRJob):
    
    def mapper(self, _, line):
        val = line.strip()
        (column_key,row_key,values) = (val[0],val[2],val[4:7])
        yield column_key, {"value":int(values)}
        yield row_key, {"value":int(values)}

    def reducer(self, key, values):
        val_arr = []
        for i in values:
            val_arr.append((i["value"]))
        if  65 <= ord(key) <= 74:
            col_max = max(val_arr)
            yield key, {"max":col_max}
        elif 75 <= ord(key) <= 84:
            row_min = min(val_arr)
            yield key, {"min":row_min}

if __name__ == '__main__':
    Part1.run()
