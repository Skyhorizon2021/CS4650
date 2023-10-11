import re
import json

from mrjob.job import MRJob

QUALITY_RE = re.compile(r"[01459]")

class Part3(MRJob):
        
    def mapper(self, _, line):
        val = line.strip()
        (column_key,row_key,values) = (val[0],val[2],val[4:7])
        yield column_key, {"value":int(values),"location":row_key}
        yield row_key, {"value":int(values),"location":column_key}

    def reducer(self, key, values):
        val_arr =[]
        location_arr = []
        location = ""
        for i in values:
            val_arr.append(i["value"])
            location_arr.append(i)
        if  65 <= ord(key) <= 74:
            col_max = max(val_arr)
            for pair in location_arr:
                if pair["value"] == col_max:
                    location += pair["location"] + " " 
            location = location.rstrip()
            yield key, {"max":col_max,"location":location}
        elif 75 <= ord(key) <= 84:
            row_min = min(val_arr)
            for pair in location_arr:
                if pair["value"] == row_min:
                    location += pair["location"] + " " 
            location = location.rstrip()
            yield key, {"min":row_min,"location":location}

if __name__ == '__main__':
    Part3.run()
