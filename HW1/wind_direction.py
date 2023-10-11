import re
import json

from mrjob.job import MRJob

QUALITY_RE = re.compile(r"[01459]")

class Wind_Direction(MRJob):
    
    def mapper(self, _, line):
        val = line.strip()
        (wind_direction, wind_quality, temp, temp_quality) = (val[60:63], val[63:64], val[87:92], val[92:93])
        if (wind_direction != "999" and re.match(QUALITY_RE,wind_quality)) and (temp != "+9999" and re.match(QUALITY_RE, temp_quality)):
            yield wind_direction, {"temp":int(temp),"count":1}

    def reducer(self, key, values):
        count = 0
        temp_arr = []
        for i in values:
            count += i["count"]
            temp_arr.append((i["temp"]))
        min_temp = min(temp_arr)
        max_temp = max(temp_arr)
        yield int(key), {"low":min_temp,"high":max_temp,"count":count}

if __name__ == '__main__':
    Wind_Direction.run()
