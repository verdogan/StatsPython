import json
import argparse
from math import floor, ceil


def bucketize(minval, maxval, hasfloat):
    if maxval < minval:
        exit(1)
    buckets = []
    if hasfloat == 't':
        if maxval == minval:
            return ["0-0"]
        maxval = float(maxval)
        minval = float(minval)
        interval = floor(100*(maxval-minval)/8)/100.0
        # print(interval)
        for i in range(0,8):
            if i == 0:
                buckets.append(str(minval + i*interval) + "-" + str(minval + (i+1)*interval))
            elif i == 7:
                buckets.append(str(floor(minval*100 + i*interval*100 + 1)/100.0) + "-" + str(maxval))
            else:
                buckets.append(str(ceil(minval*100 + i*interval*100 + 1)/100.0) + "-" + str(ceil(100*minval + 100*(i+1)*interval)/100.0))
    else:
        minval = int(minval)
        maxval = int(maxval)
        optcount = maxval - minval + 1
        if optcount >= 9:
            if optcount % 8 == 0:
                for i in range(0,8):
                    buckets.append(str(minval + int(i*optcount/8)) + "-" + str(minval + int((i+1)*optcount/8) - 1))
            elif optcount % 7 == 0:
                for i in range(0,7):
                    buckets.append(str(minval + int(i * optcount / 7)) + "-" + str(minval + int((i + 1) * optcount / 7) - 1))
            elif optcount % 6 == 0:
                for i in range(0,6):
                    buckets.append(str(minval + int(i * optcount / 6)) + "-" + str(minval + int((i + 1) * optcount / 6) - 1))
            elif optcount % 5 == 0:
                for i in range(0,5):
                    buckets.append(str(minval + int(i * optcount / 5)) + "-" + str(minval + int((i + 1) * optcount / 5) - 1))
            else:
                for i in range(0, 8):
                    buckets.append(str(minval + int(i*optcount/8)) + "-" + str(minval + int((i+1)*optcount/8) - 1))
        elif optcount == 8:
            # buckets = ["0-1", "2-3", "4-5", "6-7"]
            buckets = []
            buckets.append(str(minval) + "-" + str(minval + 1))
            buckets.append(str(minval + 2) + "-" + str(minval + 3))
            buckets.append(str(minval + 4) + "-" + str(minval + 5))
            buckets.append(str(minval + 6) + "-" + str(minval + 7))
        elif optcount == 7:
            # buckets = ["0-1", "2-3", "4-6"]
            buckets = []
            buckets.append(str(minval) + "-" + str(minval + 1))
            buckets.append(str(minval + 2) + "-" + str(minval + 3))
            buckets.append(str(minval + 4) + "-" + str(minval + 6))
        elif optcount == 6:
            # buckets = ["0-1", "2-3", "4-5"]
            buckets = []
            buckets.append(str(minval) + "-" + str(minval + 1))
            buckets.append(str(minval + 2) + "-" + str(minval + 3))
            buckets.append(str(minval + 4) + "-" + str(minval + 5))
        elif optcount == 5:
            # buckets = ["0-1", "2-4"]
            buckets = []
            buckets.append(str(minval) + "-" + str(minval + 1))
            buckets.append(str(minval + 2) + "-" + str(minval + 4))
        elif optcount == 4:
            # buckets = ["0-1", "2-3"]
            buckets = []
            buckets.append(str(minval) + "-" + str(minval + 1))
            buckets.append(str(minval + 2) + "-" + str(minval + 3))
        elif optcount == 3:
            # buckets = ["0-1", "2-2"]
            buckets = []
            buckets.append(str(minval) + "-" + str(minval + 1))
            buckets.append(str(minval + 2) + "-" + str(minval + 2))
        elif optcount == 2:
            # buckets = ["0-0", "1-1"]
            buckets = []
            buckets.append(str(minval) + "-" + str(minval))
            buckets.append(str(minval + 1) + "-" + str(minval + 1))
        else:
            exit(1)
    # buckets = json.dumps(buckets)
    return buckets

"""
parser = argparse.ArgumentParser()
parser.add_argument('--minval', '-n', help="", type=float, default=0)
parser.add_argument('--maxval', '-x', help="", type=float, default=0)
parser.add_argument('--hasfloat', '-f', help="", type=str, default="")
args = parser.parse_args()
minval = args.minval
maxval = args.maxval
hasfloat = args.hasfloat


print(bucketize(minval, maxval, hasfloat))
"""