#encode: utf-8

import sys

for line in sys.stdin:
    fileds = line.strip().split(' ')
    impr = int(fileds[0])
    click = int(fileds[1])
    feature = {}
    key = []
    for item in fileds[2:]:
        i = item.split(':')
        feature.setdefault(int(i[0]), int(i[1]))
        key.append(int(i[0]))

    key.sort()
    output = '%d %d'%(impr,click)
    for idx in key:
        output += ' %d:%d'%(idx, feature[idx])
 
    print output
   

