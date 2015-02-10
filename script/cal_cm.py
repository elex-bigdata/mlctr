#coding=utf-8
from sklearn.metrics import confusion_matrix
import sys
def cm(ptag,otag):
    mat = confusion_matrix(otag, ptag)
    return mat

if __name__ == '__main__':
    ptag = []
    otag = []
    for line in sys.stdin:
        item = line.rstrip('\n').split(' ')
        otag.append(item[0])
        ptag.append(item[1])
        
    mat = cm(ptag,otag)
    print mat
