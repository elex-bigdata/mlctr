#coding=utf-8
'''
Created on 2015-2-6

@author: Administrator
'''
import sys

if __name__ == '__main__':
    for line in sys.stdin:
        item = line.rstrip('\n').split(' ')
        print '%s %.6f' %(item[0],float(item[1]))