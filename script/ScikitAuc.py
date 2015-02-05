#coding=utf-8
'''
Created on 2015-2-4

@author: Administrator
'''
from numpy import genfromtxt
from sklearn import metrics
from sklearn.metrics import confusion_matrix
import sys

def evaluateModel(ptagfile,otagfile,pscorefile):
    ptag = genfromtxt(ptagfile, delimiter=',',dtype=int)
    otag = genfromtxt(otagfile, delimiter=',',dtype=int)
    pscore = genfromtxt(pscorefile, delimiter=',',dtype=float)
    fpr, tpr, thresholds = metrics.roc_curve(otag, pscore)
    auc = metrics.auc(fpr, tpr)
    mat = confusion_matrix(otag, ptag)
    return auc,mat

if __name__ == '__main__':
    '''-1为不点击，1为点击，输入为三个文本文件，依次是是否点击的预测结果文件，原始点击的输入文件，点击率预测结果文件。
                   每个文件的行数相等，每行关系对应，所有的文件每行都只有一列。
                   调用方式为：python auc.py ptaggj_1.txt otaggj_1.txt pscoregj.txt >> gjresult.txt 2>&1 &'''
    if len(sys.argv) ==4:
        print sys.argv
        auc,mat = evaluateModel(sys.argv[1],sys.argv[2],sys.argv[3])
        print auc
        print mat
