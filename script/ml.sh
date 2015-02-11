#!/bin/sh
hadoop jar /home/hadoop/git_project_home/mlctr/target/mlctr-0.0.1-SNAPSHOT.jar com.elex.ssp.mlctr.Scheduler 0 5
hadoop fs -getmerge /ssp/mlctr/test/output/part-r-* /data1/ssp/mlctr/test_id.txt
hadoop fs -getmerge /ssp/mlctr/train/output/part-r-* /data1/ssp/mlctr/train_id.txt
/home/hadoop/wuzhongju/ctr/liblinear_modified/train -s 6 /data1/ssp/mlctr/train_id.txt /home/hadoop/wuzhongju/ctr/model/ctr.model
/home/hadoop/wuzhongju/ctr/liblinear_modified/predict -b 1 /data1/ssp/mlctr/test_id.txt /home/hadoop/wuzhongju/ctr/model/ctr.model /home/hadoop/wuzhongju/ctr/output/ctr.out
cat /home/hadoop/wuzhongju/ctr/output/ctr.out | python /home/hadoop/wuzhongju/ctr/liblinear_modified/calauc.py
