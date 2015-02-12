#!/bin/sh
jardir='/home/hadoop/git_project_home/mlctr/target/'
workdir='/home/hadoop/wuzhongju/ctr/'
datadir='/data1/ssp/mlctr/'
hadoop jar $jardir'mlctr-0.0.1-SNAPSHOT.jar' com.elex.ssp.mlctr.Scheduler 0 5 >>$workdir'train.log' 2>&1
rm -rf $datadir'*.txt'
hadoop fs -getmerge /ssp/mlctr/test/output/part-r-* $datadir'test_id.txt'
hadoop fs -getmerge /ssp/mlctr/train/output/part-r-* $datadir'train_id.txt'
size=`du -sh $datadir'train_id.txt' | awk -F "G" '{print $1}'`
echo $size
if [ $size -gt 10 ];then
   ratio=`echo "scale=2;10/$size" | bc`
   echo $ratio
   java -cp .:$jardir'mlctr-0.0.1-SNAPSHOT.jar' com.elex.ssp.mlctr.liblinear.GJFormatSample $ratio $datadir'train_id.txt' $datadir'train_sam.txt'
   $workdir'liblinear_modified/'train -s 6 $datadir'train_sam.txt' model/ctr.model
   $workdir'liblinear_modified/'predict -b 1 $datadir'test_id.txt' model/ctr.model output/ctr.out
else
   $workdir'liblinear_modified/'train -s 6 $datadir'train_id.txt' model/ctr.model
   $workdir'liblinear_modified/'predict -b 1 $datadir'test_id.txt' model/ctr.model output/ctr.out
fi
cat $workdir'output/ctr.out' | python $workdir'liblinear_modified/calauc.py'

today=`date -d now +%Y%m%d`
cp $workdir'model/ctr.model' $workdir'bak/'$today'.model'
todaydir=$workdir$today
yestoday=$workdir`date -d yesterday +%Y%m%d`
rm -rf $yestoday
rm -rf $yestoday'.gz2'
mkdir $todaydir

cat $workdir'data/vec/0'* >> $todaydir'/user-vec.txt'
cp $workdir'model/ctr.model' $todaydir'/ctr.model'
cat $workdir'data/idx/merge.txt' >> $todaydir'/idx.txt'
cat $workdir'data/idx/user.idx' >> $todaydir'/idx.txt'
tar -cjf $today'.gz2' $todaydir
scp $today'.gz2' elex@10.102.66.212:/data/ml_model
