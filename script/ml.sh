#!/bin/sh

cluster

jarfile='/home/hadoop/git_project_home/mlctr/target/mlctr-2.0.jar'
workdir='/home/hadoop/wuzhongju/ctr/'
datadir='/data1/ssp/mlctr/'
hadoop jar $jarfile com.elex.ssp.mlctr.Scheduler 0 5 >>$workdir'train.log' 2>&1
rm -rf $datadir'*.txt'
hadoop fs -getmerge /ssp/mlctr/test/output/part-r-* $datadir'test_id.txt'
hadoop fs -getmerge /ssp/mlctr/train/output/part-r-* $datadir'train_id.txt'

today=`date -d now +%Y%m%d`
yestoday=$workdir`date -d yesterday +%Y%m%d`

mv $yestoday'.gz2' $workdir'bak/'

size=`du -sh $datadir'train_id.txt' | awk -F "G" '{print $1}'`
echo $size
if [ $size -gt 10 ];then
   ratio=`echo "scale=2;10/$size" | bc`
   echo $ratio
   java -cp .:$jarfile com.elex.ssp.mlctr.liblinear.GJFormatSample $ratio $datadir'train_id.txt' $datadir'train_sam.txt'
   $workdir'liblinear_modified/'train -s 6 $datadir'train_sam.txt' $workdir'model/ctr.model'   
else
   $workdir'liblinear_modified/'train -s 6 $datadir'train_id.txt' $workdir'model/ctr.model'
fi
$workdir'liblinear_modified/'predict -b 1 $datadir'test_id.txt' $workdir'model/ctr.model' $workdir'output/ctr.out'
cat $workdir'output/ctr.out' | python $workdir'liblinear_modified/calauc.py'

todaydir=$workdir$today
rm -rf $yestoday
rm -rf $yestoday'.gz2'
mkdir $todaydir

#cat $workdir'data/vec/0'* >> $todaydir'/user-vec.txt'
cp $workdir'model/ctr.model' $todaydir'/ctr.model'
cat $workdir'data/idx/merge.txt' >> $todaydir'/idx.txt'
#cat $workdir'data/idx/user.idx' >> $todaydir'/idx.txt'

cd $workdir
tar -cjf $today'.gz2' $today
scp $workdir$today'.gz2' elex@10.102.66.212:/data/ml_model
tail /home/hadoop/wuzhongju/ctr/train.log | mailx -s "mlctr" wuzhongju@126.com


function cluster(){
	begin=`date -d -30day +%Y%m%d`
	hive -e "add jar /home/hadoop/wuzhongju/ssp/feUDF-1.0.jar;CREATE TEMPORARY FUNCTION qn AS 'com.elex.ssp.udf.Query';CREATE TEMPORARY FUNCTION concatspace AS 'com.elex.ssp.udf.GroupConcatSpace';CREATE TEMPORARY FUNCTION mlsed AS 'com.elex.ssp.udf.Sed';INSERT OVERWRITE TABLE odin.user_docs select uid,concatspace(mlsed(qn(keyword))) from odin.search where array_contains(array('br','in'),nation) and day>'"$begin"' AND uid is not null group by uid" >>/home/hadoop/wuzhongju/userportrait/cluster.log 2>&1
	hadoop jar $jarfile com.elex.ssp.mlctr.FormatConvertor /user/hive/warehouse/odin.db/user_docs /ssp/userportrait/userdocs/user-doc txt-seq >>/home/hadoop/wuzhongju/userportrait/cluster.log 2>&1
	mahout seq2sparse  -i /ssp/userportrait/userdocs -o /ssp/userportrait/uservec -ow -wt tfidf -x 90 -s 3 -md 3 -seq -n 2 -nr 20 -nv >>/home/hadoop/wuzhongju/userportrait/cluster.log 2>&1
	mahout kmeans -i /ssp/userportrait/uservec/tfidf-vectors -c  /ssp/userportrait/kmeans/centroids -o /ssp/userportrait/kmeans/output -k 50 -x 10 -ow  --tempDir /ssp/userportrait/kmeans/temp -cl -cd 0.08 >>/home/hadoop/wuzhongju/userportrait/cluster.log 2>&1
	hadoop jar $jarfile com.elex.ssp.mlctr.FormatConvertor /ssp/userportrait/kmeans/output/clusteredPoints /home/hadoop/wuzhongju/ctr/data/user_odp_cluster/cluster_points.txt seq-txt
}
