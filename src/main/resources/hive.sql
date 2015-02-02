add jar /home/hadoop/wuzhongju/ssp/feUDF-1.0.jar;
CREATE TEMPORARY FUNCTION concatspace AS 'com.elex.ssp.udf.GroupConcatSpace';

INSERT OVERWRITE LOCAL DIRECTORY '/home/hadoop/wuzhongju/ctr/data/input' ROW format delimited FIELDS TERMINATED BY ',' stored AS textfile 
SELECT 
  l.uid,
  l.pid,
  l.nation,
  l.ip,
  l.ua,
  l.os,
  l.width,
  l.height,
  l.adid,
  l.time,
  l.dt,
  l.ref,
  l.opt,
  w.q 
FROM
  (SELECT 
    t.uid,
    concatspace (
      CONCAT_WS(
        ':',
        t.word,
        CAST(t.tfidf AS STRING),
        t.source
      )
    ) AS q 
  FROM
    odin.tfidf t 
  GROUP BY t.uid) w 
  RIGHT outer JOIN odin.log_merge l 
    ON w.uid = l.uid 
WHERE l.day = '20150119' 
  AND array_contains (array ('br'), l.nation) 
  
/**#################索引规模预估-start#########################**/
select count(1) from(select distinct sourec,word from tfidf)a;====1013808
select count(1) from(select distinct word from tfidf)a;====956219
select count(1) from(select distinct uid from tfidf)a;============3400819

SELECT 
  CASE
    a.ft 
    WHEN 'user' 
    THEN 'user' 
    ELSE 'other' 
  END,
  COUNT(1) 
FROM
  (SELECT DISTINCT 
    ft,
    fv 
  FROM
    feature_merge where ft !='keyword') a 
GROUP BY 
  CASE
    a.ft 
    WHEN 'user' 
    THEN 'user' 
    ELSE 'other' 
  END 
/**
other=3975639
query=3971371
user=8642779
#################索引规模预估-finish#########################

#################索引材料准备-start#########################**/
INSERT OVERWRITE LOCAL DIRECTORY '/home/hadoop/wuzhongju/ctr/data/idx/other' ROW format delimited FIELDS TERMINATED BY ',' stored AS textfile 
select distinct concat_ws('_',ft,fv) from feature_merge where ft !='keyword' and ft !='user' and ft not like 'query%'


INSERT OVERWRITE LOCAL DIRECTORY '/home/hadoop/wuzhongju/ctr/data/idx/user' ROW format delimited FIELDS TERMINATED BY ',' stored AS textfile 
select distinct concat_ws('_',ft,fv) from feature_merge where ft ='user'

INSERT OVERWRITE LOCAL DIRECTORY '/home/hadoop/wuzhongju/ctr/data/idx/fixed' ROW format delimited FIELDS TERMINATED BY ',' stored AS textfile 
select distinct concat_ws('_','os',os) from log_merge where day='20150121'

INSERT OVERWRITE LOCAL DIRECTORY '/home/hadoop/wuzhongju/ctr/data/idx/fixed' ROW format delimited FIELDS TERMINATED BY ',' stored AS textfile 
select distinct concat_ws('_','nation',nation) from log_merge where day='20150121'

INSERT OVERWRITE LOCAL DIRECTORY '/home/hadoop/wuzhongju/ctr/data/idx/word' ROW format delimited FIELDS TERMINATED BY ',' stored AS textfile 
select distinct concat_ws('_',source,word) from tfidf

/**#################索引材料准备-finish#########################
 * 
 * 
 * 
 * =====================新建hive索引表========================**/

create table ssp_idx_mapping(idx_key string,idx int) partitioned by(idx_type string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' stored as textfile;


/**================创建hbase索引表===================**/

create 'ssp_idx_mapping',{NAME => 'idx', VERSIONS => 1, TTL => 172800, IN_MEMORY => true},{NUMREGIONS => 5,SPLITALGO =>'UniformSplit'}

/**===============去掉只出现一次的word特征值====================================**/
select count(1) from (select concat(source,word),count(1) as rc from tfidf group by concat(source,word))a where a.rc=1;===815561

select count(distinct concat(source,word)) from tfidf;==1149763

/**====================统计impr低于某个阈值的用户数========================**/
select count(1) from(select fv,sum(impr) as s_i from feature_merge where ft='user' and array_contains (array ('br','in'), nation) group by fv)a where a.s_i <10;==4779741




