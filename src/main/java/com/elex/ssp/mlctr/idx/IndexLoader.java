package com.elex.ssp.mlctr.idx;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import com.elex.ssp.mlctr.FormatConvertor;
import com.elex.ssp.mlctr.HiveOperator;
import com.elex.ssp.mlctr.PropertiesUtils;

public class IndexLoader {

	/**
	 * @param args
	 * @throws SQLException 
	 * @throws IOException 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
		
		load();

	}
	
	
	public static void load() throws SQLException, IOException, ClassNotFoundException, InstantiationException, IllegalAccessException{
        loadIndexToHive();
		
		List<String> mergeFiles = new ArrayList<String>();		
		//files.add(IdxType.user.getDist());
		//files.add(IdxType.word.getDist());
		mergeFiles.add(PropertiesUtils.getIdxMergeFilePath());		
		loadIndexToHbase(mergeFiles,"com.elex.ssp.mlctr.idx.HBasePutterMergeIdx");
		
		Configuration conf = new Configuration();
		FileUtil.copyMerge(FileSystem.get(conf), new Path(PropertiesUtils.getHiveWareHouse()+"/user_tag"), 
				FileSystem.getLocal(conf), new Path(PropertiesUtils.getUserOdpClusterPath()+"/odp.txt"), false, conf, "\n");		
		List<String> odpFiles = new ArrayList<String>();		
		odpFiles.add(PropertiesUtils.getUserOdpClusterPath()+"/odp.txt");
		loadIndexToHbase(odpFiles,"com.elex.ssp.mlctr.idx.HBasePutterUserOdp");
		
		FormatConvertor.readSeqfileToLocal(PropertiesUtils.getUserClusterResultPath(), PropertiesUtils.getUserOdpClusterPath()+"/cluster_points.txt");
		String hql = "load data local inpath '"+ PropertiesUtils.getUserOdpClusterPath()+"/cluster_points.txt"+ "' overwrite into table "
				+ PropertiesUtils.getIdxHiveTableName()+ " partition(idx_type='cluster')";
		HiveOperator.loadDataToHiveTable(hql);
		System.out.println(hql);
		List<String> clusterFiles = new ArrayList<String>();
		clusterFiles.add(PropertiesUtils.getUserOdpClusterPath()+"/cluster_points.txt");
		loadIndexToHbase(clusterFiles,"com.elex.ssp.mlctr.idx.HBasePutterUserCluster");
		createUserOdpClusterUnionFile();
		
		
	}
	
	public static void createUserOdpClusterUnionFile() throws SQLException {
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		stmt.execute("add jar " + PropertiesUtils.getHiveUdfJar());
		stmt.execute("CREATE TEMPORARY FUNCTION concatspace AS 'com.elex.ssp.udf.GroupConcatSpace'");
		String hql = "INSERT OVERWRITE LOCAL DIRECTORY '"
				+ PropertiesUtils.getUserWordVectorPath()
				+ "' ROW format delimited FIELDS TERMINATED BY ',' stored AS textfile"
				+ " select case when odp.uid is null then cluster.idx_key else odp.uid end as user,odp.tag as odp,cluster.idx as cluster from user_tag odp full outer join "
				+ PropertiesUtils.getIdxHiveTableName()
				+ " cluster on odp.uid = cluster.idx_key where cluster.idx_type='cluster'";
		stmt.execute(hql);
		System.out.println(hql);
		stmt.close();
	}

	public static void loadIndexToHive() throws SQLException {

		String hql = "load data local inpath '"+ PropertiesUtils.getIdxMergeFilePath()+ "' overwrite into table "
				+ PropertiesUtils.getIdxHiveTableName()+ " partition(idx_type='merge')";

		HiveOperator.loadDataToHiveTable(hql);
		System.out.println(hql);

		/*hql = "load data local inpath '" + IdxType.user.getDist()+ "' overwrite into table "
				+ PropertiesUtils.getIdxHiveTableName()+ " partition(idx_type='user')";

		HiveOperator.loadDataToHiveTable(hql);
		System.out.println(hql);

		hql = "load data local inpath '" + IdxType.word.getDist()+ "' overwrite into table "
				+ PropertiesUtils.getIdxHiveTableName()+ " partition(idx_type='word')";

		HiveOperator.loadDataToHiveTable(hql);
		System.out.println(hql);*/

	}

	public static void loadIndexToHbase(List<String> files,String putterClassName) throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
		ExecutorService service = new ThreadPoolExecutor(5, 20, 60,TimeUnit.MILLISECONDS, new LinkedBlockingDeque<Runnable>());
		long startTime = System.currentTimeMillis();
		int batchSize = 50000;
		int total = 0;
		FileInputStream fis = null;
		BufferedReader reader = null;
		List<Future<String>> jobs = new ArrayList<Future<String>>();
		String line;
		List<String> lines = new ArrayList<String>();
		
		for (String file : files) {
			fis = new FileInputStream(file);
			reader = new BufferedReader(new InputStreamReader(fis));
			while ((line = reader.readLine()) != null) {
				lines.add(line);
				total++;
				if (lines.size() == batchSize) {
					 Class onwClass = Class.forName(putterClassName);
					 HBasePutter putter = (HBasePutter) onwClass.newInstance();
					 putter.setFile(file);
					 putter.setLines(lines);
					 putter.setTableName(PropertiesUtils.getIdxHbaseTable());
					 
					jobs.add(service.submit(putter));
					lines = new ArrayList<String>();					
				}
			}
			if (lines.size() > 0) {
				 Class onwClass = Class.forName(putterClassName);
				 HBasePutter putter = (HBasePutter) onwClass.newInstance();
				 putter.setFile(file);
				 putter.setLines(lines);
				 putter.setTableName(PropertiesUtils.getIdxHbaseTable());
				jobs.add(service.submit(putter));
			}
		}
		
		for(Future<String> job : jobs){
            try {                
                System.out.println("Insert " + job.get(3,TimeUnit.MINUTES) + "/" + total + " lines spend " + (System.currentTimeMillis() - startTime) + "ms ");
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                //TODO; 线程内部异常处理
                e.printStackTrace();
            } catch (TimeoutException e) {
                //TODO: 超时异常
                e.printStackTrace();
            }
        }		

        service.shutdownNow();

	}

}
