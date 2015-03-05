package com.elex.ssp.mlctr.idx;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import com.elex.ssp.mlctr.vector.Feature;
import com.elex.ssp.mlctr.vector.FeaturePrefix;

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
		String dst = PropertiesUtils.getUserOdpClusterPath()+"/odp.txt";
		File file = new File(dst);
		if(file.isFile() && file.exists()){
			file.delete();
		}
		FileUtil.copyMerge(FileSystem.get(conf), new Path(PropertiesUtils.getHiveWareHouse()+"/user_tag"), 
				FileSystem.getLocal(conf), new Path(dst), false, conf, "\n");		
		List<String> odpFiles = new ArrayList<String>();		
		odpFiles.add(PropertiesUtils.getUserOdpClusterPath()+"/odp.txt");
		loadIndexToHbase(odpFiles,"com.elex.ssp.mlctr.idx.HBasePutterUserOdp");
		
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
		
		String hql="insert into table "+PropertiesUtils.getIdxHiveTableName()+" partition(idx_type='cluster') select uid,CONCAT('odp_',tag) from user_tag";
		stmt.execute(hql);
		
		hql = "INSERT OVERWRITE LOCAL DIRECTORY '"
				+ PropertiesUtils.getUserWordVectorPath()
				+ "' ROW format delimited FIELDS TERMINATED BY ',' stored AS textfile"
				+ " select b.user, concatspace(CONCAT_WS(':',a.idx,'1')) from "
				+ " (select idx_key as key,idx from "+ PropertiesUtils.getIdxHiveTableName()+" where idx_type = 'merge')a"
				+ " join "
				+ " (select idx_key as user,idx from "+ PropertiesUtils.getIdxHiveTableName()+" where idx_type = 'cluster')b"				
				+ " ON a.key=b.idx group by b.user";
		stmt.execute(hql);
		System.out.println(hql);
		stmt.close();				
		
	}
	
	public static Map<String, String> loadOdpClusterIdxMap() {

		BufferedReader reader;
		Map<String, String> odp_cluster_map = new HashMap<String, String>();
		try {
			reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(PropertiesUtils.getIdxMergeFilePath()))));
			String line = reader.readLine();
			while (line != null) {
				String[] vList = line.split(",");
				if (vList.length == 2) {
					if (vList[0].startsWith(FeaturePrefix.odp.getsName() + "_")|| vList[0].startsWith(FeaturePrefix.cluster.getsName() + "_")) {
						odp_cluster_map.put(vList[0], vList[1]);
					}
				}

				line = reader.readLine();
			}
			reader.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return odp_cluster_map;
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
