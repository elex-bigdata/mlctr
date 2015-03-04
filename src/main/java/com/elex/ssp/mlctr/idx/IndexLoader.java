package com.elex.ssp.mlctr.idx;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hbase.client.HTableInterface;

import com.elex.ssp.mlctr.HbaseBasis;
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
		
		List<String> files = new ArrayList<String>();
		
		//files.add(IdxType.user.getDist());
		//files.add(IdxType.word.getDist());
		files.add(PropertiesUtils.getIdxMergeFilePath());
		
		loadIndexToHbase(files,"com.elex.ssp.mlctr.idx.HBasePutterMergeIdx");
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
		HTableInterface tableName = HbaseBasis.getConn().getTable(PropertiesUtils.getIdxHbaseTable());
		
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
					 putter.setTable(tableName);
					 
					jobs.add(service.submit(putter));
					lines = new ArrayList<String>();					
				}
			}
			if (lines.size() > 0) {
				 Class onwClass = Class.forName(putterClassName);
				 HBasePutter putter = (HBasePutter) onwClass.newInstance();
				 putter.setFile(file);
				 putter.setLines(lines);
				 putter.setTable(tableName);
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
