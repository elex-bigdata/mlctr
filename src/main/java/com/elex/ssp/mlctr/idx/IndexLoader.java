package com.elex.ssp.mlctr.idx;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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
	 */
	public static void main(String[] args) throws SQLException, IOException {
		
		loadIndexToHive();
		
		List<String> files = new ArrayList<String>();
		
		files.add(IdxType.user.getDist());
		files.add(IdxType.word.getDist());
		files.add(PropertiesUtils.getIdxMergeFilePath());
		
		loadIndexToHbase(files);

	}

	public static void loadIndexToHive() throws SQLException {

		String hql = "load data local '"+ PropertiesUtils.getIdxMergeFilePath()+ "' overwrite into table "
				+ PropertiesUtils.getIdxHiveTableName()+ " partition(idx_type='merge')";

		HiveOperator.loadDataToHiveTable(hql);
		System.out.println(hql);

		hql = "load data local '" + IdxType.user.getDist()+ "' overwrite into table "
				+ PropertiesUtils.getIdxHiveTableName()+ " partition(idx_type='user')";

		HiveOperator.loadDataToHiveTable(hql);
		System.out.println(hql);

		hql = "load data local '" + IdxType.word.getDist()+ "' overwrite into table "
				+ PropertiesUtils.getIdxHiveTableName()+ " partition(idx_type='word')";

		HiveOperator.loadDataToHiveTable(hql);
		System.out.println(hql);

	}

	public static void loadIndexToHbase(List<String> files) throws IOException {
		ExecutorService service = new ThreadPoolExecutor(5, 20, 60,TimeUnit.MILLISECONDS, new LinkedBlockingDeque<Runnable>());
		long startTime = System.currentTimeMillis();
		int batchSize = 200000;
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
				if (lines.size() == batchSize) {
					jobs.add(service.submit(new HBasePutter(tableName, lines,file)));
					lines = new ArrayList<String>();
					total++;
				}
			}
			if (lines.size() > 0) {
				jobs.add(service.submit(new HBasePutter(tableName, lines,file)));
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
