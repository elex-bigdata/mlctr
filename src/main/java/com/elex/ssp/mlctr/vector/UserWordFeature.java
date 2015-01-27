package com.elex.ssp.mlctr.vector;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.elex.ssp.mlctr.HiveOperator;
import com.elex.ssp.mlctr.PropertiesUtils;
import com.elex.ssp.mlctr.idx.IndexLoader;

public class UserWordFeature {

	/**
	 * @param args
	 * @throws SQLException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws SQLException, IOException {
		createUserWordFeatureFile();
		loadUserWordVectorToHbase();
	}

	public static void createUserWordFeatureFile() throws SQLException {
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		stmt.execute("add jar " + PropertiesUtils.getHiveUdfJar());
		stmt.execute("CREATE TEMPORARY FUNCTION concatspace AS 'com.elex.ssp.udf.GroupConcatSpace'");
		String hql = "INSERT OVERWRITE LOCAL DIRECTORY '"
				+ PropertiesUtils.getUserWordVectorPath()
				+ "' ROW format delimited FIELDS TERMINATED BY ',' stored AS textfile"
				+ " SELECT CONCAT('u_',t.uid),concatspace(CONCAT_WS(':',cast(s.idx as string),cast(t.tfidf as string))) FROM "
				+ PropertiesUtils.getIdxHiveTableName()
				+ " s JOIN tfidf t ON CONCAT_WS('_',t.source,t.word) = s.idx_key WHERE s.idx_type='word' GROUP BY CONCAT('u_',t.uid)";
		stmt.execute(hql);
		System.out.println(hql);
		stmt.close();
	}

	public static void loadUserWordVectorToHbase() throws IOException {
		
		List<String> vector_files = new ArrayList<String>();
		File[] files = new File(PropertiesUtils.getUserWordVectorPath()).listFiles();
		for (File raw : files) {
			if (!raw.getName().endsWith("crc")) {
				vector_files.add(raw.getAbsolutePath());				
			}
		}
		
		IndexLoader.loadIndexToHbase(vector_files,true);
		

	}

}
