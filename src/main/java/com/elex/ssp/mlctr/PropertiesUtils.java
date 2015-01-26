package com.elex.ssp.mlctr;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import com.elex.ssp.mlctr.idx.IdxType;

public class PropertiesUtils {

	private static Properties pop = new Properties();
	static {
		InputStream is = null;
		try {
			is = PropertiesUtils.class.getClassLoader().getResourceAsStream("conf.properties");
			pop.load(is);
		} catch (Exception e) {
			e.printStackTrace();

		} finally {
			try {
				if (is != null)
					is.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static String getRootDir() {

		return pop.getProperty("rootdir");
	}

	public static String getHiveurl() {
		return pop.getProperty("hive.url");
	}

	public static String getHiveUser() {
		return pop.getProperty("hive.user");
	}


	public static String getIdxPath() {
		return pop.getProperty("idx");
	}

	public static String getIdxCountPath() {
		return pop.getProperty("idx.count");
	}

	public static String getIdxMergeFilePath() {
		
		return pop.getProperty("idx.merge");
	}

	public static List<String> getMergeFileLists() {
		String[] names = pop.getProperty("idx.merge.files").split(",");
		List<String> filePathList = new ArrayList<String>();
		for(IdxType idx:IdxType.values()){
			if(Arrays.asList(names).contains(idx.name())){
				filePathList.add(idx.name());
			}
		}
		return filePathList;
	}

	public static String getIdxHiveTableName() {
		
		return pop.getProperty("idx.hive.table.name");
	}

	public static String getIdxHbaseTable() {

		return pop.getProperty("idx.hbase.table.name");
	}

}