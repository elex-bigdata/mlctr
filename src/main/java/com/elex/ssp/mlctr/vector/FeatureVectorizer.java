package com.elex.ssp.mlctr.vector;

import java.sql.Connection;
import java.sql.Statement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.elex.ssp.mlctr.Constants;
import com.elex.ssp.mlctr.HdfsUtil;
import com.elex.ssp.mlctr.HiveOperator;
import com.elex.ssp.mlctr.PropertiesUtils;

public class FeatureVectorizer extends Configured implements Tool {

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new FeatureVectorizer(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		Job job = Job.getInstance(conf, "FeatureVectorizer");
		job.setJarByClass(FeatureVectorizer.class);
		job.setMapperClass(VectorizeMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(VectorizeReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setNumReduceTasks(Integer.parseInt(args[2]));
		//job.setPartitionerClass(VectorizePartitioner.class);

		String setHql = "set hive.merge.smallfiles.avgsize=160000000";// 合并小于160M的小文件

		if (args[0].equals("train")) {
			String inPath = PropertiesUtils.getMachineLearningRootDir()+ "/train/input";
			String hql = "INSERT OVERWRITE DIRECTORY '"+ inPath+ "'"
					+ " select uid,pid,ip,time,nation,ua,os,adid,ref,opt,impr,click from log_merge where uid is not null and day>'"
					+ Constants.getLastNDay(PropertiesUtils.getTrainDays())+ "'" + " and day <'" + Constants.getLastNDay(5)
					+ "' and array_contains(array("+ PropertiesUtils.getNations()+ "),nation) and adid like '5%'";

			if (!args[1].equals("skip")) {
				System.out.println(hql);
				Connection con = HiveOperator.getHiveConnection();
				Statement stmt = con.createStatement();
				stmt.execute(setHql);
				stmt.execute(hql);
				stmt.close();
			}

			Path in = new Path(inPath);
			FileInputFormat.addInputPath(job, in);

			job.setOutputFormatClass(TextOutputFormat.class);
			MultipleOutputs.addNamedOutput(job, "plain",TextOutputFormat.class, Text.class, Text.class);
			MultipleOutputs.addNamedOutput(job, "idpositive",TextOutputFormat.class, Text.class, Text.class);
			MultipleOutputs.addNamedOutput(job, "idnegtive",TextOutputFormat.class, Text.class, Text.class);
			String output = PropertiesUtils.getMachineLearningRootDir()+ "/train/output";
			HdfsUtil.delFile(fs, output);
			FileOutputFormat.setOutputPath(job, new Path(output));

		} else if (args[0].equals("test")) {
			String inPath = PropertiesUtils.getMachineLearningRootDir()+ "/test/input";
			String hql = "INSERT OVERWRITE DIRECTORY '"+ inPath+ "'"
					+ " select uid,pid,ip,time,nation,ua,os,adid,ref,opt,impr,click"
					+ " from log_merge where uid is not null and  day>'"
					+ Constants.getLastNDay(5) + "'"+ " and array_contains(array("
					+ PropertiesUtils.getNations()+ "),nation) and adid like '5%'";

			if (!args[1].equals("skip")) {
				System.out.println(hql);
				Connection con = HiveOperator.getHiveConnection();
				Statement stmt = con.createStatement();
				stmt.execute(setHql);
				stmt.execute(hql);
				stmt.close();
			}

			Path in = new Path(inPath);
			FileInputFormat.addInputPath(job, in);

			job.setOutputFormatClass(TextOutputFormat.class);
			MultipleOutputs.addNamedOutput(job, "plain",TextOutputFormat.class, Text.class, Text.class);
			MultipleOutputs.addNamedOutput(job, "idpositive",TextOutputFormat.class, Text.class, Text.class);
			MultipleOutputs.addNamedOutput(job, "idnegtive",TextOutputFormat.class, Text.class, Text.class);
			String output = PropertiesUtils.getMachineLearningRootDir()+ "/test/output";
			HdfsUtil.delFile(fs, output);
			FileOutputFormat.setOutputPath(job, new Path(output));

		} else {
			System.err.println("FeatureVectorizer wrong arg value!!!" + args[0]);
		}

		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	//cat user.idx | cut -c 3 | sort | uniq -c | sort -nr
	/*public class VectorizePartitioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {

			char uid_first = key.toString().substring(2, 3).toCharArray()[0];
			
			return uid_first % numPartitions;
		}

	}*/

}
