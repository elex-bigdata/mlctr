package com.elex.ssp.mlctr;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.classify.WeightedVectorWritable;
import org.apache.mahout.math.NamedVector;

import com.elex.ssp.mlctr.idx.IdxType;
import com.elex.ssp.mlctr.vector.FeaturePrefix;

public class FormatConvertor {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) {
		if (args.length != 3) {
			System.out.print("com.elex.ssp.mlctr.FormatConvertor:参数不足！");
			System.exit(1);
		} else {

			try {
				if (args[2].equals("txt-seq")) {
					txtToSeq(new Path(args[0]), new Path(args[1]));
				} else if (args[2].equals("seq-txt")) {
					readSeqfileToLocal(args[0], args[1]);
				} else {
					System.out.print("com.elex.ssp.mlctr.FormatConvertor:不支持的转换方式==="+ args[2] + "！");
					System.exit(1);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}

		}

		// readLocalDfsFile();

	}

	public static void txtToSeq(Path src, Path dist) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		BufferedReader reader = null;
		SequenceFile.Writer writer = SequenceFile.createWriter(conf,
				Writer.file(dist), SequenceFile.Writer.keyClass(Text.class),
				SequenceFile.Writer.valueClass(Text.class));

		Path hdfs_src;
		FileStatus[] srcFiles = fs.listStatus(src);
		String line;

		for (FileStatus file : srcFiles) {

			if (!file.isDirectory()) {
				hdfs_src = file.getPath();
				if (file.getPath().getName().startsWith("0")) {
					try {
						reader = new BufferedReader(new InputStreamReader(fs.open(hdfs_src)));
						line = reader.readLine();
						while (line != null) {
							if (line.split(",").length == 2) {
								writer.append(new Text(line.split(",")[0]),new Text(line.split(",")[1]));
							}
							line = reader.readLine();
						}

					} catch (IOException e) {
						e.printStackTrace();
					} finally {
						IOUtils.closeStream(reader);
					}
				}
			}
		}

		writer.close();

	}

	public static void readSeqfileToLocal(String uri, String localDist)
			throws IOException {				
		Configuration conf = new Configuration();				
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] files = fs.listStatus(new Path(uri));
		
		Set<String> clusters = new HashSet<String>();
		SequenceFile.Reader reader = null;
		
		BufferedWriter out = new BufferedWriter(new FileWriter(new File(localDist)));
		Path hdfs_src =null;
		
		 for(FileStatus file:files){
			 if(!file.isDirectory()){
				 hdfs_src = file.getPath();
	        		if(file.getPath().getName().contains("part")){
	        			try {
	        				reader = new SequenceFile.Reader(conf, Reader.file((hdfs_src)));
	        				
	        				//Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);	        				
	        				//Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
	        				
	        				IntWritable cluster = new IntWritable();
	        				WeightedVectorWritable user = new WeightedVectorWritable();
	        				NamedVector userVec;
	        				while (reader.next(cluster, user)) {
	        					if(!clusters.contains(FeaturePrefix.cluster.getsName()+"_"+cluster.toString())){
	        						clusters.add(FeaturePrefix.cluster.getsName()+"_"+cluster.toString());
	        					}
	        					userVec = (NamedVector) user.getVector();
	        					out.write(userVec.getName().replace(",", "") + "," +  FeaturePrefix.cluster.getsName()+"_"+cluster.toString()+ "\r\n");
	        					
	        				}	        					        				
	        				
	        			}  catch (IOException e) {
	        				e.printStackTrace();
	        			}finally {
	        				
	        				IOUtils.closeStream(reader);
	        				
	        			}
	        		}
				 
			 }
		 }
		 
		 out.close();
		 		 
		 out = new BufferedWriter(new FileWriter(new File(IdxType.cluster.getSrc()+"/cluster.txt")));
		 Iterator<String> ite = clusters.iterator();
		 
		 while(ite.hasNext()){
			 
			 out.write(ite.next()+"\r\n");
		 }
		 
		 out.close();
		 	
	}
	
	public static void readLocalDfsFile() {
		// FSDataInputStream fsin = new FSDataInputStream(new
		// DataInputStream(new FileInputStream(new File("D:\\call"))));
		Configuration conf = new Configuration();
		FileSystem fs;
		SequenceFile.Reader reader = null;
		try {
			fs = FileSystem.getLocal(conf);
			FSDataInputStream fsin = fs.open(new Path("D:\\c0"));
			reader = new SequenceFile.Reader(conf, Reader.stream(fsin));
			IntWritable key = new IntWritable();
			WeightedVectorWritable value = new WeightedVectorWritable();
			while (reader.next(key, value)) {
				System.out.print(key.toString() + "===="
						+ value.getVector().asFormatString() + "\r\n");
			}
		} catch (IOException e) {

		} finally {
			IOUtils.closeStream(reader);
		}

	}

}
