package com.elex.ssp.mlctr.vector;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.elex.ssp.TimeUtils;
import com.elex.ssp.mlctr.Constants;

//TextOutputFormat的输出文件key为long的字节偏移量
public class VectorizeMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] values = value.toString().split("\\x01");

		int impr = 0, click = 0;
		String[] tF = TimeUtils.getTimeDimension(new String[] { values[3],values[4] });
		if (tF.length == 3) {
			String newKey = FeaturePrefix.user.getsName() + "_" + values[0] ;		            

			impr = values[10] == null ? 0 : Integer.parseInt(values[10]);
			click = values[11] == null ? 0 : Integer.parseInt(values[11]);

			String newVal = impr + "\t" + click+"\t"
					+ FeaturePrefix.project.getsName() + "_" + values[1]+ "\t"
					+ FeaturePrefix.area.getsName() + "_"+ Constants.getArea(values[2]) + "\t"
					+ FeaturePrefix.time.getsName() + "_" + tF[0] + "\t"
					+ FeaturePrefix.time.getsName() + "_" + tF[1] + "\t"
					+ FeaturePrefix.time.getsName() + "_" + tF[2] + "\t"
					+ FeaturePrefix.nation.getsName() + "_" + values[4] + "\t"
					+ FeaturePrefix.browser.getsName() + "_" + values[5] + "\t"
					+ FeaturePrefix.os.getsName() + "_" + values[6] + "\t"
					+ FeaturePrefix.adid.getsName() + "_" + values[7] + "\t"
					+ FeaturePrefix.ref.getsName() + "_" + values[8] + "\t"
					+ FeaturePrefix.opt.getsName() + "_" + values[9];

			context.write(new Text(newKey), new Text(newVal));
		}

	}

}
