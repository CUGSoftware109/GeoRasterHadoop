package cug.hadoop.geo.algorithm;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapred.lib.InputSampler.RandomSampler;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import cug.hadoop.geo.fileFormat.TileInputFormat;
import cug.hadoop.geo.fileFormat.TileOutputFormat;
import cug.hadoop.geo.utils.ClassifyMsg;


public class GreaterThan {
	public static class MyMapper extends Mapper<LongWritable, BytesWritable, LongWritable, BytesWritable> {

		public void map(LongWritable key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
			byte[] bytes = value.copyBytes();
			byte[] b = null;
			ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
			ZipInputStream zip = new ZipInputStream(bis);
			while (zip.getNextEntry() != null) {
				byte[] buf = new byte[1024];
				int num = -1;
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				while ((num = zip.read(buf, 0, buf.length)) != -1) {
					baos.write(buf, 0, num);
				}
				b = baos.toByteArray();
				baos.flush();
				baos.close();
			}
			zip.close();
			bis.close();
			context.write(key, new BytesWritable(b));
		}
	}

	/**
	 * @author root
	 *
	 */
	public static class MyReducer extends Reducer<LongWritable, BytesWritable, LongWritable, BytesWritable> {
		private static final float INVAILDDATA = 1.70141E38f;
		private static final float GREATER_THAN_PARAM = 500.0f;
		private static ArrayList<ClassifyMsg> classifyMsgList;
		public static long[] sumList;

		/**
		 * Parse tuple information and store it in classifyMsgList
		 * 
		 * @param classify_inputPath
		 * @return
		 * @throws IOException
		 */
		public static void getClassifyMsgList(String classify_inputPath) throws IOException {
			Configuration conf = new Configuration();
			classifyMsgList = new ArrayList<ClassifyMsg>();
			FSDataInputStream fsis = null;
			try {
				FileSystem fs = FileSystem.get(URI.create("hdfs://masters"), conf);
				fsis = fs.open(new Path("hdfs://master:9000" + classify_inputPath));
				byte[] bytes = new byte[4096];
				fsis.read(bytes);
				String s = new String(bytes, "utf-8");
				s = s.replaceAll("( )( )+", "");
				s = s.trim();
				String[] yuanzu = s.split(" ");
				sumList = new long[yuanzu.length + 1];
				for (int i = 0; i < yuanzu.length; i++) {
					yuanzu[i] = yuanzu[i].replace("(", " ");
					yuanzu[i] = yuanzu[i].replace(")", " ");
					yuanzu[i] = yuanzu[i].trim();
					String[] px = yuanzu[i].split(",");
					if (px.length != 3) {
						System.out.println("The parameter format is incorrect！");
						System.exit(0);
					}
					float min = Float.parseFloat(px[0]);
					float max = Float.parseFloat(px[1]);
					float level = Float.parseFloat(px[2]);
					classifyMsgList.add(new ClassifyMsg(min, max, level));
				}
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					fsis.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}


		public int fromBytestoInt(byte[] b, int start) {
			int temp;
			if (b.length >= start + 4)
				temp = (0xff & b[start]) | (0xff00 & (b[start + 1] << 8)) | (0xff0000 & (b[start + 2] << 16))
						| (0xff000000 & (b[start + 3] << 24));
			else
				temp = Integer.MAX_VALUE;
			return temp;
		}


		public Float fromBytestoFloat(byte[] b, int start) {
			float temp;
			int tempi = fromBytestoInt(b, start);
			if (tempi == Integer.MAX_VALUE)
				temp = Float.MAX_VALUE;
			else
				temp = Float.intBitsToFloat(tempi);
			return temp;
		}

		public void reduce(LongWritable key, Iterable<BytesWritable> values, Context context)
				throws IOException, InterruptedException {
			Iterator<BytesWritable> ite = values.iterator();
			byte[] valueBytes = ite.next().copyBytes();
			float currentElevation, currentSlope;
			int length = valueBytes.length;
			float[] change_tile = new float[length / 4 - 1];

			for (int j = 4; j < length; j += 4) {
				currentElevation = fromBytestoFloat(valueBytes, j);
				if (currentElevation == INVAILDDATA ) {
					change_tile[j / 4 - 1] = INVAILDDATA;
					continue;
				}
				if(currentElevation>GREATER_THAN_PARAM){
					change_tile[j / 4 - 1] = 1.0f;
				}else{
					change_tile[j / 4 - 1] = 0.0f;
				}
			}
			int temp_data;
			byte[] result_bytes = new byte[length - 4];
			for (int i = 0; i < result_bytes.length; i += 4) {
				temp_data = Float.floatToIntBits(change_tile[i / 4]);
				result_bytes[i] = (byte) (temp_data & 0xff);
				result_bytes[i + 1] = (byte) ((temp_data & 0xff00) >> 8);
				result_bytes[i + 2] = (byte) ((temp_data & 0xff0000) >> 16);
				result_bytes[i + 3] = (byte) ((temp_data & 0xff000000) >> 24);
			}
			context.write(key, new BytesWritable(result_bytes));
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		if (args.length != 6) {
			System.out
				.println("Usage: [inputPath1] [inputPath2] [outputPath] [reduceTaskNum] [mapCompress] [reduceCompress]");
			return;
		}

		if ("y".equals(args[4])) {
			conf.set("mapreduce.map.output.compress", "true");
		}
		if ("y".equals(args[5])) {
			conf.setBoolean("mapreduce.output.fileoutputformat.compress", true);
			conf.setClass("mapreduce.output.fileoutputformat.compress.codec", GzipCodec.class, CompressionCodec.class);
		}
		conf.setBoolean("dfs.support.append", true);
		// Job job = new Job(conf, "Reclassify");//Job(Configuration conf,
		// String jobName) 设置job名称
		String[] temp = args[2].split("/");

		Job job = Job.getInstance(conf, "GreaterThan_" + temp[temp.length - 1]);
		job.setJarByClass(GreaterThan.class);
		job.setMapperClass(MyMapper.class);
		// job.setCombinerClass(MyReducer.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(BytesWritable.class);
		job.setInputFormatClass(TileInputFormat.class);
		job.setOutputFormatClass(TileOutputFormat.class);
		String s1 = args[0];
		String s2 = args[1];
		// MyReducer.getClassifyMsgList(args[2]);
		String s3 = args[2];
		String inputPath1 = "hdfs://masters" + s1;
		//String inputPath2 = "hdfs://masters" + s2;
		String outputPath = "hdfs://masters" + s3;
		TileInputFormat.addInputPath(job, new Path(inputPath1));
		//TileInputFormat.addInputPath(job, new Path(inputPath2));
		TileOutputFormat.setOutputPath(job, new Path(outputPath));
		int tasksNum = Integer.parseInt(args[3]);
		job.setNumReduceTasks(tasksNum);

		long startMili = System.currentTimeMillis();
		if (tasksNum > 1) {
			job.setPartitionerClass(TotalOrderPartitioner.class);
			RandomSampler<LongWritable, BytesWritable> sampler = new InputSampler.RandomSampler<LongWritable, BytesWritable>(
					0.1, 5000, 20);
			InputSampler.writePartitionFile(job, sampler);
			String partitionFile = TotalOrderPartitioner.getPartitionFile(conf);
			URI partitionUri = new URI(partitionFile);
			job.addCacheArchive(partitionUri);
		}

		boolean state = job.waitForCompletion(true);
		long endMili = System.currentTimeMillis();
		System.out.println("total time：" + (endMili - startMili) + "miniSecond");

		/*
		 * for(int i = 0;i<MyMapper.sumList.length;i++){
		 * System.out.println(MyMapper.sumList[i]); }
		 */

		System.exit(state ? 0 : 1);
	}
}