package cug.hadoop.geo.algorithm;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapred.lib.InputSampler.RandomSampler;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import cug.hadoop.geo.fileFormat.TileInputFormat;
import cug.hadoop.geo.fileFormat.TileOutputFormat;


public class LocalMax {
  public static class TokenizerMapper extends Mapper<LongWritable, BytesWritable, LongWritable, BytesWritable>{
    
    public void map(LongWritable key, BytesWritable value, Context context
        ) throws IOException, InterruptedException {
    	   byte []bytes = value.copyBytes();
    	   byte []b = null;
    	   
			ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
			ZipInputStream  zip = new ZipInputStream(bis);
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
		
    	
    	   BytesWritable bw = new BytesWritable(b); 
         context.write(key, bw);                            
    }
  }

  public static class IntSumReducer 
  extends Reducer<LongWritable,BytesWritable,LongWritable,BytesWritable> { 
    
    public void reduce(LongWritable key, Iterable<BytesWritable> values, 
        Context context 
        ) throws IOException, InterruptedException { 
            	Iterator<BytesWritable> ite = values.iterator();        
            	int temp;    
            	float t;
            	BytesWritable bw = ite.next();
    	         byte[] bytes =bw.getBytes();
    	         int length = bw.getLength();
    	        	float[] max = new float[length/4];
                 for(int j =0;j<length;j+=4){
                	   temp=(0xff & bytes[j]) | (0xff00 & (bytes[j+1] << 8)) | (0xff0000 & (bytes[j+2] << 16)) | (0xff000000 & (bytes[j+3] << 24));
                	   t=Float.intBitsToFloat(temp);
                	   max[j/4] = t;                
                        }
                  int m =1;
               while(ite.hasNext()){
                	 bw = ite.next();
                	 bytes = bw.getBytes();
                   for(int j =0;j<length;j+=4){
                	    temp=(0xff & bytes[j]) | (0xff00 & (bytes[j+1] << 8)) | (0xff0000 & (bytes[j+2] << 16)) | (0xff000000 & (bytes[j+3] << 24));
                 	    t=Float.intBitsToFloat(temp);             
                 	    if(t > max[j/4] )
 	    			         max[j/4] = t; 
                 	           
                          }       
                 }
          int data;
          byte[] maxbytes = new byte[length];
    	    for(int i = 0;i<length;i+=4){
    	    	     data = Float.floatToIntBits(max[i/4]);
    	    	     maxbytes[i] = (byte) (data & 0xff);  
    	    	     maxbytes[i+1] = (byte) ((data & 0xff00) >> 8);  
    	    	     maxbytes[i+2] = (byte) ((data & 0xff0000) >> 16);  
    	    	     maxbytes[i+3] = (byte) ((data & 0xff000000) >> 24);     	    	     
    	    }   
        	context.write(key, new BytesWritable(maxbytes)); 
    	
    }
  }

  public static void main(String[] args) throws Exception {
    /**  
    * JobConf：map/reduce的job配置类，向hadoop框架描述map-reduce执行的工作  
    * 构造方法：JobConf()、JobConf(Class exampleClass)、JobConf(Configuration conf)等  
    */    
	  if(args.length !=6){
			System.out.println("参数个数不匹配，应为6");
			return;
	 }	 
	 Configuration conf = new Configuration();  
	 
	 if("y".equals(args[4])){
	 conf.setBoolean("mapreduce.output.fileoutputformat.compress",true);
	 }
	 if("y".equals(args[5])){
	 conf.setClass("mapreduce.output.fileoutputformat.compress.codec",GzipCodec.class, CompressionCodec.class);
	 conf.set("mapreduce.map.output.compress","true");
	 }
	 conf.setBoolean("dfs.support.append", true);
	 Job job = Job.getInstance(conf, "localMax");//Job(Configuration conf, String jobName) 设置job名称和  
	 job.setJarByClass(LocalMax.class);  
	 
	 job.setMapperClass(TokenizerMapper.class); //为job设置Mapper类   
	 //job.setCombinerClass(IntSumReducer.class); //为job设置Combiner类    
	 job.setReducerClass(IntSumReducer.class); //为job设置Reduce类     
	 
	 job.setOutputKeyClass(LongWritable.class);        //设置输出key的类型  
	 job.setOutputValueClass(BytesWritable.class);//  设置输出value的类型 
	 job.setInputFormatClass(TileInputFormat.class);
	 job.setOutputFormatClass(TileOutputFormat.class);
	 job.setPartitionerClass(TotalOrderPartitioner.class);
	 int tasksNum = Integer.parseInt(args[3]);
	 job.setNumReduceTasks(tasksNum);
	 String s1 = args[0];
	 String s2 = args[1];
	 String inputPath1="hdfs://master:9000"+s1;
	 String inputPath2="hdfs://master:9000"+s2;
	// Path partitionFile = new Path("hdfs://192.168.1.200:9000/partitionFile");
	 String s3 = args[2];
	 String outputPath="hdfs://master:9000"+s3;
	 FileInputFormat.addInputPath(job, new Path(inputPath1)); 
	 FileInputFormat.addInputPath(job, new Path(inputPath2)); 
	 FileOutputFormat.setOutputPath(job, new Path(outputPath));//为map-reduce任务设置OutputFormat实现类  设置输出路径
	 long startMili=System.currentTimeMillis();// 当前时间对应的毫秒数
	 if(tasksNum>1){
	 // RandomSampler第一个参数表示key会被选中的概率，第二个参数是一个选取samples数，第三个参数是最大读取input splits数
		 RandomSampler<LongWritable, BytesWritable> sampler = new InputSampler.RandomSampler<LongWritable, BytesWritable>(0.1, 1000, 10);	 
		 // 设置partition file全路径到conf  
	  //  TotalOrderPartitioner.setPartitionFile(conf, partitionFile);     
	     // 写partition file到mapreduce.totalorderpartitioner.path  
	    InputSampler.writePartitionFile(job, sampler); 
	    String partitionFile = TotalOrderPartitioner.getPartitionFile(conf);
	    URI partitionUri= new URI(partitionFile);//？？
	    job.addCacheArchive(partitionUri);//添加一个档案进行本地化
	 }
	 boolean state= job.waitForCompletion(true);
	 long endMili=System.currentTimeMillis();
	 System.out.println("总耗时为："+(endMili-startMili)+"毫秒");
	 System.exit(state? 0 : 1);  
  }
}
