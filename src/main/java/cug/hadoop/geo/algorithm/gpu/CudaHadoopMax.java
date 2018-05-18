package cug.hadoop.geo.algorithm.gpu;

import static jcuda.driver.JCudaDriver.*;
import jcuda.*;
import jcuda.driver.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.zip.ZipInputStream;

import jcuda.Pointer;
import jcuda.Sizeof;
import jcuda.driver.CUcontext;
import jcuda.driver.CUdevice;
import jcuda.driver.CUdeviceptr;
import jcuda.driver.CUfunction;
import jcuda.driver.CUmodule;
import jcuda.driver.JCudaDriver;
import jcuda.runtime.cudaStream_t;

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


//压缩瓦片文件
public class CudaHadoopMax {	
	 private static final float INVAILDDATA=1.70141E38f;//填充的无用数据，与地图中无用数据一致，为1.70141E38
		  public static class MyMapper extends Mapper<LongWritable, BytesWritable, LongWritable, BytesWritable>{
			
			public void map(LongWritable key, BytesWritable value, Context context ) throws IOException, InterruptedException {
		    	//瓦片解压**************************************************   
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
				   context.write(key, new BytesWritable(b));                        
		        }
  }

/**
 * @author root
 *
 */
public static class MyReducer extends Reducer<LongWritable,BytesWritable,LongWritable,BytesWritable> { 
	  
	   private static ArrayList<ClassifyMsg> classifyMsgList; //保存元组信息到该list中
		public static long[] sumList;//统计每个元组中的元素个数	
		
		/**
		 * 解析元组信息，存入classifyMsgList中
		 * @param classify_inputPath
		 * @return
		 * @throws IOException
		 */
		public static void  getClassifyMsgList(String classify_inputPath) throws IOException{
			 Configuration conf = new Configuration(); 
			 classifyMsgList = new ArrayList<ClassifyMsg>(); 
		    FSDataInputStream fsis = null;
		    try{  
	  		      FileSystem fs = FileSystem.get(URI.create("hdfs://master:9000"), conf);
			      fsis = fs.open(new Path("hdfs://master:9000"+classify_inputPath));
			  	   byte[] bytes = new byte[4096];
				   fsis.read(bytes);
				   String s= new  String(bytes, "utf-8");
				   s=s.replaceAll("( )( )+","");// 去掉字符中间多于2个的空格
				   s =s.trim();//去掉首尾多余的空格
			      String[] yuanzu =s.split(" ");//按空格分割元组
			      sumList = new long[yuanzu.length+1];//求和数组共有元组个数加一项，多的一项为为补充0.0f的
			      for(int i = 0; i <yuanzu.length;i++){
			    	  yuanzu[i] = yuanzu[i].replace("(", " ");
			    	  yuanzu[i] = yuanzu[i].replace(")", " ");
			    	  yuanzu[i] = yuanzu[i].trim();
			    	  String[] px = yuanzu[i].split(",");
			    	  if(px.length!=3){
			    		  System.out.println("参数格式不正确！");
			    		  System.exit(0);
			    	  }
			    	  float min = Float.parseFloat(px[0]);
			    	  float max = Float.parseFloat(px[1]);
			    	  float level = Float.parseFloat(px[2]);
			    	  classifyMsgList.add( new ClassifyMsg(min,max,level));
			      }
		      }	catch(IOException e)	{
		           e.printStackTrace();
		    }finally{
		    	try {
					fsis.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		    }	
		}
		
		/**
		 * 辅助类，bytes转int
		 * @param b
		 * @param start
		 * @return
		 */
		public int fromBytestoInt(byte[] b,int start){
			 int temp;
			 if(b.length >= start+4)
			    temp=(0xff & b[start]) | (0xff00 & (b[start+1] << 8)) | (0xff0000 & (b[start+2] << 16)) | (0xff000000 & (b[start+3] << 24));
			 else
				 temp = Integer.MAX_VALUE;
			 return temp;
		}
	
		/**
		 * 辅助类，bytes转Float
		 * @param b
		 * @param start
		 * @return
		 */
		public Float fromBytestoFloat(byte[] b,int start){
			 float temp;
			 int tempi = fromBytestoInt(b,start);
			 if(tempi ==Integer.MAX_VALUE)
				 temp = Float.MAX_VALUE;
			 else
				 temp = Float.intBitsToFloat(tempi);
			 return temp;
		}
    public void reduce(LongWritable key, Iterable<BytesWritable> values, 
        Context context) throws IOException, InterruptedException {				
            	Iterator<BytesWritable> ite = values.iterator();
            		// 利用valueList准备存储values中的所有值
            	List<byte[]> valueList = new ArrayList<>(); 
            	while(ite.hasNext()){
            		valueList.add(ite.next().copyBytes());
            	    }
            	 //如果小于2个块，说明坡度数据和高程数据中存在一个无效块，则重分类后必为无效块，所以这里直接写出无效块而无需再计算
            	    if(valueList.size()<2)
            	    {
            	    	 int length = (valueList.get(0)).length-4;// 无效块的字节长度
            	    	 int temp_data = Float.floatToIntBits(INVAILDDATA);
         	          byte[] result_bytes = new byte[length];
            	    	 for(int i = 0;i<length;i+=4){
      	      	    	   result_bytes[i] = (byte) (temp_data & 0xff);  
      	      	    	   result_bytes[i+1] = (byte) ((temp_data & 0xff00) >> 8);  
      	      	    	   result_bytes[i+2] = (byte) ((temp_data & 0xff0000) >> 16);  
      	      	    	   result_bytes[i+3] = (byte) ((temp_data & 0xff000000) >> 24);     	    	     
          	      	    }
            	    	 context.write(key,new BytesWritable(result_bytes));     
            	    	 return;
            	    	
            	    }
                              //区分坡度数据与高程数据
            	    	  byte[] bElevation = valueList.get(0);
            	    	  byte[] bSlope = valueList.get(1);
            	    	      //获取bElevation和bSlope的第一个int值，判断bElevation和bSlope分别属于哪类数据
            	    	  int rasId0 = fromBytestoInt(bElevation, 0);
            	    	  int rasId1 = fromBytestoInt(bSlope, 0);
            	    	  
            	    	  if(rasId0 == 0&&rasId1 == 1){}
            	    	  else if(rasId0 == 1&& rasId1 ==0){
            	    		    bElevation = valueList.get(1);
            	    		    bSlope = valueList.get(0);
            	    	       }
            	    	  else{
            	    		    System.out.println("Input data Error!");
            	    		    System.exit(0);
            	    	       }           	    	      	
            				float currentElevation ,currentSlope;
            	    	  
            				int length = bElevation.length;//字节数组长度 
            				float[] change_tile = new float[length/4-1];
            				
            		        // Initialize the driver and create a context for the first device.
            		      
            				// System.out.println(System.getenv("GPU"));
            				String gpuLable = System.getenv("GPU");
            				//System.out.println(gpuLable);
            		        if(gpuLable != null&&Integer.valueOf(gpuLable)!=0)
            				{
            					//int gpu_id = Integer.valueOf(System.getenv("GPU_ID"));
            					//System.out.printf("GPU number: %d\n",Integer.valueOf(System.getenv("GPU")));
            					//System.out.printf("    GPU ID: %s\n",gpu_id);
            		        	System.out.println("[GPU INFO] GPU is Available for this task, you can call GPU code here!");
            		        	
            					//call GPU code here!
            		            float hostInputA[] = new float[length/4-1];
                		        float hostInputB[] = new float[length/4-1];
                				
                		        int floatArrayLength = 0;
                		        for(int j=4;j<length;j+=4){	    	
    	            		    	hostInputA[floatArrayLength] = fromBytestoFloat(bElevation, j);
    	            		    	hostInputB[floatArrayLength] = fromBytestoFloat(bSlope, j);
    	            		    	floatArrayLength++;			
                		        }
                		        
            					  cuInit(0);
                  		     CUdevice device = new CUdevice();
                  		     cuDeviceGet(device, 0);
                  		     CUcontext context1 = new CUcontext();
                  		     cuCtxCreate(context1, 0, device);

                  		     change_tile = dataProcess(length/4 - 1,hostInputA,hostInputB);
            			
            				}
            			else
            				{
                               
            					System.out.println("[GPU INFO] GPU is NOT Available for this task, you should call CPU code here!");
            					
            					 for(int j = 4;j<length;j+=4){
                 	            	currentElevation = fromBytestoFloat(bElevation, j);
                 	            	currentSlope = fromBytestoFloat(bSlope, j);
                 	           	   if(currentElevation==INVAILDDATA||currentSlope == INVAILDDATA){ //如果高程或坡度值是无效值，则结果值也为无效值     	    
            	        	            change_tile[j/4-1] =INVAILDDATA;
            	        	           // sumList[classifyMsgList.size()]+=1L;//最后无用项计数
            	        	            continue;
            	                          }
                 	           	   else{
                 	           		   change_tile[j/4-1] = currentElevation+currentSlope;
                 	           	   }
                 	           }
            					
            				}
            		        
        	            int temp_data;
        	            byte[] result_bytes = new byte[length-4];
        	      	   for(int i = 0;i<result_bytes.length;i+=4){
        	      	    	   temp_data = Float.floatToIntBits(change_tile[i/4]);
        	      	    	   result_bytes[i] = (byte) (temp_data & 0xff);  
        	      	    	   result_bytes[i+1] = (byte) ((temp_data & 0xff00) >> 8);  
        	      	    	   result_bytes[i+2] = (byte) ((temp_data & 0xff0000) >> 16);  
        	      	    	   result_bytes[i+3] = (byte) ((temp_data & 0xff000000) >> 24);     	    	     
            	      	    }   	
                  	context.write(key,new BytesWritable(result_bytes));     
    }
  }

public static float[] dataProcess(int numElements, float hostInputA[], float hostInputB[]) throws IOException {
	
	// Create the PTX file by calling the NVCC
    byte[] ptxFileName = preparePtxFile();
	 
    // Load the ptx file.
    CUmodule module = new CUmodule();
    cuModuleLoadData(module, ptxFileName);
	  // Obtain a function pointer to the "add" function.
    CUfunction function = new CUfunction();
    cuModuleGetFunction(function, module, "add");  
   
    
    // Allocate the device input data, and copy the
	// host input data to the device
	CUdeviceptr deviceInputA = new CUdeviceptr();
	cuMemAlloc(deviceInputA, numElements * Sizeof.FLOAT);
	cuMemcpyHtoD(deviceInputA, Pointer.to(hostInputA), numElements
			* Sizeof.FLOAT);
	CUdeviceptr deviceInputB = new CUdeviceptr();
	cuMemAlloc(deviceInputB, numElements * Sizeof.FLOAT);
	cuMemcpyHtoD(deviceInputB, Pointer.to(hostInputB), numElements
			* Sizeof.FLOAT);

	// Allocate device output memory
	CUdeviceptr deviceOutput = new CUdeviceptr();
	cuMemAlloc(deviceOutput, numElements * Sizeof.FLOAT);

	// Set up the kernel parameters: A pointer to an array
	// of pointers which point to the actual values.
	Pointer kernelParameters = Pointer.to(
			Pointer.to(new int[] { numElements }),
			Pointer.to(deviceInputA), Pointer.to(deviceInputB),
			Pointer.to(deviceOutput));
	
	// Call the kernel function.
	int blockSizeX = 512;
	int gridSizeX = 24;
	//int gridSizeX = (int) Math.ceil((double) numElements / blockSizeX);
	cuLaunchKernel(function, gridSizeX, 1, 1, // Grid dimension
			blockSizeX, 1, 1, // Block dimension
			16000, null, // Shared memory size and stream
			kernelParameters, null // Kernel- and extra parameters
	);
	cuCtxSynchronize();

	// Allocate host output memory and copy the device output
	// to the host.
	float hostOutput[] = new float[numElements];
	cuMemcpyDtoH(Pointer.to(hostOutput), deviceOutput, numElements
			* Sizeof.FLOAT);

//	for (int i = 0; i < numElements; i++) {
//		if(hostOutput[i] == INVAILDDATA) continue;
//		System.out.println("hostOutput[" + i + "] " + hostOutput[i]);
//	}

	// Clean up.
	cuMemFree(deviceInputA);
	cuMemFree(deviceInputB);
	cuMemFree(deviceOutput);
	
	return hostOutput;

}

private static byte[] preparePtxFile() throws IOException
{
    //int endIndex = cuFileName.lastIndexOf('.');
    //if (endIndex == -1)
   // {
  //      endIndex = cuFileName.length()-1;
  //  }
    //去当前路径
  // String path = Thread.currentThread().getContextClassLoader().getResource("").getPath();
   //String ptxFileName = path +"/resource/feilei.ptx";
  //  String currentDir = System.getProperty("user.dir");
    InputStream in =CudaHadoopMax.class.getResourceAsStream("/resource/add.ptx");
  //  String ptxFileName =currentDir +cuFileName.substring(0, endIndex+1)+"ptx";
    if(in!=null){
    	int length = 1024*10;
    	byte[] fenleiBytes = new byte[length];
    	int realSize = in.read(fenleiBytes);
    	// 以下代码暂时考虑length长度不够的情况
    	byte[] result = new byte[realSize];
    	for (int i = 0; i < realSize; i++) {
			result[i] = fenleiBytes[i];
		}
    	return result;
    }
    
    else return null;
}


  public static void main(String[] args) throws Exception {
	   
				    Configuration conf = new Configuration();         	   
			    /**  
			    * JobConf：map/reduce的job配置类，向hadoop框架描述map-reduce执行的工作  
			    * 构造方法：JobConf()、JobConf(Class exampleClass)、JobConf(Configuration conf)等  
			    */    
				 if(args.length !=6){
						System.out.println("需要的6个参数不匹配，应为inputPath1,inputPath2,outputPath,reduceTaskNum,mapCpmpress,reduceCompress");
						return;
				 }	  
			  
				 if("y".equals(args[4])){
					conf.set("mapreduce.map.output.compress","true");
				 }
				 if("y".equals(args[5])){
				   conf.setBoolean("mapreduce.output.fileoutputformat.compress",true);
				   conf.setClass("mapreduce.output.fileoutputformat.compress.codec",GzipCodec.class, CompressionCodec.class);	 
				 }
				 conf.setBoolean("dfs.support.append", true);
				 //Job job = new Job(conf, "Reclassify");//Job(Configuration conf, String jobName) 设置job名称
				 Job job =Job.getInstance(conf, "MountainReclassify");
				 job.setJarByClass(CudaHadoopMax.class);  	 
				 job.setMapperClass(MyMapper.class); //为job设置Mapper类   
				// job.setCombinerClass(MyReducer.class); //为job设置Combiner类    
				 job.setReducerClass(MyReducer.class); //为job设置Reduce类     
				 job.setOutputKeyClass(LongWritable.class);        //设置输出key的类型  
				 job.setOutputValueClass(BytesWritable.class);//  设置输出value的类型 
				 job.setInputFormatClass(TileInputFormat.class);
				 job.setOutputFormatClass(TileOutputFormat.class);
				 String s1 = args[0];
				 String s2 = args[1];
				 //MyReducer.getClassifyMsgList(args[2]);
				 String s3 = args[2];
				 String inputPath1="hdfs://master:9000"+s1;
				 String inputPath2="hdfs://master:9000/"+s2;	 
			    String outputPath="hdfs://master:9000"+s3;
				 TileInputFormat.addInputPath(job, new Path(inputPath1)); 
				 TileInputFormat.addInputPath(job, new Path(inputPath2)); 
				 TileOutputFormat.setOutputPath(job, new Path(outputPath));//为map-reduce任务设置OutputFormat实现类  设置输出路径
				 int tasksNum = Integer.parseInt(args[3]);
				 job.setNumReduceTasks(tasksNum);
				 
				 long startMili=System.currentTimeMillis();// 当前时间对应的毫秒数
				 if(tasksNum>1){
				 job.setPartitionerClass(TotalOrderPartitioner.class);
				 // RandomSampler第一个参数表示key会被选中的概率，第二个参数是一个选取samples数，第三个参数是最大读取input splits数
				 RandomSampler<LongWritable, BytesWritable> sampler = new InputSampler.RandomSampler<LongWritable, BytesWritable>(0.1, 5000, 20);
			    InputSampler.writePartitionFile(job, sampler); 
			    String partitionFile = TotalOrderPartitioner.getPartitionFile(conf);
			    URI partitionUri= new URI(partitionFile);//？？
			    job.addCacheArchive(partitionUri);//添加一个档案进行本地化
				 }
				
				
				 boolean state= job.waitForCompletion(true);
				 long endMili=System.currentTimeMillis();
				 System.out.println("总耗时为："+(endMili-startMili)+"毫秒");
				 
				/* for(int i = 0;i<MyMapper.sumList.length;i++){
					      System.out.println(MyMapper.sumList[i]);
				 }*/
				 
				 System.exit(state? 0 : 1);  
			  }
}