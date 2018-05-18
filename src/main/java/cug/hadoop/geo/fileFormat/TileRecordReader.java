package cug.hadoop.geo.fileFormat;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordReader;

import cug.hadoop.geo.utils.HadoopDataInputStream;



/**
 * Return a single record (filename, "") where the filename is taken from
 * the file split.
 */
public class TileRecordReader extends RecordReader<LongWritable, BytesWritable> {
  private FSDataInputStream inputStream = null;
  private InputStream in = null;
  private long start,end,pos;
  private Configuration conf = null;
  private FileSplit fileSplit = null;
  private LongWritable key = new LongWritable();
  private BytesWritable value = new BytesWritable();
  private boolean processed = false;
  
  public TileRecordReader() throws IOException {
  }

  /*关闭文件流
   * */
  public void close() {
    try {
      if(inputStream != null)
        inputStream.close();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  /*
   * 获取处理进度
   **/
  public float getProgress() {
    return ((processed == true)? 1.0f : 0.0f);
  }

  /*
   * 获取当前的Key
   * */
  public LongWritable getCurrentKey() throws IOException,
  InterruptedException {
    // TODO Auto-generated method stub
    return key;
  }

  /* 获取当前的Value
   * */
  public BytesWritable getCurrentValue() throws IOException,InterruptedException {
    // TODO Auto-generated method stub
    return value;
  }

  /*
   * 进行初始化工作，打开文件流，根据分块信息设置起始位置和长度等等
   * */
  public void initialize(InputSplit inputSplit, TaskAttemptContext context)
      throws IOException, InterruptedException {
    // TODO Auto-generated method stub
    fileSplit = (FileSplit)inputSplit;
    conf = context.getConfiguration();
   
    this.start = fileSplit.getStart();
    this.end = fileSplit.getStart() + fileSplit.getLength();
    	 
    Path path = fileSplit.getPath();
    FileSystem fs = path.getFileSystem(conf);
	 this.inputStream = fs.open(path);
	 inputStream.seek(start);
	 this.pos = this.start;   
  }

  /*生成下一个键值对
   **/
  public boolean nextKeyValue() throws IOException, InterruptedException {
    // TODO Auto-generated method stub
	 
    if(this.pos < this.end) 
    {
    	//******************************************************************************
    	   int byteLength = 0;
    	   HadoopDataInputStream mydis = new HadoopDataInputStream(inputStream);
    	   
    	   if(end-pos>=4){
    	       key.set(mydis.readInt());
    	       if(key.get()==0){
    	    	   //System.out.println("分片读取结束！！！！！！！！！！！！！");
    	    	   processed = true;    	    	  
       		   return false;
       	         }
    	    }else{// 如果当前位置距离文件末尾不足4个字节，则必定后面为填充无用数据
    	      	processed = true;    	    	  
        		   return false;
    	    }
    	   
    	   byteLength =mydis.readInt();
    	   byte[] bytes = new byte[byteLength];
    	   int len = mydis.read(bytes, 0, byteLength);
    	   int temp;
    	   while(len < byteLength){
    	      temp = mydis.read(bytes, len, byteLength-len);
    	      len += temp;    	      
    	    }
    	   BytesWritable bw = new BytesWritable(bytes);
         value.set(bw);
         this.pos = mydis.getPos();  
 	      return true;
    } 
    else
    {
      processed = true;
      return false;
    }
  }
  
}
