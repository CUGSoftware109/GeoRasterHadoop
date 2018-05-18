package cug.hadoop.geo.tile.split;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import cug.hadoop.geo.utils.Params;
import cug.hadoop.geo.utils.TileHeadFile;

/*
 * 切分并压缩瓦片
 */
public class ZIPTile {
	
	private static final short START=60;//数据头偏移量
	private short TILE_SIZE;
	
	//private  int rasId;

	public static void main(String[] args){
		if(args.length !=1){
			System.out.println("参数个数不匹配，应为params文件路径、栅格数据类型(0代表高程数据，1代表坡度数据)！");
			return;
		}
		
		Params p = new Params(args[0]);
		String[] params=p.getParams();
		if(params.length == 0){
			System.exit(0);
		}
		String tile_size = params[0];
		short TILE_SIZE =Short.parseShort(tile_size);
		String RAS_PATH = params[1];
		String DES_PATH = params[2];
	   SplitRasFile splitRasFile = new SplitRasFile();
	   
	    
		//获取处理文件类型：高程栅格rasId为0，坡度栅格rasId为1
	   int rasId = Integer.parseInt(params[3]);
		if(rasId !=0 && rasId !=1){
			System.out.println("栅格数据格式选择错误：需选择是高程或坡度栅格数据！");
			System.exit(0);
		}
	   
		File f = new File(DES_PATH);
		if(!f.exists()){
			 f.mkdir();//如果文件不存在，则创建一个文件夹
		}
		if(!f.isDirectory()){//如果文件不是一个文件夹
			 System.out.println("目标路径必须是一个文件夹");
			 System.exit(0);
		}
		System.out.println(RAS_PATH+"文件开始转换为tile文件");
		ArrayList<Integer> invaildTileNoList = splitRasFile.doSplit(START, TILE_SIZE, RAS_PATH, DES_PATH);// 生成tile文件，未压缩		  
           
		ZIPTile zipTile = new ZIPTile();
		
	    
		zipTile.doZIP(DES_PATH,rasId,invaildTileNoList);//压缩tile文件，生成tileZip文件

		
		  
	}
	
	public ZIPTile(){
	}
	
	public void doZIP(String path,int rasId,ArrayList<Integer> invaildTileNoList){
		  System.out.println("tile文件开始压缩......"); 
		  this.init(path);
		  this.ZIP(path,rasId,invaildTileNoList);
	}
	
	
	/**
	 * 初始化瓦片文件数据，得到头文件相关信息
	 * @param path
	 */
	private void init(String path){
		 TileHeadFile tileHeadFile = new TileHeadFile(path+"/headMsg");
		 this.TILE_SIZE = tileHeadFile.getTILE_SIZE();	 
	}
       //lable表示压缩文件的标志，后续指表中的RasId
	private void ZIP(String path,int rasId,ArrayList<Integer> invaildTileNoList){
		 int size = TILE_SIZE*TILE_SIZE*4;
		 //申明输入输出流
		 FileInputStream fis = null;
		 FileOutputStream fos = null;
		 ByteArrayOutputStream byOut = null;
		 ZipOutputStream zipOut = null;
		 
	   try {
			  fis = new FileInputStream(path+"/tile");
			  fos = new FileOutputStream(path+"/tileZip");
			  byOut=new ByteArrayOutputStream();
			  zipOut = new ZipOutputStream(byOut);

			// zipOut.putNextEntry(new ZipEntry("Zip")); //压缩之前必须有，否则报错
			 
			 byte bytes[] = new byte[size]; //  存储原始tile字节
			 byte bytesAddRasId[] = new byte[size+4]; //存储原始tile字节加rasId
			 byte zipBytes[] = null; //存储压缩后的字节
			 int count;   //读出的字节数
			 int zipCount;  //压缩后的字节数
			 int currentTileNo=1; //当前瓦片编号
			 int total=0;
			 int usenessBytesNum=0;
			 
			 int time = 0;//invaildTileList的取值
			 while((count = fis.read(bytes,0,size))!=-1){  //对当前瓦片字节数组循环，读出字节数组到bytes,每次读256kb
				 if(!invaildTileNoList.isEmpty()){//如果invaildTileNoList非空
					if (currentTileNo == invaildTileNoList.get(time)) {// 如果当前瓦片是无效瓦片
						currentTileNo++;
						if (time == invaildTileNoList.size() - 1) {// time已到最大值，说明无效瓦片已遍历结束，此时time不能继续加1，否则越界
							continue;
						}
						time++;
						continue;
					}
				 }
				   ZipEntry entry = new ZipEntry("zip"+currentTileNo);
				   //将bytes数组的内容赋值到bytesAddRasId数组中
				   for(int i =0;i<bytes.length;i++){
					   bytesAddRasId[i+4] = bytes[i]; 
				   }
				   //bytesAddRasId中前四位存储rasId
				   bytesAddRasId[0] = (byte) ( rasId& 0xff);
				   bytesAddRasId[1] = (byte) ((rasId & 0xff00) >> 8);
				   bytesAddRasId[2] = (byte) ((rasId & 0xff0000) >> 16);
				   bytesAddRasId[3] = (byte) ((rasId & 0xff000000) >> 24);
				      
				   entry.setSize(bytesAddRasId.length);
				   zipOut.putNextEntry(entry);     
			      zipOut.write(bytesAddRasId);//将字节数组写入压缩流中
			      zipOut.closeEntry();// 关闭zipEntry
			      
			      zipBytes = byOut.toByteArray(); //得到压缩后的数组
			      zipCount = byOut.size();  
			      byte mixBytes[] = new byte[zipCount+8]; 
			      for(int i = zipCount-1;i>=0;i--){ //将zipbytes字节后移12位，留出前面12位存储瓦片编号和字节长度信息,以及rasId
			    	   mixBytes[i+8] = zipBytes[i];  
			        }
			      byOut.reset();
			         			        //前四个字节存储瓦片编号int
			      mixBytes[0] = (byte) (currentTileNo & 0xff);
			      mixBytes[1] = (byte) ((currentTileNo & 0xff00) >> 8);
			      mixBytes[2] = (byte) ((currentTileNo & 0xff0000) >> 16);
			      mixBytes[3] = (byte) ((currentTileNo & 0xff000000) >> 24);
			        //存储压缩字节长度zipCount加上一个int型rasId，即zipCount
			      mixBytes[4] = (byte) (zipCount & 0xff);
			      mixBytes[5] = (byte) ((zipCount & 0xff00) >> 8);
			      mixBytes[6] = (byte) ((zipCount & 0xff0000) >> 16);
			      mixBytes[7] = (byte) ((zipCount & 0xff000000) >> 24);
			      
			       //再用四个字节存储RasId到value中
			   
			    		  
			      currentTileNo++;
			   //   System.out.println(currentTileNo+" "+zipCount);
			        
			      total += (zipCount + 8);
			      if(total > 128*1024*1024){//如果大于128MB
			    	   total =  total-zipCount - 8;
			    	   usenessBytesNum = 128*1024*1024 - total;
			    	   byte usenessBytes[] = new byte[usenessBytesNum];
			    	   fos.write(usenessBytes);
			    	   total = zipCount + 8;
			        }
			      
			        //写到磁盘
			      fos.write(mixBytes,0,zipCount+8);
			 }
		}catch (Exception e) {
			e.printStackTrace();
			System.out.println("tile文件压缩失败！"); 
			return;
		}finally{
			try {		
				   zipOut.close();    				
					byOut.close();
					fos.close();
					fis.close(); 		
			} catch (IOException e) {			
				e.printStackTrace();
			}
			
		}
	   System.out.println("tile文件压缩成功，生成结果文件目录："+ path); 
	}
	

}
