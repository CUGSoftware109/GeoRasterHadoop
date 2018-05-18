package cug.hadoop.geo.tile.split;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;

import cug.hadoop.geo.fileInfo.RasHead;
import cug.hadoop.geo.utils.HBaseClient;
import cug.hadoop.geo.utils.TileHeadFile;
import cug.hadoop.geo.utils.TileSplitDataOutputStream;


/*
 * 将ras栅格文件切成瓦片文件，未压缩
 */
public class SplitRasFile {	
	//private static final short START=60;//数据头偏移量
	private static final float INVAILDDATA=1.70141E38f;//填充的无用数据，与地图中无用数据一致，为1.70141E38
	
	private  short tileSize; 
	private  int COL;//grd数据的像素列
	private  int ROW;//grd数据的像素行
	private  int COL_NUM=0;//数据块的列数
	private  int ROW_NUM=0;//数据块的行数
	private  boolean stateX;//数据块是否需要填充X的标志
	private  boolean stateY;//数据头偏移量否需要填充Y的标志
	
	private short COL_ADD;//数据需要填充的像素列
	private short ROW_ADD;//数据需要填充的像素行
	
	private double x1,x2,y1,y2,z1,z2;
	private RandomAccessFile raf = null;	

	public ArrayList<Integer>  doSplit(short START,short TILE_SIZE,String RAS_PATH,String DES_PATH)  {
		// TODO Auto-generated method stub	
		   tileSize = TILE_SIZE;
		  String rowKey = this.getRowKey(DES_PATH);
		  ArrayList<Integer> invaildTileNoList = new ArrayList<Integer>();
		try {
			raf = new RandomAccessFile(RAS_PATH,"r");  
			this.init(TILE_SIZE,RAS_PATH,rowKey);//初始化
         this.split(START,TILE_SIZE,DES_PATH,rowKey,invaildTileNoList);//分片
         this.writeHeadFile(DES_PATH+"/headMsg");//写新的头文件信息         
		  } catch (Exception e) {
			  System.out.println("分片异常！文件转换失败");
			  e.printStackTrace();
		  }finally{
		    try {
			      raf.close();
			      System.out.println("转换为tile文件完成！"); 
			      
		     } catch (IOException e) {
			   e.printStackTrace();
		     }
		}
		   return invaildTileNoList;
	}
	
	/**
	 * 初始化，得到文件相validNo关信息
	 * @param raf
	 * @throws IOException
	 */
	private void init(short TILE_SIZE,String RAS_PATH,String rowKey) throws IOException{//读入数据初始化
			RasHead rashead = new RasHead(RAS_PATH);		  
			this.ROW = rashead.getNx();
			this.COL = rashead.getNy();
			//this.no_use = rashead.getNo_use();
			this.x1 = rashead.getX1();
			this.x2 = rashead.getX2();
			this.y1 = rashead.getY1();
			this.y2 = rashead.getY2();
			this.z1 = rashead.getZ1();
			this.z2 = rashead.getZ2();
		   System.out.println("ROW="+ROW);
		   System.out.println("COL="+COL);
		
		if(COL%TILE_SIZE!=0){//数据块列尾需要填充
			COL_NUM=(short) ((COL/TILE_SIZE)+1);
			COL_ADD=(short) (COL_NUM*TILE_SIZE-COL);
			System.out.println("COL_NUM="+COL_NUM);
			System.out.println("COL_ADD="+COL_ADD);
			stateY = true;
		}else{//数据块列尾不需要填充
			COL_NUM=(short) (COL/TILE_SIZE);
			COL_ADD=0;  
			stateY = false;
		}
		if(ROW%TILE_SIZE!=0){//数据块行尾需要填充
			ROW_NUM=(short) ((ROW/TILE_SIZE)+1);
			ROW_ADD=(short) ((ROW_NUM*TILE_SIZE)-ROW);
			System.out.println("ROW_NUM="+ROW_NUM);
			System.out.println("ROW_ADD="+ROW_ADD);
			stateX = true;
		}else{
			ROW_NUM=(short) (ROW/TILE_SIZE);
			ROW_ADD=0;
			stateX = false;
		}
		saveHeadMsgtoHBase(rowKey);
		//元信息写入HBase数据
	}
	/**
	 * 处理分块
	 * @param raf
	 * @throws IOException
	 */
	@SuppressWarnings("resource")
	private void split(short START,short TILE_SIZE,String DES_PATH,String rowKey,ArrayList<Integer> invaildTileNoList) throws IOException{//切分
	       long startPX = Long.MAX_VALUE;
		   int byteSize = TILE_SIZE*4;
		   byte[] bytes = new byte[byteSize];
		   
		   //noUseTileNoList数组存储无效块编号，最后导入HBase
		   
		   StringBuilder valueString = new StringBuilder();
		   
		   TileSplitDataOutputStream mydos = null;
		   DataOutputStream dos =null;
			FileOutputStream fos = null;
			
		   fos = new  FileOutputStream(DES_PATH +"/tile");	   
			dos = new DataOutputStream(fos);//输出流
			mydos = new TileSplitDataOutputStream(dos);
			//无效块标记，true代表是无效块，false代表有效块
			boolean label; 
				for(int x=0;x<ROW_NUM;x++){//x，y代表逻辑上的第x行，第y列的数据块
					for(int y=0;y<COL_NUM;y++){
						label = true; //默认为无效块
						bytes = null;//bytes数组清空
					   bytes = new byte[byteSize];//再new一个新的bytes数组
					   startPX = x*COL*4L*TILE_SIZE+y*TILE_SIZE*4L+START;//当前实际要读取的位置,加L是为了强制转换为long类型					 
						int i = 0;
						int check = 0;
						
						if((y==COL_NUM-1&&stateY)||(x==ROW_NUM-1&&stateX)){//如果处于边界，且要填充数据
						    if(y==COL_NUM-1&&stateY){//列
						    	raf.seek(startPX);
								while((check=raf.read(bytes, 0, (TILE_SIZE-COL_ADD)*4))!=-1&&i<TILE_SIZE){
								   if(label == true){
									   label = isValid(bytes);
								   }
								  dos.write(bytes,0,(TILE_SIZE-COL_ADD)*4);//读入有数据信息内容
								  i++;
								  short temp = COL_ADD;
								  while(temp>0){//填充无用数据
								    mydos.writeFloat(INVAILDDATA);
								    temp--;
							      }
								  startPX+=4L*COL;
								  if(startPX>=4L*COL*ROW+START){//如果当前操作像素节点超过输入流总节点，退出循环
									  break;
								  }
								  raf.seek(startPX);
								}
							}
							if(x==ROW_NUM-1&&stateX){//行填充
								if(!(y==COL_NUM-1&&stateY)){//如果当前不处于所有数据块的最后一块，防止最后一块被写入2次
									raf.seek(startPX);
								   for(int row=0;row<TILE_SIZE-ROW_ADD;row++){
								        raf.read(bytes,0,byteSize); 
								        if(label == true){
											   label = isValid(bytes);
										   }
								        dos.write(bytes,0,byteSize);
								        startPX+=4L*COL;
								        raf.seek(startPX);
								       }
								}
								 short temp = ROW_ADD;
								 while(temp>0){//填充无用数据
									 for(int t=0;t<TILE_SIZE;t++){
								      mydos.writeFloat(INVAILDDATA);
									 }
								    temp--;
							     }			
							 }
						}else{//正常情况读取一块数据块内容
							  i=0;	
						     raf.seek(startPX);
							while(raf.read(bytes, 0, byteSize)!=-1&&i<TILE_SIZE){
								 if(label == true){
									   label = isValid(bytes);
								   }
								  dos.write(bytes,0,byteSize);//读入有数据信息内容
								  i++;
								  startPX+=4L*COL;  
								  raf.seek(startPX);
						     }
						}
						
						if(label == true){
							//写出无效值的块号
							int TileNo = x*COL_NUM+(y+1);
							invaildTileNoList.add(TileNo);
							valueString.append(TileNo+" ");
						}					
					}
				}
			  	dos.close();
	            fos.close();	
	         //nonoUseTileNoList中数据导入HBase
	            StringBuilder family = new StringBuilder("RAS_BLK");
				StringBuilder qualifier = new StringBuilder("InvaildTileNo");
				StringBuilder[] sbs = {family,qualifier,valueString};
				ArrayList<StringBuilder[]> al= new ArrayList<StringBuilder[]>();
				al.add(sbs);
				HBaseClient.addRecord("RAS_INFO",rowKey,al);
	}
	
/**
 * 写新的头文件
 * @param headPath
 * @throws IOException
 */
	 private void writeHeadFile(String headPath) throws IOException{
		  TileHeadFile sp = new TileHeadFile(ROW, COL, x1,x2,y1,y2,z1,z2,ROW_NUM, COL_NUM, ROW_ADD, COL_ADD);
		  sp.writeHeadFile(headPath);
	}
	 
	/**
	 * 判断字节数组中是否全为无效值，是则返回true，否则返回false
	 * @param bytes
	 * @return
	 */
	private boolean isValid(byte[] bytes){
		 float tmp;
		 for(int i=0;i<bytes.length;i+=4){
			  tmp = fromBytestoFloat(bytes, i);
			   if(tmp != INVAILDDATA) {
				    return false;
			   }
		 }
		 return true;
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
		 Float temp;
		 int tempi = fromBytestoInt(b,start);
		 if(tempi ==Integer.MAX_VALUE)
			 temp = Float.MAX_VALUE;
		 else
			 temp = Float.intBitsToFloat(tempi);
		 return temp;
	}
	
	/** 辅助类，获取rowkey,以便后面数据写入HBase、
	 * @param RAS_PATH
	 * @return
	 */
	private String getRowKey(String DES_PATH){
		String[] tmpStringList = DES_PATH.split("/");
		return tmpStringList[tmpStringList.length-1];
	}
	
	/**
	 * 保存元信息到HBase
	 * @throws IOException 
	 */
	private  void saveHeadMsgtoHBase(String rowKey) throws IOException{

		String [] qualifiers = {"TileSize","Row", "Col","XMin","XMax","YMin","YMax","ZMin","ZMax","TileRowNum","TileColNum","TileRowPixelAdd","TileColPixelAdd"};
		String [] values = {tileSize+"",ROW+"",COL+"",x1+"",x2+"",y1+"",y2+"",z1+"",z2+"",ROW_NUM+"",COL_NUM+"",ROW_ADD+"",COL_ADD+""};
		ArrayList<StringBuilder[]> ls = new ArrayList<StringBuilder[]>();
		StringBuilder family = new StringBuilder("RAS_BASE");
		for (int i=0; i<qualifiers.length; i++){
			StringBuilder Qualifier = new StringBuilder(qualifiers[i]);
			StringBuilder value = new StringBuilder(values[i]);
			StringBuilder [] sbs = {family,Qualifier,value};
			ls.add(sbs);
		}					
		HBaseClient.addRecord("RAS_INFO",rowKey , ls);
	}
}
