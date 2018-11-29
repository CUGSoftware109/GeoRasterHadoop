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
 * Cut the ras raster file into a tile file, uncompressed
 */
public class SplitRasFile {	
	//private static final short START=60;//Header offset
	private static final float INVAILDDATA=1.70141E38f;//Filled useless data, consistent with the useless data in the map, is 1.701141E38
	
	private  short tileSize; 
	private  int COL;//Pixel column of grd data
	private  int ROW;//Pixel row of grd data
	private  int COL_NUM=0;//Number of columns in the data block
	private  int ROW_NUM=0;//Number of rows in the data block
	private  boolean stateX;//Whether the data block needs to fill the X mark
	private  boolean stateY;//Data header offset does not need to fill the Y flag
	
	private short COL_ADD;//The pixel column that the data needs to fill
	private short ROW_ADD;//The row of pixels that the data needs to fill
	
	private double x1,x2,y1,y2,z1,z2;
	private RandomAccessFile raf = null;	

	public ArrayList<Integer>  doSplit(short START,short TILE_SIZE,String RAS_PATH,String DES_PATH)  {
		// TODO Auto-generated method stub	
		   tileSize = TILE_SIZE;
		  String rowKey = this.getRowKey(DES_PATH);
		  ArrayList<Integer> invaildTileNoList = new ArrayList<Integer>();
		try {
			raf = new RandomAccessFile(RAS_PATH,"r");  
			this.init(TILE_SIZE,RAS_PATH,rowKey);//init
         this.split(START,TILE_SIZE,DES_PATH,rowKey,invaildTileNoList);//split
         this.writeHeadFile(DES_PATH+"/headMsg");//Write new header file information
		  } catch (Exception e) {
			  System.out.println("Fragmentation is abnormal! File conversion failed");
			  e.printStackTrace();
		  }finally{
		    try {
			      raf.close();
			      System.out.println("Convert to tile file complete！");
			      
		     } catch (IOException e) {
			   e.printStackTrace();
		     }
		}
		   return invaildTileNoList;
	}
	
	/**
	 * Initialize, get the file phase validNo information
	 * @param
	 * @throws IOException
	 */
	private void init(short TILE_SIZE,String RAS_PATH,String rowKey) throws IOException{//Read data initialization
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
		
		if(COL%TILE_SIZE!=0){//The end of the data block column needs to be filled
			COL_NUM=(short) ((COL/TILE_SIZE)+1);
			COL_ADD=(short) (COL_NUM*TILE_SIZE-COL);
			System.out.println("COL_NUM="+COL_NUM);
			System.out.println("COL_ADD="+COL_ADD);
			stateY = true;
		}else{//Data block column tail does not need to be filled
			COL_NUM=(short) (COL/TILE_SIZE);
			COL_ADD=0;  
			stateY = false;
		}
		if(ROW%TILE_SIZE!=0){//The end of the data block needs to be filled
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
		//Meta information is written into HBase data
	}
	/**
	 * Processing block
	 * @param
	 * @throws IOException
	 */
	@SuppressWarnings("resource")
	private void split(short START,short TILE_SIZE,String DES_PATH,String rowKey,ArrayList<Integer> invaildTileNoList) throws IOException{//切分
	       long startPX = Long.MAX_VALUE;
		   int byteSize = TILE_SIZE*4;
		   byte[] bytes = new byte[byteSize];
		   
		   //noUseTileNoList array stores invalid block numbers and finally imports HBase
		   
		   StringBuilder valueString = new StringBuilder();
		   
		   TileSplitDataOutputStream mydos = null;
		   DataOutputStream dos =null;
			FileOutputStream fos = null;
			
		   fos = new  FileOutputStream(DES_PATH +"/tile");	   
			dos = new DataOutputStream(fos);//Output stream
			mydos = new TileSplitDataOutputStream(dos);
			//Invalid block tag, true means invalid block, false means valid block
			boolean label; 
				for(int x=0;x<ROW_NUM;x++){//x, y represents the logical xth row, the data block of the yth column
					for(int y=0;y<COL_NUM;y++){
						label = true; //Default is invalid block
						bytes = null;//Byte array empty
					   bytes = new byte[byteSize];//Then new a new byte array
					   startPX = x*COL*4L*TILE_SIZE+y*TILE_SIZE*4L+START;//The position currently to be read, plus L is for casting to long type
						int i = 0;
						int check = 0;
						
						if((y==COL_NUM-1&&stateY)||(x==ROW_NUM-1&&stateX)){//If it is at the boundary and you want to fill the data
						    if(y==COL_NUM-1&&stateY){//列
						    	raf.seek(startPX);
								while((check=raf.read(bytes, 0, (TILE_SIZE-COL_ADD)*4))!=-1&&i<TILE_SIZE){
								   if(label == true){
									   label = isValid(bytes);
								   }
								  dos.write(bytes,0,(TILE_SIZE-COL_ADD)*4);//Read data content
								  i++;
								  short temp = COL_ADD;
								  while(temp>0){//Populate useless data
								    mydos.writeFloat(INVAILDDATA);
								    temp--;
							      }
								  startPX+=4L*COL;
								  if(startPX>=4L*COL*ROW+START){//Exit the loop if the currently operating pixel node exceeds the input stream total node
									  break;
								  }
								  raf.seek(startPX);
								}
							}
							if(x==ROW_NUM-1&&stateX){//Line fill
								if(!(y==COL_NUM-1&&stateY)){//If the current block is not currently in the last block, prevent the last block from being written 2 times.
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
								 while(temp>0){//Populate useless data
									 for(int t=0;t<TILE_SIZE;t++){
								      mydos.writeFloat(INVAILDDATA);
									 }
								    temp--;
							     }			
							 }
						}else{//Read a block of data in normal condition
							  i=0;	
						     raf.seek(startPX);
							while(raf.read(bytes, 0, byteSize)!=-1&&i<TILE_SIZE){
								 if(label == true){
									   label = isValid(bytes);
								   }
								  dos.write(bytes,0,byteSize);//Read data content
								  i++;
								  startPX+=4L*COL;  
								  raf.seek(startPX);
						     }
						}
						
						if(label == true){
							//Write the block number of the invalid value
							int TileNo = x*COL_NUM+(y+1);
							invaildTileNoList.add(TileNo);
							valueString.append(TileNo+" ");
						}					
					}
				}
			  	dos.close();
	            fos.close();	
	         //Import data from nonoUseTileNoList into HBase
	            StringBuilder family = new StringBuilder("RAS_BLK");
				StringBuilder qualifier = new StringBuilder("InvaildTileNo");
				StringBuilder[] sbs = {family,qualifier,valueString};
				ArrayList<StringBuilder[]> al= new ArrayList<StringBuilder[]>();
				al.add(sbs);
				HBaseClient.addRecord("RAS_INFO",rowKey,al);
	}
	
/**
 * Write a new header file
 * @param headPath
 * @throws IOException
 */
	 private void writeHeadFile(String headPath) throws IOException{
		  TileHeadFile sp = new TileHeadFile(ROW, COL, x1,x2,y1,y2,z1,z2,ROW_NUM, COL_NUM, ROW_ADD, COL_ADD);
		  sp.writeHeadFile(headPath);
	}
	 
	/**
	 * Determines whether the byte array is all invalid, and returns true, otherwise returns false
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
	 * Auxiliary class, bytes to int
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
	 * Auxiliary class, bytes to float
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
	
	/** Auxiliary class, get rowkey, so that the latter data is written to HBase,
	 * @param
	 * @return
	 */
	private String getRowKey(String DES_PATH){
		String[] tmpStringList = DES_PATH.split("/");
		return tmpStringList[tmpStringList.length-1];
	}
	
	/**
	 * Save meta information to HBase
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
