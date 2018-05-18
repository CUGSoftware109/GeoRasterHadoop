package cug.hadoop.geo.tile.merge;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.zip.GZIPInputStream;

import cug.hadoop.geo.fileInfo.RasHead;
import cug.hadoop.geo.utils.HBaseClient;
import cug.hadoop.geo.utils.TileSplitDataOutputStream;


/**
 * @author root
 *将瓦片文件还原成ras文件
 */
public class SplitTileFile {

	private static final float INVAILDDATA=1.70141E38f;//填充的无用数据，与地图中无用数据一致，为1.70141E38
	private int ROW;// grd数据的像素行
	private int COL;// grd数据的像素列
	private short TILE_SIZE;

	private double x1, x2, y1, y2; 
	private int ROW_NUM = 0;// 数据块的行数
	private int COL_NUM = 0;// 数据块的列数
	//private boolean stateX;// 数据块是否需要填充X的标志
	//private boolean stateY;// 数据头偏移量否需要填充Y的标志

	private short COL_ADD;// 数据需要填充的像素列
	private short ROW_ADD;// 数据需要填充的像素行	

	private static RandomAccessFile raf = null;
	private static FileInputStream fis = null;
	private static FileOutputStream fos = null;
	private static TileSplitDataOutputStream mydos =null;

	public static void main(String[] args){
		if(args.length != 7){
			System.out.println("需要的7参数个数不匹配，应为：gzTilePath,  grdPath, tableName, elevationRowKey, slopeRowKey, z1l, z2");
			return;
		}
		
		 String gzTilePath = args[0];
		 String grdPath = args[1];
		 String tableName = args[2];
		 String elevationRowKey = args[3];
		 String slopeRowKey = args[4];
		 double z1 = Double.parseDouble(args[5]);
		 double z2 = Double.parseDouble(args[6]);
		 SplitTileFile splitTileFile = new SplitTileFile();
		 String decompressResultPath = gzTilePath +"/decompressResult";
		 String completeResultPath= gzTilePath +"/completeResult";
		 try {
			 splitTileFile.init(tableName, elevationRowKey);
			 //首先解压结果文件
			splitTileFile.decompress(gzTilePath,decompressResultPath);
			//然后结果文件插入无效数据还原
			System.out.println("开始插入无效块，还原ras文件......");
			splitTileFile.insertInvaildDataBlk(tableName,elevationRowKey,slopeRowKey,decompressResultPath,completeResultPath);
			System.out.println("还原ras文件成功！");
			System.out.println("开始生成ras文件......");
			//s.doSplit(grdPath, gzTilePath, headMsgPath,z1,z2);
			splitTileFile.doSplit(grdPath,completeResultPath,z1,z2);
			System.out.println("ras文件生成成功！ras文件目录："+grdPath);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("生成ras文件失败");
			e.printStackTrace();
		}finally{
			if(raf != null){
				 try {
					raf.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if(fis !=null){
				try {
					fis.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if(fos !=null){
				try {
					fos.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if(mydos!=null){
				try {
					mydos.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	public void doSplit(String grdPath, String tilePath,double z1,double z2) throws IOException {	
			raf = new RandomAccessFile(tilePath, "r");
			this.writeGrdHead(grdPath,z1,z2);//写grd文件头
			this.writeGrdFile(grdPath);//写grd文件内容
	}

	/**
	 * 初始化瓦片文件数据，得到头文件相关信息
	 * 
	 * @param path
	 * @throws IOException 
	 */
	private void init(String tableName,String rowKey) throws IOException {
	/*	TileHeadFile tileHeadFile = new TileHeadFile(headMsgPath);
		this.TILE_SIZE = tileHeadFile.getTILE_SIZE();
		this.ROW = tileHeadFile.getROW();
		this.COL = tileHeadFile.getCOL();
		this.x1 = tileHeadFile.getX1();
		this.x2 = tileHeadFile.getX2();
		this.y1 = tileHeadFile.getY1();
		this.y2 = tileHeadFile.getY2();
		//this.z1 = tileHeadFile.getZ1();
	//	this.z2 = tileHeadFile.getZ2();
		this.ROW_NUM = tileHeadFile.getROW_NUM();
		this.COL_NUM = tileHeadFile.getCOL_NUM();
		this.ROW_ADD = tileHeadFile.getROW_ADD();
		this.COL_ADD = tileHeadFile.getCOL_ADD();

		this.stateX = ROW_ADD == 0 ? false : true;
		this.stateY = COL_ADD == 0 ? false : true;*/
		
		this.TILE_SIZE =Short.parseShort(HBaseClient.selectOneRow(tableName, rowKey, "RAS_BASE", "TileSize"));
		this.COL =Integer.parseInt(HBaseClient.selectOneRow(tableName, rowKey, "RAS_BASE", "Col"));
		this.ROW = Integer.parseInt(HBaseClient.selectOneRow(tableName, rowKey, "RAS_BASE", "Row"));
		this.COL_NUM = Integer.parseInt(HBaseClient.selectOneRow(tableName, rowKey, "RAS_BASE", "TileColNum"));
		this.ROW_NUM = Integer.parseInt(HBaseClient.selectOneRow(tableName, rowKey, "RAS_BASE", "TileRowNum"));
		this.COL_ADD = Short.parseShort(HBaseClient.selectOneRow(tableName, rowKey, "RAS_BASE", "TileColPixelAdd"));
		this.x1 = Double.parseDouble(HBaseClient.selectOneRow(tableName, rowKey, "RAS_BASE", "XMin"));
		this.x2= Double.parseDouble(HBaseClient.selectOneRow(tableName, rowKey, "RAS_BASE", "XMax"));
		this.y1= Double.parseDouble(HBaseClient.selectOneRow(tableName, rowKey, "RAS_BASE", "YMin"));
		this.y2= Double.parseDouble(HBaseClient.selectOneRow(tableName, rowKey, "RAS_BASE", "YMax"));
	}
	
	
	/**
	 * 插入无效块到解压后的文件中，这是总体第二部，是还原RAS文件的过程
	 * @param tableName  表名
	 * @param elevationRowKey 高程数据rowKey
	 * @param slopeRowKey  坡度数据rowKey
	 * @param decompressResultPath  tile瓦片解压后路径
	 * @param completeResultPath 插入无效块后完整tile路径
	 * @throws IOException
	 */
	private void insertInvaildDataBlk(String tableName,String elevationRowKey,String slopeRowKey,String decompressResultPath,String completeResultPath) throws IOException{
		int byteSize = TILE_SIZE *TILE_SIZE *4;
		byte[] bytes = new byte[byteSize];
		 fis = new FileInputStream(decompressResultPath);
		 mydos = new TileSplitDataOutputStream(new DataOutputStream(new FileOutputStream(completeResultPath)));
		 TreeSet<Integer> invaildDataBlkNo =	getInvaildDataBlkNo(tableName,elevationRowKey,slopeRowKey);
		 Iterator<Integer> ite = invaildDataBlkNo.iterator();
		 int currentInvaildBlkNo =0;//当前无效块号
		 int currentReadBlkNo = 1;//当前需要读写的块号
		 while(ite.hasNext()){
			 currentInvaildBlkNo = ite.next();
			 while(currentInvaildBlkNo-currentReadBlkNo>0){
				 fis.read(bytes, 0, byteSize);
				  mydos.write(bytes, 0, byteSize);
				  currentReadBlkNo++;
			 }
			 //写出一块无用数据
			 for(int i =0 ;i<TILE_SIZE*TILE_SIZE;i++){
				 mydos.writeFloat(INVAILDDATA);
			 }
			 //写出无用数据后,当前块号需要加1
			 currentReadBlkNo++;
		 }
		 //最后一个无效块插入后，后面极有可能还有有效数据需要继续写出
		 while(ROW_NUM*COL_NUM-currentReadBlkNo>=0){
			   fis.read(bytes, 0, byteSize);
			   mydos.write(bytes, 0, byteSize);
			   currentReadBlkNo++;
		 }
		 fis = null;
		 mydos = null;
	}


	/**
	 * 获取无效块号，存入一个TreeSet容器中
	 * @param tableName
	 * @param elevationRowKey
	 * @param slopeRowKey
	 * @return
	 * @throws IOException
	 */
	private TreeSet<Integer> getInvaildDataBlkNo(String tableName,String elevationRowKey,String slopeRowKey) throws IOException{
		String elevationInvaildBlk = HBaseClient.selectOneRow(tableName, elevationRowKey, "RAS_BLK", "InvaildTileNo");
		String slopeInvaildBlk = HBaseClient.selectOneRow(tableName, slopeRowKey, "RAS_BLK", "InvaildTileNo");
		
		elevationInvaildBlk = elevationInvaildBlk.trim();
		slopeInvaildBlk = slopeInvaildBlk.trim();
		
		String[] elevationInvaildBlkArray = elevationInvaildBlk.split(" ");
		String[] slopeInvaildBlkArray = slopeInvaildBlk.split(" ");
		// 两个数组元素求交集，肯定无重复元素
		TreeSet<Integer> elevationInvaildBlkSet = new TreeSet<Integer>();
		TreeSet<Integer> slopeInvaildBlkSet = new TreeSet<Integer>();
		//数组元素加入TreeSet集合
		for(int i = 0 ;i <elevationInvaildBlkArray.length;i++){
			elevationInvaildBlkSet.add(Integer.parseInt(elevationInvaildBlkArray[i]));
		}
		for(int i = 0 ;i <slopeInvaildBlkArray.length;i++){
			slopeInvaildBlkSet.add(Integer.parseInt(slopeInvaildBlkArray[i]));
		}
		//两个set集合求交集
		elevationInvaildBlkSet.retainAll(slopeInvaildBlkSet);
		
		return elevationInvaildBlkSet;	
	}
	/**
	 * 瓦片文件还原为grd文件
	 * 
	 * @param GRD_PATH
	 * @throws IOException
	 */
	private void writeGrdFile(String grdPath) throws IOException {
		int byteSize = TILE_SIZE * 4;
		byte[] bytes = new byte[byteSize];
		fos = new FileOutputStream(grdPath, true);// 继续往grd里追加内容而不是覆盖
		long startPX = 0;
		int temp = 0;
		int current_row = 0;
		int current_tile_row = 0;
		while (current_row < ROW) {
			current_tile_row = current_row / TILE_SIZE;
			startPX = (TILE_SIZE * 4L * (current_row % TILE_SIZE))
					+ (4L*current_tile_row * COL_NUM * TILE_SIZE * TILE_SIZE);
			raf.seek(startPX);
			temp = COL_NUM;
			while (temp > 1) {
				raf.read(bytes, 0, byteSize);
				fos.write(bytes, 0, byteSize);
				startPX += 4L*TILE_SIZE * TILE_SIZE ;// 当前实际要读取的位置
				raf.seek(startPX);
				temp--;
			}
			raf.read(bytes, 0, (TILE_SIZE - COL_ADD) * 4);
			fos.write(bytes, 0, (TILE_SIZE - COL_ADD) * 4);
			current_row++;
		}
	}


	/**
	 * 写grd头文件
	 * @param grdPath 生成grd文件路径
	 * @param z1 代指ZMin
	 * @param z2 代指ZMax
	 */
	private void writeGrdHead(String grdPath,double z1,double z2) {
		RasHead rasHead = new RasHead(ROW, COL,x1, x2, y1, y2,z1,z2);
		rasHead.writeGrdHead(grdPath);

	}

	/**
	 * 解压原始文件，生成解压后的tile文件，这是处理第一步
	 * @param gzTilePath
	 * @param decompressResultPath
	 */
	private void decompress(String gzTilePath,String decompressResultPath)  {
		File dir = new File(gzTilePath);
		
		File newFile = new File(decompressResultPath);
		if(newFile.exists()){//如果存在就先删除
			//newDir.delete();
			System.out.println("tile文件解压拼接完成!");
			return;
		}	
	//	File[] contentFile = dir.listFiles();
		List<File> files = Arrays.asList(dir.listFiles());
		//解压文件以文件名排序
		Collections.sort(files, new Comparator<File>() {
			   public int compare(File o1, File o2) {
				if (o1.isDirectory() && o2.isFile())
			          return -1;
				if (o1.isFile() && o2.isDirectory())
			          return 1;
				return o1.getName().compareTo(o2.getName());
			   }
			});		
		
		String fileName = "";
		FileOutputStream fos1 = null;
		GZIPInputStream GZIPin =null;
		try{
	        fos1 = new FileOutputStream(newFile,true); 
			for (int i = 0; i < files.size(); i++) {
				fileName = ((File)files.get(i)).getName();
				if (fileName.endsWith(".gz")) {
					// jieya
				 GZIPin = new GZIPInputStream(new FileInputStream((File)files.get(i)));
					byte[] buf = new byte[1024];
			      int len;
			      while ((len = GZIPin.read(buf)) > 0) {
			          fos1.write(buf, 0, len);
			        }
			      System.out.println(fileName+"解压完成");
			      //GZIPin.close();
				}
			}
			System.out.println("tile文件解压拼接完成!");
			//fos.close();
	    }catch(IOException e){		
	    
	    }finally{
           if(GZIPin!=null){
        	   try {
				GZIPin.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
           }
         if(fos1 !=null){
        	 try {
				fos1.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
         }
           
	     }
	}
}