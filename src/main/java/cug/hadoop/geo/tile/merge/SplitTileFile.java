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
 *Restore tile files to ras file
 */
public class SplitTileFile {

	private static final float INVAILDDATA=1.70141E38f;//Filled useless data, consistent with the useless data in the map, is 1.701141E38
	private int ROW;// Pixel row of grd data
	private int COL;// Pixel column of grd data
	private short TILE_SIZE;

	private double x1, x2, y1, y2; 
	private int ROW_NUM = 0;// Number of rows in the data block
	private int COL_NUM = 0;// Number of columns in the data block
	//private boolean stateX;// Whether the data block needs to fill the X mark
	//private boolean stateY;// Data header offset does not need to fill the Y flag

	private short COL_ADD;// The pixel column that the data needs to fill
	private short ROW_ADD;// The row of pixels that the data needs to fill

	private static RandomAccessFile raf = null;
	private static FileInputStream fis = null;
	private static FileOutputStream fos = null;
	private static TileSplitDataOutputStream mydos =null;

	public static void main(String[] args){
		if(args.length != 7){
			System.out.println("The number of 7 parameters required does not match and should be：gzTilePath,  grdPath, tableName, elevationRowKey, slopeRowKey, z1l, z2");
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
			 //First extract the result file
			splitTileFile.decompress(gzTilePath,decompressResultPath);
			//Then the result file is inserted into invalid data restore
			System.out.println("Start inserting invalid blocks and restore ras files......");
			splitTileFile.insertInvaildDataBlk(tableName,elevationRowKey,slopeRowKey,decompressResultPath,completeResultPath);
			System.out.println("Restore ras file successfully！");
			System.out.println("Start generating ras files......");
			//s.doSplit(grdPath, gzTilePath, headMsgPath,z1,z2);
			splitTileFile.doSplit(grdPath,completeResultPath,z1,z2);
			System.out.println("The ras file was generated successfully! Ras file directory："+grdPath);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("Failed to generate ras file");
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
			this.writeGrdHead(grdPath,z1,z2);//Write grd file header
			this.writeGrdFile(grdPath);//Write grd file content
	}

	/**
	 * Initialize tile file data to get header file related information
	 * 
	 * @param
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
	 * Insert invalid block into the decompressed file, this is the second part of the whole process, which is to restore the RAS file.
	 * @param tableName
	 * @param elevationRowKey
	 * @param slopeRowKey
	 * @param decompressResultPath
	 * @param completeResultPath
	 * @throws IOException
	 */
	private void insertInvaildDataBlk(String tableName,String elevationRowKey,String slopeRowKey,String decompressResultPath,String completeResultPath) throws IOException{
		int byteSize = TILE_SIZE *TILE_SIZE *4;
		byte[] bytes = new byte[byteSize];
		 fis = new FileInputStream(decompressResultPath);
		 mydos = new TileSplitDataOutputStream(new DataOutputStream(new FileOutputStream(completeResultPath)));
		 TreeSet<Integer> invaildDataBlkNo =	getInvaildDataBlkNo(tableName,elevationRowKey,slopeRowKey);
		 Iterator<Integer> ite = invaildDataBlkNo.iterator();
		 int currentInvaildBlkNo =0;
		 int currentReadBlkNo = 1;
		 while(ite.hasNext()){
			 currentInvaildBlkNo = ite.next();
			 while(currentInvaildBlkNo-currentReadBlkNo>0){
				 fis.read(bytes, 0, byteSize);
				  mydos.write(bytes, 0, byteSize);
				  currentReadBlkNo++;
			 }
			 for(int i =0 ;i<TILE_SIZE*TILE_SIZE;i++){
				 mydos.writeFloat(INVAILDDATA);
			 }
			 currentReadBlkNo++;
		 }
		 while(ROW_NUM*COL_NUM-currentReadBlkNo>=0){
			   fis.read(bytes, 0, byteSize);
			   mydos.write(bytes, 0, byteSize);
			   currentReadBlkNo++;
		 }
		 fis = null;
		 mydos = null;
	}


	/**
	 * Get the invalid block number and store it in a TreeSet container
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
		TreeSet<Integer> elevationInvaildBlkSet = new TreeSet<Integer>();
		TreeSet<Integer> slopeInvaildBlkSet = new TreeSet<Integer>();
		for(int i = 0 ;i <elevationInvaildBlkArray.length;i++){
			elevationInvaildBlkSet.add(Integer.parseInt(elevationInvaildBlkArray[i]));
		}
		for(int i = 0 ;i <slopeInvaildBlkArray.length;i++){
			slopeInvaildBlkSet.add(Integer.parseInt(slopeInvaildBlkArray[i]));
		}
		elevationInvaildBlkSet.retainAll(slopeInvaildBlkSet);
		
		return elevationInvaildBlkSet;	
	}
	/**
	 * Restore tile files to grd files
	 * 
	 * @param
	 * @throws IOException
	 */
	private void writeGrdFile(String grdPath) throws IOException {
		int byteSize = TILE_SIZE * 4;
		byte[] bytes = new byte[byteSize];
		fos = new FileOutputStream(grdPath, true);
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
				startPX += 4L*TILE_SIZE * TILE_SIZE ;
				raf.seek(startPX);
				temp--;
			}
			raf.read(bytes, 0, (TILE_SIZE - COL_ADD) * 4);
			fos.write(bytes, 0, (TILE_SIZE - COL_ADD) * 4);
			current_row++;
		}
	}


	/**
	 * Write grd header file
	 * @param grdPath Generate grd file path
	 * @param z1 ZMin
	 * @param z2 ZMax
	 */
	private void writeGrdHead(String grdPath,double z1,double z2) {
		RasHead rasHead = new RasHead(ROW, COL,x1, x2, y1, y2,z1,z2);
		rasHead.writeGrdHead(grdPath);

	}

	/**
	 * Unzip the original file and generate the decompressed tile file. This is the first step.
	 * @param gzTilePath
	 * @param decompressResultPath
	 */
	private void decompress(String gzTilePath,String decompressResultPath)  {
		File dir = new File(gzTilePath);
		
		File newFile = new File(decompressResultPath);
		if(newFile.exists()){
			//newDir.delete();
			System.out.println("Tile file decompression stitching completed!");
			return;
		}	
	//	File[] contentFile = dir.listFiles();
		List<File> files = Arrays.asList(dir.listFiles());
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
			      System.out.println(fileName+"Decompression completed");
			      //GZIPin.close();
				}
			}
			System.out.println("Tile file decompression stitching completed!");
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