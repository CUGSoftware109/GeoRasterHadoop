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
 * Split and compress tiles
 */
public class ZIPTile {
	
	private static final short START=60;//Header offset
	private short TILE_SIZE;
	
	//private  int rasId;

	public static void main(String[] args){
		if(args.length !=1){
			System.out.println("The number of parameters does not match, it should be params file path, raster data type (0 for elevation data, 1 for slope data)!");
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
	   
	    
		//Get the processing file type: elevation raster rasId is 0, gradient raster rasId is 1
	   int rasId = Integer.parseInt(params[3]);
		if(rasId !=0 && rasId !=1){
			System.out.println("Raster data format selection error: need to select elevation or slope raster data!");
			System.exit(0);
		}
	   
		File f = new File(DES_PATH);
		if(!f.exists()){
			 f.mkdir();//Create a folder if the file does not exist
		}
		if(!f.isDirectory()){//If the file is not a folder
			 System.out.println("The target path must be a folder");
			 System.exit(0);
		}
		System.out.println(RAS_PATH+"The file starts to be converted to a tile file");
		ArrayList<Integer> invaildTileNoList = splitRasFile.doSplit(START, TILE_SIZE, RAS_PATH, DES_PATH);// Generate tile file, uncompressed
           
		ZIPTile zipTile = new ZIPTile();
		
	    
		zipTile.doZIP(DES_PATH,rasId,invaildTileNoList);//Compress tile files to generate tileZip files

		
		  
	}
	
	public ZIPTile(){
	}
	
	public void doZIP(String path,int rasId,ArrayList<Integer> invaildTileNoList){
		  System.out.println("Tile file starts to compress......");
		  this.init(path);
		  this.ZIP(path,rasId,invaildTileNoList);
	}
	
	
	/**
	 * Initialize tile file data to get header file related information
	 * @param path
	 */
	private void init(String path){
		 TileHeadFile tileHeadFile = new TileHeadFile(path+"/headMsg");
		 this.TILE_SIZE = tileHeadFile.getTILE_SIZE();	 
	}
       //Label indicates the flag of the compressed file, followed by the RasId in the table
	private void ZIP(String path,int rasId,ArrayList<Integer> invaildTileNoList){
		 int size = TILE_SIZE*TILE_SIZE*4;
		 //Affirmation of input and output streams
		 FileInputStream fis = null;
		 FileOutputStream fos = null;
		 ByteArrayOutputStream byOut = null;
		 ZipOutputStream zipOut = null;
		 
	   try {
			  fis = new FileInputStream(path+"/tile");
			  fos = new FileOutputStream(path+"/tileZip");
			  byOut=new ByteArrayOutputStream();
			  zipOut = new ZipOutputStream(byOut);

			// zipOut.putNextEntry(new ZipEntry("Zip")); //Must be there before compression, otherwise an error
			 
			 byte bytes[] = new byte[size]; //  Store raw tile bytes
			 byte bytesAddRasId[] = new byte[size+4]; //Store raw tile bytes plus rasId
			 byte zipBytes[] = null; //Store compressed bytes
			 int count;   //Number of bytes read
			 int zipCount;  //Compressed bytes
			 int currentTileNo=1; //Current tile number
			 int total=0;
			 int usenessBytesNum=0;
			 
			 int time = 0;//invaildTileList value
			 while((count = fis.read(bytes,0,size))!=-1){  //Loop through the current tile byte array, read the byte array to bytes, read 256kb each time
				 if(!invaildTileNoList.isEmpty()){//If invaildTileNoList is not empty
					if (currentTileNo == invaildTileNoList.get(time)) {// If the current tile is an invalid tile
						currentTileNo++;
						if (time == invaildTileNoList.size() - 1) {// Time has reached the maximum value, indicating that the invalid tile has been traversed. At this time, time cannot continue to increase by 1, otherwise it will cross the boundary.
							continue;
						}
						time++;
						continue;
					}
				 }
				   ZipEntry entry = new ZipEntry("zip"+currentTileNo);
				   //Assign the contents of the bytes array to the bytesAddRasId array
				   for(int i =0;i<bytes.length;i++){
					   bytesAddRasId[i+4] = bytes[i]; 
				   }
				   //The first four bits in bytesAddRasId store rasId
				   bytesAddRasId[0] = (byte) ( rasId& 0xff);
				   bytesAddRasId[1] = (byte) ((rasId & 0xff00) >> 8);
				   bytesAddRasId[2] = (byte) ((rasId & 0xff0000) >> 16);
				   bytesAddRasId[3] = (byte) ((rasId & 0xff000000) >> 24);
				      
				   entry.setSize(bytesAddRasId.length);
				   zipOut.putNextEntry(entry);     
			      zipOut.write(bytesAddRasId);//Write a byte array to the compressed stream
			      zipOut.closeEntry();// close zipEntry
			      
			      zipBytes = byOut.toByteArray(); //Get the compressed array
			      zipCount = byOut.size();  
			      byte mixBytes[] = new byte[zipCount+8]; 
			      for(int i = zipCount-1;i>=0;i--){ //Move the zipbytes byte back 12 bits, leaving the previous 12-bit storage tile number and byte length information, and rasId
			    	   mixBytes[i+8] = zipBytes[i];  
			        }
			      byOut.reset();
			         			        //The first four bytes store the tile number int
			      mixBytes[0] = (byte) (currentTileNo & 0xff);
			      mixBytes[1] = (byte) ((currentTileNo & 0xff00) >> 8);
			      mixBytes[2] = (byte) ((currentTileNo & 0xff0000) >> 16);
			      mixBytes[3] = (byte) ((currentTileNo & 0xff000000) >> 24);
			        //Store compressed byte length zipCount plus an int rasId, ie zipCount
			      mixBytes[4] = (byte) (zipCount & 0xff);
			      mixBytes[5] = (byte) ((zipCount & 0xff00) >> 8);
			      mixBytes[6] = (byte) ((zipCount & 0xff0000) >> 16);
			      mixBytes[7] = (byte) ((zipCount & 0xff000000) >> 24);
			      
			       //Store the RasId to value in four bytes.
			   
			    		  
			      currentTileNo++;
			   //   System.out.println(currentTileNo+" "+zipCount);
			        
			      total += (zipCount + 8);
			      if(total > 128*1024*1024){//If it is greater than 128MB
			    	   total =  total-zipCount - 8;
			    	   usenessBytesNum = 128*1024*1024 - total;
			    	   byte usenessBytes[] = new byte[usenessBytesNum];
			    	   fos.write(usenessBytes);
			    	   total = zipCount + 8;
			        }
			      
			        //Write to disk
			      fos.write(mixBytes,0,zipCount+8);
			 }
		}catch (Exception e) {
			e.printStackTrace();
			System.out.println("Tile file compression failed！");
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
	   System.out.println("The tile file is successfully compressed, and the result file directory is generated.："+ path);
	}
	

}
