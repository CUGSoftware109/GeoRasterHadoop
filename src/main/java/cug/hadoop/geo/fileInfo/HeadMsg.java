package cug.hadoop.geo.fileInfo;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;

import cug.hadoop.geo.utils.TileSplitDataInputStream;

public class HeadMsg {
	private  int COL;//Pixel column of grd data
	private  int ROW;//Pixel row of grd data
	private  int COL_NUM=0;//Number of columns in the data block
	private  int ROW_NUM=0;//Number of rows in the data block
	private  short COL_ADD;//The pixel column that the data needs to fill
	private  short ROW_ADD;//The row of pixels that the data needs to fill
   private  short TILE_SIZE;
	private double x1,x2,y1,y2,z1,z2;
	
	public HeadMsg(FSDataInputStream fsis){
		TileSplitDataInputStream mydis =null;
		    try {
		    	  mydis = new TileSplitDataInputStream(fsis);
				 this.TILE_SIZE = mydis.readShort();
				  this.ROW = mydis.readInt();
				  this.COL = mydis.readInt();
				  this.x1 = mydis.readDouble();
				  this.x2 = mydis.readDouble();
				  this.y1 = mydis.readDouble();
				  this.y2 = mydis.readDouble();
				  this.z1 = mydis.readDouble();
				  this.z2 = mydis.readDouble();
				  this.ROW_NUM = mydis.readInt();
				  this.COL_NUM = mydis.readInt();
				  this.ROW_ADD = mydis.readShort();
				  this.COL_ADD = mydis.readShort();
			} catch (IOException e) {
				e.printStackTrace();
			}finally{
				try {
					mydis.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		    
		
	}

	public int getCOL() {
		return COL;
	}

	public int getROW() {
		return ROW;
	}

	public int getCOL_NUM() {
		return COL_NUM;
	}

	public int getROW_NUM() {
		return ROW_NUM;
	}

	public short getCOL_ADD() {
		return COL_ADD;
	}

	public short getROW_ADD() {
		return ROW_ADD;
	}

	public short getTILE_SIZE() {
		return TILE_SIZE;
	}

	public double getX1() {
		return x1;
	}

	public double getX2() {
		return x2;
	}

	public double getY1() {
		return y1;
	}

	public double getY2() {
		return y2;
	}

	public double getZ1() {
		return z1;
	}

	public double getZ2() {
		return z2;
	}
	
	

	

}
