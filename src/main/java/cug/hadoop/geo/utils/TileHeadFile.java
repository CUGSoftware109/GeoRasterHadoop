package cug.hadoop.geo.utils;

import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;


/**
 * @author root
 *
 */
public class TileHeadFile {

	private  int COL;//grd数据的像素列
	private  int ROW;//grd数据的像素行
	private  int COL_NUM=0;//数据块的列数
	private  int ROW_NUM=0;//数据块的行数
	private  short COL_ADD;//数据需要填充的像素列
	private  short ROW_ADD;//数据需要填充的像素行
   private  short TILE_SIZE;//瓦片尺寸
	private double x1,x2,y1,y2,z1,z2;	
	
	public TileHeadFile(int ROW,int COL,double x1,double x2,double y1,double y2,double z1,
		double z2,int ROW_NUM,int COL_NUM,short ROW_ADD,
		short COL_ADD) throws IOException{
		  this.ROW = ROW; 
		  this.COL = COL; 
		  this.x1 = x1;
		  this.x2 = x2;
		  this.y1 = y1;
		  this.y2 = y2;
		  this.z1 = z1;
		  this.z2 = z2;
		  this.ROW_NUM = ROW_NUM;
		  this.COL_NUM = COL_NUM;
		  this.ROW_ADD = ROW_ADD;
		  this.COL_ADD = COL_ADD;
		  short TILE_SIZE = (short) ((COL+COL_ADD)/COL_NUM);
		  this.TILE_SIZE = TILE_SIZE;
	 	       
	}
	
	public TileHeadFile(String path) {
		// TODO Auto-generated constructor stub
		this.readHeadFile(path);
	}

	/**
	 * 写头文件
	 * @throws IOException
	 */
	public void writeHeadFile(String path) {
		DataOutputStream dos = null;
		TileSplitDataOutputStream mydos = null;
		try {
			dos = new DataOutputStream(new FileOutputStream(path));
		   mydos = new TileSplitDataOutputStream(dos);
		     mydos.writeShort(TILE_SIZE);
		     mydos.writeInt(ROW);
		     mydos.writeInt(COL);
			  mydos.writeDouble(x1);
			  mydos.writeDouble(x2);
			  mydos.writeDouble(y1);
			  mydos.writeDouble(y2);
			  mydos.writeDouble(z1);
			  mydos.writeDouble(z2);		 
			  mydos.writeInt(ROW_NUM);
			  mydos.writeInt(COL_NUM);
			  mydos.writeShort(ROW_ADD);
			  mydos.writeShort(COL_ADD);
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
		     try {
			      mydos.close();
		         } catch (IOException e) {
			      e.printStackTrace();
		            }
		          }
	}
	
   public void readHeadFile(String path) {
	     TileSplitDataInputStream mydis = null;
		try {
			  mydis = new TileSplitDataInputStream(new FileInputStream(path));
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
		} catch (FileNotFoundException e) {	
			e.printStackTrace();
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
 	
	public short getTILE_SIZE() {
	return TILE_SIZE;
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
