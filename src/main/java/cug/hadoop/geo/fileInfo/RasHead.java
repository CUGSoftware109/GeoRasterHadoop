package cug.hadoop.geo.fileInfo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import cug.hadoop.geo.utils.TileSplitDataInputStream;
import cug.hadoop.geo.utils.TileSplitDataOutputStream;


//头文件类
public class RasHead {

	private int nx;
	private int ny;
	//private int no_use;
	private double x1;
	private double x2;
	private double y1;
	private double y2;
	private double z1;
	private double z2;
	
	
	public RasHead(String path){
		   this.readHeadFile(path);
		
	}
	public RasHead(int nx,int ny,double x1,double x2,double y1,double y2,double z1,double z2){
		   this.nx = nx;
		   this.ny = ny;
		   this.x1 = x1;
		   this.x2 = x2;
		   this.y1 = y1;
		   this.y2 = y2;
		   this.z1 = z1;
		   this.z2 = z2;			
	}
	
	public void writeGrdHead(String path){
		DataOutputStream dos = null;
		TileSplitDataOutputStream mydos = null;
		try {
			dos = new DataOutputStream(new FileOutputStream(path));
		   mydos = new TileSplitDataOutputStream(dos);
		   String lable = "DSBB";
		    byte[] bytes = lable.getBytes();
		     dos.write(bytes);
		     mydos.writeInt(nx);
			  mydos.writeInt(ny);
			  mydos.writeDouble(x1);
			  mydos.writeDouble(x2);
			  mydos.writeDouble(y1);
			  mydos.writeDouble(y2);
			  mydos.writeDouble(z1);
			  mydos.writeDouble(z2);
		} catch (FileNotFoundException e) {			
			e.printStackTrace();
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
			   long i = mydis.skip(4);
			   this.nx = mydis.readInt();
			   this.ny = mydis.readInt();	   
			   this.x1 = mydis.readDouble();
			   this.x2 = mydis.readDouble();
			   this.y1 = mydis.readDouble();
			   this.y2 = mydis.readDouble();
			   this.z1 = mydis.readDouble();
			   this.z2 = mydis.readDouble();
			   System.out.println(nx);
			   System.out.println(ny);		
			   System.out.println(x1);
			   System.out.println(x2);
			   System.out.println(y1);
			   System.out.println(y2);
			   System.out.println(z1);
			   System.out.println(z2);
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
	
	public double getX1() {
		return x1;
	}

	public void setX1(double x1) {
		this.x1 = x1;
	}

	public double getX2() {
		return x2;
	}

	public void setX2(double x2) {
		this.x2 = x2;
	}

	public double getY1() {
		return y1;
	}

	public void setY1(double y1) {
		this.y1 = y1;
	}

	public double getY2() {
		return y2;
	}

	public void setY2(double y2) {
		this.y2 = y2;
	}

	public double getZ1() {
		return z1;
	}

	public void setZ1(double z1) {
		this.z1 = z1;
	}

	public double getZ2() {
		return z2;
	}

	public void setZ2(double z2) {
		this.z2 = z2;
	}

	public int getNx() {
		return nx;
	}
	public void setNx(int nx) {
		this.nx = nx;
	}
	public int getNy() {
		return ny;
	}
	public void setNy(int ny) {
		this.ny = ny;
	}	
}
