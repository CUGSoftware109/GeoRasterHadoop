package cug.hadoop.geo.utils;

import java.io.DataOutputStream;

import java.io.IOException;

public class HadoopDataOutputStream  {

	private DataOutputStream dos;
	
   public HadoopDataOutputStream(DataOutputStream dos) {  
		    this.dos = dos; 
	 } 
	public  void writeFloat(float f) throws IOException{	
		byte[] bytes = new byte[4];
		bytes = this.getBytes(f);
		dos.write(bytes);
	}
	
	public void writeShort(short s) throws IOException{
		byte[] bytes = new byte[2];
		bytes = this.getBytes(s);
		dos.write(bytes);
	}
	
	public void writeDouble(double d) throws IOException{
		 byte[] bytes = new byte[8];
		 bytes = this.getBytes(d);
		 dos.write(bytes);
	}
	
	public void close() throws IOException{
		dos.close();
	}
	
	private  byte[] getBytes(short data)
	    {
	       byte[] bytes = new byte[2];
	       bytes[0] = (byte) (data & 0xff);
	       bytes[1] = (byte) ((data & 0xff00) >> 8);
	       return bytes;
	    }
	 

	private  byte[] getBytes(int data)
	    {
	        byte[] bytes = new byte[4];
	        bytes[0] = (byte) (data & 0xff);
	        bytes[1] = (byte) ((data & 0xff00) >> 8);
	        bytes[2] = (byte) ((data & 0xff0000) >> 16);
	        bytes[3] = (byte) ((data & 0xff000000) >> 24);
	        return bytes;
	    }
	    
	private  byte[] getBytes(float data)
	    {
	        int intBits = Float.floatToIntBits(data);
	        return getBytes(intBits);
	    }
	    
	private  byte[] getBytes(long data)
	    {
	        byte[] bytes = new byte[8];
	        bytes[0] = (byte) (data & 0xff);
	        bytes[1] = (byte) ((data >> 8) & 0xff);
	        bytes[2] = (byte) ((data >> 16) & 0xff);
	        bytes[3] = (byte) ((data >> 24) & 0xff);
	        bytes[4] = (byte) ((data >> 32) & 0xff);
	        bytes[5] = (byte) ((data >> 40) & 0xff);
	        bytes[6] = (byte) ((data >> 48) & 0xff);
	        bytes[7] = (byte) ((data >> 56) & 0xff);
	        return bytes;
	    }
	    
	private  byte[] getBytes(double data)
	    {
	        long intBits = Double.doubleToLongBits(data);
	        return getBytes(intBits);
	    }
}
