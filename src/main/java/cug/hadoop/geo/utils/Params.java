package cug.hadoop.geo.utils;

import java.io.FileInputStream;

public class Params {
	private String paramsPath ;
	public Params(String paramsPath){
		this.paramsPath = paramsPath;
	}
	public String[] getParams(){
		try {
			FileInputStream  fis = new FileInputStream(paramsPath);
			byte[] bytes = new byte[2048];
			fis.read(bytes);
			String s= new  String(bytes, "utf-8");
			s=s.replaceAll("( )( )+"," ");
			s =s .trim();
			String[] params =s.split(" ");
			if(params.length!=4){
				System.out.println("Parameter error！");
				return null;
			}
			params[1] = params[1].trim();
			params[2] = params[2].trim();
			fis.close();
			return params;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		
	}
	
}
