package cug.hadoop.geo.utils;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseClient { 
	
	static Configuration cfg = HBaseConfiguration.create();
	
	/**
	 * @param tableName
	 * @param rowKey
	 * @param v
	 * @throws IOException
	 */
	@SuppressWarnings("deprecation")
	public static void addRecord(String tableName, String rowKey, ArrayList<StringBuilder[]> v) throws IOException{
		HTable table = new HTable(cfg, tableName);
		Put put = new Put(Bytes.toBytes(rowKey));
		
		for (StringBuilder[] sbs : v){			
			put.add(Bytes.toBytes(sbs[0].toString()),Bytes.toBytes(sbs[1].toString()),Bytes.toBytes(sbs[2].toString()));
			table.put(put);
		}
		
	}
			
	/**
	 * @param tablename
	 * @param rowKey
	 * @throws IOException
	 */
	@SuppressWarnings("deprecation")
	public static String selectOneRow(String tablename, String rowKey, String family, String qualifier) throws IOException{
		HTable table = new HTable(cfg, tablename);
		Get g = new Get(rowKey.getBytes());
		g.addColumn(family.getBytes(), qualifier.getBytes());
		Result rs = table.get(g);
		String value = "";
		for (KeyValue kv: rs.raw()){
			//System.out.println(new String(kv.getRow())+"");
			//System.out.println(new String(kv.getFamily())+"");
			//System.out.println(new String(kv.getQualifier())+"");
			//System.out.println(kv.getTimestamp()+"");
			//System.out.println(new String(kv.getValue())+"");
			value = new String(kv.getValue());
		}
		return value;
		
	}



}
