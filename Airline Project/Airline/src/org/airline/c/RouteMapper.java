package org.airline.c;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.airline.utility.RouteConstant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

public class RouteMapper extends Mapper<LongWritable, Text, Text, Text>{
	Path[] catchfiles = new Path[0];
	List<String> airlineMap = new ArrayList();
	public void setup(Context context) throws IOException, InterruptedException {
		 Configuration conf = context.getConfiguration();
		 try {
			catchfiles = DistributedCache.getLocalCacheFiles(conf);
			BufferedReader reader = new BufferedReader(new FileReader(catchfiles[0].toString())); 
			String strRead;
			while ((strRead = reader.readLine())!= null) 
				  {
				 	airlineMap.add(strRead);
				  }
				    
			} catch (FileNotFoundException e) {
	              e.printStackTrace();
           }catch(IOException e) {
				e.printStackTrace();
			}
	 }
	 
	 protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,Text>.Context context) throws IOException ,InterruptedException {
		 String[] parts  = value.toString().split(",");
		String codeShare = parts[RouteConstant.r_codeShare];
		if (codeShare.equalsIgnoreCase("Y")) {
			for (String a : airlineMap) {
				String airline[] = a.toString().split(",");
				String routeAId = parts[RouteConstant.r_airlineId];
				if (routeAId.equals(airline[0])) {
					context.write(new Text(airline[1]), new Text(""));
				}
			}
		}
	 };
}
