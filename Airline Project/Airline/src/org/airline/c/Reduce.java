package org.airline.c;

import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text, Text, Text, Text>{
	TreeMap<String, String> treeMap = new TreeMap<String, String>();
	protected void reduce(Text key, Iterable<Text> values, org.apache.hadoop.mapreduce.Reducer<Text,Text,Text,Text>.Context context) throws java.io.IOException ,InterruptedException {
		for (Text text : values) {
			treeMap.put(key.toString(), text.toString());
		}
	}
	protected void cleanup(org.apache.hadoop.mapreduce.Reducer<Text,Text,Text,Text>.Context context) throws java.io.IOException ,InterruptedException {
		for(Entry<String, String> entry : treeMap.entrySet()) {
			  String key = entry.getKey();
			  String value = entry.getValue();
			  context.write(new Text(key), new Text(value));
			}
	};
}
