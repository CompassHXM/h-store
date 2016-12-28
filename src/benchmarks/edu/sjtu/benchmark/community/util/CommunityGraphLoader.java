package edu.sjtu.benchmark.community.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Random;

public class CommunityGraphLoader{
	String filename;
	BufferedReader br = null;
	
	int max_item_id = Integer.MAX_VALUE;
	public CommunityGraphLoader(String filename) throws FileNotFoundException {
		this.filename = filename;
		if(filename==null || filename.isEmpty())
			throw new FileNotFoundException("You must specify a filename to instantiate the TwitterGraphLoader... (probably missing in your workload configuration?)");
		
		File file = new File(filename);
		FileReader fr = new FileReader(file);
		br = new BufferedReader(fr);
	}
	public void setMaxItemId(int max_item_id) {
		this.max_item_id = max_item_id;
	}
	public GraphEdge readNextEdge() throws IOException {
		int item0 = Integer.MAX_VALUE;
		int item1 = Integer.MAX_VALUE;
		while(item0 > max_item_id) {
			String line = br.readLine();
			if (line == null) return null;
			String[] sa = line.split("\\s+");
			item0 = Integer.parseInt(sa[0]);
			item1 = Integer.parseInt(sa[1]);
			System.out.println(item0+" "+item1);
		}
		

		if(item0 > max_item_id) {
			return null;
		}
		
		return new GraphEdge(item0,item1);
	}
};