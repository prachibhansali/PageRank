import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;



public class PageRank {
	static HashMap<String,Integer> crawledDocs = new HashMap<String,Integer>();

	public static void main(String[] args) throws Exception{
		//String location="/Users/prachibhansali/Documents/IR/Assignment4/wt2g_inlinks.txt";
		//String fileext = ".txt";
		//SimplePageRank.computePageRank(location);

		String location="/Users/prachibhansali/Documents/IR/Assignment4/saved/";
		String fileext = ".json";

		HashMap<String,ArrayList<String>> outlinkmapping = new HashMap<String,ArrayList<String>>();
		HashMap<String,JSONArray> inlinkmapping = new HashMap<String,JSONArray> ();


		fetchOutlinksFromFiles(location,outlinkmapping,fileext,inlinkmapping);
		InlinkMapping(inlinkmapping);
		System.out.println("Done inlinks to file " + inlinkmapping.size() + "***************");
		SimplePageRank.computePageRank(inlinkmapping);
	}

	private static void InlinkMapping(HashMap<String, JSONArray> inlinkmapping) {
		Iterator<String> itr = inlinkmapping.keySet().iterator();
		ArrayList<String> keysToRemove = new ArrayList<String>();
		while(itr.hasNext())
		{
			String inlink= itr.next();
			if(!crawledDocs.containsKey(inlink))
				keysToRemove.add(inlink);
		}
		for(int i=0;i<keysToRemove.size();i++)
			inlinkmapping.remove(keysToRemove.get(i));
		System.out.println("map size = "+inlinkmapping.size());		
	}

	private static void convertToInlinkMap(HashMap<String, ArrayList<String>> outlinkmapping,HashMap<String,JSONArray> inlinkmapping) throws Exception, FileNotFoundException {
		Iterator<String> keys = outlinkmapping.keySet().iterator();
		int count =0;
		while(keys.hasNext())
		{
			String key = (String) keys.next();
			ArrayList<String> a = outlinkmapping.get(key);
			System.out.println("Started for "+count++ + + outlinkmapping.size() +a.size());

			for(int i=0;i<a.size();i++){
				String inlink = a.get(i);
				Object inlinks = inlinkmapping.containsKey(inlink) ?  inlinkmapping.get(inlink) : new JSONArray();
				((JSONArray)inlinks).add(key);
				inlinkmapping.put(inlink, (JSONArray) inlinks);
			}
			System.out.println("Endded for "+count++  + "  " + outlinkmapping.size() +"  "+a.size());

		}
		System.out.println("Done");


	}

	/*private static void printInlinkMapping(HashMap<String, JSONArray> inlinkmapping) throws FileNotFoundException, Exception {
		Iterator<String> ks = inlinkmapping.keySet().iterator();
		PrintWriter out = new PrintWriter(new FileOutputStream(inlinklocation,true));
		while(ks.hasNext())
		{
			String key = (String) ks.next();
			out.print(key+"\t");
			JSONArray a = inlinkmapping.get(key);
			for(int i=0;i<a.size();i++)
				out.print(a.get(i)+"\t");
			out.println();
		}
		out.close();
	}*/

	private static void fetchOutlinksFromFiles(String location,
			HashMap<String, ArrayList<String>> outlinkmapping,String fileext, HashMap<String, JSONArray> inlinkmapping) throws IOException {

		File [] files = new File(location).listFiles();
		for(int i=0;i<files.length ;i++)
		{
			BufferedReader br = null;
			if(files[i].isFile() && files[i].getName().endsWith(fileext)){
				try {
					br = new BufferedReader(new FileReader(files[i]));
				} catch (FileNotFoundException e1) {
					System.out.println("File not found");
				}
				String jsonstr = "";
				if(fileext.equals(".txt")) 
				{
					while((jsonstr=br.readLine())!=null) {
						JSONObject json=null;
						try {
							json = (JSONObject) JSONValue.parse(jsonstr);

							outlinkmapping.put((String)json.get("docno"),fetchOutlinkMappings((JSONArray)json.get("out_links")));
							System.out.println(outlinkmapping.size());
						} catch (Exception e) {
							System.out.println("json object could not be created "+e.toString());
						}
					}
				}
				else
				{
					JSONParser parser = new JSONParser();
					Object obj=null;
					try {
						obj = parser.parse(new FileReader(files[i]));
						//System.out.println(obj);
					} catch (ParseException e1) {
						System.out.println("Could not parse the file "+files[i]+" as json");
					}
					JSONObject json=null;
					try {
						json = (JSONObject)obj;
						fetchMulitpleOutlinkMappings((JSONArray)json.get("hits"),inlinkmapping);
						System.out.println("ON file number =============== "+files[i]);
					} catch (Exception e) {
						System.out.println("json object could not be created "+e.toString());
					}
				}
				br.close();
			}
		}
	}

	private static void fetchMulitpleOutlinkMappings(JSONArray arr, HashMap<String, JSONArray> inlinkmapping) throws FileNotFoundException {
		HashMap<String, ArrayList<String>> outlinkmapping = new HashMap<String, ArrayList<String>>();
		for(int i=0;i<arr.size();i++)
		{
			Set<String> urlset = new HashSet<String>();

			try {
				JSONObject obj = (JSONObject)arr.get(i);
				String id = (String)obj.get("_id");
				crawledDocs.put(id,0);
				//System.out.println(id);
				JSONArray outlinks =(JSONArray) ((JSONObject)obj.get("fields")).get("out_links");
				for(int j=0;j<outlinks.size();j++)
				{
					try {
						urlset.add((String)outlinks.get(j));
						//System.out.println("done");
					} catch (Exception e) {
						System.out.println("Did not get the outlink");
					}
				}
				outlinkmapping.put(id,new ArrayList<String>(urlset));
				//System.out.println("done");
			} catch (Exception e) {
				System.out.println("Did not get the outlink");
			}
		}
		try {
			convertToInlinkMap(outlinkmapping,inlinkmapping);
		} catch (Exception e) {
			System.out.println("inlink issue");
		}

	}

	private static ArrayList<String> fetchOutlinkMappings(JSONArray arr) 
	{
		Set<String> urlset = new HashSet<String>();
		for(int i=1;i<arr.size();i++)
		{
			try {
				urlset.add((String)arr.get(i));
				//System.out.println("done");
			} catch (Exception e) {
				System.out.println("Did not get the outlink");
			}
		}
		return new ArrayList<String>(urlset);
	}
}
