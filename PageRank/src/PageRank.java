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
	static String inlinklocation="/Users/prachibhansali/Documents/IR/Assignment4/inlinks.txt";

	public static void main(String[] args) throws Exception{
		//String location="/Users/prachibhansali/Documents/IR/Assignment4/wt2g_inlinks.txt";
		//String fileext = ".txt";
		//SimplePageRank.computePageRank(location);
		
		String location="/Users/prachibhansali/Documents/IR/Assignment4/saved/";
		String fileext = ".json";

		HashMap<String,ArrayList<String>> outlinkmapping = new HashMap<String,ArrayList<String>>();
		HashMap<String,JSONArray> inlinkmapping = new HashMap<String,JSONArray> ();


		fetchOutlinksFromFiles(location,outlinkmapping,fileext,inlinkmapping);
		//printInlinkMapping(inlinkmapping);
		System.out.println("Done inlinks to file " + inlinkmapping.size() + "***************");
		SimplePageRank.computePageRank(inlinkmapping);
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

	private static void printInlinkMapping(HashMap<String, JSONArray> inlinkmapping) throws FileNotFoundException, Exception {
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
	}

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
				//System.out.println(id);
				JSONArray outlinks =(JSONArray) ((JSONObject)obj.get("fields")).get("out_links");
				for(int j=1;j<outlinks.size();j++)
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

	/*public static void main(String[] args) throws Exception{
		String location = "/Users/prachibhansali/Downloads/wt2g_inlinks.txt";
		//String location = "matrix";
		//HashMap<Integer,String> docids = new HashMap<Integer,String>();
		HashMap<String,Integer> docnames =  new HashMap<String,Integer>();
		//readOutlinksFromFile(location,docids,docnames);
		readInlinksFromFile(location,docnames);
		//double[][] transitionMatrix = createTransitionMatrix(location,docnames,docids);
		float[][] transitionMatrix = createColumnTransitionMatrix(location,docnames);

		float[][] urls = new float[1][docnames.size()];
		computeEndState(new MatrixCustom(1,1,docnames.size()),new MatrixCustom(transitionMatrix,docnames.size(),docnames.size()));
	}

	private static float[][] createColumnTransitionMatrix(String location,
			HashMap<String, Integer> docnames) throws Exception{
		int mapsize = docnames.size();
		float[][] transition = new float[mapsize][mapsize];

		BufferedReader br = new BufferedReader(new FileReader(new File(location)));
		String line = null;
		while((line=br.readLine())!=null)
		{
			String [] str = line.split("\\s");
			String url = str[0];
			int column = docnames.get(url);
			System.out.println("url : "+url);
			for(int i=1;i<str.length;i++){
				if(docnames.containsKey(str[i]) && !str[i].equals(url)){
					System.out.println("inlinked url : "+str[i]);
					transition[docnames.get(str[i])][column] = 1;
				}
			}
		}
		for(int i=0;i<mapsize;i++)
		{
			int totaloutlinks = 0;
			for(int j=0;j<mapsize;j++)
				if(transition[i][j]!=0)
					totaloutlinks++;
			System.out.println("totaloutlinks : "+totaloutlinks);

			for(int j=0;j<mapsize;j++)
				if(transition[i][j]!=0)
					{
					System.out.println("direct url : "+i+" to "+j);
					transition[i][j]/=totaloutlinks;
					}
		}

		return transition;
	}

	private static void readInlinksFromFile(String location,
			HashMap<String, Integer> docnames) throws Exception {
		BufferedReader br = new BufferedReader(new FileReader(new File(location)));
		String line = null;
		while((line=br.readLine())!=null)
		{
			String [] str = line.split("\\s+");
			String url = str[0];
			if(!docnames.containsKey(url))
			{
				int size = docnames.size();
				System.out.println("Added "+url +" "+size);
				docnames.put(url, size);
			}
		}
		br.close();
	}

	private static void computeEndState(MatrixCustom urlState, MatrixCustom transitionMatrix) throws FileNotFoundException {
		MatrixCustom prevUrlState = null;
		MatrixCustom initState = new MatrixCustom(urlState);
		System.out.println("init transition matrix : \n"+transitionMatrix);
		do
		{
			prevUrlState = new MatrixCustom(urlState);
			urlState = new MatrixCustom(urlState.multiply(transitionMatrix));
			urlState = new MatrixCustom(new MatrixCustom(urlState.multiply(0.85f)).add((float) (0.15/urlState.getColumn())));
			//transitionMatrix = transitionMatrix.square();
			System.out.println("Current : \n"+urlState.toString());

		}
		while(!prevUrlState.isSimilar(urlState));

		System.out.println("final : \n"+transitionMatrix.toString()+"\n");
	}

	private static double[][] createTransitionMatrix(String location,
			HashMap<String, Integer> docnames, HashMap<Integer, String> docids) throws Exception {
		int mapsize = docnames.size();
		double[][] transition = new double[mapsize][mapsize];

		BufferedReader br = new BufferedReader(new FileReader(new File(location)));
		String line = null;
		while((line=br.readLine())!=null)
		{
			String [] str = line.split("\t");
			int totaloutlinks = 0;
			String url = str[0];
			int row = docnames.get(url);
			System.out.println("url : "+url);
			for(int i=1;i<str.length;i++){
				if(docnames.containsKey(str[i]) && !str[i].equals(url)){
					totaloutlinks++;
					transition[row][docnames.get(str[i])] = 1;
				}
			}
			System.out.println("totaloutlinks : "+totaloutlinks);
			for(int i=0;i<mapsize;i++){
				if(transition[row][i]!=0)
					transition[row][i]/=totaloutlinks;
			}
		}
		return transition;
	}

	private static void readOutlinksFromFile(
			String location,HashMap<Integer,String> docids,HashMap<String,Integer> docnames) throws Exception {
		BufferedReader br = new BufferedReader(new FileReader(new File(location)));
		String line = null;
		while((line=br.readLine())!=null)
		{
			String [] str = line.split("\t");
			String url = str[0];
			if(!docnames.containsKey(url))
			{
				int size = docnames.size();
				System.out.println("Added "+url +" "+size);
				docnames.put(url, size);
				docids.put(size, url);
			}
		}
		br.close();
	}
	 */
}
