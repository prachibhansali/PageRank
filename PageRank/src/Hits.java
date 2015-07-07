import java.util.*;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class Hits
{
	public static void main(String args[]) throws IOException, ParseException
	{
		Hits hits = new Hits();
		hits.computeHits();
	}

	public void computeHits() throws IOException, ParseException
	{
		int d =50;
		//String index = "maritime_disaster";
		//String type = "document";
		String query = "maritime accidents";

		String hitslocation="/Users/prachibhansali/Documents/IR/Assignment4/hits/";
		String fileext = ".json";

		String location="/Users/prachibhansali/Documents/IR/Assignment4/saved/";
		//String fileext = ".txt";

		HashMap<String,Set<String>> outlinkmapping = new HashMap<String,Set<String>>();
		HashMap<String,JSONArray> inlinkmapping = new HashMap<String,JSONArray> ();

		System.out.println("fetching outlinks");

		//HashMap<String,Boolean> rootset = fetchTopDocuments(index,type,query,outlinkmapping);
		HashMap<String,Boolean> rootset = fetchTopDocumentsFromFile(hitslocation,fileext,query);

		System.out.println("Root size "+rootset.size());

		Set<String> crawledDocuments = new HashSet<String>();
		fetchCrawledDocumentsOnly(location,crawledDocuments);

		fetchOutlinksFromFiles(location,outlinkmapping,fileext,inlinkmapping,crawledDocuments);
		System.out.println("done fetching outlinks "+outlinkmapping.size());

		HashMap<String,Float> authority = new HashMap<String,Float>();
		HashMap<String,Float> hub = new HashMap<String,Float>();


		System.out.println("done");
		Iterator<String> itr = new HashMap<String,Boolean>(rootset).keySet().iterator();
		Iterator<String> oitr = new HashMap<String,Boolean>(rootset).keySet().iterator();

		while(oitr.hasNext())
		{
			String url = oitr.next();
			Set<String> a = outlinkmapping.get(url);
			for(String s : a)
				rootset.put(s, false);
		}

		while(itr.hasNext())
		{
			String url = itr.next();
			if(inlinkmapping.containsKey(url)) {
				JSONArray arr = inlinkmapping.get(url);
				for(int i=0;i<arr.size() && i<d;i++)
					rootset.put((String) arr.get(i), false);
			}
		}
		System.out.println(rootset.size());
		
		PrintWriter pw =new PrintWriter("rootset");
		Set<String> rs = rootset.keySet();
		for(String s : rs)
			pw.println(s);
		pw.close();

		HashMap<String,Set<String>> outlinks = new HashMap<String,Set<String>>();
		HashMap<String,Set<String>> inlinks = new HashMap<String,Set<String>>();
		itr = new HashMap<String,Boolean>(rootset).keySet().iterator();

		while(itr.hasNext())
		{
			String url = itr.next();
			if(outlinkmapping.containsKey(url))
				outlinks.put(url, outlinkmapping.get(url));
			if(inlinkmapping.containsKey(url)) inlinks.put(url, fetchSetFromJSON(inlinkmapping.get(url)));
		}
		clearNonLinksFromRoot(outlinks,rootset);
		System.out.println("Cleared non links for outlinks");

		clearNonInLinksFromRoot(inlinks,rootset);
		System.out.println("Cleared non links for inlinks");

		System.out.println("here"+inlinks.size());
		System.out.println("here"+outlinks.size());

		initialize(authority,rootset);
		initialize(hub,rootset);

		compute(inlinks,outlinks,authority,hub);
	}

	private void fetchCrawledDocumentsOnly(String location,
			Set<String> crawledDocuments) throws FileNotFoundException, IOException, ParseException {
		File[] files = new File(location).listFiles();
		for(int i=0;i<files.length;i++)
		{
			if(!files[i].isFile() || !files[i].getName().endsWith(".json")) continue;
			JSONParser parser = new JSONParser();
			Object obj=null;
			try {
				obj = parser.parse(new FileReader(files[i]));
				//System.out.println(obj);
			} catch (ParseException e1) {
				System.out.println("Could not parse the file "+files[i]+" as json");
			}
			JSONObject json = (JSONObject)obj;
			JSONArray arr = (JSONArray) json.get("hits");
			for(int j=0;j<arr.size();j++)
			{
				String id = (String) ((JSONObject)arr.get(j)).get("_id");
				crawledDocuments.add(id);
			}
		}
		System.out.println("CRAWLED SIZE = "+crawledDocuments.size());
	}

	private void clearNonInLinksFromRoot(HashMap<String, Set<String>> inlinks,
			HashMap<String, Boolean> rootset) {
		Iterator<String> itr = new HashMap<String, Set<String>>(inlinks).keySet().iterator();

		while(itr.hasNext())
		{
			String url = itr.next();
			if(!inlinks.containsKey(url)) continue;
			Set<String> s = new HashSet<String>(inlinks.get(url));
			for(String str : s)
				if(!rootset.containsKey(str))
					inlinks.get(url).remove(str);
		}
	}

	private void initialize(HashMap<String, Float> map,
			HashMap<String, Boolean> rootset) {
		for(String s : rootset.keySet())
			map.put(s, 1.0f);
	}

	private void compute(HashMap<String, Set<String>> inlinks,
			HashMap<String, Set<String>> outlinks,
			HashMap<String, Float> authority, HashMap<String, Float> hub) throws FileNotFoundException {

		HashMap<String, Float> prevAuthority = null;
		HashMap<String, Float> prevHub = null;
		int count=0;

		while(!isSimilar(prevAuthority,authority) || !isSimilar(prevHub,hub))
		{
			System.out.println(count++);
			if(!isSimilar(prevAuthority,authority))
			{
				prevAuthority = new HashMap<String, Float>(authority);
				authority = computeNewScores(inlinks,authority,hub);
				authority = normalizeScores(authority);
			}
			if(isSimilar(prevAuthority,authority)) prevAuthority = authority;
			if(!isSimilar(prevHub,hub))
			{
				prevHub = new HashMap<String, Float>(hub);
				hub = computeNewScores(outlinks,hub,prevAuthority);
				hub = normalizeScores(hub);
			}
		}

		getTopDocuments(500,authority,"authority.txt");
		getTopDocuments(500,hub,"hub.txt");

	}

	private static void getTopDocuments(int num, HashMap<String, Float> scoremap, String filename) throws FileNotFoundException {
		PrintWriter out = new PrintWriter(filename);
		Iterator<String> itr = scoremap.keySet().iterator();
		ArrayList<String> urls = new ArrayList<String>();
		ArrayList<Float> scores = new ArrayList<Float>();

		while(itr.hasNext()){
			String key = itr.next();
			if(scores.size() < num)
			{
				scores.add(scoremap.get(key));
				urls.add(key);
			}
			else 
			{
				int index = scores.indexOf(Collections.min(scores));
				if(scores.get(index) < scoremap.get(key))
				{
					scores.set(index, scoremap.get(key));
					urls.set(index, key);
				}
			}
		}

		while(scores.size()>0)
		{
			int index = scores.indexOf(Collections.max(scores));
			out.println(urls.get(index)+"\t"+scores.get(index));
			urls.remove(index);
			scores.remove(index);
		}
		out.close();
	}

	private HashMap<String, Float> normalizeScores(HashMap<String, Float> scores) {
		Iterator<String> keys = scores.keySet().iterator();
		float mean = 0f;
		while(keys.hasNext())
		{
			String key = (String) keys.next();
			mean += ((scores.get(key)) * (scores.get(key)));
		}
		mean = (float) Math.sqrt(mean);
		keys = scores.keySet().iterator();
		while(keys.hasNext())
		{
			String key = (String) keys.next();
			scores.put(key, scores.get(key)/(float)mean);
		}
		return scores;
	}

	private HashMap<String,Float> computeNewScores(
			HashMap<String, Set<String>> links,
			HashMap<String, Float> scores, HashMap<String, Float> medium) {
		Iterator<String> keys = scores.keySet().iterator();
		HashMap<String, Float> newScore = new HashMap<String, Float>(scores);
		while(keys.hasNext()){
			String key = (String) keys.next();
			if(!links.containsKey(key)) newScore.put(key, 0f);
			else newScore.put(key, (float) (computeScore(scores,links.get(key),medium)));
		}
		return newScore;
	}

	private float computeScore(HashMap<String, Float> scores, Set<String> set,
			HashMap<String, Float> medium) {
		float score =0;
		for(String s : set)
		{
			score+=medium.get(s);
		}
		return score;
	}

	private static boolean isSimilar(HashMap<String, Float> prevscores,
			HashMap<String, Float> scores) {
		if(prevscores==null) return false;
		double delta = 0.0000000036;
		Iterator<String> keys = scores.keySet().iterator();
		while(keys.hasNext()){
			String key = (String) keys.next();
			Float currval = scores.get(key);
			Float prevval = prevscores.get(key);
			if(Math.abs(prevval-currval) > delta)
			{
				return false;
			}

		}
		return true;
	}

	private void clearNonLinksFromRoot(HashMap<String, Set<String>> links,
			HashMap<String, Boolean> rootset) {

		Iterator<String> itr = new HashMap<String, Set<String>>(links).keySet().iterator();

		while(itr.hasNext())
		{
			String url = itr.next();
			Set<String> s = new HashSet<String>(links.get(url));
			for(String str : s)
				if(!rootset.containsKey(str))
					links.get(url).remove(str);
		}

	}

	private Set<String> fetchSetFromJSON(JSONArray arr) {
		Set<String> s = new HashSet<String>();
		for(int i=0;i<arr.size();i++)
			s.add((String) arr.get(i));
		return s;
	}

	private static void fetchOutlinksFromFiles(String location,
			HashMap<String, Set<String>> outlinkmapping,String fileext, HashMap<String, JSONArray> inlinkmapping, Set<String> crawledDocuments) throws IOException {

		File [] files = new File(location).listFiles();
		for(int i=0;i<files.length ;i++)
		{
			BufferedReader br = null;
			if(!files[i].isFile() || !files[i].getName().endsWith(fileext)) continue;
			if(fileext.equals(".txt")) 
			{
				try {
					br = new BufferedReader(new FileReader(files[i]));
				} catch (FileNotFoundException e1) {
					System.out.println("File not found");
				}
				String jsonstr = "";
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
			else {
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
					fetchMulitpleOutlinkMappings(outlinkmapping,(JSONArray)json.get("hits"),inlinkmapping,crawledDocuments);
					System.out.println("ON file number =============== "+files[i]);
				} catch (Exception e) {
					System.out.println("json object could not be created "+e.toString());
				}
			}
		}
		try {
			convertToInlinkMap(outlinkmapping,inlinkmapping);
		} catch (Exception e) {
			System.out.println("inlink issue");
		}

	}

	private static void fetchMulitpleOutlinkMappings(HashMap<String, Set<String>> outlinkmapping, JSONArray arr, HashMap<String, JSONArray> inlinkmapping, Set<String> crawledDocuments) 
			throws FileNotFoundException {
		//HashMap<String, Set<String>> outlinkmapping = new HashMap<String, Set<String>>();
		for(int i=0;i<arr.size();i++)
		{
			Set<String> urlset = new HashSet<String>();
			//System.out.println(arr.get(i));
			try {
				JSONObject obj = (JSONObject)arr.get(i);
				String id = (String)obj.get("_id");
				//System.out.println(id);
				JSONArray outlinks = new JSONArray();
				if(obj.containsKey("fields") && ((JSONObject)obj.get("fields")).containsKey("out_links"))
					outlinks = (JSONArray) ((JSONObject)obj.get("fields")).get("out_links");
				//System.out.println(outlinks);
				for(int j=0;j<outlinks.size();j++)
				{
					try {
						if(crawledDocuments.contains((String)outlinks.get(j)))
							urlset.add((String)outlinks.get(j));
						//System.out.println("done");
					} catch (Exception e) {
						System.out.println("Did not get the outlink "+e.toString());
					}
				}
				outlinkmapping.put(id,urlset);
				//System.out.println("done");
			} catch (Exception e) {
				System.out.println("Did not get the outlink "+e.toString());
			}
		}
	}

	private static void convertToInlinkMap(HashMap<String, Set<String>> outlinkmapping,HashMap<String,JSONArray> inlinkmapping) throws Exception, FileNotFoundException {
		Iterator<String> keys = outlinkmapping.keySet().iterator();
		System.out.println("outlink map count "+outlinkmapping.size());
		while(keys.hasNext())
		{
			String key = (String) keys.next();
			Set<String> s = outlinkmapping.get(key);
			ArrayList<String> a = new ArrayList<String>(s);
			//System.out.println("Started for "+count++ + + outlinkmapping.size() +a.size());

			for(int i=0;i<a.size();i++)
			{
				String inlink = a.get(i);
				if(outlinkmapping.containsKey(inlink))
				{
					Object inlinks = inlinkmapping.containsKey(inlink) ?  inlinkmapping.get(inlink) : new JSONArray();
					((JSONArray)inlinks).add(key);
					inlinkmapping.put(inlink, (JSONArray) inlinks);
				}
			}
			//System.out.println("Endded for "+count++  + "  " + outlinkmapping.size() +"  "+a.size());
		}
		System.out.println("inlink map count "+inlinkmapping.size());

		System.out.println("Done");
	}

	private static Set<String> fetchOutlinkMappings(JSONArray arr) 
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
		return urlset;
	}


	private static HashMap<String, Boolean> fetchTopDocumentsFromFile(String location, String fileext, String query) throws FileNotFoundException, IOException {

		HashMap<String,Boolean> validurls = new HashMap<String,Boolean>();
		int count  = 0;
		File [] files = new File(location).listFiles();
		for(int i=0;i<files.length ;i++)
		{
			if(!files[i].isFile() || !files[i].getName().endsWith(".json")) continue;
			System.out.println(files[i]);
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
				JSONArray arr = (JSONArray) ((JSONObject)json.get("hits")).get("hits");
				for(int j=0;j<arr.size();j++)
				{
					if(count ==1000) break;
					JSONObject o = (JSONObject) arr.get(j);
					validurls.put((String) o.get("_id"), true);
					count++;
				}
			} catch (Exception e) {
				System.out.println("json object could not be created "+e.toString());
			}
			if(count ==1000) break;
		}
		return validurls;
	}

	/*private static HashMap<String, Boolean> fetchTopDocuments(String index, String type, String query, HashMap<String, Set<String>> outlinkmapping) {

		String url = "http://localhost:9200/"+index+"/"+type+"/_search?q=text:maritime%20accidents";

		HttpURLConnection http = null;
		try {
			http = (HttpURLConnection) new URL(url).openConnection();
			http.setAllowUserInteraction(true);
			http.setDoInput(true);
			http.setRequestMethod("GET");
			http.connect();
		} catch (IOException e1) {
			System.out.println("URL IO ERROR");
		}

		BufferedReader in = null;
		String res="";

		try {
			in = new BufferedReader(new InputStreamReader(http.getInputStream()));
			String inputLine;
			while ((inputLine = in.readLine()) != null) 
				res+=inputLine;
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			http.getResponseCode();
		} catch (IOException e) {
			System.out.println("Response code "+e.toString());
		}

		Object obj = JSONValue.parse(res);
		JSONArray arr = (JSONArray) ((JSONObject) ((JSONObject) obj).get("hits")).get("hits");
		HashMap<String,Boolean> validurls = new HashMap<String,Boolean>();
		http.disconnect();

		for(int i=0;i<arr.size() && i<1000;i++)
		{
			String id = (String) ((JSONObject)arr.get(i)).get("_id");
			validurls.put(id,true);
			//System.out.println("es valid id "+id);
			JSONArray outlinks = (JSONArray) ((JSONObject) ((JSONObject)arr.get(i)).get("_source")).get("out_links");
			for(int j=0;j<outlinks.size();j++)
			{
				if(((String) outlinks.get(j)).equals("")) continue;
				validurls.put((String) outlinks.get(j),false);
				//System.out.println("es valid "+(String) outlinks.get(j));
			}
			System.out.println(i+" "+arr.size());
		}
		/*SearchResponse resp = client.prepareSearch("ap_dataset").setTypes(type)
				.setQuery(QueryBuilders.matchQuery("text", query)).setSize(1000).execute().actionGet();

		SearchHit[] h = resp.getHits().getHits();
		HashMap<String,Boolean> validurls = new HashMap<String,Boolean>();

		for(int i=0;i<h.length;i++)
		{
			String id = h[i].getId();
			validurls.put(id, true);
			System.out.println("es valid "+id);
			JSONArray arr = (JSONArray)h[i].getSource().get("out_links");
			Set<String> a = new HashSet<String>();
			for(int j=0;j<arr.size();j++)
				a.add((String) arr.get(j));
			for(String s : a)
			{
				System.out.println("es valid "+s);
				validurls.put(s, false);
			}
		}
		client.close();
		node.close();

		return validurls;
	}*/
}