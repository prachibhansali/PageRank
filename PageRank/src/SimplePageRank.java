import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;

import org.json.simple.JSONArray;


public class SimplePageRank {
	
	public static void computePageRank(String location) throws Exception
	{
		//String location = "/Users/prachibhansali/Documents/IR/Assignment4/wt2g_inlinks.txt";

		HashMap<Integer,ArrayList<Integer>> inlinks = new HashMap<Integer,ArrayList<Integer>>();
		HashMap<String,Integer> docnames =  new HashMap<String,Integer>();
		HashMap<Integer,String> docids =  new HashMap<Integer,String>();
		HashMap<Integer,Integer> olinkcount =  new HashMap<Integer,Integer>();

		generateDocNamesFromFile(location,docnames,docids);
		readInlinkMappingFromFile(location,inlinks,docnames,docids);

		compute(inlinks,olinkcount,docnames,docids);
	}

	public static void computePageRank(HashMap<String, JSONArray> inlinkmapping) throws Exception
	{
		HashMap<Integer,ArrayList<Integer>> inlinks = new HashMap<Integer,ArrayList<Integer>>();
		HashMap<String,Integer> docnames =  new HashMap<String,Integer>();
		HashMap<Integer,String> docids =  new HashMap<Integer,String>();
		HashMap<Integer,Integer> olinkcount =  new HashMap<Integer,Integer>();

		generateDocNamesFromFile(inlinkmapping,docnames,docids);
		readInlinkMappingFromFile(inlinkmapping,inlinks,docnames,docids);
				
		compute(inlinks,olinkcount,docnames,docids);
	}

	private static void compute(HashMap<Integer, ArrayList<Integer>> inlinks, HashMap<Integer, Integer> olinkcount, HashMap<String, Integer> docnames, HashMap<Integer, String> docids) {
		generateOutputCountMapping(inlinks,olinkcount);

		HashMap<Integer,Float> PR = null;
		PR = iterateToConverge(inlinks,docnames,olinkcount,docids);
		int num = 500;
		getTopDocuments(num,PR,docids);
	}

	private static void generateDocNamesFromFile(String location,
			HashMap<String, Integer> docnames, HashMap<Integer, String> docids) throws Exception {
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
				docids.put(size, url);
			}
		}
		br.close();
	}



	private static void readInlinkMappingFromFile(String location,
			HashMap<Integer, ArrayList<Integer>> inlinks, HashMap<String, Integer> docnames, HashMap<Integer, String> docids) throws Exception {
		BufferedReader br = new BufferedReader(new FileReader(new File(location)));
		String line = null;
		while((line=br.readLine())!=null)
		{
			String [] str = line.split("\\s+");
			int url = docnames.get(str[0]);

			Set<Integer> a = new HashSet<Integer>();
			for(int i=1;i<str.length;i++)
			{
				String u = str[i];
				if(!docnames.containsKey(u))
				{
					int size = docnames.size();
					docnames.put(u,size);
					docids.put(size,u);
				}

				int key = docnames.get(u);
				a.add(key);
			}
			inlinks.put(url, new ArrayList<Integer>(a));
		}
		br.close();
	}

	private static void getTopDocuments(int num, HashMap<Integer, Float> pR, HashMap<Integer, String> docids) {
		Iterator<Integer> itr = pR.keySet().iterator();
		ArrayList<String> urls = new ArrayList<String>();
		ArrayList<Float> scores = new ArrayList<Float>();

		while(itr.hasNext()){
			int key = (int) itr.next();
			if(scores.size() < num)
			{
				scores.add(pR.get(key));
				urls.add(docids.get(key));
			}
			else 
			{
				int index = scores.indexOf(Collections.min(scores));
				if(scores.get(index) < pR.get(key))
				{
					scores.set(index, pR.get(key));
					urls.set(index, docids.get(key));
				}
			}
		}

		while(scores.size()>0)
		{
			int index = scores.indexOf(Collections.max(scores));
			System.out.println(urls.get(index)+" "+scores.get(index));
			urls.remove(index);
			scores.remove(index);
		}

	}

	private static void generateOutputCountMapping(
			HashMap<Integer, ArrayList<Integer>> inlinks,
			HashMap<Integer, Integer> olinkcount) 
	{
		Iterator<Integer> itr = inlinks.keySet().iterator();
		while(itr.hasNext())
		{
			int key = (int) itr.next();
			Set<Integer> links = new HashSet<Integer>(inlinks.get(key));
			Iterator<Integer> linkitr = links.iterator();
			while(linkitr.hasNext())
			{
				int link = (int)linkitr.next();
				int count = olinkcount.containsKey(link) ? olinkcount.get(link) : 0;
				olinkcount.put(link, ++count);
			}
		}
	}


	private static HashMap<Integer,Float> iterateToConverge(
			HashMap<Integer, ArrayList<Integer>> inlinks,
			HashMap<String, Integer> docnames, HashMap<Integer, Integer> olinkcount, HashMap<Integer, String> docids) {
		HashMap<Integer,Float> prevPR = null;
		HashMap<Integer,Float> PR = new HashMap<Integer,Float>();

		Iterator<String> keys = docnames.keySet().iterator();
		while(keys.hasNext())
		{
			String key = (String)keys.next();
			PR.put(docnames.get(key), 1.0f);
		}

		float randomcons = (float) (0.15/(float)docnames.size());
		int count=0;
		while(!isSimilar(prevPR,PR,docids))
		{
			System.out.println(++count);
			prevPR = new HashMap<Integer,Float>(PR);
			//if(prevPR!=null) print("prev : \n",prevPR);
			PR = new HashMap<Integer,Float>(converge(randomcons,PR,inlinks,olinkcount));
		}

		print("current : \n",PR);
		return PR;
	}


	private static void print(String string, HashMap<Integer, Float> pR) {
		Iterator<Integer> keys = pR.keySet().iterator();
		System.out.print(string);
		while(keys.hasNext()){
			int key = (int) keys.next();
			System.out.print(pR.get(key)+"\t");
		}
		System.out.println();
	}

	private static boolean isSimilar(HashMap<Integer, Float> prevPR,
			HashMap<Integer, Float> pR, HashMap<Integer, String> docids) {
		if(prevPR==null) return false;
		double delta = 0.001;
		Iterator<Integer> keys = pR.keySet().iterator();
		while(keys.hasNext()){
			int key = (int) keys.next();
			Float currval = pR.get(key);
			Float prevval = prevPR.get(key);
			if(Math.abs(prevval-currval) > delta)
			{
				System.out.println(docids.get(key)+" "+Math.abs(currval-prevval));
				return false;
			}

		}
		return true;
	}

	private static HashMap<Integer, Float> converge(float randomcons, HashMap<Integer, Float> pR,
			HashMap<Integer, ArrayList<Integer>> inlinks, HashMap<Integer, Integer> olinkcount) {
		System.out.println(inlinks.size());
		Iterator<Integer> keys = inlinks.keySet().iterator();
		HashMap<Integer, Float> newPR = new HashMap<Integer, Float>();
		while(keys.hasNext()){
			int key = (int) keys.next();
			newPR.put(key, (float) (randomcons+0.85*computeInlinksPR(pR,inlinks.get(key),olinkcount)));
		}
		return newPR;
	}

	private static float computeInlinksPR(HashMap<Integer, Float> pR,
			ArrayList<Integer> inlinks, HashMap<Integer, Integer> olinkcount) {
		float score =0;
		for(int i=0;i<inlinks.size();i++)
		{
			int doc = inlinks.get(i);
			score+=(pR.get(doc))/olinkcount.get(doc);
		}
		return score;
	}

	private static void generateDocNamesFromFile(HashMap<String, JSONArray> inlinkmapping,
			HashMap<String, Integer> docnames, HashMap<Integer, String> docids) throws Exception {

		Set<String> urls = inlinkmapping.keySet();
		Iterator<String> itr = urls.iterator();
		while(itr.hasNext())
		{
			String url = (String) itr.next();
			int size = docnames.size();
			//System.out.println("Added "+url +" "+size);
			docnames.put(url, size);
			docids.put(size, url);
		}
		System.out.println("Documents added");
		
	}



	private static void readInlinkMappingFromFile(HashMap<String, JSONArray> inlinkmapping,
			HashMap<Integer, ArrayList<Integer>> inlinks, HashMap<String, Integer> docnames,HashMap<Integer, String> docids) throws Exception {

		Set<String> links = inlinkmapping.keySet();
		Iterator<String> itr = links.iterator();
		while(itr.hasNext())
		{
			String u = itr.next();
			int url = docnames.get(u);

			JSONArray a = (JSONArray)inlinkmapping.get(u);
			Set<Integer> urlints = new HashSet<Integer>();
			for(int i=0;i<a.size();i++)
			{
				if(!docnames.containsKey(u))
				{
					int size = docnames.size();
					docnames.put(u,size);
					docids.put(size,u);
				}

				int key = docnames.get(u);
				urlints.add(key);
			}
			inlinks.put(url, new ArrayList<Integer>(urlints));
		}
	}
}
