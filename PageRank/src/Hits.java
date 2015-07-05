import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


public class Hits {
	static String rootpath="/Users/prachibhansali/Documents/IR/Assignment1/PseudoOutputs/";

	public static void main(String[] args) throws Exception {
		String indexname = "ap_dataset",type="document";
		float bcons=0.75f, k1=1.2f, k2=100;

		Node node = nodeBuilder().client(true).node();
		Client client = node.client();

		HashMap<String,HashMap<String,Float>> index =  new HashMap<String,HashMap<String,Float>>();

		String query = "";
		BufferedReader br = new BufferedReader(new FileReader("query.txt"));

		System.out.println("****Fetching stop words*****");
		File stopwords = new File("/Users/prachibhansali/Documents/workspace/ElasticSearch/AP_DATA/stoplist.txt");
		BufferedReader b = new BufferedReader(new FileReader(stopwords));
		HashMap<String,Integer> h =new HashMap<String,Integer>();
		String stopword;
		while((stopword=b.readLine())!=null)
			h.put(stopword.toLowerCase().trim(), 0);

		HashMap<Integer,ArrayList<String>> queryKeywords = new  HashMap<Integer,ArrayList<String>>();
		ArrayList<Integer> queryId = new ArrayList<Integer>();
		HashMap<String,Long> docFreq = new HashMap<String,Long>();

		// Get all df for every erm
		SearchResponse response2 = client.prepareSearch(indexname).setTypes("document").addField("_none")
				.setQuery(QueryBuilders.matchAllQuery())
				.addAggregation(AggregationBuilders.terms("fetch_all_terms").field("text").size(180000))
				.execute().actionGet();
		Terms alltermcounts = response2.getAggregations().get("fetch_all_terms");
		List<Terms.Bucket> bkts= alltermcounts.getBuckets();
		Iterator itr = bkts.iterator();
		while(itr.hasNext()){
			Terms.Bucket bucket = (Bucket) itr.next();
			docFreq.put(bucket.getKey(),bucket.getDocCount());
		}

		// Parse through all the queries
		while((query = br.readLine())!=null && !query.trim().equals(""))
		{
			List<String> terms = new LinkedList<String>();
			query = removeSpecialChars(query);
			String [] allterms = query.split("-|\\s+");

			ArrayList<String> alterms = new ArrayList<String>();
			for(int k=0;k<allterms.length;k++)
				alterms.add(allterms[k]);	

			int Qnum = Integer.parseInt(allterms[0]);
			queryId.add(Qnum);
			System.out.println("Running for query " + Qnum);

			for(int i=4; i<alterms.size(); i++) {
				System.out.println("terms == " + alterms.get(i));
				String term = allterms[i].toLowerCase().trim();
				terms.add(term);					
				if(!index.containsKey(term))
				{
					System.out.println("***************** Fetching TF for " + term +"******************");
					index.put(term,retrievePostings(indexname,"document",client,term,docFreq));
				}
			}
			queryKeywords.put(Qnum, new ArrayList<String> (terms));
		}

		// Fetching total term freqs for every query term
		System.out.println("Start calculating ttfs");
		HashMap<String,Float> TTF = new HashMap<String,Float>();
		Iterator terms_itr = index.keySet().iterator();
		PrintWriter pw = new PrintWriter("terms.txt");
		while(terms_itr.hasNext())
		{
			String term = (String) terms_itr.next();
			Iterator doc_itr = index.get(term).keySet().iterator();
			float count = 0;
			while(doc_itr.hasNext()){
				String docname = (String) doc_itr.next();
				count+=index.get(term).get(docname);
			}
			pw.println(term+" "+count);
			TTF.put(term, count);
		}
		pw.close();
		System.out.println("Done calculating ttfs");

		// Fetching Document lengths individually
		double totaldoclength=0f;
		System.out.println("Start calculating doclengths");
		HashMap<String,Float> doclengths = new HashMap<String,Float>();

		BufferedReader dlength = new BufferedReader(new InputStreamReader(new FileInputStream("doclengths.txt")));
		String l;
		while((l=dlength.readLine())!=null)
		{
			String docname = l.split(" ")[0].trim();
			float length = Float.parseFloat(l.split(" ")[1].trim());
			doclengths.put(docname, length);
			totaldoclength+=length;
		}
		System.out.println("Done calculating doclenghths");
		
		HashMap<String,Float> authority = new HashMap<String,Float>();
		HashMap<String,Float> hub = new HashMap<String,Float>();
		
		int totaldocs = doclengths.keySet().size();
		double avg = totaldoclength/(double)totaldocs;
		System.out.println("Average = "+avg);
		ArrayList<String> okapibm_ranks = null;
		for(int j=0; j<queryId.size();j++)
		{
			okapibm_ranks = computeForokapiBM25(queryId.get(j),bcons,k1,k2,queryKeywords.get(queryId.get(j)),index,avg,doclengths,docFreq,totaldocs);
		}
		
		HashMap<String,ArrayList<String>> outlinkmapping = new HashMap<String,ArrayList<String>>();
		HashMap<String,JSONArray> inlinkmapping = new HashMap<String,JSONArray> ();
		String location="/Users/prachibhansali/Documents/IR/Assignment3/indexes-bfs/";
		
		fetchOutlinksFromFiles(location,outlinkmapping,okapibm_ranks);
		location="/Users/prachibhansali/Documents/IR/Assignment4/inlinks.txt";
		convertToInlinkMap(outlinkmapping,inlinkmapping,location);
		
		
		
	} 
	
	private static void convertToInlinkMap(HashMap<String, ArrayList<String>> outlinkmapping,HashMap<String,JSONArray> inlinkmapping, String location) throws JSONException, FileNotFoundException {
		Iterator<String> keys = outlinkmapping.keySet().iterator();

		while(keys.hasNext())
		{
			String key = (String) keys.next();
			ArrayList<String> a = outlinkmapping.get(key);

			for(int i=0;i<a.size();i++){
				String inlink = a.get(i);
				if(outlinkmapping.containsKey(inlink)){
					JSONArray inlinks = inlinkmapping.containsKey(inlink) ? inlinkmapping.get(inlink) : new JSONArray();
					inlinks.put(key);
					inlinkmapping.put(inlink, inlinks);
				}
			}
		}

		Iterator<String> ks = inlinkmapping.keySet().iterator();
		PrintWriter out = new PrintWriter(location);
		while(ks.hasNext())
		{
			String key = (String) ks.next();
			out.print(key+"\t");
			JSONArray a = inlinkmapping.get(key);
			for(int i=0;i<a.length();i++)
				out.print(a.get(i)+"\t");
			out.println();
		}
		out.close();
	}

	
	private static void fetchOutlinksFromFiles(String location,
			HashMap<String, ArrayList<String>> outlinkmapping, ArrayList<String> okapibm_ranks) throws IOException {

		File [] files = new File(location).listFiles();
		for(int i=0;i<files.length ;i++)
		{
			BufferedReader br = null;
			if(files[i].isFile() && files[i].getName().endsWith(".txt")){
				try {
					br = new BufferedReader(new FileReader(files[i]));
				} catch (FileNotFoundException e1) {
					System.out.println("File not found");
				}
				String jsonstr = "";
				while((jsonstr=br.readLine())!=null) {
					JSONObject json=null;
					try {
						json = new JSONObject(jsonstr);
						if(!json.has("docno")) continue;
						outlinkmapping.put(json.getString("docno"),fetchOutlinkMappings(json.getJSONArray("out_links")));
						System.out.println(json.getString("docno")+" "+outlinkmapping.size());
					} catch (Exception e) {
						System.out.println("json object could not be created "+e.toString());
					}
				}
			}
		}
	}

	private static ArrayList<String> fetchOutlinkMappings(JSONArray arr) {
		ArrayList<String> urls = new ArrayList<String>();
		for(int i=1;i<arr.length();i++)
		{
			try {
				urls.add(arr.getString(i));
				//System.out.println("done");
			} catch (JSONException e) {
				System.out.println("Did not get the outlink");
			}
		}
		return urls;
	}
	
	private static String removeSpecialChars(String query) {
		query=query.substring(0,query.trim().length()-1);
		query=query.replaceAll(",", "")
				.replaceAll("\\(", "")
				.replaceAll("\\)", "")
				.replaceAll("\"", "")
				.replaceAll("\\.", "");
		return query;
	}

	private static HashMap<String, Float> retrievePostings(String index,String type,Client client,String term, HashMap<String, Long> docFreq) throws IOException 
	{
		HashMap<String,Float> termFreq = new HashMap<String,Float>();
		SearchResponse resp = client.prepareSearch(index).setTypes(type).addField("_none")
				.setQuery(new FunctionScoreQueryBuilder(FilterBuilders.queryFilter(QueryBuilders.matchQuery("text", term))).add(
						ScoreFunctionBuilders.scriptFunction("_index[field][term].tf()", "groovy")
						.param("field", "text").param("term", term)).boostMode("replace"))
						.setSize(85000).execute().actionGet();

		System.out.println("Postings for " + term + "="+ resp.getHits().getTotalHits());
		SearchHit[] hits = resp.getHits().getHits();
		if(!docFreq.containsKey(term)) 
			docFreq.put(term,(long) hits.length);
		//PrintWriter prnt = new PrintWriter(new BufferedWriter(new FileWriter("terms",true)));
		//prnt.println("******** for term : " + term);
		for(SearchHit hit : hits)
		{
			String docname = hit.getId();
			float tf = hit.score();
			//prnt.println(docname + " " + tf);
			termFreq.put(docname, tf);
		}
		//prnt.close();
		return termFreq;
	}
	
	private static ArrayList<String> computeForokapiBM25(Integer qnum, float bcons,
			float k1, float k2, ArrayList<String> terms,
			HashMap<String, HashMap<String, Float>> index, double avg,
			HashMap<String, Float> doclengths, HashMap<String, Long> docFreq,long totaldocs) throws IOException {

		HashMap<String,Double> okapiBM25 = new HashMap<String,Double>();
		for(String term : getDistinct(terms))
		{
			Iterator docnames= index.get(term).keySet().iterator();
			int qtf = getquerytermfreq(term,terms);
			System.out.println("okapi " + term);
			long df = docFreq.get(term);
			while(docnames.hasNext())
			{
				String dname = (String) docnames.next();	
				double initval=okapiBM25.containsKey(dname) ? okapiBM25.get(dname) : 0;
				okapiBM25.put(dname,initval+computeForokapiBM25ForDoc(qtf,bcons,k1,k2,totaldocs,doclengths.get(dname)
						,df,avg,dname,index.get(term).get(dname)));			
			}
		}
		return rankDocuments(qnum,okapiBM25,"okapiBM25.txt",1000);
	}

	private static int getquerytermfreq(String term, ArrayList<String> terms) {
		int count =0;
		for(int i=0;i<terms.size();i++)
			if(terms.get(i).equals(term)) count++;
		return count;
	}

	private static Double computeForokapiBM25ForDoc(int tfwq,float bcons, float k1,
			float k2, long totaldocs, float doclength, Long dfw, double avg,
			String dname, float tfw) {
		double idf = Math.log((totaldocs+0.5)/(dfw+0.5));
		double constant = ((1-bcons) + bcons*(doclength/avg));
		return (idf * computeconstant(tfw,k1,constant) * computeconstant(tfwq,k2,1));
	}

	private static double computeconstant(float tfw, float k, double constant) {
		return ((tfw + k * tfw)/(tfw + k * constant));				
	}

	private static ArrayList<String> rankDocuments(int qnum,HashMap<String,Double> result,String filename,int num) throws IOException {

		ArrayList<Double> rankedscores = new ArrayList<Double>(result.values());
		ArrayList<String> rankedDocs = new ArrayList<String>(result.keySet());
		PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(rootpath+filename,true)));
		PrintWriter outless = new PrintWriter(new BufferedWriter(new FileWriter(rootpath+filename+"Less",true)));
		ArrayList<String> rankedDocuments = new ArrayList<String>(50);
		for(int i=1;i<=100 && !rankedDocs.isEmpty();i++)
		{
			int position = max(rankedscores);
			if(rankedscores.get(position) == 0) break;
			if(rankedDocuments.size() != num ) {
				rankedDocuments.add(rankedDocs.get(position));
			}
			out.println(qnum + " Q0 " + rankedDocs.get(position)+" "+ i + " " + rankedscores.get(position) + " Exp");
			if(i<=100) outless.println(qnum + " Q0 " + rankedDocs.get(position)+" "+ i + " " + rankedscores.get(position) + " Exp");
			rankedscores.remove(position);
			rankedDocs.remove(position);
		}
		out.close();
		outless.close();
		return rankedDocuments;
	}

	private static int max(ArrayList<Double> scores) {
		double max = -Double.MAX_VALUE;
		int position=0;
		for(int i=0; i<scores.size(); i++)
		{
			if(scores.get(i)>=max) 
			{
				position =i;
				max=scores.get(i);
			}
		}
		return position;
	}

	private static ArrayList<String> getDistinct(ArrayList<String> terms) {
		Set<String> s =new HashSet<String>(terms);
		return new ArrayList<String>(s);
	}
}
