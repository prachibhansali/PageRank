import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;


public class TestMatrix {

	public static void main(String[] args) throws Exception {
		//String location = "matrix";
		String location = "/Users/prachibhansali/Downloads/wt2g_inlinks.txt";
		
		HashMap<String,Integer> docnames =  new HashMap<String,Integer>();
		//readOutlinksFromFile(location,docnames);
		//float[][] transitionMatrix = createTransitionMatrix(location,docnames);
		readInlinksFromFile(location,docnames);
		//double[][] transitionMatrix = createTransitionMatrix(location,docnames,docids);
		float[][] transitionMatrix = createColumnTransitionMatrix(location,docnames);
		
		
	}

	private static float[][] createColumnTransitionMatrix(String location,
			HashMap<String, Integer> docnames) throws Exception{
		int mapsize = docnames.size();
		//float[][] transition = new float[mapsize][mapsize];
		ArrayList<ArrayList<Integer>> keys;
		ArrayList<ArrayList<Float>> values;
		
		HashMap<Integer,ArrayList<Integer>> map = new HashMap<Integer,ArrayList<Integer>>();
		
		BufferedReader br = new BufferedReader(new FileReader(new File(location)));
		String line = null;
		while((line=br.readLine())!=null)
		{
			String [] str = line.split("\\s");
			String url = str[0];
			int column = docnames.get(url);
			System.out.println("url : "+url);
			for(int i=1;i<str.length;i++)
			{
				int index =docnames.get(str[i]);
				if(docnames.containsKey(str[i]) && !str[i].equals(url))
				{
					ArrayList<Integer> k = map.containsKey(index) ? map.get(index) : new ArrayList<Integer>();
					System.out.println("inlinked url : "+str[i]);
					k.add(column);
					System.out.println("added : "+column+" for "+index);
					map.put(index, k);
				}  
				
			}
		}
		System.out.println("size "+map.size());
		for(int i=0;i<docnames.size();i++)
		{
			if(!map.containsKey(i))
				map.put(i, new ArrayList<Integer>());
		}
		keys = new ArrayList<ArrayList<Integer>>();
		values = new ArrayList<ArrayList<Float>>();
		System.out.println("size "+map.size()+" "+docnames.size());
		
		//Iterator<Integer> i = map.keySet().iterator();
		
		for(int index=0;index<docnames.size();index++)
		{
			System.out.println(index);
			keys.add(map.get(index));
			//System.out.println("add key : "+index);
			ArrayList<Float> v = new ArrayList<Float>();
			for(int j=0;j<map.get(index).size();j++)
				v.add(1/(float)(map.get(index).size()));
			values.add(index,v);
		}

		Matrix m = new Matrix(keys,values,docnames.size(),docnames.size());
		PrintWriter out = new PrintWriter("temp");
		out.println("Matrix Multiplied : \n"+(new Matrix(1,1,docnames.size()).multiply(m)).toString());
		//return transition;
		out.close();
		return null;
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
		System.out.println("Total urls to consider "+docnames.size());
		br.close();
	}

	
	private static float[][] createTransitionMatrix(String location,
			HashMap<String, Integer> docnames) throws Exception {
		int mapsize = docnames.size();
		float[][] transition = new float[mapsize][mapsize];

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
			String location,HashMap<String,Integer> docnames) throws Exception {
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
			}
		}
		br.close();
	}
	
}
