import java.io.*;
import java.util.*;

import Jama.Matrix;
import Jama.EigenvalueDecomposition;

public class PageRank {

	public static void main(String[] args) throws Exception{
		String location = "/Users/prachibhansali/Downloads/wt2g_inlinks.txt";
		//String location = "matrix";
		HashMap<Integer,String> docids = new HashMap<Integer,String>();
		HashMap<String,Integer> docnames =  new HashMap<String,Integer>();
		//readOutlinksFromFile(location,docids,docnames);
		readInlinksFromFile(location,docids,docnames);
		//double[][] transitionMatrix = createTransitionMatrix(location,docnames,docids);
		double[][] transitionMatrix = createColumnTransitionMatrix(location,docnames,docids);
		float[][] urls = new float[1][docnames.size()];
		computeEndState(new MatrixCustom(1,1,docnames.size()),new MatrixCustom(transitionMatrix,docnames.size(),docnames.size()));
	}

	private static double[][] createColumnTransitionMatrix(String location,
			HashMap<String, Integer> docnames, HashMap<Integer, String> docids) throws Exception{
		int mapsize = docnames.size();
		double[][] transition = new double[mapsize][mapsize];

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
			HashMap<Integer, String> docids, HashMap<String, Integer> docnames) throws Exception {
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

	private static void computeEndState(MatrixCustom urlState, MatrixCustom transitionMatrix) throws FileNotFoundException {
		MatrixCustom prevUrlState = null;
		MatrixCustom initState = new MatrixCustom(urlState);
		System.out.println("init transition matrix : \n"+transitionMatrix);
		do
		{
			prevUrlState = new MatrixCustom(urlState);
			urlState = new MatrixCustom(urlState.multiply(transitionMatrix));
			urlState = new MatrixCustom(new MatrixCustom(urlState.multiply(0.85)).add(0.15/urlState.getColumn()));
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

}
