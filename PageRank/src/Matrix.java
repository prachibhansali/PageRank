import java.util.ArrayList;


public class Matrix {
	private ArrayList<ArrayList<Integer>> keys;
	private ArrayList<ArrayList<Float>> values;
	private int row;
	private int column;

	public Matrix(ArrayList<ArrayList<Integer>> keys,ArrayList<ArrayList<Float>> values,int row,int column){
		this.keys = keys;
		this.values = values;
		this.row = row;
		this.column =column;
	}
	
	public Matrix() {
		
	}
	
	public Matrix(float[][] valuesarr,int row,int column){
		
		keys = new ArrayList<ArrayList<Integer>>();
		values = new ArrayList<ArrayList<Float>>();
		
		for(int i=0;i<row;i++)
		{
			ArrayList<Integer> rowkeys = new ArrayList<Integer>();
			ArrayList<Float> rowvalues = new ArrayList<Float>();
			for(int j=0;j<column;j++)
			{
				float value = valuesarr[i][j];
				if(value==0) continue;
				rowkeys.add(j);
				rowvalues.add(valuesarr[i][j]);
			}
			keys.add(rowkeys);
			values.add(rowvalues);
		}
		this.row=row;
		this.column=column;
	}

	public Matrix(int num,int row,int column)
	{
		this.keys = new ArrayList<ArrayList<Integer>>();
		for(int i=0;i<row;i++)
		{
			System.out.println(row);
			ArrayList<Integer> rowlist = new ArrayList<Integer>();
			for(int j=0;j<column;j++)
				rowlist.add(j);
			keys.add(rowlist);
		}
		this.values = new ArrayList<ArrayList<Float>>();
		for(int i=0;i<row;i++)
		{
			ArrayList<Float> rowlist = new ArrayList<Float>();
			for(int j=0;j<column;j++)
				rowlist.add((float)num);
			values.add(rowlist);
		}
		this.row = row;
		this.column =column;
	}

	public Matrix(int row,int column)
	{
		keys = new ArrayList<ArrayList<Integer>>();
		values = new ArrayList<ArrayList<Float>>();
		this.row = row;
		this.column = column;
	}

	public Matrix(Matrix a)
	{
		keys = new ArrayList<ArrayList<Integer>>(a.keys);
		values = new ArrayList<ArrayList<Float>>(a.values);
		row = a.row;
		column = a.column;
	}
	
	public Matrix multiply(Matrix b)
	{
		Matrix c = new Matrix(this);
		ArrayList<ArrayList<Float>> cvalues = new ArrayList<ArrayList<Float>>();
		for(int i=0;i<row;i++)
		{
			System.out.println("row "+i);
			ArrayList<Float> rowvalues = values.get(i);
			ArrayList<Float> newvalues = new ArrayList<Float>();

			for(int j=0;j<b.column;j++)
			{
				System.out.println("column "+j);
				
				float columnscore = 0.0f;
				for(int k=0;k<column;k++)
				{
					ArrayList<Integer> columnposn = b.getKeyMatrix().get(k);
					if(!columnposn.contains(j));
					else {
						float value= b.getValueMatrix().get(k).get(columnposn.indexOf(j));
						columnscore+=rowvalues.get(k) * value;
					}
				}
				newvalues.add(columnscore);
			}
			cvalues.add(newvalues);
		}
		c.values = new ArrayList<ArrayList<Float>>(cvalues);
		return c;
	}
	
	public ArrayList<ArrayList<Integer>> getKeyMatrix() {
		return keys;
	}
	
	public ArrayList<ArrayList<Float>> getValueMatrix() {
		return values;
	}

	public int getRow() {
		return row;
	}

	public int getColumn() {
		return column;
	}

	public String toString(){
		String res ="";
		for(int i=0;i<keys.size();i++)
		{
			for(int j=0;j<keys.get(i).size();j++)
				res+=keys.get(i).get(j)+"\t";
			res+="\n";
		}
		
		res+="\n***************\n";
		
		for(int i=0;i<values.size();i++)
		{
			for(int j=0;j<values.get(i).size();j++)
				res+=values.get(i).get(j)+"\t";
			res+="\n";
		}
		
		return res;
	}
	
	
}
