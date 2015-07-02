
public class MatrixCustom {
	private double [][] matrix;
	private int row;
	private int column;

	public MatrixCustom(double[][] transitionMatrix,int row,int column){
		this.matrix = transitionMatrix;
		this.row = row;
		this.column =column;
	}

	public MatrixCustom(int num,int row,int column)
	{
		this.matrix = new double[row][column];
		for(int i=0;i<row;i++)
			for(int j=0;j<column;j++)
				matrix[i][j] = num;
		this.row = row;
		this.column =column;
	}

	public MatrixCustom(int row,int column)
	{
		matrix = new double[row][column];
		this.row = row;
		this.column = column;
	}

	public MatrixCustom(MatrixCustom a)
	{
		matrix = a.matrix;
		row = a.row;
		column = a.column;
	}

	public MatrixCustom multiply(MatrixCustom b){
		if(column!=b.row) return null;
		MatrixCustom m = new MatrixCustom(row,b.column);

		for(int i=0;i<row;i++)
			for(int j=0;j<b.column;j++)
				for(int k=0;k<column;k++)
					m.matrix[i][j] += matrix[i][k] * b.matrix[k][j];
		return m;
	}

	public MatrixCustom multiply(double num,MatrixCustom b){
		if(column!=b.row) return null;
		MatrixCustom m = new MatrixCustom(row,b.column);

		for(int i=0;i<row;i++)
			for(int j=0;j<b.column;j++)
				for(int k=0;k<column;k++)
					m.matrix[i][j] += matrix[i][k] * b.matrix[k][j];
		return m;
	}

	public double[][] getMatrix() {
		return matrix;
	}

	public int getRow() {
		return row;
	}

	public int getColumn() {
		return column;
	}

	public MatrixCustom square() {
		return multiply(this);
	}

	public boolean isSimilar(MatrixCustom b) {
		if(equals(b)) return false;
		for(int i=0;i<row;i++)
			for(int j=0;j<column;j++)
				if(Math.abs(matrix[i][j]-b.matrix[i][j]) > 0.0001)
					return false;
		return true;
	}

	public boolean equals(MatrixCustom b){
		for(int i=0;i<row;i++)
			for(int j=0;j<column;j++)
				if(matrix[i][j]!=b.matrix[i][j])
					return false;
		return true;
	}

	public String toString(){
		String res ="";
		for(int i=0;i<row;i++)
		{
			for(int j=0;j<column;j++)
				res+=matrix[i][j]+"\t";
			res+="\n";
		}
		return res;
	}

	public MatrixCustom multiply(double d) {
		for(int i=0;i<row;i++)
			for(int j=0;j<column;j++)
				matrix[i][j] = matrix[i][j] * d;
		//System.out.println("multiply: " + toString());
		return this;
	}
	
	public MatrixCustom add(double d) {
		for(int i=0;i<row;i++)
			for(int j=0;j<column;j++)
				matrix[i][j] = matrix[i][j] + d;
		//System.out.println("add: " + toString());
		return this;
	}
}
