package moa.clusterers.dstream;

/**
 * Density Grids are defined in equation 3 (section 3.1) of Chen and Tu 2007 as:
 * In D-Stream, we partition the d−dimensional space S into density grids. Suppose 
 * for each dimension, its space Si, i =1, ··· ,d is divided into pi partitions as
 * Si = Si,1 U Si,2 U ··· U Si,pi (2)
 * then the data space S is partitioned into N = PRODUCT (i=1..d) pi density grids. 
 * For a density grid g that is composed of S1,j1 ×S2,j2 ···×Sd,jd , ji =1, ...,pi,
 * we denote it as 
 * g =(j1,j2, ··· ,jd). (3)
 * 
 * Citation: Y. Chen and L. Tu, “Density-Based Clustering for Real-Time Stream Data,” in
 * Proceedings of the 13th ACM SIGKDD international conference on Knowledge discovery and
 * data mining, 2007, pp. 133–142.
 */
public class DensityGrid extends Object
{
	int[] coordinates;
	int dimensions;
	
	/**
	 * A constructor method for a density grid
	 * 
	 * @param c the coordinates of the density grid
	 */
	public DensityGrid(int[]c)
	{
		dimensions = c.length;
		coordinates = new int[dimensions];
		
		for (int i = 0 ; i < dimensions ; i++)
		{
			coordinates[i] = c[i];
		}
	}

	/**
	 * A constructor method for a density grid
	 * 
	 * @param dg the density grid to copy
	 */
	public DensityGrid(DensityGrid dg)
	{
		int[] dgCoord = dg.getCoordinates();
		dimensions = dg.getDimensions();
		coordinates = new int[dimensions];
		
		for (int i = 0 ; i < dimensions ; i++)
		{
			coordinates[i] = dgCoord[i];
		}
	}
	
	/**
	 * Overrides Object's method equals to declare that two DensityGrids are equal iff their
	 * dimensions are the same and each of their corresponding coordinates are the same.
	 * 
	 * @param o the object being compared for equality
	 * @return  TRUE if the objects being compared are equal; FALSE otherwise
	 */
	@Override
	public boolean equals(Object o)
	{
		if(o == null)
			return false;
		if(o.getClass() != DensityGrid.class)
			return false;
		
		DensityGrid dg = (DensityGrid) o;
		
		if(dg.getDimensions() != dimensions)
			return false;
		
		int[] dgCoord = dg.getCoordinates();
		for(int i = 0 ; i < dimensions ; i++)
		{
			if(dgCoord[i] != coordinates[i])
				return false;
		}
		
		return true;		
	}
	
	/**
	 * Overrides Object's method hashCode to generate a hashCode for DensityGrids based on
	 * their coordinates.
	 *
	 * @return hc the Object's hash code
	 */
	@Override
	public int hashCode()
	{
		int[] primes = {31, 37, 41, 43, 47, 53, 59};
		int hc = 1;
		
		for (int i = 0 ; i < dimensions ; i++)
		{
			hc = (hc * primes[i%7]) + coordinates[i];
		}
		
		return hc;
	}
	
	/**
	 * Varies one of the density grid's coordinates (c) by a given value (v). Usually v is 1 or -1
	 * and the method is used to iterate through the density grid's neighbours.
	 * 
	 * @param c the index of the coordinate to be varied
	 * @param v the amount of offset to be applied
	 */
	public void vary(int c, int v)
	{
		if (c >= 0 && c < dimensions)
			coordinates[c] += v;
	}
	
	/**
	 * @return coordinates the coordinates of the density grid
	 */
	public int[] getCoordinates() {
		return coordinates;
	}

	/**
	 * @return dimensions the number of dimensions for the density grid
	 */
	public int getDimensions() {
		return dimensions;
	}

}
