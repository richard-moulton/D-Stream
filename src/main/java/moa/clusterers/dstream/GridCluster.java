package moa.clusterers.dstream;

import moa.AbstractMOAObject;
import java.util.HashMap;
import java.util.Map;

/** Citation: Y. Chen and L. Tu, “Density-Based Clustering for Real-Time Stream Data,” in
 *  Proceedings of the 13th ACM SIGKDD international conference on Knowledge discovery and
 *  data mining, 2007, pp. 133–142.
 */

/*
 * Grid Clusters are defined in Definition 3.6 of Chen and Tu 2007 as:
 * Let G =(g1, ·· · ,gm) be a grid group, if every inside grid of G is 
 * a dense grid and every outside grid is either a dense grid or a 
 * transitional grid, then G is a grid cluster.
 */
public class GridCluster extends AbstractMOAObject
{
	private HashMap<int[], Boolean> grids;
	private int clusterLabel;
	
	public GridCluster(int label)
	{
		this.grids = new HashMap<int[],Boolean>();
		this.clusterLabel = label;
	}
	
	
	/*
	 * Add density grid, g, to the cluster
	 */
	public void addGrid(int[] g)
	{
		Boolean inside = isInside(g);
		this.grids.put(g, inside);
	}
	
	/*
	 * Remove density grid, g, from the cluster
	 */
	public void removeGrid(int[] g)
	{
		this.grids.remove(g);
	}
	
	/*
	 * Absorb the argument cluster into this cluster
	 */
	public void absorbCluster(GridCluster gridClus)
	{
		// Add each density grid from gridClus into this.grids
		for (Map.Entry<int[], Boolean> grid : gridClus.getGrids().entrySet())
		{
			int[]g = grid.getKey();
			
			this.grids.put(g, false);
		}
		
		// Determine which density grids in this.grids are 'inside' and which are 'outside'
		for (Map.Entry<int[], Boolean> grid : gridClus.getGrids().entrySet())
		{
			int[]g = grid.getKey();
			Boolean inside = isInside(g);
			
			this.grids.put(g, inside);
		}
		
	}
	
	public void splitCluster()
	{
		//TODO implement this function
	}
	
	/*
	 * Returns TRUE if the argument density grid, g, is an inside grid of the cluster
	 * Inside Grids are defined in Definition 3.5 of Chen and Tu 2007 as:
	 * Consider a grid group G and a grid g ∈ G, suppose g =(j1, ··· ,jd), if g has 
	 * neighboring grids in every dimension i =1, ·· · ,d, then g is an inside grid 
	 * in G.Otherwise g is an outside grid in G.
	 */
	public Boolean isInside(int[] g)
	{
		int d = g.length;
		Boolean inside = true;
		
		for (int i = 0 ; i < d ; i++)
		{
			int[] gprime = g.clone();
			gprime[i] = g[i] - 1;
			
			if(!this.grids.containsKey(gprime))
			{
				inside = false;
				break;
			}
			
			gprime[i] = g[i] + 1;
			
			if(!this.grids.containsKey(gprime))
			{
				inside = false;
				break;
			}
		}
		
		return inside;
	}
	
	/**
	 * @return the size
	 */
	public int getSize() {
		return grids.size();
	}

	/**
	 * @return the clusterLabel
	 */
	public int getClusterLabel() {
		return clusterLabel;
	}

	public HashMap<int[],Boolean> getGrids()
	{
		return this.grids;
	}
	
	/**
	 * @param clusterLabel the clusterLabel to set
	 */
	public void setClusterLabel(int clusterLabel) {
		this.clusterLabel = clusterLabel;
	}
	
	@Override
	public void getDescription(StringBuilder sb, int indent) {
		sb.append("Cluster of grids.");
	}
	
}
