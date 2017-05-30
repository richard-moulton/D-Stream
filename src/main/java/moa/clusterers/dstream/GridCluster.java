package moa.clusterers.dstream;

import moa.cluster.Cluster;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import com.yahoo.labs.samoa.instances.Instance;

/**
 * Grid Clusters are defined in Definition 3.6 of Chen and Tu 2007 as:
 * Let G =(g1, ·· · ,gm) be a grid group, if every inside grid of G is 
 * a dense grid and every outside grid is either a dense grid or a 
 * transitional grid, then G is a grid cluster.
 * 
 * Citation: Y. Chen and L. Tu, “Density-Based Clustering for Real-Time Stream Data,” in
 * Proceedings of the 13th ACM SIGKDD international conference on Knowledge discovery and
 * data mining, 2007, pp. 133–142.
 */
public class GridCluster extends Cluster
{
	private HashMap<int[], Boolean> grids;
	private int clusterLabel;
	
	public GridCluster(int label)
	{
		this.grids = new HashMap<int[],Boolean>();
		this.clusterLabel = label;
	}
	
	public GridCluster(HashMap<int[],Boolean> hashMap, int label)
	{
		this.grids = new HashMap<int[],Boolean>();
		
		for (Map.Entry<int[], Boolean> grid : hashMap.entrySet())
		{
			int[] g = grid.getKey();
			Boolean inside = grid.getValue();
			
			this.grids.put(g,  inside);
		}
		
		this.clusterLabel = label;
		
	}
	
	/**
	 * @param g the density grid to add to the cluster
	 */
	public void addGrid(int[] g)
	{
		Boolean inside = isInside(g);
		this.grids.put(g, inside);
	}
	
	/**
	 * @param g the density grid to remove from the cluster
	 */
	public void removeGrid(int[] g)
	{
		this.grids.remove(g);
	}
	
	/**
	 * @param gridClus the GridCluster to be absorbed into this cluster
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
	
	/**
	 * Inside Grids are defined in Definition 3.5 of Chen and Tu 2007 as:
	 * Consider a grid group G and a grid g ∈ G, suppose g =(j1, ··· ,jd), if g has 
	 * neighboring grids in every dimension i =1, ·· · ,d, then g is an inside grid 
	 * in G.Otherwise g is an outside grid in G.
	 * 
	 * @param g the density grid being labelled inside or out
	 * @return TRUE if g is an inside grid, FALSE otherwise
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
	 * Inside Grids are defined in Definition 3.5 of Chen and Tu 2007 as:
	 * Consider a grid group G and a grid g ∈ G, suppose g =(j1, ··· ,jd), if g has 
	 * neighboring grids in every dimension i =1, ·· · ,d, then g is an inside grid 
	 * in G.Otherwise g is an outside grid in G.
	 * 
	 * @param g the density grid being labelled as inside or outside
	 * @param h the density grid being proposed for addition
	 * @return TRUE if g would be an inside grid, FALSE otherwise
	 */
	public Boolean isInside(int[] g, int[] h)
	{
		int d = g.length;
		Boolean inside = true;
		
		for (int i = 0 ; i < d ; i++)
		{
			int[] gprime = g.clone();
			gprime[i] = g[i] - 1;
			
			if(!this.grids.containsKey(gprime) && !gprime.equals(h))
			{
				inside = false;
				break;
			}
			
			gprime[i] = g[i] + 1;
			
			if(!this.grids.containsKey(gprime) && !gprime.equals(h))
			{
				inside = false;
				break;
			}
		}
		
		return inside;
	}

	/**
	 * @return the class label assigned to the cluster
	 */
	public int getClusterLabel() {
		return clusterLabel;
	}

	public HashMap<int[],Boolean> getGrids()
	{
		return this.grids;
	}
	
	/**
	 * @param clusterLabel the class label to assign to the cluster
	 */
	public void setClusterLabel(int clusterLabel) {
		this.clusterLabel = clusterLabel;
	}
	
	@Override
	public void getDescription(StringBuilder sb, int indent) {
		sb.append("Cluster of grids.");
	}


	@Override
	public double[] getCenter() {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * @return the number of density grids in the cluster
	 */
	@Override
	public double getWeight() {
		return this.grids.size();
	}


	@Override
	public double getInclusionProbability(Instance instance) {
		// TODO Auto-generated method stub
		return 0;
	}


	@Override
	public Instance sample(Random random) {
		// TODO Auto-generated method stub
		return null;
	}
	
}
