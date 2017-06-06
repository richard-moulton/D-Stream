package moa.clusterers.dstream;

import moa.cluster.Cluster;
import moa.clusterers.macro.NonConvexCluster;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import com.yahoo.labs.samoa.instances.Instance;
import moa.cluster.CFCluster;

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
public class GridCluster extends NonConvexCluster
{
	private HashMap<DensityGrid, Boolean> grids;
	private int clusterLabel;
	
	public GridCluster(CFCluster cluster, List<CFCluster> microclusters, int label)
	{
		super(cluster, microclusters);
		this.grids = new HashMap<DensityGrid,Boolean>();
		this.clusterLabel = label;
	}
	
	public GridCluster(CFCluster cluster, List<CFCluster> microclusters, HashMap<DensityGrid,Boolean> hashMap, int label)
	{
		super(cluster, microclusters);
		this.grids = new HashMap<DensityGrid,Boolean>();
		
		for (Map.Entry<DensityGrid, Boolean> grid : hashMap.entrySet())
		{
			DensityGrid dg = grid.getKey();
			Boolean inside = grid.getValue();
			
			this.grids.put(dg,  inside);
		}
		
		this.clusterLabel = label;
		
	}
	
	/**
	 * @param dg the density grid to add to the cluster
	 */
	public void addGrid(DensityGrid dg)
	{
		Boolean inside = isInside(dg);
		this.grids.put(dg, inside);
	}
	
	/**
	 * @param dg the density grid to remove from the cluster
	 */
	public void removeGrid(DensityGrid dg)
	{
		this.grids.remove(dg);
	}
	
	/**
	 * @param gridClus the GridCluster to be absorbed into this cluster
	 */
	public void absorbCluster(GridCluster gridClus)
	{
		DensityGrid dg;
		Boolean inside;
		Iterator<Map.Entry<DensityGrid, Boolean>> grid;
		HashMap<DensityGrid, Boolean> newCluster = new HashMap();
		
		System.out.println("Absorb cluster "+gridClus.getClusterLabel()+" into cluster "+this.getClusterLabel()+".");
		
		// Add each density grid from gridClus into this.grids
		grid = gridClus.getGrids().entrySet().iterator();
		while (grid.hasNext())
		{
			Map.Entry<DensityGrid, Boolean> entry = grid.next();
			dg = entry.getKey();
			this.grids.put(dg, false);
		}
		System.out.println("...density grids added");
		
		// Determine which density grids in this.grids are 'inside' and which are 'outside'
		grid = this.getGrids().entrySet().iterator();
		while(grid.hasNext())
		{
			Map.Entry<DensityGrid, Boolean> entry = grid.next();
			dg = entry.getKey();
			inside = isInside(dg);
			newCluster.put(dg, inside);
		}
		this.grids = newCluster;
		System.out.println("...inside/outside determined");

	}
	
	/**
	 * Inside Grids are defined in Definition 3.5 of Chen and Tu 2007 as:
	 * Consider a grid group G and a grid g ∈ G, suppose g =(j1, ··· ,jd), if g has 
	 * neighboring grids in every dimension i =1, ·· · ,d, then g is an inside grid 
	 * in G.Otherwise g is an outside grid in G.
	 * 
	 * @param dg the density grid to label as being inside or out
	 * @return TRUE if g is an inside grid, FALSE otherwise
	 */
	public Boolean isInside(DensityGrid dg)
	{
		int d = dg.getDimensions();
		Boolean inside = true;
		
		Iterator<DensityGrid> dgNeighbourhood = dg.getNeighbours().iterator();
		
		while(dgNeighbourhood.hasNext())
		{
			DensityGrid dgprime = dgNeighbourhood.next();
			if(!this.grids.containsKey(dgprime))
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
	 * @param dg the density grid being labelled as inside or outside
	 * @param dgH the density grid being proposed for addition
	 * @return TRUE if g would be an inside grid, FALSE otherwise
	 */
	public Boolean isInside(DensityGrid dg, DensityGrid dgH)
	{
		Boolean inside = true;
		Iterator<DensityGrid> dgNeighbourhood = dg.getNeighbours().iterator();
		
		while(dgNeighbourhood.hasNext())
		{
			DensityGrid dgprime = dgNeighbourhood.next();
			if(!this.grids.containsKey(dgprime) && !dgprime.equals(dgH))
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

	public HashMap<DensityGrid,Boolean> getGrids()
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

	/**
	 * @return the number of density grids in the cluster
	 */
	@Override
	public double getWeight() {
		return this.grids.size();
	}

	/**
	 * Iterates through the DensityGrids in the cluster and calculates the inclusion probability for each.
	 * 
	 * @return 1.0 if instance matches any of the density grids; 0.0 otherwise.
	 */
	@Override
	public double getInclusionProbability(Instance instance) {
		Iterator<Map.Entry<DensityGrid, Boolean>> gridIter = grids.entrySet().iterator();
		
		while(gridIter.hasNext())
		{
			Map.Entry<DensityGrid, Boolean> grid = gridIter.next();
			DensityGrid dg = grid.getKey();
			if(dg.getInclusionProbability(instance) == 1.0)
				return 1.0;
		}
		
		return 0.0;
	}
	
	/**
	 * @return a String listing each coordinate of the density grid
	 */
	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder(10*this.grids.size());
		for(Map.Entry<DensityGrid, Boolean> grids : this.grids.entrySet())
		{
			DensityGrid dg = grids.getKey();
			Boolean inside = grids.getValue();
			
			sb.append("("+dg.toString()+" Inside: "+inside+") ");
		}
		
		return sb.toString();
	}
	
}
