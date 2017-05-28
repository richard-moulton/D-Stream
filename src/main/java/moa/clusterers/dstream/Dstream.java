/*
 *    Dstream.java
 *    
 *    @author Richard Hugh Moulton    
 */

package moa.clusterers.dstream;

import java.util.HashMap;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Map;

import com.github.javacliparser.IntOption;
import com.github.javacliparser.FloatOption;
import com.yahoo.labs.samoa.instances.Instance;

import moa.cluster.Clustering;
import moa.clusterers.AbstractClusterer;
import moa.core.Measurement;

/** Citation: Y. Chen and L. Tu, “Density-Based Clustering for Real-Time Stream Data,” in
 *  Proceedings of the 13th ACM SIGKDD international conference on Knowledge discovery and
 *  data mining, 2007, pp. 133–142.
 */
public class Dstream extends AbstractClusterer {

	private static final long serialVersionUID = 8759754409276716725L;
	private static final int NO_CLASS = -1;

	public IntOption timeGapOption = new IntOption("timeGap", 'g',
			"Time gap between calls to the offline component", 100);

	public FloatOption decayFactorOption = new FloatOption("decayFactor",
			'd', "The decay factor, lambda, in (0,1)", 0.5, 0.001, 0.999);

	public FloatOption cmOption = new FloatOption("Cm", 'm', "Controls the "
			+ "threshold for dense grids, > 1", 2.0, 1.001, Double.MAX_VALUE);

	public FloatOption clOption = new FloatOption("Cl", 'l', "Controls the "
			+ "threshold for sparse grids, in (0,1)", 0.5, 0.001, 0.999);

	public FloatOption betaOption = new FloatOption("Beta", 'b', "Adjusts the "
			+ "window of protection for renaming previously deleted grids as"
			+ "sporadic, > 0", 1.0, 0.001, Double.MAX_VALUE);

	private int currTime;				// the current time in the data stream. Starts at 0.
	
	private int gap;					// User defined parameter: Time gap between calls to the offline component
	private double decayFactor;			// User defined parameter, represented as lambda in Chen and Tu 2007
	private double cm;					// User defined parameter: Controls the threshold for dense grids
	private double cl;					// User defined parameter: Controls the threshold for sparse grids
	private double beta;				// User defined parameter: Adjusts the window of protection for renaming
										// previously deleted grids as being sporadic
	
	private double dm;					// Density threshold for dense grids; controlled by cm; given in eq 8 of Chen and Tu 2007
	private double dl;					// Density threshold for sparse grids; controlled by cl; given in eq 9 of Chen and Tu 2007
	
	private int d;						// The number of dimensions in the data stream; defined in section 3.1 of Chen and Tu 2007
	private int N;						// The number of density grids; defined after eq 2 in Chen and Tu 2007
	
	private boolean initialized;		// True if initialization of D-Stream is complete, false otherwise
	
	private HashMap<int[],CharacteristicVector> grid_list;	// A list of all density grids which are being monitored; given in
															// figure 1 of Chen and Tu 2007
	private ArrayList<GridCluster> grid_clusters;			// A list of all Grid Clusters, which are defined in Definition 3.6
															// of Chen and Tu 2007
	
	private int[]minVals;				// the minimum value seen for a numerical dimension; used to calculate N
	private int[]maxVals;				// the maximum value seen for a numerical dimension; used to calculate N


	/* @see moa.clusterers.Clusterer#isRandomizable()
	 * D-Stream is not randomizable.
	 */
	@Override
	public boolean isRandomizable() {
		return false;
	}

	/* @see moa.clusterers.Clusterer#getVotesForInstance(com.yahoo.labs.samoa.instances.Instance)
	 * D-Stream does not vote on instances.
	 */
	@Override
	public double[] getVotesForInstance(Instance inst) {
		return null;
	}

	/* @see moa.clusterers.Clusterer#getClusteringResult()
	 */
	@Override
	public Clustering getClusteringResult() {
		// TODO Write getClusteringResult method
		return null;
	}

	/* (non-Javadoc)
	 * @see moa.clusterers.AbstractClusterer#resetLearningImpl()
	 */
	@Override
	public void resetLearningImpl() {
		this.setCurrTime(0);

		this.gap = timeGapOption.getValue();
		this.decayFactor = decayFactorOption.getValue();
		this.cm = cmOption.getValue();
		this.cl = clOption.getValue();
		this.beta = betaOption.getValue();

		this.initialized = false;
		this.grid_list = new HashMap<int[], CharacteristicVector>();
		this.grid_clusters = new ArrayList<GridCluster>();

		this.dm = -1.0;
		this.dl = -1.0;
		this.d = -1;
		this.N = -1;
		this.minVals = null;
		this.maxVals = null;

	}

	/* (non-Javadoc)
	 * @see moa.clusterers.AbstractClusterer#trainOnInstanceImpl(com.yahoo.labs.samoa.instances.Instance)
	 * 
	 * trainOnInstanceImpl implements the procedure given in Figure 1 of Chen and Tu 2007
	 */
	@Override
	public void trainOnInstanceImpl(Instance inst) {
		int[]g;							// the density grid to which 'inst' belongs
		CharacteristicVector cvOfG;		// the CharacteristicVector for density grid g
		boolean recalculateN = false;	// flag indicating whether N needs to be recalculated after this instance

		// 1. Read record x = (x1,x2,...,xd)
		// Passed Instance inst
		if (!this.initialized)
		{
			this.d = inst.numAttributes();
			this.minVals = new int[this.d];
			this.maxVals = new int[this.d];

			for(int i = 0 ; i < this.d ; i++)
			{
				if (inst.attribute(i).isNumeric())
				{
					maxVals[i] = (int) inst.value(i);
					minVals[i] = (int) inst.value(i);
				}
			}
			recalculateN = true;
		}		

		// 2. Determine the density grid g that contains x
		g = new int[this.d];

		for (int i = 0 ; i < this.d ; i++)
		{
			if (inst.attribute(i).isNumeric())
			{
				g[i] = (int) inst.value(i);
				if (g[i] > maxVals[i])
				{
					maxVals[i] = g[i];
					recalculateN = true;
				}
				else if (g[i] < minVals[i])
				{
					minVals[i] = g[i];
					recalculateN = true;
				}
			}
			else
			{
				String instValue = inst.stringValue(i);
				g[i] = inst.attribute(i).indexOfValue(instValue);
			}

		}

		if (recalculateN)
		{
			int n = 1;
			for (int i = 0 ; i < this.d ; i++)
			{
				if (inst.attribute(i).isNominal())
					n = n * inst.attribute(i).numValues();
				else
					n = n * (1+maxVals[i]-minVals[i]);
			}
			this.N = n;
			this.dl = this.cl/(this.N * (1.0 - this.decayFactor));
			this.dm = this.cm/(this.N * (1.0 - this.decayFactor));
		}

		// 3. If (g not in grid_list) insert g to grid_list

		if(!this.grid_list.containsKey(g))
		{
			cvOfG = new CharacteristicVector(this.getCurrTime(), -1, 0, -1, false);
			this.grid_list.put(g, cvOfG);
		}

		// 4. Update the characteristic vector of g
		cvOfG = this.grid_list.get(g);

		cvOfG.updateGridDensity(this.getCurrTime(), this.getDecayFactor());

		cvOfG.setUpdateTime(this.getCurrTime());

		// 5. If tc == gap, then initial clustering
		// and
		// 6. If tc mod gap == 0, then:
		//    a. Detect and remove sporadic grids from grid_list
		//    b. Adjust clustering
		if (this.getCurrTime() % gap == 0 && this.getCurrTime() != 0)
		{
			if (this.getCurrTime() == gap)
				this.initialClustering();
			else
			{
				this.removeSporadic();
				this.adjustClustering();
			}
		}

		// 7. Increment tc
		this.incCurrTime();

	}

	/* (non-Javadoc)
	 * @see moa.clusterers.AbstractClusterer#getModelMeasurementsImpl()
	 */
	@Override
	protected Measurement[] getModelMeasurementsImpl() {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	/* (non-Javadoc)
	 * @see moa.clusterers.AbstractClusterer#getModelDescription(java.lang.StringBuilder, int)
	 */
	@Override
	public void getModelDescription(StringBuilder out, int indent) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	/*
	 * initialCustering implements the procedure given in Figure 3 of Chen and Tu 2007
	 */
	private void initialClustering() {
		// 1. Update the density of all grids in grid_list

		updateGridListDensity();

		// 2. Assign each dense grid to a distinct cluster
		// and
		// 3. Label all other grids as NO_CLASS	
		for (Map.Entry<int[], CharacteristicVector> grid : grid_list.entrySet())
		{
			int[] g = grid.getKey();
			CharacteristicVector cvOfG = grid.getValue();

			if(cvOfG.isDense(this.getDM()))
			{
				int gridClass = assignGridToCluster(g, cvOfG);
				cvOfG.setGridClass(gridClass);
				this.grid_clusters.get(gridClass).addGrid(g);
			}
			else
				cvOfG.setGridClass(NO_CLASS);

			this.grid_list.replace(g, cvOfG);
		}

		// 4. Make changes to grid labels by doing:
		//    a. For each cluster c
		//    b. For each outside grid g of c
		//    c. For each neighbouring grid h of g
		//    d. If h belongs to c', label c and c' with 
		//       the label of the largest cluster
		//    e. Else if h is transitional, assign it to c
		//    f. While changes can be made

		boolean changesMade = false;

		do
		{
			Iterator<GridCluster> gridClusIter = this.grid_clusters.iterator();

			// a. For each cluster c
			while (gridClusIter.hasNext())
			{
				GridCluster c = gridClusIter.next();

				// b. for each grid, g, of c
				for (Map.Entry<int[], Boolean> grid : c.getGrids().entrySet())
				{
					int[] g = grid.getKey();
					Boolean inside = grid.getValue();

					// b. for each OUTSIDE grid, g, of c
					if (!inside)
					{
						// c. for each neighbouring grid, h, of g
						for (int i = 0 ; i < this.d ; i++)
						{
							int[] gprime = g.clone();
							gprime[i] = g[i] - 1;

							// If gprime is in grid_list...
							if(this.grid_list.containsKey(gprime))
							{
								int class1 = this.grid_list.get(g).getGridClass();
								int class2 = this.grid_list.get(gprime).getGridClass();

								// If gprime is in cluster c', merge c and c' into the larger of the two
								if (class2 != -1)
								{							
									if (this.grid_clusters.get(class1).getSize() < this.grid_clusters.get(class2).getSize())
										mergeClusters(class1, class2);
									else
										mergeClusters(class2, class1);
									
									changesMade = true;
								}
								// If h is transitional, assign it to c
								else if (this.grid_list.get(gprime).isTransitional(dm, dl))
								{
									CharacteristicVector cv = this.grid_list.get(gprime);
									cv.setGridClass(class1);
									this.grid_clusters.get(class1).addGrid(gprime);
									this.grid_list.put(g, cv);
									changesMade = true;
								}
							}

							gprime[i] = g[i] + 1;

							// If gprime is in grid_list...
							if(this.grid_list.containsKey(gprime))
							{
								int class1 = this.grid_list.get(g).getGridClass();
								int class2 = this.grid_list.get(gprime).getGridClass();

								// If gprime is in cluster c', merge c and c' into the larger of the two
								if (class2 != -1)
								{							
									if (this.grid_clusters.get(class1).getSize() < this.grid_clusters.get(class2).getSize())
										mergeClusters(class1, class2);
									else
										mergeClusters(class2, class1);
									
									changesMade = true;
								}
								// If h is transitional, assign it to c
								else if (this.grid_list.get(gprime).isTransitional(dm, dl))
								{
									CharacteristicVector cv = this.grid_list.get(gprime);
									cv.setGridClass(class1);
									this.grid_clusters.get(class1).addGrid(gprime);
									this.grid_list.put(g, cv);
									changesMade = true;
								}
							}
						}
					}
				}
			}
		}while(changesMade);	// while changes are being made
	}

	/*
	 * adjustClustering implements the procedure given in Figure 4 of Chen and Tu 2007
	 */
	private void adjustClustering() {
		// 1. Update the density of all grids in grid_list

		updateGridListDensity();

		// 2. For each grid g whose attribute is changed since last call
		//    a. If g is sparse
		//    b. If g is dense
		//    c. If g is transitional
		

	}

	/*
	 * removeSporadic implements the procedure described in section 4.2 of Chen and Tu 2007
	 */
	private void removeSporadic() {
		// 1. For each grid g in grid_list
		//    a. If g is sporadic
		//       i. If currTime - tg > gap, delete g from grid_list
		//       ii. Else if (S1 && S2), mark as sporadic
		//       iii. Else, mark as normal
		//    b. Else
		//       i. If (S1 && S2), mark as sporadic
		
		// For each grid g in grid_list
		for (Map.Entry<int[], CharacteristicVector> grid : grid_list.entrySet())
		{
			int[] g = grid.getKey();
			CharacteristicVector cv = grid.getValue();
			
			// If g is sporadic
			if (cv.isSporadic())
			{
				// If currTime - tg > gap, delete g from grid_list
				if ((this.getCurrTime() - cv.getUpdateTime()) > gap)
				{
					this.grid_clusters.get(cv.getGridClass()).removeGrid(g);
					cv.setGridClass(NO_CLASS);
					this.grid_list.put(g, cv);
				}
				// Else if (S1 && S2), mark as sporadic - Else mark as normal
				else
				{
					cv.setSporadic(checkIfSporadic(cv));
					this.grid_list.put(g, cv);
				}
				
			}
			// Else if (S1 && S2), mark as sporadic
			else
			{
				cv.setSporadic(checkIfSporadic(cv));
				this.grid_list.put(g, cv);
			}
		}
		
	}

	// Determine whether a sparse density grid is sporadic using rules S1 and S2 of Chen and Tu 2007
	public boolean checkIfSporadic(CharacteristicVector cv)
	{
		// Check S1
		if(cv.getGridDensity() < densityThresholdFunction(cv.getUpdateTime(), this.cl, this.getDecayFactor(), this.N))
		{
			// Check S2
			if(cv.getRemoveTime() != -1 && this.getCurrTime() >= ((1 + this.beta)*cv.getRemoveTime()))
				return true;
			else
				return false;
		}
		
		return false; 
	}
	
	/*
	 * densityThresholdFunction implements the function pi given in Definition 4.1 of
	 * Chen and Tu 2007
	 */
	public double densityThresholdFunction(int tg, double cl, double decayFactor, int N)
	{
		double densityThreshold = (cl * (1.0 - Math.pow(decayFactor, (this.getCurrTime()-tg+1))))/(N * (1 - decayFactor));
		
		return densityThreshold;
	}
	
	
	/*
	 * isNeighbour determines whether two grids are neighbours based on their coordinates
	 * Neighbouring Grids are defined in Definition 3.3 of Chen and Tu 2007 as:
	 * Consider two density grids g1 =(j1 1,j1 2, ··· ,j1 d)and 
	 * g2 =(j2 1,j2 2, ·· · ,j2 d), if there exists k,1 ≤ k ≤ d, such that:
	 * 1) j1 i = j2 i ,i =1, ·· · ,k −1,k +1, ··· ,d; and
	 * 2) |j1 k −j2 k| =1,
	 * then g1 and g2 are neighboring grids in the kth dimension, denoted as g1 ∼ g2. 
	 */
	public boolean isNeighbour (int[] g1, int[] g2)
	{
		int differences = 0;

		for(int i = 0 ; i < this.d ; i++)
		{
			differences = Math.abs(g1[i]-g2[i]);

			if (differences > 1)
				return false;
		}

		if (differences == 1)
			return true;
		else
			return false;
	}

	/*
	 * assignGridtoCluster looks through grid_list to find the neighbours of the density grid g1.
	 * g1 is assigned to the cluster of any neighbouring grid which is itself assigned to a cluster.
	 * If multiple neighbouring grids are assigned to clusters, g1 is assigned to the one with the
	 * lowest index and merges the density grids from the high index cluster into the low index cluster.
	 */
	public int assignGridToCluster(int[] g1, CharacteristicVector cv1)
	{
		int class1 = cv1.getGridClass();	// The argument density grid's class

		// Iterate through the density grids in grid_list to find g1's neighbouring grids
		// TODO check whether it is faster to iterate through grid_list or the full set of g1's neighbours
		for (Map.Entry<int[], CharacteristicVector> grid : grid_list.entrySet())
		{
			int[] g2 = grid.getKey();
			CharacteristicVector cv2 = grid.getValue();
			int class2 = cv2.getGridClass();				// The current density grid's class

			// If g1 and g2 are neighbouring density grids, update clusters as required
			if (isNeighbour(g1, g2))
			{
				if (class1 == -1)
				{
					cv1.setGridClass(class2);
					class1 = class2;
				}
				else if (class2 != -1 )
				{
					if(this.grid_clusters.get(class1).getSize() > this.grid_clusters.get(class2).getSize())
					{
						class1 = class2; //TODO make sure this logic survives mergeClusters
						mergeClusters(class1, class2);
					}
					else if(this.grid_clusters.get(class1).getSize() < this.grid_clusters.get(class2).getSize())
					{
						mergeClusters(class2, class1);
					}
				}
			}			
		}

		// If g1 had no neighbouring density grids, or had no neighbouring density grids which were
		// assigned to a cluster, then assign it to a new cluster.
		if (class1 == -1)
		{
			class1 = grid_clusters.size();
			grid_clusters.add(new GridCluster(class1));
		}

		return class1;
	}

	/*
	 * Reassign all grids belonging in the small cluster to the big cluster
	 * Merge the GridCluster objects representing each cluster
	 */
	public void mergeClusters (int smallClus, int bigClus)
	{		
		// Iterate through the density grids in grid_list to find those which are in highClass
		for (Map.Entry<int[], CharacteristicVector> grid : grid_list.entrySet())
		{
			int[] g = grid.getKey();
			CharacteristicVector cv = grid.getValue();

			// Assign density grids in highClass to lowClass
			if(cv.getGridClass() == smallClus)
			{
				cv.setGridClass(bigClus);
				this.grid_list.replace(g, cv);
			}
		}

		// Merge the GridCluster objects representing each cluster
		grid_clusters.get(bigClus).absorbCluster(grid_clusters.get(smallClus));
		grid_clusters.remove(smallClus);

		// Clean up by adjusting the class label for all clusters whose index is moved up
		for(int i = smallClus ; i < grid_clusters.size() ; i++)
		{
			GridCluster gc = grid_clusters.get(i);

			for (Map.Entry<int[], Boolean> grid : gc.getGrids().entrySet())
			{
				int[]g = grid.getKey();
				CharacteristicVector cv = grid_list.get(g);

				cv.setGridClass(i);

				this.grid_list.put(g, cv);
			}
		}
	}

	/*
	 * Iterates through grid_list and updates the density for each density grid therein.
	 */
	public void updateGridListDensity()
	{
		for (Map.Entry<int[], CharacteristicVector> grid : grid_list.entrySet())
		{
			int[] g = grid.getKey();
			CharacteristicVector cvOfG = grid.getValue();

			cvOfG.updateGridDensity(this.getCurrTime(), this.getDecayFactor());

			this.grid_list.replace(g, cvOfG);
		}
	}

	public int getCurrTime()
	{
		return this.currTime;
	}

	public void setCurrTime(int t)
	{
		this.currTime = t;
	}

	public void incCurrTime()
	{
		this.currTime++;
	}

	public double getDecayFactor()
	{
		return this.decayFactor;
	}

	public double getDM()
	{
		return this.dm;
	}

	public double getDL()
	{
		return this.dl;
	}
}
