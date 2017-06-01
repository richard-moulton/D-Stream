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
	private static final int SPARSE = 0;
	private static final int TRANSITIONAL = 1;
	private static final int DENSE = 2;

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

	/**
	 * The data stream's current internal time. Starts at 0.
	 */
	private int currTime; 
	
	/**
	 * User defined parameter: Time gap between calls to the offline component
	 */
	private int gap;
	
	/**
	 * User defined parameter, represented as lambda in Chen and Tu 2007
	 */
	private double decayFactor;
	
	/**
	 * User defined parameter: Controls the threshold for dense grids
	 */
	private double cm;
	
	/**
	 * User defined parameter: Controls the threshold for sparse grids
	 */
	private double cl;
	
	/**
	 * User defined parameter: Adjusts the window of protection for renaming 
	 * previously deleted grids as being sporadic
	 */
	private double beta;
	
	/**
	 * Density threshold for dense grids; controlled by cm; given in eq 8 of Chen and Tu 2007
	 * 
	 * @see cm
	 */
	private double dm;
	
	/**
	 * Density threshold for sparse grids; controlled by cl; given in eq 9 of Chen and Tu 2007
	 * 
	 * @see cl
	 */
	private double dl;
	
	/**
	 * The number of dimensions in the data stream; defined in section 3.1 of Chen and Tu 2007
	 */
	private int d;
	
	/**
	 * The number of density grids; defined after eq 2 in Chen and Tu 2007
	 */
	private int N;
	
	/**
	 * True if initialization of D-Stream is complete, false otherwise
	 */
	private boolean initialized;
	
	/**
	 * A list of all density grids which are being monitored;
	 * given in figure 1 of Chen and Tu 2007
	 */
	private HashMap<DensityGrid,CharacteristicVector> grid_list;
	
	/**
	 * A list of all Grid Clusters, which are defined in 
	 * Definition 3.6 of Chen and Tu 2007
	 */
	private ArrayList<GridCluster> grid_clusters;
	
	/**
	 * The minimum value seen for a numerical dimension; used to calculate N
	 * 
	 * @see N
	 */
	private int[]minVals;
	
	/**
	 * The maximum value seen for a numerical dimension; used to calculate N
	 * 
	 * @see N
	 */
	private int[]maxVals;

	/**
	 *  @see moa.clusterers.Clusterer#isRandomizable()
	 * D-Stream is not randomizable.
	 */
	@Override
	public boolean isRandomizable() {
		return false;
	}

	/**
	 * @see moa.clusterers.Clusterer#getVotesForInstance(com.yahoo.labs.samoa.instances.Instance)
	 * D-Stream does not vote on instances.
	 */
	@Override
	public double[] getVotesForInstance(Instance inst) {
		return null;
	}

	/**
	 *  @see moa.clusterers.Clusterer#getClusteringResult()
	 */
	@Override
	public Clustering getClusteringResult() {
		Clustering c = new Clustering();
		for(GridCluster gc : grid_clusters)
		{
			c.add(gc);
		}
		return c;
	}

	/**
	 * @see moa.clusterers.AbstractClusterer#resetLearningImpl()
	 */
	@Override
	public void resetLearningImpl() {
		System.out.println("Dstream . resetLearningImpl");
		this.setCurrTime(0);
		System.out.println("Current time set...");
		
		this.gap = timeGapOption.getValue();
		this.decayFactor = decayFactorOption.getValue();
		this.cm = cmOption.getValue();
		this.cl = clOption.getValue();
		this.beta = betaOption.getValue();
		System.out.println("Option values set...");

		this.initialized = false;
		this.grid_list = new HashMap<DensityGrid, CharacteristicVector>();
		this.grid_clusters = new ArrayList<GridCluster>();
		System.out.println("Data structures initialized...");

		this.dm = -1.0;
		this.dl = -1.0;
		this.d = -1;
		this.N = -1;
		this.minVals = null;
		this.maxVals = null;
		System.out.println("Dependent values initialized...\n");
		printDStreamState();
	}

	/**
	 * @see moa.clusterers.AbstractClusterer#trainOnInstanceImpl(com.yahoo.labs.samoa.instances.Instance)
	 * 
	 * trainOnInstanceImpl implements the procedure given in Figure 1 of Chen and Tu 2007
	 */
	@Override
	public void trainOnInstanceImpl(Instance inst) {
		
		System.out.println("Dstream . trainOnInstanceImpl");
		printout(inst);
		int[]g;
		DensityGrid dg;
		CharacteristicVector cv;
		boolean recalculateN = false;	// flag indicating whether N needs to be recalculated after this instance

		// 1. Read record x = (x1,x2,...,xd)
		System.out.println("Step 1");
		// Passed Instance inst
		if (!this.initialized)
		{
			System.out.println("...not initialized");
			this.d = inst.numAttributes();
			System.out.println("d = "+this.d);
			this.minVals = new int[this.d];
			this.maxVals = new int[this.d];
			System.out.println("...data initialized");
			
			for(int i = 0 ; i < this.d ; i++)
			{
				System.out.print(i+" ");
				if (inst.attribute(i).isNumeric())
				{
					maxVals[i] = (int) inst.value(i);
					minVals[i] = (int) inst.value(i);
				}
			}
			System.out.println("\n...arrays initialized");
			recalculateN = true;
			this.initialized = true;
			System.out.println("...boolean values initialized");
		}		

		// 2. Determine the density grid g that contains x
		System.out.println("Step 2");
		g = new int[this.d];

		System.out.print("Density Grid:");
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
				g[i] = (int) inst.value(i);
			}

			System.out.print(" "+g[i]);
		}

		if (recalculateN)
		{
			System.out.print("\nrecalculateN:");
			int n = 1;
			for (int i = 0 ; i < this.d ; i++)
			{
				System.out.print(" "+n);
				if (inst.attribute(i).isNominal())
					n = n * inst.attribute(i).numValues();
				else
					n = n * (1+maxVals[i]-minVals[i]);
			}
			System.out.println(" "+n);
			this.N = n;
			this.dl = this.cl/(this.N * (1.0 - this.decayFactor));
			this.dm = this.cm/(this.N * (1.0 - this.decayFactor));
			System.out.println("dl = " + this.dl + ", dm = " + this.dm);
		}

		dg = new DensityGrid(g);
		
		// 3. If (g not in grid_list) insert g to grid_list
		System.out.println("\nStep 3");
		
		if(!this.grid_list.containsKey(dg))
		{
			System.out.println("dg wasn't in grid_list!");
			cv = new CharacteristicVector(this.getCurrTime(), -1, 0, -1, false, this.getDL(), this.getDM());
			this.grid_list.put(dg, cv);
		}
		System.out.println("The size of grid_list is now "+grid_list.size());
		
		// 4. Update the characteristic vector of g
		System.out.println("Step 4");
		cv = this.grid_list.get(g);

		cv.updateGridDensity(this.getCurrTime(), this.getDecayFactor());

		cv.setUpdateTime(this.getCurrTime());
		
		printDensityGrid(dg);

		// 5. If tc == gap, then initial clustering
		// and
		// 6. If tc mod gap == 0, then:
		//    a. Detect and remove sporadic grids from grid_list
		//    b. Adjust clustering
		System.out.println("Current Time is " + this.getCurrTime() + " and gap is " + this.gap);
		if (this.getCurrTime() % gap == 0 && this.getCurrTime() != 0)
		{
			if (this.getCurrTime() == gap)
			{
				System.out.println("Step 5 x6x");
				this.initialClustering();
			}
			else
			{
				System.out.println("Step x5x 6");
				this.removeSporadic();
				this.adjustClustering();
			}
		}

		// 7. Increment tc
		System.out.println("Step 7\n");
		this.incCurrTime();

	}

	/**
	 * @see moa.clusterers.AbstractClusterer#getModelMeasurementsImpl()
	 */
	@Override
	protected Measurement[] getModelMeasurementsImpl() {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	/**
	 * @see moa.clusterers.AbstractClusterer#getModelDescription(java.lang.StringBuilder, int)
	 */
	@Override
	public void getModelDescription(StringBuilder out, int indent) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	/**
	 * Implements the procedure given in Figure 3 of Chen and Tu 2007
	 */
	private void initialClustering() {
		System.out.println("INITIAL CLUSTERING CALLED");
		// 1. Update the density of all grids in grid_list

		updateGridListDensity();

		// 2. Assign each dense grid to a distinct cluster
		// and
		// 3. Label all other grids as NO_CLASS	
		for (Map.Entry<DensityGrid, CharacteristicVector> grid : grid_list.entrySet())
		{
			DensityGrid dg = grid.getKey();
			CharacteristicVector cvOfG = grid.getValue();

			if(cvOfG.getAttribute() == DENSE)
			{
				int gridClass = assignGridToCluster(dg, cvOfG);
				cvOfG.setGridClass(gridClass);
				this.grid_clusters.get(gridClass).addGrid(dg);
			}
			else
				cvOfG.setGridClass(NO_CLASS);

			this.grid_list.replace(dg, cvOfG);
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
				for (Map.Entry<DensityGrid, Boolean> grid : c.getGrids().entrySet())
				{
					DensityGrid dg = grid.getKey();
					Boolean inside = grid.getValue();

					// b. for each OUTSIDE grid, g, of c
					if (!inside)
					{
						// c. for each neighbouring grid, h, of g
						for (int i = 0 ; i < this.d ; i++)
						{
							DensityGrid dgprime = new DensityGrid(dg);
							dgprime.vary(i, -1);

							// If dgprime is in grid_list...
							if(this.grid_list.containsKey(dgprime))
							{
								int class1 = this.grid_list.get(dg).getGridClass();
								int class2 = this.grid_list.get(dgprime).getGridClass();

								// If dgprime is in cluster c', merge c and c' into the larger of the two
								if (class2 != -1)
								{							
									if (this.grid_clusters.get(class1).getWeight() < this.grid_clusters.get(class2).getWeight())
										mergeClusters(class1, class2);
									else
										mergeClusters(class2, class1);
									
									changesMade = true;
								}
								// If h is transitional, assign it to c
								else if (this.grid_list.get(dgprime).isTransitional(dm, dl))
								{
									CharacteristicVector cv = this.grid_list.get(dgprime);
									cv.setGridClass(class1);
									this.grid_clusters.get(class1).addGrid(dgprime);
									this.grid_list.put(dg, cv);
									changesMade = true;
								}
							}

							dgprime.vary(i, 2);

							// If dgprime is in grid_list...
							if(this.grid_list.containsKey(dgprime))
							{
								int class1 = this.grid_list.get(dg).getGridClass();
								int class2 = this.grid_list.get(dgprime).getGridClass();

								// If dgprime is in cluster c', merge c and c' into the larger of the two
								if (class2 != -1)
								{							
									if (this.grid_clusters.get(class1).getWeight() < this.grid_clusters.get(class2).getWeight())
										mergeClusters(class1, class2);
									else
										mergeClusters(class2, class1);
									
									changesMade = true;
								}
								// If h is transitional, assign it to c
								else if (this.grid_list.get(dgprime).isTransitional(dm, dl))
								{
									CharacteristicVector cv = this.grid_list.get(dgprime);
									cv.setGridClass(class1);
									this.grid_clusters.get(class1).addGrid(dgprime);
									this.grid_list.put(dg, cv);
									changesMade = true;
								}
							}
						}
					}
				}
			}
		}while(changesMade);	// while changes are being made
	}

	/**
	 * Implements the procedure given in Figure 4 of Chen and Tu 2007
	 */
	private void adjustClustering() {
		System.out.println("ADJUST CLUSTERING CALLED");
		// 1. Update the density of all grids in grid_list

		updateGridListDensity();

		// 2. For each grid g whose attribute is changed since last call
		//    a. If g is sparse
		//    b. If g is dense
		//    c. If g is transitional
		for (Map.Entry<DensityGrid, CharacteristicVector> grid : grid_list.entrySet())
		{
			DensityGrid dg = grid.getKey();
			CharacteristicVector cv = grid.getValue();
			int gClass = cv.getGridClass();
			
			if(cv.isAttChanged())
			{
				if (cv.getAttribute() == SPARSE)
				{
					this.grid_clusters.get(cv.getGridClass()).removeGrid(dg);
					cv.setGridClass(NO_CLASS);
				}
				else if (cv.getAttribute() == DENSE)
				{
					// Among all neighbours of dg, find the grid h whose cluster ch has the largest size
					GridCluster ch;								// The cluster, ch, of h
					DensityGrid hChosen = new DensityGrid(dg);	// The chosen grid h, whose cluster ch has the largest size
					double hChosenSize = -1.0;					// The size of ch, the largest cluster
					DensityGrid dgH;							// The neighbour of g being considered
					int hClass = -1;							// The class label of h
					int hChosenClass = -1;						// The class label of ch
					
					for (int i = 0 ; i < this.d ; i++)
					{
						dgH = new DensityGrid(dg);
						dgH.vary(i, -1);
						
						if (this.grid_list.containsKey(dgH))
						{
							hClass = this.grid_list.get(dgH).getGridClass();
							ch = this.grid_clusters.get(hClass);
						
							if (ch.getWeight() > hChosenSize)
							{
								hChosenSize = ch.getWeight();
								hChosenClass = hClass;
								hChosen = new DensityGrid(dgH);
								
							}
						}
						
						dgH.vary(i, 2);
						
						if (this.grid_list.containsKey(dgH))
						{
							hClass = this.grid_list.get(dgH).getGridClass();
							ch = this.grid_clusters.get(hClass);
						
							if (ch.getWeight() > hChosenSize)
							{
								hChosenSize = ch.getWeight();
								hChosenClass = hClass;
								hChosen = new DensityGrid(dgH);
								
							}
						}
					}
					
					if (!hChosen.equals(dg))
					{
						ch = this.grid_clusters.get(hChosenClass);
						
						// If h is a dense grid
						if (this.grid_list.get(hChosen).getAttribute() == DENSE)
						{
							// If dg is labelled as NO_CLASS
							if(gClass == NO_CLASS)
							{
								cv.setGridClass(hChosenClass);
								ch.addGrid(dg);
								
							}
							// Else if dg belongs to cluster c
							else
							{
								GridCluster c = this.grid_clusters.get(gClass);
								double gSize = c.getWeight();
								
								if (gSize <= hChosenSize)
									ch.absorbCluster(c);
								else
									c.absorbCluster(ch);
							}
						}
					
						// Else if h is a transitional grid
						else if (this.grid_list.get(hChosen).getAttribute() == TRANSITIONAL)
						{
							// If dg is labelled as no class and if h is an outside grid if dg is added to ch
							if (gClass == NO_CLASS && !ch.isInside(hChosen, dg))
							{
								cv.setGridClass(hChosenClass);
								ch.addGrid(dg);
							}
							// Else if g is in cluster c and |c| >= |ch|
							else
							{
								GridCluster c = this.grid_clusters.get(gClass);
								double gSize = c.getWeight();
								
								if (gSize >= hChosenSize)
								{
									// Move h from cluster ch to cluster c
									ch.removeGrid(hChosen);
									c.addGrid(hChosen);
									CharacteristicVector cvhChosen = this.grid_list.get(hChosen);
									cvhChosen.setGridClass(gClass);
									this.grid_list.put(hChosen, cvhChosen);
								}
							}
						}
					}
					
				}
				// Else g is a transitional grid
				else
				{
					// Among all neighbours of g, find the grid h whose cluster ch has the largest size
					// and satisfies that g would be an outside grid if added to it
					GridCluster ch;								// The cluster, ch, of h
					DensityGrid hChosen = new DensityGrid(dg);	// The chosen grid h, whose cluster ch has the largest size
					double hChosenSize = -1.0;					// The size of ch, the largest cluster
					DensityGrid dgH;							// The neighbour of dg being considered
					int hClass = -1;							// The class label of h
					int hChosenClass = -1;						// The class label of ch
										
					for (int i = 0 ; i < this.d ; i++)
					{
						dgH = new DensityGrid(dg);
						dgH.vary(i, -1);
						
						if (this.grid_list.containsKey(dgH))
						{
							hClass = this.grid_list.get(dgH).getGridClass();
							ch = this.grid_clusters.get(hClass);
						
							if ((ch.getWeight() > hChosenSize) && ch.isInside(dg, dg))
							{
								hChosenSize = ch.getWeight();
								hChosenClass = hClass;
								hChosen = new DensityGrid(dgH);
							}
						}
						
						dgH.vary(i,2);
						
						if (this.grid_list.containsKey(dgH))
						{
							hClass = this.grid_list.get(dgH).getGridClass();
							ch = this.grid_clusters.get(hClass);
						
							if ((ch.getWeight() > hChosenSize) && ch.isInside(dg, dg))
							{
								hChosenSize = ch.getWeight();
								hChosenClass = hClass;
								hChosen = new DensityGrid(dgH);
							}
						}
					}
					
					if (!hChosen.equals(dg))
					{
						ch = this.grid_clusters.get(hChosenClass);
						ch.addGrid(dg);
						if(gClass != -1)
							this.grid_clusters.get(gClass).removeGrid(dg);
						
						cv.setGridClass(hChosenClass);
					}
					else
					{
						int newClass = this.grid_clusters.size()+1;
						GridCluster c = new GridCluster(newClass);
						c.addGrid(dg);
						this.grid_clusters.add(c);
					}
				}
				
				this.grid_list.put(dg, cv);
			}
		}

	}

	/**
	 * Implements the procedure described in section 4.2 of Chen and Tu 2007
	 */
	private void removeSporadic() {
		System.out.println("REMOVE SPORADIC CALLED");
		// 1. For each grid g in grid_list
		//    a. If g is sporadic
		//       i. If currTime - tg > gap, delete g from grid_list
		//       ii. Else if (S1 && S2), mark as sporadic
		//       iii. Else, mark as normal
		//    b. Else
		//       i. If (S1 && S2), mark as sporadic
		
		// For each grid g in grid_list
		for (Map.Entry<DensityGrid, CharacteristicVector> grid : grid_list.entrySet())
		{
			DensityGrid dg = grid.getKey();
			CharacteristicVector cv = grid.getValue();
			
			// If g is sporadic
			if (cv.isSporadic())
			{
				// If currTime - tg > gap, delete g from grid_list
				if ((this.getCurrTime() - cv.getUpdateTime()) > gap)
				{
					this.grid_clusters.get(cv.getGridClass()).removeGrid(dg);
					cv.setGridClass(NO_CLASS);
					this.grid_list.put(dg, cv);
				}
				// Else if (S1 && S2), mark as sporadic - Else mark as normal
				else
				{
					cv.setSporadic(checkIfSporadic(cv));
					this.grid_list.put(dg, cv);
				}
				
			}
			// Else if (S1 && S2), mark as sporadic
			else
			{
				cv.setSporadic(checkIfSporadic(cv));
				this.grid_list.put(dg, cv);
			}
		}
		
	}

	/**
	 * Determines whether a sparse density grid is sporadic using rules S1 and S2 of Chen and Tu 2007
	 * 
	 * @param cv - the CharacteristicVector of the density grid being assessed for sporadicity
	 */
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
	
	/**
	 * Implements the function pi given in Definition 4.1 of Chen and Tu 2007
	 * 
	 * @param tg - the update time in the density grid's characteristic vector
	 * @param cl - user defined parameter which controls the threshold for sparse grids
	 * @param decayFactor - user defined parameter which is represented as lambda in Chen and Tu 2007
	 * @param N - the number of density grids, defined after eq 2 in Chen and Tu 2007
	 */
	public double densityThresholdFunction(int tg, double cl, double decayFactor, int N)
	{
		double densityThreshold = (cl * (1.0 - Math.pow(decayFactor, (this.getCurrTime()-tg+1))))/(N * (1 - decayFactor));
		
		return densityThreshold;
	}
	
	
	/**
	 * Determines whether two density grids are neighbours based on their coordinates.
	 * 
	 * Neighbouring Grids are defined in Definition 3.3 of Chen and Tu 2007 as:
	 * Consider two density grids g1 =(j1 1,j1 2, ··· ,j1 d)and 
	 * g2 =(j2 1,j2 2, ·· · ,j2 d), if there exists k,1 ≤ k ≤ d, such that:
	 * 1) j1 i = j2 i ,i =1, ·· · ,k −1,k +1, ··· ,d; and
	 * 2) |j1 k −j2 k| =1,
	 * then g1 and g2 are neighboring grids in the kth dimension, denoted as g1 ∼ g2. 
	 * 
	 * @param dg1 first density grid
	 * @param dg2 second density grid
	 */
	public boolean isNeighbour (DensityGrid dg1, DensityGrid dg2)
	{
		int differences = 0;
		int[] dg1Coord = dg1.getCoordinates();
		int[] dg2Coord = dg2.getCoordinates();
		
		for(int i = 0 ; i < this.d ; i++)
		{
			differences = Math.abs(dg1Coord[i]-dg2Coord[i]);

			if (differences > 1)
				return false;
		}

		if (differences == 1)
			return true;
		else
			return false;
	}

	/**
	 * Assigns dg1 to the cluster of any neighbouring grid which is itself assigned to a cluster.
	 * If multiple neighbouring grids are assigned to clusters, dg1 is assigned to the one with the
	 * lowest index and merges the density grids from the high index cluster into the low index cluster.
	 * 
	 * Searches through grid_list or dg1's neighbourhood, which ever is faster.
	 * 
	 * @param dg1 the density grid being assigned to a cluster
	 * @param cv1 - CharacteristicVector of density grid dg1
	 */
	public int assignGridToCluster(DensityGrid dg1, CharacteristicVector cv1)
	{
		int class1 = cv1.getGridClass();	// The argument density grid's class
		DensityGrid dg2;

		// Iterate through the density grids in grid_list to find dg1's neighbouring grids
		if (this.grid_list.size() < 2*this.d)
		{
			for (Map.Entry<DensityGrid, CharacteristicVector> grid : this.grid_list.entrySet())
			{
				dg2 = grid.getKey();
				CharacteristicVector cv2 = grid.getValue();
				int class2 = cv2.getGridClass();				// The current density grid's class

				// If g1 and g2 are neighbouring density grids, update clusters as required
				if (isNeighbour(dg1, dg2))
				{
					if (class2 != -1)
					{
						if (class1 == -1)
						{
							cv1.setGridClass(class2);
							this.grid_clusters.get(class2).addGrid(dg1);
							class1 = class2;
						}
						else
						{
							if(this.grid_clusters.get(class1).getWeight() > this.grid_clusters.get(class2).getWeight())
							{
								class1 = class2; //TODO make sure this logic survives mergeClusters
								mergeClusters(class1, class2);
							}
							else if(this.grid_clusters.get(class1).getWeight() < this.grid_clusters.get(class2).getWeight())
							{
								mergeClusters(class2, class1);
							}
						}
					}
				}			
			}
		}
		// Iterate through dg1's neighbourhood
		else
		{
			for (int i = 0 ; i < this.d ; i++)
			{
				dg2 = new DensityGrid(dg1);
				dg2.vary(i, -1);

				// If dg2 is in grid_list...
				if(this.grid_list.containsKey(dg2))
				{
					CharacteristicVector cv2 = this.grid_list.get(dg2);
					int class2 = cv2.getGridClass();				// The current density grid's class
					
					if (class1 == -1)
					{
						cv1.setGridClass(class2);
						this.grid_clusters.get(class2).addGrid(dg1);
						class1 = class2;
					}
					else
					{
						if(this.grid_clusters.get(class1).getWeight() > this.grid_clusters.get(class2).getWeight())
						{
							class1 = class2; //TODO make sure this logic survives mergeClusters
							mergeClusters(class1, class2);
						}
						else if(this.grid_clusters.get(class1).getWeight() < this.grid_clusters.get(class2).getWeight())
						{
							mergeClusters(class2, class1);
						}
					}
				}

				dg2.vary(i, 2);

				// If dg2 is in grid_list...
				if(this.grid_list.containsKey(dg2))
				{
					CharacteristicVector cv2 = this.grid_list.get(dg2);
					int class2 = cv2.getGridClass();				// The current density grid's class
					
					if (class1 == -1)
					{
						cv1.setGridClass(class2);
						this.grid_clusters.get(class2).addGrid(dg1);
						class1 = class2;
					}
					else
					{
						if(this.grid_clusters.get(class1).getWeight() > this.grid_clusters.get(class2).getWeight())
						{
							class1 = class2; //TODO make sure this logic survives mergeClusters
							mergeClusters(class1, class2);
						}
						else if(this.grid_clusters.get(class1).getWeight() < this.grid_clusters.get(class2).getWeight())
						{
							mergeClusters(class2, class1);
						}
					}
				}
			}
		}

		// If dg1 had no neighbouring density grids, or had no neighbouring density grids which were
		// assigned to a cluster, then assign it to a new cluster.
		if (class1 == -1)
		{
			class1 = grid_clusters.size();
			grid_clusters.add(new GridCluster(class1));
		}

		return class1;
	}

	/**
	 * Reassign all grids belonging in the small cluster to the big cluster
	 * Merge the GridCluster objects representing each cluster
	 * 
	 * @param smallClus - the index of the smaller cluster
	 * @param bigClus - the index of the bigger cluster
	 */
	public void mergeClusters (int smallClus, int bigClus)
	{		
		// Iterate through the density grids in grid_list to find those which are in highClass
		for (Map.Entry<DensityGrid, CharacteristicVector> grid : grid_list.entrySet())
		{
			DensityGrid dg = grid.getKey();
			CharacteristicVector cv = grid.getValue();

			// Assign density grids in highClass to lowClass
			if(cv.getGridClass() == smallClus)
			{
				cv.setGridClass(bigClus);
				this.grid_list.replace(dg, cv);
			}
		}

		// Merge the GridCluster objects representing each cluster
		grid_clusters.get(bigClus).absorbCluster(grid_clusters.get(smallClus));
		grid_clusters.remove(smallClus);

		// Clean up by adjusting the class label for all clusters whose index is moved up
		for(int i = smallClus ; i < grid_clusters.size() ; i++)
		{
			GridCluster gc = grid_clusters.get(i);

			for (Map.Entry<DensityGrid, Boolean> grid : gc.getGrids().entrySet())
			{
				DensityGrid dg = grid.getKey();
				CharacteristicVector cv = grid_list.get(dg);

				cv.setGridClass(i);

				this.grid_list.put(dg, cv);
			}
		}
	}

	/**
	 * Iterates through grid_list and updates the density for each density grid therein
	 */
	public void updateGridListDensity()
	{
		for (Map.Entry<DensityGrid, CharacteristicVector> grid : grid_list.entrySet())
		{
			DensityGrid dg = grid.getKey();
			CharacteristicVector cvOfG = grid.getValue();

			cvOfG.updateGridDensity(this.getCurrTime(), this.getDecayFactor());

			this.grid_list.replace(dg, cvOfG);
		}
	}

	/**
	 * @return currTime - the stream's internal time
	 */
	public int getCurrTime()
	{
		return this.currTime;
	}

	/**
	 * @param t - sets the stream's internal time to 't'
	 */
	public void setCurrTime(int t)
	{
		this.currTime = t;
	}

	/**
	 * Increments the stream's internal time
	 */
	public void incCurrTime()
	{
		this.currTime++;
	}

	/**
	 * @return decay factor - represented as lambda in Chen and Tu 2007
	 */
	public double getDecayFactor()
	{
		return this.decayFactor;
	}

	/**
	 * @return dm - the density threshold for dense grids. It is controlled by cl and given in eq 8 of Chen and Tu 2007
	 */
	public double getDM()
	{
		return this.dm;
	}

	/**
	 * @return dl - the density threshold for sparse grids. It is controlled by cl and given in eq 9 of Chen and Tu 2007
	 */
	public double getDL()
	{
		return this.dl;
	}
	
	public void printout(Instance inst)
	{
		System.out.print("Instance //");
		
		for (int i = 0 ; i < inst.numAttributes() ; i++)
			System.out.print(" "+inst.value(i));
		
		System.out.println("//");
	}
	
	public void printDStreamState()
	{
		System.out.println("State of D-Stream algorithm");
		System.out.println("Time Gap Option: "+this.gap);
		System.out.println("Decay Factor Option: "+this.decayFactor);
		System.out.println("C_m Option: "+this.cm);
		System.out.println("C_l Option: "+this.cl);
		System.out.println("Beta Option: "+this.beta);
		
		
	}
	
	public void printDensityGrid(DensityGrid dg)
	{
		int[] dgCoord = dg.getCoordinates();
		CharacteristicVector cv = this.grid_list.get(dg);
		System.out.print("Key:");
		for (int i = 0 ; i < dg.dimensions ; i++)
			System.out.print(" "+dgCoord[i]);
		System.out.print("\nCharacteristicVector: ");
		System.out.print("(tg "+cv.getUpdateTime()+") ");
		System.out.print("(tm "+cv.getRemoveTime()+") ");
		System.out.print("(D "+cv.getGridDensity()+") ");
		System.out.print("(class "+cv.getGridClass()+") ");
		System.out.print("(status "+cv.isSporadic()+")\n");
		
	}
}
