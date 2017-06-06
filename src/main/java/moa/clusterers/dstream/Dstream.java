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

import moa.cluster.CFCluster;
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

	public FloatOption decayFactorOption = new FloatOption("decayFactor",
			'd', "The decay factor, lambda, in (0,1)", 0.998, 0.001, 0.999);

	public FloatOption cmOption = new FloatOption("Cm", 'm', "Controls the "
			+ "threshold for dense grids, > 1", 3.0, 1.001, Double.MAX_VALUE);

	public FloatOption clOption = new FloatOption("Cl", 'l', "Controls the "
			+ "threshold for sparse grids, in (0,1)", 0.8, 0.001, 0.999);

	public FloatOption betaOption = new FloatOption("Beta", 'b', "Adjusts the "
			+ "window of protection for renaming previously deleted grids as "
			+ "sporadic, > 0", 0.3, 0.001, Double.MAX_VALUE);

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
	private ArrayList<GridCluster> cluster_list;
	
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
		for(GridCluster gc : cluster_list)
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
		
		this.decayFactor = decayFactorOption.getValue();
		this.cm = cmOption.getValue();
		this.cl = clOption.getValue();
		this.beta = betaOption.getValue();
		System.out.println("Option values set...");

		this.initialized = false;
		this.grid_list = new HashMap<DensityGrid, CharacteristicVector>();
		this.cluster_list = new ArrayList<GridCluster>();
		System.out.println("Data structures initialized...");

		this.gap = 1;
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
		
		//System.out.print("Dstream . trainOnInstanceImpl (");
		//printout(inst);
		int[]g;
		DensityGrid dg;
		CharacteristicVector cv;
		boolean recalculateN = false;	// flag indicating whether N needs to be recalculated after this instance

		// 1. Read record x = (x1,x2,...,xd)
		//System.out.print(") Step 1");
		// Passed Instance inst
		if (!this.initialized)
		{
			System.out.println("Not yet initialized");
			this.d = inst.numAttributes();
			System.out.println("d = "+this.d);
			this.minVals = new int[this.d];
			this.maxVals = new int[this.d];
			System.out.println("...data initialized");
			
			for(int i = 0 ; i < this.d ; i++)
			{
				//System.out.print(i+" ");
				if (inst.attribute(i).isNumeric())
				{
					maxVals[i] = (int) inst.value(i);
					minVals[i] = (int) inst.value(i);
				}
			}
			System.out.println("...arrays initialized");
			recalculateN = true;
			this.initialized = true;
			System.out.println("...boolean values initialized");
		}		

		// 2. Determine the density grid g that contains x
		//System.out.print(" & Step 2 ");
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
				g[i] = (int) inst.value(i);
			}
		}

		if (recalculateN)
		{
			//System.out.print(" recalculateN:");
			int n = 1;
			for (int i = 0 ; i < this.d ; i++)
			{
				//System.out.print(" "+n);
				if (inst.attribute(i).isNominal())
					n = n * inst.attribute(i).numValues();
				else
					n = n * (1+maxVals[i]-minVals[i]);
			}
			//System.out.print(" "+n);
			this.N = n;
			this.dl = this.cl/(this.N * (1.0 - this.decayFactor));
			this.dm = this.cm/(this.N * (1.0 - this.decayFactor));
			//System.out.print(" dl = " + this.dl + ", dm = " + this.dm);
			
			//Calculate the value for gap using the method defined in eq 26 of Chen and Tu 2007 
			double optionA = Math.log(this.cl/this.cm)/Math.log(this.getDecayFactor());
			double optionB = Math.log((this.N-this.cm)/(this.N-this.cl))/Math.log(this.getDecayFactor());
			gap = (int)Math.floor(Math.min(optionA, optionB));
		}

		dg = new DensityGrid(g);
		//System.out.print(dg.toString()+" ");
		
		// 3. If (g not in grid_list) insert dg to grid_list
		//System.out.println(" & Step 3 or 4");
		
		if(!this.grid_list.containsKey(dg))
		{
			//System.out.print("3 - dg wasn't in grid_list!");
			cv = new CharacteristicVector(this.getCurrTime(), -1, this.getDecayFactor(), -1, false, this.getDL(), this.getDM());
			this.grid_list.put(dg, cv);
			//System.out.print(" The size of grid_list is now "+grid_list.size());
		}
		// 4. Update the characteristic vector of dg
		else
		{
			//System.out.print("4 - dg was in grid_list!");
			cv = this.grid_list.get(dg);
				
			cv.updateGridDensity(this.getCurrTime(), this.getDecayFactor(), this.getDL(), this.getDM(), true);
				
			cv.setUpdateTime(this.getCurrTime());
		
			//System.out.print(" "+dg.toString()+" "+cv.toString());
		
			grid_list.put(dg, cv);
		}

		// 5. If tc == gap, then initial clustering
		// and
		// 6. If tc mod gap == 0, then:
		//    a. Detect and remove sporadic grids from grid_list
		//    b. Adjust clustering
		//System.out.print(" & Current Time is " + this.getCurrTime() + " and gap is " + this.gap);
		if (this.getCurrTime() % gap == 0 && this.getCurrTime() != 0)
		{
			if (this.getCurrTime() == gap)
			{
				//System.out.print(" & Step 5 x6x");
				this.initialClustering();
			}
			else
			{
				//System.out.print(" & Step x5x 6");
				this.removeSporadic();
				this.adjustClustering();
			}
		}

		// 7. Increment tc
		//System.out.println(" & Step 7");
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
		printDStreamState();
		// 1. Update the density of all grids in grid_list

		updateGridListDensity();
		//printGridList();
				
		// 2. Assign each dense grid to a distinct cluster
		// and
		// 3. Label all other grids as NO_CLASS	
		Iterator<Map.Entry<DensityGrid, CharacteristicVector>> glIter = this.grid_list.entrySet().iterator();
		HashMap<DensityGrid, CharacteristicVector> newGL = new HashMap<DensityGrid, CharacteristicVector>();
		
		while(glIter.hasNext())
		{
			Map.Entry<DensityGrid, CharacteristicVector> grid = glIter.next();
			DensityGrid dg = grid.getKey();
			CharacteristicVector cvOfG = grid.getValue();

			if(cvOfG.getAttribute() == DENSE)
			{
				int gridClass = this.cluster_list.size();
				cvOfG.setGridClass(gridClass);
				GridCluster gc = new GridCluster ((CFCluster)dg, new ArrayList<CFCluster>(), gridClass);
				gc.addGrid(dg);
				this.cluster_list.add(gc);
			}
			else
				cvOfG.setGridClass(NO_CLASS);

			newGL.put(dg, cvOfG);
		}

		this.grid_list = newGL;
		printGridClusters();
		
		// 4. Make changes to grid labels by doing:
		//    a. For each cluster c
		//    b. For each outside grid g of c
		//    c. For each neighbouring grid h of g
		//    d. If h belongs to c', label c and c' with 
		//       the label of the largest cluster
		//    e. Else if h is transitional, assign it to c
		//    f. While changes can be made

		boolean changesMade;

		do{	
			changesMade = adjustLabels();
		}while(changesMade);	// while changes are being made
		
		//printGridList();
		printGridClusters();
	}

	/**
	 * Makes first change available to it by following the steps:
	 * a. For each cluster c
	 * b. For each outside grid g of c
	 * c. For each neighbouring grid h of g
	 * d. If h belongs to c', label c and c' with the label of the largest cluster
	 * e. Else if h is transitional, assign it to c
	 * 
	 * @return TRUE if a change was made to any cluster's labels, FALSE otherwise
	 */
	private boolean adjustLabels()
	{
		Iterator<GridCluster> gridClusIter = this.cluster_list.iterator();

		// a. For each cluster c
		while (gridClusIter.hasNext())
		{
			GridCluster c = gridClusIter.next();
			//System.out.print("Adjusting from cluster "+c.getClusterLabel()+", standby...");

			// b. for each grid, dg, of c
			for (Map.Entry<DensityGrid, Boolean> grid : c.getGrids().entrySet())
			{
				DensityGrid dg = grid.getKey();
				Boolean inside = grid.getValue();
				//System.out.print(" Inspecting density grid, dg:"+dg.toString()+", standby...");
				
				// b. for each OUTSIDE grid, dg, of c
				if (!inside)
				{
					//System.out.println(" Density grid dg is outside!");
					// c. for each neighbouring grid, dgprime, of dg
					Iterator<DensityGrid> dgNeighbourhood = dg.getNeighbours().iterator();
					
					while(dgNeighbourhood.hasNext())
					{
						DensityGrid dgprime = dgNeighbourhood.next();
						//System.out.print("Inspecting neighbour, dgprime:"+dgprime.toString()+", standby...");
						
						if(this.grid_list.containsKey(dgprime))
						{
							CharacteristicVector cv1 = this.grid_list.get(dg);
							CharacteristicVector cv2 = this.grid_list.get(dgprime);
							//System.out.print(" 1: "+cv1.toString()+", 2: "+cv2.toString());
							int class1 = cv1.getGridClass();
							int class2 = cv2.getGridClass();
							//System.out.println(" // classes "+class1+" and "+class2+".");

							// ...and if dgprime isn't already in the same cluster as dg...
							if (class1 != class2)
							{
								// If dgprime is in cluster c', merge c and c' into the larger of the two
								if (class2 != NO_CLASS)
								{
									//System.out.println("C is "+class1+" and C' is "+class2+".");
									if (this.cluster_list.get(class1).getWeight() < this.cluster_list.get(class2).getWeight())
										mergeClusters(class1, class2);
									else
										mergeClusters(class2, class1);

									return true;

								}
								// If dgprime is transitional and outside of c, assign it to c
								else if (cv2.isTransitional(dm, dl))
								{
									//System.out.println("h is transitional and is assigned to cluster "+class1);
									cv2.setGridClass(class1);
									this.cluster_list.get(class1).addGrid(dgprime);
									this.grid_list.put(dg, cv2);
									return true;
								}
							}
						}
					}
				}
			}
		}
		return false;
	}
	
	/**
	 * Implements the procedure given in Figure 4 of Chen and Tu 2007
	 */
	private void adjustClustering() {
		System.out.println("ADJUST CLUSTERING CALLED");
		//printDStreamState();
		//printGridClusters();
		// 1. Update the density of all grids in grid_list

		updateGridListDensity();
		
		// 2. For each grid dg whose attribute is changed since last call
		//    a. If dg is sparse
		//    b. If dg is dense
		//    c. If dg is transitional
		HashMap<DensityGrid, CharacteristicVector> glNew = new HashMap<DensityGrid, CharacteristicVector>();
		for (Map.Entry<DensityGrid, CharacteristicVector> grid : grid_list.entrySet())
		{
			DensityGrid dg = grid.getKey();
			CharacteristicVector cv = grid.getValue();
			int dgClass = cv.getGridClass();
			
			if(cv.isAttChanged())
			{
				System.out.print(dg.toString()+" is changed and now ");
				if (cv.getAttribute() == SPARSE)
				{
					System.out.println("sparse. Remove it from cluster "+dgClass+".");
					if (dgClass != NO_CLASS)
						this.cluster_list.get(dgClass).removeGrid(dg);
					cv.setGridClass(NO_CLASS);
				}
				else if (cv.getAttribute() == DENSE)
				{
					System.out.println("dense. Search among its neighbours for h, whose cluster ch is the largest.");
					// Among all neighbours of dg, find the grid h whose cluster ch has the largest size
					GridCluster ch;								// The cluster, ch, of h
					DensityGrid hChosen = new DensityGrid(dg);	// The chosen grid h, whose cluster ch has the largest size
					double hChosenSize = -1.0;					// The size of ch, the largest cluster
					DensityGrid dgH;							// The neighbour of g being considered
					int hClass = -1;							// The class label of h
					int hChosenClass = -1;						// The class label of ch
					Iterator<DensityGrid> dgNeighbourhood = dg.getNeighbours().iterator();
					
					while (dgNeighbourhood.hasNext())
					{
						dgH = dgNeighbourhood.next();
					
						if (this.grid_list.containsKey(dgH))
						{
							hClass = this.grid_list.get(dgH).getGridClass();
							if (hClass != NO_CLASS && hClass != dgClass)
							{
								ch = this.cluster_list.get(hClass);
						
								if (ch.getWeight() > hChosenSize)
								{
									hChosenSize = ch.getWeight();
									hChosenClass = hClass;
									hChosen = new DensityGrid(dgH);
								}
							}
						}
					}
					
					if (!hChosen.equals(dg))
					{
						ch = this.cluster_list.get(hChosenClass);
						
						// If h is a dense grid
						if (this.grid_list.get(hChosen).getAttribute() == DENSE)
						{
							System.out.println("h is dense.");
							// If dg is labelled as NO_CLASS
							if(dgClass == NO_CLASS)
							{
								cv.setGridClass(hChosenClass);
								ch.addGrid(dg);
								
							}
							// Else if dg belongs to cluster c and h belongs to c'
							else
							{
								GridCluster c = this.cluster_list.get(dgClass);
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
							System.out.print("h is transitional.");
							// If dg is labelled as no class and if h is an outside grid if dg is added to ch
							if (dgClass == NO_CLASS && !ch.isInside(hChosen, dg))
							{
								cv.setGridClass(hChosenClass);
								ch.addGrid(dg);
								System.out.println(" dg is added to cluster "+hChosenClass+".");
							}
							// Else if dg is in cluster c and |c| >= |ch|
							else
							{
								GridCluster c = this.cluster_list.get(dgClass);
								double gSize = c.getWeight();
								
								if (gSize >= hChosenSize)
								{
									// Move h from cluster ch to cluster c
									ch.removeGrid(hChosen);
									c.addGrid(hChosen);
									CharacteristicVector cvhChosen = this.grid_list.get(hChosen);
									cvhChosen.setGridClass(dgClass);
									glNew.put(hChosen, cvhChosen);
									System.out.println(" h is added to cluster "+dgClass+".");
								}
							}
						}
					}
					
				}
				// Else dg is a transitional grid
				else
				{
					System.out.println("transitional.");
					// Among all neighbours of dg, find the grid h whose cluster ch has the largest size
					// and satisfies that dg would be an outside grid if added to it
					GridCluster ch;								// The cluster, ch, of h
					DensityGrid hChosen = new DensityGrid(dg);	// The chosen grid h, whose cluster ch has the largest size
					double hChosenSize = -1.0;					// The size of ch, the largest cluster
					DensityGrid dgH;							// The neighbour of dg being considered
					int hClass = -1;							// The class label of h
					int hChosenClass = -1;						// The class label of ch
					Iterator<DensityGrid> dgNeighbourhood = dg.getNeighbours().iterator();
					
					while (dgNeighbourhood.hasNext())
					{
						dgH = dgNeighbourhood.next();
						
						if (this.grid_list.containsKey(dgH))
						{
							hClass = this.grid_list.get(dgH).getGridClass();
							if (hClass != NO_CLASS && hClass != dgClass)
							{
								ch = this.cluster_list.get(hClass);
						
								if ((ch.getWeight() > hChosenSize) && ch.isInside(dg, dg))
								{
									hChosenSize = ch.getWeight();
									hChosenClass = hClass;
									hChosen = new DensityGrid(dgH);
								}
							}
						}
					}
					
					if (hClass != NO_CLASS && !hChosen.equals(dg))
					{
						ch = this.cluster_list.get(hChosenClass);
						ch.addGrid(dg);
						if(dgClass != NO_CLASS)
							this.cluster_list.get(dgClass).removeGrid(dg);
						
						cv.setGridClass(hChosenClass);
					}
					else
					{
						int newClass = this.cluster_list.size()+1;
						GridCluster c = new GridCluster((CFCluster)dg, new ArrayList<CFCluster>(), newClass);
						c.addGrid(dg);
						this.cluster_list.add(c);
					}
				}
				
				glNew.put(dg, cv);
			}
		}
		
		System.out.println("There are "+glNew.size()+" entries to update from glNew to grid_list.");
		for(Map.Entry<DensityGrid, CharacteristicVector> grid: glNew.entrySet())
		{
			this.grid_list.put(grid.getKey(), grid.getValue());
		}

		//printGridList();
		printGridClusters();
	}

	/**
	 * Implements the procedure described in section 4.2 of Chen and Tu 2007
	 */
	private void removeSporadic() {
		System.out.println("\nREMOVE SPORADIC CALLED");
		// 1. For each grid g in grid_list
		//    a. If g is sporadic
		//       i. If currTime - tg > gap, delete g from grid_list
		//       ii. Else if (S1 && S2), mark as sporadic
		//       iii. Else, mark as normal
		//    b. Else
		//       i. If (S1 && S2), mark as sporadic
		
		// For each grid g in grid_list
		Iterator<Map.Entry<DensityGrid, CharacteristicVector>> glIter = this.grid_list.entrySet().iterator();
		HashMap<DensityGrid, CharacteristicVector> newGL = new HashMap<DensityGrid, CharacteristicVector>();
				
		while(glIter.hasNext())
		{
			Map.Entry<DensityGrid, CharacteristicVector> grid = glIter.next();
			DensityGrid dg = grid.getKey();
			CharacteristicVector cv = grid.getValue();
			
			// If g is sporadic
			if (cv.isSporadic())
			{
				// If currTime - tg > gap, delete g from grid_list
				if ((this.getCurrTime() - cv.getUpdateTime()) > gap)
				{
					this.cluster_list.get(cv.getGridClass()).removeGrid(dg);
					this.grid_list.remove(dg);
					//System.out.println("Removed "+dg.toString());
				}
				// Else if (S1 && S2), mark as sporadic - Else mark as normal
				else
				{
					cv.setSporadic(checkIfSporadic(cv));
					//System.out.println("within gap" + dg.toString() + " sporadicity assessed "+cv.isSporadic());
					newGL.put(dg, cv);
				}
				
			}
			// Else if (S1 && S2), mark as sporadic
			else
			{
				cv.setSporadic(checkIfSporadic(cv));
				//System.out.println(dg.toString() + " sporadicity assessed "+cv.isSporadic());
				newGL.put(dg, cv);
			}
		}
		
		for(Map.Entry<DensityGrid, CharacteristicVector> grid : newGL.entrySet())
		{
			this.grid_list.put(grid.getKey(), grid.getValue());
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
		int class1 = cv1.getGridClass();
		
		// Iterate through grid_list or through dg1's neighbours, whichever is faster
		if (this.grid_list.size() < 2*this.d)
			class1 = searchThroughGridList(dg1, cv1);
		else
			class1 = searchThroughNeighbours(dg1, cv1);

		// If dg1 remains unassigned, then assign it to a new cluster.
		if (class1 == NO_CLASS)
		{
			class1 = cluster_list.size();
			cluster_list.add(new GridCluster((CFCluster)dg1, new ArrayList<CFCluster>(),class1));
		}

		return class1;
	}

	/**
	 * Determines which cluster to assign dg1 to by iterating through the entries in grid_list.
	 * 
	 * @param dg1 the density grid being assigned a cluster
	 * @param cv1 the characteristic vector of dg1
	 * 
	 * @return an int representing the cluster to assign dg1 to
	 */
	private int searchThroughGridList(DensityGrid dg1, CharacteristicVector cv1)
	{
		int class1 = cv1.getGridClass();	// The argument density grid's class
		DensityGrid dg2;
		CharacteristicVector cv2;
		int class2;
		
		for (Map.Entry<DensityGrid, CharacteristicVector> grid : this.grid_list.entrySet())
		{
			dg2 = grid.getKey();
			cv2 = grid.getValue();
			class2 = cv2.getGridClass();				// The current density grid's class

			// If dg1 and dg2 are neighbouring density grids, update clusters as required
			if (isNeighbour(dg1, dg2))
			{
				if (class2 != NO_CLASS)
				{
					if (class1 == NO_CLASS)
					{
						cv1.setGridClass(class2);
						this.cluster_list.get(class2).addGrid(dg1);
						class1 = class2;
					}
					else
					{
						if(this.cluster_list.get(class1).getWeight() > this.cluster_list.get(class2).getWeight())
						{
							class1 = class2; //TODO make sure this logic survives mergeClusters
							mergeClusters(class1, class2);
						}
						else if(this.cluster_list.get(class1).getWeight() < this.cluster_list.get(class2).getWeight())
						{
							mergeClusters(class2, class1);
						}
					}
				}
			}			
		}
		
		return class1;
	}
	
	/**
	 * Determines which cluster to assign dg1 to by iterating through dg1's neighbours.
	 * 
	 * @param dg1 the density grid being assigned a cluster
	 * @param cv1 the characteristic vector of dg1
	 * 
	 * @return an int representing the cluster to assign dg1 to
	 */
	private int searchThroughNeighbours(DensityGrid dg1, CharacteristicVector cv1)
	{
		int class1 = cv1.getGridClass();	// The argument density grid's class
		DensityGrid dg2;
		CharacteristicVector cv2;
		int class2;
		Iterator<DensityGrid> dg1Neighbourhood = dg1.getNeighbours().iterator();
		
		while(dg1Neighbourhood.hasNext())
		{
			dg2 = dg1Neighbourhood.next();
			
			// If dg2 is in grid_list...
			if(this.grid_list.containsKey(dg2))
			{
				cv2 = this.grid_list.get(dg2);
				class2 = cv2.getGridClass();				// The current density grid's class

				if (class2 != NO_CLASS)
				{
					if (class1 == NO_CLASS)
					{
						cv1.setGridClass(class2);
						this.cluster_list.get(class2).addGrid(dg1);
						class1 = class2;
					}
					else
					{
						if(this.cluster_list.get(class1).getWeight() > this.cluster_list.get(class2).getWeight())
						{
							class1 = class2; //TODO make sure this logic survives mergeClusters
							mergeClusters(class1, class2);
						}
						else if(this.cluster_list.get(class1).getWeight() < this.cluster_list.get(class2).getWeight())
						{
							mergeClusters(class2, class1);
						}
					}
				}
			}
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
		System.out.println("Merge clusters "+smallClus+" and "+bigClus+".");
		// Iterate through the density grids in grid_list to find those which are in highClass
		for (Map.Entry<DensityGrid, CharacteristicVector> grid : grid_list.entrySet())
		{
			DensityGrid dg = grid.getKey();
			CharacteristicVector cv = grid.getValue();

			// Assign density grids in smallClus to bigClus
			if(cv.getGridClass() == smallClus)
			{
				cv.setGridClass(bigClus);
				this.grid_list.put(dg, cv);
			}
		}
		System.out.println("Density grids assigned to cluster "+bigClus+".");
		
		// Merge the GridCluster objects representing each cluster
		cluster_list.get(bigClus).absorbCluster(cluster_list.get(smallClus));
		cluster_list.remove(smallClus);
		System.out.println("Cluster "+smallClus+" removed from list.");
		
		// Clean up by adjusting the class label for all clusters whose index is moved up
		for(int i = smallClus ; i < cluster_list.size() ; i++)
		{
			GridCluster gc = cluster_list.get(i);

			for (Map.Entry<DensityGrid, Boolean> grid : gc.getGrids().entrySet())
			{
				DensityGrid dg = grid.getKey();
				CharacteristicVector cv = grid_list.get(dg);

				cv.setGridClass(i);

				this.grid_list.put(dg, cv);
			}
			System.out.print("Cluster "+(i+1)+" --> "+i+" / ");
		}
		System.out.println();
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

			cvOfG.updateGridDensity(this.getCurrTime(), this.getDecayFactor(), this.getDL(), this.getDM(), false);

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
		for (int i = 0 ; i < inst.numAttributes() ; i++)
			System.out.print(inst.value(i)+" ");
	}
	
	public void printDStreamState()
	{
		System.out.println("State of D-Stream algorithm");
		System.out.println("Time Gap: "+this.gap+", Decay Factor: "+this.decayFactor);
		System.out.println("C_m: "+this.cm+", C_l: "+this.cl);
		System.out.println("D_m: "+this.dm+", D_l: "+this.dl);
		System.out.println("Beta: "+this.beta);
	}
	
	public void printGridList()
	{
		System.out.println("Grid List. Size "+this.grid_list.size()+".");
		for (Map.Entry<DensityGrid, CharacteristicVector> grid : grid_list.entrySet())
		{
			DensityGrid dg = grid.getKey();
			CharacteristicVector cv = grid.getValue();
			
			System.out.println(dg.toString()+" "+cv.toString());
		}
	}
	
	public void printGridClusters()
	{
		System.out.println("List of Clusters. Total "+this.cluster_list.size()+".");
		for(GridCluster gc : this.cluster_list)
		{
			System.out.println(gc.getClusterLabel()+": "+gc.getWeight()+" {"+gc.toString()+"}");
		}
	}
}
