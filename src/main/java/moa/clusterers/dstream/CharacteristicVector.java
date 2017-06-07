package moa.clusterers.dstream;

/**
 * The Characteristic Vector of a density grid is defined in 
 * Definition 3.2 of Chen and Tu 2007 as:
 * The characteristic vector of a grid g is a tuple (tg,tm,D, label,status), 
 * where tg is the last time when g is updated, tm is the last time when g 
 * is removed from grid list as a sporadic grid (if ever), D is the grid 
 * density at the last update, label is the class label of the grid, and 
 * status = {SPORADIC, NORMAL} is a label used for removing sporadic grids.
 * 
 * Citation: Y. Chen and L. Tu, “Density-Based Clustering for Real-Time Stream Data,” in
 * Proceedings of the 13th ACM SIGKDD international conference on Knowledge discovery and
 * data mining, 2007, pp. 133–142.
 */
public class CharacteristicVector {
	
	private static final int SPARSE = 0;
	private static final int TRANSITIONAL = 1;
	private static final int DENSE = 2;
	
	/**
	 * t_g in the characteristic vector tuple; 
	 * The last time when g is updated
	 */
	private int updateTime;
	
	/**
	 * t_m in the characteristic vector tuple;* 
	 * the last time when g is removed from grid_list as a sporadic
	 * grid (if ever).
	 */
	private int removeTime;
	
	/**
	 * D in the characteristic vector tuple; 
	 * the grid density at the last update
	 */
	private double gridDensity;
	
	/**
	 * label in the characteristic vector tuple; 
	 * the class label of the grid
	 */
	private int gridClass;
	
	/**
	 * status in the characteristic vector tuple; 
	 * status = {SPORADIC, NORMAL}
	 */
	private boolean isSporadic;
	
	/**
	 * attribute mentioned in figure 4, line 3 of Chen and Tu 2007;
	 * attribute = {SPARSE, TRANSITIONAL, DENSE}
	 */
	private int attribute;
	
	/**
	 * Flag marking whether there was a change in the attribute field
	 * the last time the grid density was updated.
	 */
	private boolean attChange;
	
	/**
	 * @category Constructor method for the Characteristic Vector of grid g
	 * @param tg - the last time when g is updated
	 * @param tm - the last time when g is removed from grid_list
	 * @param D - the grid density at the last update
	 * @param label - the class label of the grid
	 * @param status - SPORADIC (true) or NORMAL (false)
	 */
	public CharacteristicVector(int tg, int tm, double D, int label, boolean status, double dl, double dm)
	{
		this.setUpdateTime(tg);
		this.setRemoveTime(tm);
		this.setGridDensity(D);
		this.setGridClass(label);
		this.setSporadic(status);
		
		if (this.isSparse(dl))
			this.attribute = SPARSE;
		else if (this.isDense(dm))
			this.attribute = DENSE;
		else
			this.attribute = TRANSITIONAL;
		
		this.attChange = false;
	}

	/**
	 * @return the time at which the grid was last updated
	 */
	public int getUpdateTime() {
		return this.updateTime;
	}

	/**
	 * @param updateTime the time at which the grid was updated
	 */
	public void setUpdateTime(int updateTime) {
		this.updateTime = updateTime;
	}

	/**
	 * @return the last time at which the grid was removed from grid_list
	 */
	public int getRemoveTime() {
		return this.removeTime;
	}

	/**
	 * @param removeTime the time at which the grid was removed from grid_list
	 */
	public void setRemoveTime(int removeTime) {
		this.removeTime = removeTime;
	}

	/**
	 * @return the density of the grid
	 */
	public double getGridDensity() {
		return this.gridDensity;
	}

	/**
	 * @param gridDensity the density of the grid
	 */
	public void setGridDensity(double gridDensity) {
		this.gridDensity = gridDensity;
	}

	/**
	 * @return the class to which the grid is assigned
	 */
	public int getGridClass() {
		return this.gridClass;
	}

	/**
	 * @param gridClass the class to assign the grid to
	 */
	public void setGridClass(int gridClass) {
		this.gridClass = gridClass;
		this.attChange = true;
	}

	/**
	 * @return TRUE if the characteristic vector is sporadic, FALSE otherwise
	 */
	public boolean isSporadic() {
		return this.isSporadic;
	}

	/**
	 * @param isSporadic TRUE if the characteristic vector is to be labelled as sporadic,
	 * FALSE otherwise
	 */
	public void setSporadic(boolean isSporadic) {
		this.isSporadic = isSporadic;
		this.attChange = true;
	}

	/**
	 * Implements the density update function given in 
	 * eq 5 (Proposition 3.1) of Chen and Tu 2007.
	 * 
	 * @param currTime the data stream's current internal time
	 * @param decayFactor the value of lambda
	 * @param dl the threshold for sparse grids
	 * @param dm the threshold for dense grids
	 * @param addRecord TRUE if a record has been added to the density grid, FALSE otherwise
	 */
	public void updateGridDensity(int currTime, double decayFactor, double dl, double dm, boolean addRecord)
	{
		// record the last attribute
		int lastAtt = this.getAttribute();
		
		// Update the density grid's density
		double densityOfG = this.getGridDensity();
		
		if(addRecord)
			densityOfG = (Math.pow(decayFactor, currTime-getUpdateTime()) * densityOfG)+1.0;
		else
			densityOfG = (Math.pow(decayFactor, currTime-getUpdateTime()) * densityOfG);
		
		setGridDensity(densityOfG);
		
		// Evaluate whether or not the density grid is now SPARSE, DENSE or TRANSITIONAL
		if (this.isSparse(dl))
			this.attribute = SPARSE;
		else if (this.isDense(dm))
			this.attribute = DENSE;
		else
			this.attribute = TRANSITIONAL;
		
		// Evaluate whether or not the density grid attribute has changed and set the attChange flag accordingly
		if (this.getAttribute() == lastAtt)
			this.attChange = false;
		else
			this.attChange = true;
	}
	
	/**
	 * Implements the test for whether a density grid is dense given
	 * in eq 8 of Chen and Tu 2007.
	 * 
	 * @param dm the threshold for dense grids
	 */
	public boolean isDense(double dm)
	{
		if (this.gridDensity >= dm)
			return true;
		else
			return false;
	}
	
	/**
	 * Implements the test for whether a density grid is sparse given
	 * in eq 9 of Chen and Tu 2007.
	 * 
	 * @param dl the threshold for sparse grids
	 */
	public boolean isSparse(double dl)
	{
		if (this.gridDensity <= dl)
			return true;
		else
			return false;
	}
	
	/**
	 * Implements the test for whether a density grid is transitional
	 * given in eq 10 of Chen and Tu 2007.
	 * 
	 * @param dm the threshold for dense grids
	 * @param dl the threshold for sparse grids
	 */
	public boolean isTransitional(double dm, double dl)
	{
		if(this.isDense(dm) || this.isSparse(dl))
			return false;
		else
			return true;
	}

	/**
	 * @return the characteristic vector's attribute {SPARSE, TRANSITIONAL, DENSE}
	 */
	public int getAttribute() {
		return this.attribute;
	}

	/**
	 * @return true if the characteristic vector's attribute changed during the last
	 * density update, false otherwise.
	 */
	public boolean isAttChanged() {
		return this.attChange;
	}
	
	/**
	 * Overrides Object's toString method.
	 * 
	 * @return a String listing each value in the characteristic vector tuple
	 */
	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder(80);
		
		sb.append("CV / A (tg tm D class status) chgflag: ");
		
		if (this.getAttribute() == DENSE)
			sb.append("D ");
		else if (this.getAttribute() == SPARSE)
			sb.append("S ");
		else
			sb.append("T ");
		
		sb.append(this.getUpdateTime()+" ");
		sb.append(this.getRemoveTime()+" ");
		sb.append(this.getGridDensity()+" ");
		sb.append(this.getGridClass()+" ");
		
		if (this.isSporadic())
			sb.append("Sporadic ");
		else
			sb.append("Normal ");
		
		if (this.isAttChanged())
			sb.append("CHANGED");
		
		return sb.toString();
	}
}
