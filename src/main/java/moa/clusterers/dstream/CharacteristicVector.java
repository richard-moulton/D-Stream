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
		return updateTime;
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
		return removeTime;
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
		return gridDensity;
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
		return gridClass;
	}

	/**
	 * @param gridClass the class to assign the grid to
	 */
	public void setGridClass(int gridClass) {
		this.gridClass = gridClass;
	}

	/**
	 * @return TRUE if the characteristic vector is sporadic, FALSE otherwise
	 */
	public boolean isSporadic() {
		return isSporadic;
	}

	/**
	 * @param isSporadic TRUE if the characteristic vector is to be labelled as sporadic,
	 * FALSE otherwise
	 */
	public void setSporadic(boolean isSporadic) {
		this.isSporadic = isSporadic;
	}

	/**
	 * Implements the density update function given in 
	 * eq 5 (Proposition 3.1) of Chen and Tu 2007.
	 * 
	 * @param currTime the data stream's current internal time
	 * @param decayFactor the value of lambda
	 */
	public void updateGridDensity(int currTime, double decayFactor)
	{
		double densityOfG = (Math.pow(decayFactor, currTime-getUpdateTime()) * getGridDensity())+1.0;
		setGridDensity(densityOfG);
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
		return attribute;
	}

	/**
	 * @return true if the characteristic vector's attribute changed during the last
	 * density update, false otherwise.
	 */
	public boolean isAttChanged() {
		return attChange;
	}
}
