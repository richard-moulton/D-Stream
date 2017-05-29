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
	
	private int updateTime;			// tg in the characteristic vector tuple
	private int removeTime;			// tm in the characteristic vector tuple
	private double gridDensity;		// D in the characteristic vector tuple
	private int gridClass;			// label in the characteristic vector tuple
	private boolean isSporadic;		// status in the characteristic vector tuple
	
	/**
	 * @category Constructor method for the Characteristic Vector of grid g
	 * @param tg - the last time when g is updated
	 * @param tm - the last time when g is removed from grid_list
	 * @param D - the grid density at the last update
	 * @param label - the class label of the grid
	 * @param status - SPORADIC (true) or NORMAL (false)
	 */
	public CharacteristicVector(int tg, int tm, double D, int label, boolean status)
	{
		this.setUpdateTime(tg);
		this.setRemoveTime(tm);
		this.setGridDensity(D);
		this.setGridClass(label);
		this.setSporadic(status);
	}

	/**
	 * @return the updateTime
	 */
	public int getUpdateTime() {
		return updateTime;
	}

	/**
	 * @param updateTime the updateTime to set
	 */
	public void setUpdateTime(int updateTime) {
		this.updateTime = updateTime;
	}

	/**
	 * @return the removeTime
	 */
	public int getRemoveTime() {
		return removeTime;
	}

	/**
	 * @param removeTime the removeTime to set
	 */
	public void setRemoveTime(int removeTime) {
		this.removeTime = removeTime;
	}

	/**
	 * @return the gridDensity
	 */
	public double getGridDensity() {
		return gridDensity;
	}

	/**
	 * @param gridDensity the gridDensity to set
	 */
	public void setGridDensity(double gridDensity) {
		this.gridDensity = gridDensity;
	}

	/**
	 * @return the gridClass
	 */
	public int getGridClass() {
		return gridClass;
	}

	/**
	 * @param gridClass the gridClass to set
	 */
	public void setGridClass(int gridClass) {
		this.gridClass = gridClass;
	}

	/**
	 * @return the isSporadic
	 */
	public boolean isSporadic() {
		return isSporadic;
	}

	/**
	 * @param isSporadic the isSporadic to set
	 */
	public void setSporadic(boolean isSporadic) {
		this.isSporadic = isSporadic;
	}

	/*
	 * updateGridDensity implements the density update function given in 
	 * eq 5 (Proposition 3.1) of Chen and Tu 2007.
	 */
	public void updateGridDensity(int currTime, double decayFactor)
	{
		double densityOfG = (Math.pow(decayFactor, currTime-getUpdateTime()) * getGridDensity())+1.0;
		setGridDensity(densityOfG);
	}
	
	/*
	 * isDense implements the test for whether a density grid is dense given
	 * in eq 8 of Chen and Tu 2007.
	 */
	public boolean isDense(double dm)
	{
		if (this.gridDensity >= dm)
			return true;
		else
			return false;
	}
	
	/*
	 * isSparse implements the test for whether a density grid is sparse given
	 * in eq 9 of Chen and Tu 2007.
	 */
	public boolean isSparse(double dl)
	{
		if (this.gridDensity <= dl)
			return true;
		else
			return false;
	}
	
	/*
	 * isTransitional implements the test for whether a density grid is transitional
	 * given in eq 10 of Chen and Tu 2007.
	 */
	public boolean isTransitional(double dm, double dl)
	{
		if(this.isDense(dm) || this.isSparse(dl))
			return false;
		else
			return true;
	}
}
