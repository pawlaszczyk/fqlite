package fqlite.base;

import fqlite.util.Auxiliary;
import fqlite.util.WorkerQueue;

/**
*  The analysis of the individual database pages can be executed in parallel. 
*  For this purpose, a corresponding task is assigned to a worker thread. 
* 
*     __________    __    _ __     
*    / ____/ __ \  / /   (_) /____ 
*   / /_  / / / / / /   / / __/ _ \
*  / __/ / /_/ / / /___/ / /_/  __/
* /_/    \___\_\/_____/_/\__/\___/ 
* 
* 
* 
* @author Dirk Pawlaszczyk
* @version 1.2
*/
public class Worker implements Runnable {

	WorkerQueue<RecoveryTask> toDo;
	Auxiliary util;
	
	/**
	 * Constructor. Used to assign a new ImportDBTask to the 
	 * Worker object.
	 * 
	 * @param rt
	 */
	public Worker(Job rt)
	{
		toDo = new WorkerQueue<RecoveryTask>();
		util = new Auxiliary(rt);
	}
	
	/**
	 * A a new task on top of the internal worker's stack. 
	 * @param t
	 */
	public void addTask(RecoveryTask t)
	{
		toDo.addLast(t);
	}
	
	/**
	 * This is the control loop. 
	 * As long as there are unfinished tasks, the worker works through them.
	 *  
	 */
	@Override
	public void run() {

		//long start = System.currentTimeMillis();
		/* go through the stack */
		while (toDo.size() > 0)
		{
			
			RecoveryTask next = toDo.pop();
			next.run();
		}
		//long end = System.currentTimeMillis();
		
		
	}

}
