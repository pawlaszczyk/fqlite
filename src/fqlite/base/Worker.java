package fqlite.base;

import fqlite.util.Auxiliary;
import fqlite.util.WorkerQueue;

/**
*  The analysis of the individual database pages can be executed in parallel. 
*  For this purpose, a corresponding task is assigned to a worker thread.
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
	 * @param rt runtime or job object
	 */
	public Worker(Job rt)
	{
		toDo = new WorkerQueue<>();
		util = new Auxiliary(rt);
	}
	
	/**
	 * A new task on top of the internal worker's stack.
	 * @param t the task object to enqueue
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

		/* go through the stack */
		while (toDo.size() > 0)
		{
			
			RecoveryTask next = toDo.pop();
			try {
				next.run();
			}
			catch (Throwable t) {
				// RecoveryTask.run() catches Exceptions only. An Error (e.g. OutOfMemoryError,
				// StackOverflowError) would otherwise kill this worker; all tasks still queued here
				// would never run, runningTasks would never reach 0 and Job.scan() would wait forever
				// ("Loading..." in the GUI).
				fqlite.log.AppLog.error("Worker: task aborted with " + t.getClass().getSimpleName() + " — " + t.getMessage());
			}
		}

		
	}

}
