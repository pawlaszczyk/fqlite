package fqlite.util;

import java.util.ArrayDeque;

/**
 * Thread-safe implementation of a Queue.
 * 
 * @author Dirk Pawlaszczyk
 * @version 1.2
 */
public class WorkerQueue<T> {

	// Internal Deque which gets decorated for synchronization.
	private ArrayDeque<T> dequeStore;

	public WorkerQueue(int initialCapacity) {
		this.dequeStore = new ArrayDeque<>(initialCapacity);
	}

	public WorkerQueue() {
		dequeStore = new ArrayDeque<>();

	}

	public synchronized void addFirst(T element) {
		this.dequeStore.addFirst(element);
	}

	public synchronized void addLast(T element) {
		this.dequeStore.addLast(element);
	}

	public synchronized T pop() {
		return this.dequeStore.pop();
	}

	public synchronized void push(T element) {
		this.dequeStore.push(element);
	}

	public synchronized T peek() {
		return this.dequeStore.peek();
	}

	public synchronized int size() {
		return this.dequeStore.size();
	}

}
