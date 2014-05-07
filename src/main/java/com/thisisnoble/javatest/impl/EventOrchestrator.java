package com.thisisnoble.javatest.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.thisisnoble.javatest.Event;
import com.thisisnoble.javatest.Orchestrator;
import com.thisisnoble.javatest.Processor;
import com.thisisnoble.javatest.Publisher;

public class EventOrchestrator implements Orchestrator {

	private List<Processor> eventProcessors = new ArrayList<Processor>();

	private Publisher publisher;

	private final ConcurrentMap<String, CompositeEvent> eventMap = new ConcurrentHashMap<String, CompositeEvent>();

	/*
	 * Register all processors during initialization.
	 * 
	 * @see
	 * com.thisisnoble.javatest.Orchestrator#register(com.thisisnoble.javatest
	 * .Processor)
	 */
	public void register(Processor processor) {
		synchronized (eventProcessors) {
			eventProcessors.add(processor);
		}
	}

	/**
	 * Receive events to be processed as well as processed children.
	 */
	public void receive(Event event) {
		CompositeEvent parent = getOrCreateCompositeEvent(event);

		synchronized (parent) {
			if (event.getParentId() != null) {
				parent.addChild(event);
			}
			processEvent(parent, event);
			if (parent.haveAllChildrenArrived()) {
				publisher.publish(parent);
			}
		}
	}

	/**
	 * Trigger each processor that may be interested in processing the event
	 * 
	 * @param parent
	 * @param event
	 */
	private void processEvent(CompositeEvent parent, Event event) {
		for (Processor processor : eventProcessors) {
			if (processor.interestedIn(event)) {
				parent.incrementProcessorCount();
				processor.process(event);
			}
		}
	}

	/**
	 * Create composite if it is a top level event else get the composite
	 * 
	 * @param event
	 * @return
	 */
	private CompositeEvent getOrCreateCompositeEvent(Event event) {
		CompositeEvent parent;
		if (event.getParentId() == null) {
			parent = new CompositeEvent(event.getId(), event);
			Event existing = eventMap.putIfAbsent(event.getId(), parent);
			if (existing != null) {
				throw new RuntimeException(
						"Unexpected. Multiple events with the same id");
			}
		} else {
			parent = eventMap.get(getTopLevelParentId(event.getParentId()));
			if (parent == null) {
				throw new RuntimeException("Parent Event Notfound "
						+ event.getParentId());
			}
		}
		return parent;
	}

	/**
	 * Parse the event id to get the id of the top level parent. upto the first
	 * hyphen.
	 * 
	 * @param parentId
	 * @return
	 */
	private String getTopLevelParentId(String parentId) {
		int x = parentId.indexOf('-');
		if (x > 0) {
			return parentId.substring(0, x);
		} else {
			return parentId;
		}
	}

	public void setup(Publisher publisher) {
		this.publisher = publisher;
	}

}
