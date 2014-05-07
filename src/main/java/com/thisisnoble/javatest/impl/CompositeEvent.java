package com.thisisnoble.javatest.impl;


import com.thisisnoble.javatest.Event;


import java.util.HashMap;
import java.util.Map;


public class CompositeEvent implements Event {


    private final String id;
    private final Event parent;
    private final Map<String, Event> children = new HashMap<>();
    private int processorCount;

    public CompositeEvent(String id, Event parent) {
        this.id = id;
        this.parent = parent;
    }


    public String getId() {
        return id;
    }

    public String getParentId()
    {
    	return null;
    }

    public Event getParent() {
        return parent;
    }

    public CompositeEvent addChild(Event child) {
        children.put(child.getId(), child);
        return this;
    }


    @SuppressWarnings("unchecked")
    public <E extends Event> E getChildById(String id) {
        return (E) children.get(id);
    }

    public int size() {
        return children.size();
    }

    public void incrementProcessorCount() {
    	processorCount++;
    }
    
    public boolean haveAllChildrenArrived() {
    	return processorCount == size();
    }
}
