package com.thisisnoble.javatest;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


public class TestPublisher implements Publisher {


    private Event lastEvent;
    private ConcurrentMap<String, Event> eventMap = new ConcurrentHashMap<String, Event>();

    @Override
    public void publish(Event event) {
        this.lastEvent = event;
        eventMap.putIfAbsent(event.getId(), event);
    }


    public Event getLastEvent() {
        Event result = lastEvent;
        lastEvent = null;
        return result;
    }

    public Event getEvent(String id)
    {
    	return eventMap.get(id);
    }

}
