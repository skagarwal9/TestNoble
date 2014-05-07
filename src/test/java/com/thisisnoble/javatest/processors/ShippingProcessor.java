package com.thisisnoble.javatest.processors;


import static com.thisisnoble.javatest.util.TestIdGenerator.shipEventId;

import com.thisisnoble.javatest.Event;
import com.thisisnoble.javatest.Orchestrator;
import com.thisisnoble.javatest.events.ShippingEvent;
import com.thisisnoble.javatest.events.TradeEvent;


public class ShippingProcessor extends AbstractProcessor {


    public ShippingProcessor(Orchestrator orchestrator) {
        super(orchestrator);
    }


    @Override
    public boolean interestedIn(Event event) {
        return event instanceof TradeEvent;
    }


    @Override
    protected Event processInternal(Event event) {
        String parId = event.getId();
        if (event instanceof TradeEvent)
            return new ShippingEvent(shipEventId(parId), parId, calculateTradeShipping(event));
        throw new IllegalArgumentException("unknown event for shipping " + event);
    }


    private double calculateTradeShipping(Event te) {
        return ((TradeEvent) te).getNotional() * 0.2;
    }
}
