package com.thisisnoble.javatest;

import static com.thisisnoble.javatest.util.TestIdGenerator.tradeEventId;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;

import com.thisisnoble.javatest.events.MarginEvent;
import com.thisisnoble.javatest.events.RiskEvent;
import com.thisisnoble.javatest.events.ShippingEvent;
import com.thisisnoble.javatest.events.TradeEvent;
import com.thisisnoble.javatest.impl.CompositeEvent;
import com.thisisnoble.javatest.impl.EventOrchestrator;
import com.thisisnoble.javatest.processors.MarginProcessor;
import com.thisisnoble.javatest.processors.RiskProcessor;
import com.thisisnoble.javatest.processors.ShippingProcessor;

public class SimpleOrchestratorTest {

	@Test
	public void tradeEventShouldTriggerAllProcessors() {
		TestPublisher testPublisher = new TestPublisher();
		Orchestrator orchestrator = setupOrchestrator();
		orchestrator.setup(testPublisher);

		TradeEvent te = new TradeEvent(tradeEventId(), 1000.0);
		orchestrator.receive(te);
		safeSleep(100);
		CompositeEvent ce = (CompositeEvent) testPublisher.getLastEvent();
		assertEquals(te, ce.getParent());
		assertEquals(5, ce.size());
		RiskEvent re1 = ce.getChildById("tradeEvt-riskEvt");
		assertNotNull(re1);
		assertEquals(50.0, re1.getRiskValue(), 0.01);
		MarginEvent me1 = ce.getChildById("tradeEvt-marginEvt");
		assertNotNull(me1);
		assertEquals(10.0, me1.getMargin(), 0.01);
		ShippingEvent se1 = ce.getChildById("tradeEvt-shipEvt");
		assertNotNull(se1);
		assertEquals(200.0, se1.getShippingCost(), 0.01);
		RiskEvent re2 = ce.getChildById("tradeEvt-shipEvt-riskEvt");
		assertNotNull(re2);
		assertEquals(10.0, re2.getRiskValue(), 0.01);
		MarginEvent me2 = ce.getChildById("tradeEvt-shipEvt-marginEvt");
		assertNotNull(me2);
		assertEquals(2.0, me2.getMargin(), 0.01);
	}

	@Test
	public void shippingEventShouldTriggerOnly2Processors() {
		TestPublisher testPublisher = new TestPublisher();
		Orchestrator orchestrator = setupOrchestrator();
		orchestrator.setup(testPublisher);

		ShippingEvent se = new ShippingEvent("ship2", 500.0);
		orchestrator.receive(se);
		safeSleep(100);
		CompositeEvent ce = (CompositeEvent) testPublisher.getLastEvent();
		assertEquals(se, ce.getParent());
		assertEquals(2, ce.size());
		RiskEvent re2 = ce.getChildById("ship2-riskEvt");
		assertNotNull(re2);
		assertEquals(25.0, re2.getRiskValue(), 0.01);
		MarginEvent me2 = ce.getChildById("ship2-marginEvt");
		assertNotNull(me2);
		assertEquals(5.0, me2.getMargin(), 0.01);
	}

	@Test
	public void tradeEventConcurrencyTest() {
		final TestPublisher testPublisher = new TestPublisher();
		final Orchestrator orchestrator = setupOrchestrator();
		orchestrator.setup(testPublisher);

		ExecutorService executorService = Executors.newFixedThreadPool(10);

		final List<Callable<String>> eventCallable = new ArrayList<Callable<String>>(
				10);

		final String tradeEventId = tradeEventId();

		for (int i = 0; i < 10; i++) {
			final String id = tradeEventId + i;
			Callable<String> callable = new Callable<String>() {
				public String call() {
					TradeEvent te = new TradeEvent(id, 1000.0);
					orchestrator.receive(te);
					return null;
				}
			};
			eventCallable.add(callable);
		}
		try {
			executorService.invokeAll(eventCallable);
		} catch (Exception e) {
			e.printStackTrace();
		}

		safeSleep(100);

		for (int i = 0; i < 10; i++) {
			String id = tradeEventId + i;
			CompositeEvent ce = (CompositeEvent) testPublisher
					.getEvent(id);
			assertEquals(5, ce.size());
			RiskEvent re1 = ce.getChildById(id + "-riskEvt");
			assertNotNull(re1);
			assertEquals(50.0, re1.getRiskValue(), 0.01);
			MarginEvent me1 = ce.getChildById(id + "-marginEvt");
			assertNotNull(me1);
			assertEquals(10.0, me1.getMargin(), 0.01);
			ShippingEvent se1 = ce.getChildById(id + "-shipEvt");
			assertNotNull(se1);
			assertEquals(200.0, se1.getShippingCost(), 0.01);
			RiskEvent re2 = ce.getChildById(id + "-shipEvt-riskEvt");
			assertNotNull(re2);
			assertEquals(10.0, re2.getRiskValue(), 0.01);
			MarginEvent me2 = ce.getChildById(id + "-shipEvt-marginEvt");
			assertNotNull(me2);
			assertEquals(2.0, me2.getMargin(), 0.01);
		}
	}

	private Orchestrator setupOrchestrator() {
		Orchestrator orchestrator = createOrchestrator();
		orchestrator.register(new RiskProcessor(orchestrator));
		orchestrator.register(new MarginProcessor(orchestrator));
		orchestrator.register(new ShippingProcessor(orchestrator));
		return orchestrator;
	}

	private void safeSleep(long l) {
		try {
			Thread.sleep(l);
		} catch (InterruptedException e) {
			// ignore
		}
	}

	private Orchestrator createOrchestrator() {
		return new EventOrchestrator();
	}
}