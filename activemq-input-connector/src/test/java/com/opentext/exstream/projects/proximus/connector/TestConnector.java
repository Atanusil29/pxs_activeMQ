package com.opentext.exstream.projects.proximus.connector;

import java.io.File;
import java.io.IOException;
import java.rmi.RemoteException;

import javax.jms.ConnectionFactory;
import javax.jms.Message;

import org.apache.activemq.junit.EmbeddedActiveMQBroker;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.core.JmsTemplate;

import streamserve.connector.StrsInDataQueueable;
import streamserve.connector.TestConfigVals;
import streamserve.connector.TestInDataQueue;



public class TestConnector {
	@Rule
	public EmbeddedActiveMQBroker embeddedBroker = new EmbeddedActiveMQBroker();

	private Logger log = LoggerFactory.getLogger(ActiveMQInputConnector.class);
	
	private static final String queueName = "queue-01";
	private static final File outputFolder  = new File("target/test-output");
	
	private ConnectionFactory connectionFactory;
	private JmsTemplate jmsTemplate;

	private ActiveMQInputConnector connector;
	private TestConfigVals config;

	@Before
	public void initialise() throws RemoteException {
		outputFolder.mkdirs();
		config = new TestConfigVals();
		config.setValue(ActiveMQInputConnector.PROPERTY_QUEUE_NAME, queueName);
		config.setValue(ActiveMQInputConnector.PROPERTY_RECEIVE_TIMEOUT, "200");
		config.setValue(ActiveMQInputConnector.PROPERTY_TIME_LIMIT, "1000");
		config.setValue(ActiveMQInputConnector.PROPERTY_COUNT_LIMIT, "500");

		connectionFactory = embeddedBroker.createConnectionFactory();
		jmsTemplate = new JmsTemplate(connectionFactory);
	}
	
	@Test
	public void testQueueReadSmall() {
		File outputFile = new File(outputFolder, "test-connector-small.json");
		TestInDataQueue inDataQueue = new TestInDataQueue();

		try {
			generateTestMessages(20);

			connector = new ActiveMQInputConnector();
			connector.strsiStart(config);
			connector.strsiPoll(inDataQueue);

			byte[] bytes = inDataQueue.getBytes();

			Assert.assertNotEquals(0, bytes.length);
			Assert.assertEquals(StrsInDataQueueable.INEVENT_EOF, inDataQueue.getSignal());

			writeTestOutput(bytes, outputFile);

		} catch (Exception e) {
			log.error("Exception: {}", e.getMessage(), e);
		}

	}
	@Test
	public void testQueueReadLarge() {
		File outputFile = new File(outputFolder, "test-connector-large.json");
		TestInDataQueue inDataQueue = new TestInDataQueue();

		try {
			generateTestMessages(200);
			
			connector = new ActiveMQInputConnector();
			connector.strsiStart(config);
			connector.strsiPoll(inDataQueue);

			byte[] bytes = inDataQueue.getBytes();

			Assert.assertNotEquals(0, bytes.length);
			Assert.assertEquals(StrsInDataQueueable.INEVENT_EOF, inDataQueue.getSignal());
			
			writeTestOutput(bytes, outputFile);

		} catch (Exception e) {
			log.error("Exception: {}", e.getMessage(), e);
		}

	}


	@Test
	public void testEmptyQueue() {
		File outputFile = new File(outputFolder, "test-connector-empty.json");
		TestInDataQueue inDataQueue = new TestInDataQueue();
		
		try {
			generateTestMessages(0);

			connector = new ActiveMQInputConnector();
			connector.strsiStart(config);
			connector.strsiPoll(inDataQueue);

			byte[] bytes = inDataQueue.getBytes();
			
			Assert.assertEquals(0, bytes.length);
			Assert.assertNotEquals(StrsInDataQueueable.INEVENT_EOF, inDataQueue.getSignal());
			
			writeTestOutput(bytes, outputFile);
			
		} catch (Exception e) {
			log.error("Exception: {}", e.getMessage(), e);
		}

	}
	
	private void generateTestMessages(int count) {
		clearQueue(jmsTemplate, queueName);

		for (int i=1; i<=count; i++) {
			jmsTemplate.convertAndSend(queueName, String.format("{\"test\":\"test %d\"}",i));
		}
	}

	private void writeTestOutput(byte[] bytes, File outputFile) throws IOException {
		if (bytes != null) {
			FileUtils.writeByteArrayToFile(outputFile, bytes);
			log.info("Output file: " + outputFile.getAbsolutePath());
			//log.debug("Output content: {}", new String(bytes));
		} else {
			log.error("Nothing has been generated");
		}
	}
	
	private void clearQueue(JmsTemplate jmsTemplate, String queueName) {
		jmsTemplate.setReceiveTimeout(1);
		while (true) {
			Message m = jmsTemplate.receive(queueName);
			if (m == null) break;
		}
	}
	
}
