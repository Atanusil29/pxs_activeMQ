package com.opentext.exstream.projects.proximus.connector;

import java.io.File;
import java.io.IOException;

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



public class TestProcess {
	@Rule
	public EmbeddedActiveMQBroker embeddedBroker = new EmbeddedActiveMQBroker();

	private Logger log = LoggerFactory.getLogger(ActiveMQInputConnector.class);
	
	private static final String queueName = "queue-01";
	private static final File outputFolder  = new File("target/test-output");
	
	private ConnectionFactory connectionFactory;
	private JmsTemplate jmsTemplate;
	private JmsConsumer jmsConsumer;
	private ActiveMQInputConnector connector;

	@Before
	public void initialise() {
		outputFolder.mkdirs();
		
		connectionFactory = embeddedBroker.createConnectionFactory();
		jmsTemplate = new JmsTemplate(connectionFactory);
		jmsConsumer = new JmsConsumer();
		jmsConsumer.setJmsTemplate(jmsTemplate);

		connector = new ActiveMQInputConnector();
		connector.setJmsConsumer(jmsConsumer);
	}
	
	@Test
	public void testQueueReadSmall() {
		File outputFile = new File(outputFolder, "test-process-small.json");

		try {
			generateTestMessages(20);
			
			byte[] bytes = connector.process();

			Assert.assertNotEquals(null, bytes);

			writeTestOutput(bytes, outputFile);

		} catch (Exception e) {
			log.error("Exception: {}", e.getMessage(), e);
		}

	}
	@Test
	public void testQueueReadLarge() {
		File outputFile = new File(outputFolder, "test-process-large.json");

		try {
			generateTestMessages(200);
			
			byte[] bytes = connector.process();

			Assert.assertNotEquals(null, bytes);
			
			writeTestOutput(bytes, outputFile);

		} catch (Exception e) {
			log.error("Exception: {}", e.getMessage(), e);
		}

	}


	@Test
	public void testEmptyQueue() {
		File outputFile = new File(outputFolder, "test-process-empty.json");
		
		try {
			generateTestMessages(0);

			byte[] bytes = connector.process();
			
			Assert.assertNull(bytes);
			
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
