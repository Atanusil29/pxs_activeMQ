package com.opentext.exstream.projects.proximus.connector;

import java.io.File;
import java.rmi.RemoteException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import streamserve.connector.StrsConfigVals;
import streamserve.connector.StrsInConnectable;
import streamserve.connector.StrsInDataQueueable;

public class ActiveMQInputConnector implements StrsInConnectable {

	Logger log = LoggerFactory.getLogger(ActiveMQInputConnector.class);

	ClassPathXmlApplicationContext context;
	JmsConsumer jmsConsumer;

	public void setJmsConsumer(JmsConsumer jmsConsumer) {
		this.jmsConsumer = jmsConsumer;
	}

	private static final String PROPERTY_QUEUE_NAME = "queue.name";
	private static final String PROPERTY_RECEIVE_TIMEOUT = "receive.timeout";
	private static final String PROPERTY_TIME_LIMIT = "time.limit";
	private static final String PROPERTY_COUNT_LIMIT = "count.limit";

	private String queueName = "queue-01";
	private long receiveTimeout = 200;
	private long timeLimit = 1000;
	private int countLimit = 500;

	@Override
	public boolean strsiPoll(StrsInDataQueueable inDataQueue) throws RemoteException {
		log.debug("strsiPoll called");
		if (inDataQueue != null) {
			try {
				byte[] bytes = process();
				if (bytes != null) {
					inDataQueue.putArray(bytes);
					inDataQueue.signalEvent(StrsInDataQueueable.INEVENT_EOF);
				}
			} catch (Exception e) {
				log.error("Unable to collate inputs", e);
			}
			return true;
		}
		return false;
	}

	@Override
	public boolean strsiStart(StrsConfigVals configVals) throws RemoteException {
		log.info("strsiStart called");
		context = new ClassPathXmlApplicationContext("applicationContext.xml");
		JmsConsumer jmsConsumer = (JmsConsumer) context.getBean("jmsConsumer");
		setJmsConsumer(jmsConsumer);

		String tmp;
		tmp = configVals.getValue(PROPERTY_QUEUE_NAME);
		if (tmp != null && tmp.length() > 0) {
			queueName = tmp;
		}

		tmp = configVals.getValue(PROPERTY_RECEIVE_TIMEOUT);
		if (tmp != null && tmp.length() > 0) {
			receiveTimeout = Long.parseLong(tmp);
		}

		tmp = configVals.getValue(PROPERTY_TIME_LIMIT);
		if (tmp != null && tmp.length() > 0) {
			timeLimit = Long.parseLong(tmp);
		}

		tmp = configVals.getValue(PROPERTY_COUNT_LIMIT);
		if (tmp != null && tmp.length() > 0) {
			countLimit = Integer.parseInt(tmp);
		}

		log.info("configuration: {}, {}, {}, {}", queueName, receiveTimeout, timeLimit, countLimit);
		log.info("connector is ready");
		return true;
	}

	@Override
	public boolean strsiStop() throws RemoteException {
		log.info("strsiStop called");
		if (context != null) {
			context.close();
		}
		context = null;
		jmsConsumer = null;
		return true;
	}

	private byte[] process() throws Exception {
		List<String> messages = jmsConsumer.receiveAllMessages(queueName, receiveTimeout, timeLimit, countLimit);
		if (messages == null || messages.size() == 0) {
			return null;
		}

		log.info("Received {} customers from {}", messages.size(), queueName);

		//Merge driver files together into a single data stream
		StringBuffer data = new StringBuffer();
		for (String driver : messages) {
			data.append(driver);
			data.append(",\r\n");	//append a comma & line-break in between each JSON file 
		}
		data.delete(data.length()-3,data.length()); //remove last comma & line-break from JSON file
		return data.toString().getBytes("UTF-8");
	}

	public static void main(String args[]) {
		ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("applicationContext.xml");
		try {
			JmsConsumer jmsConsumer = (JmsConsumer) context.getBean("jmsConsumer");
			ActiveMQInputConnector connector = new ActiveMQInputConnector();
			connector.setJmsConsumer(jmsConsumer);
			byte[] bytes = connector.process();
			if (bytes != null) {
				File file = new File("sample.json");
				FileUtils.writeByteArrayToFile(file, bytes);
				System.out.println("Output file: " + file.getAbsolutePath());
			} else {
				System.out.println("Nothing has been generated");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			context.close();
		}
	}

}
