package com.opentext.exstream.projects.proximus.connector;

import java.util.ArrayList;
import java.util.List;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.core.JmsTemplate;

public class JmsConsumer {

	Logger log = LoggerFactory.getLogger(JmsConsumer.class);

	private JmsTemplate jmsTemplate;

	public JmsTemplate getJmsTemplate() {
		return jmsTemplate;
	}

	public void setJmsTemplate(JmsTemplate jmsTemplate) {
		this.jmsTemplate = jmsTemplate;
	}

	public List<String> receiveAllMessages(String destination, long receiveTimeout, long timeLimit, int countLimit) throws JMSException {
		jmsTemplate.setReceiveTimeout(receiveTimeout);

		List<String> messages = new ArrayList<String>();
		Message message;
		int count = 0;
		long startTime = System.currentTimeMillis();
		while (true) {
			message = jmsTemplate.receive(destination);
			if (message == null) {
				log.debug("Receive timeout {} for {}", receiveTimeout, destination);
				break;
			}
			TextMessage textMessage = (TextMessage) message;
			messages.add(textMessage.getText());
			count++;
			if ((countLimit > 0) && (count >= countLimit)) {
				log.warn("Reached count limit {} for {}", countLimit, destination);
				break;
			}
			if ((timeLimit > 0) && (System.currentTimeMillis() - startTime >= timeLimit)) {
				log.warn("Reached time limit {} for {}", timeLimit, destination);
				break;
			}
		}

		return messages;
	}

}
