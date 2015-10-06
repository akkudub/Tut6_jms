
/**
 * Matric 1: A0103516U
 * Name   1: Akshat Dubey
 * 
 * Matric 2: A0102800A
 * Name   2: Suranjana Sengupta
 *
 * This file implements a pipe that transfer messages using JMS.
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import javax.jms.*;
import javax.jms.Queue;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.*;

public class JmsPipe implements IPipe, MessageListener {

	// your code here

	private QueueConnectionFactory qconFactory;
	private QueueConnection qcon;
	private QueueSession qsession;
	private QueueSender qsender;
	private QueueReceiver qreceiver;
	private Queue queue;
	private TextMessage msg;
	private LinkedList<Order> orderQueue;

	JmsPipe(String factory, String queue) throws Exception {
		orderQueue = new LinkedList<Order>();
		InitialContext ic = getInitialContext();
		init(ic, queue, factory);
	}

	public void init(Context ctx, String queueName, String factory) throws NamingException, JMSException {
		qconFactory = (QueueConnectionFactory) ctx.lookup(factory);
		qcon = qconFactory.createQueueConnection();
		qsession = qcon.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
		queue = (Queue) ctx.lookup(queueName);
		qreceiver = qsession.createReceiver(queue);
		qreceiver.setMessageListener(this);
		qsender = qsession.createSender(queue);
		msg = qsession.createTextMessage();
		qcon.start();
	}

	private static InitialContext getInitialContext() throws NamingException {
		Properties props = new Properties();
		props.put(Context.INITIAL_CONTEXT_FACTORY, "org.jnp.interfaces.NamingContextFactory");
		props.put(Context.PROVIDER_URL, "jnp://localhost:1099");
		props.put(Context.URL_PKG_PREFIXES, "org.jboss.naming:org.jnp.interfaces");
		return new InitialContext(props);
	}

	@Override
	public void write(Order s){
		try{
		msg.setText(s.toString());
		qsender.send(msg);
		}catch (JMSException jmse){
			System.err.println("An exception occurred: " + jmse.getMessage());
		}
	}

	@Override
	public Order read() {
		if (!orderQueue.isEmpty()) {
			return orderQueue.remove();
		}
		return null;
	}

	@Override
	public void onMessage(Message msg) {
		try {
			String msgText;
			if (msg instanceof TextMessage) {
				msgText = ((TextMessage) msg).getText();
			} else {
				msgText = msg.toString();
			}

			System.out.println("Message Received by JMSPipe: " + msgText);

			Order newOrder = Order.fromString(msgText);
			orderQueue.add(newOrder);

		} catch (JMSException jmse) {
			System.err.println("An exception occurred: " + jmse.getMessage());
		}
	}

	@Override
	public void close() {
		try{
			qsender.close();
			qreceiver.close();
			qsession.close();
			qcon.close();
		} catch (JMSException jmse){
			System.err.println("An exception occurred: " + jmse.getMessage());
		}
	}

}
