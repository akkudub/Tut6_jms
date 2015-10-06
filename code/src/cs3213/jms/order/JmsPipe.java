package cs3213.jms.order;

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
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

public class JmsPipe implements IPipe, MessageListener {

    // your code here

    private QueueConnectionFactory qconFactory;
    private QueueConnection qcon;
    private QueueSession qsession;
    private QueueSender qsender;
    private QueueReceiver qreceiver;
    private Queue queue;
    private TextMessage msg;
    private List<Order> orderQueue;

    JmsPipe(String factory, String queue){
    	orderQueue = new LinkedList<Order>();
    	InitialContext ic = getInitialContext();
        init(ic, queue, factory);
    }

    public void init(Context ctx, String queueName, String factory)
            throws NamingException, JMSException {
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

    private static InitialContext getInitialContext()
            throws NamingException {
        Properties props = new Properties();
        props.put(Context.INITIAL_CONTEXT_FACTORY, "org.jnp.interfaces.NamingContextFactory");
        props.put(Context.PROVIDER_URL, "jnp://localhost:1099");
        props.put(Context.URL_PKG_PREFIXES, "org.jboss.naming:org.jnp.interfaces");
        return new InitialContext(props);
    }

    @Override
    public void write(Order s) throws JMSException{
    	msg.setText(s.toString);
        qsender.send(msg);
    }
    
	@Override
    public Order read(){
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

            System.out.println("Message Received: " + msgText);

            if (msgText.equalsIgnoreCase("quit")) {
                synchronized (this) {
                    quit = true;
                    this.notifyAll(); // ask main thread to quit
                }
            }else{
            	Order newOrder = Order.fromString(msgText);
            	orderQueue.add(newOrder);
            }
        } catch (JMSException jmse) {
            System.err.println("An exception occurred: " + jmse.getMessage());
        }
    }

    @Override
    public void close(){
        qsender.close();
        qreceiver.close();
        qsession.close();
        qcon.close();
    }
    
}
