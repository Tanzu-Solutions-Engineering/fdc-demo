package com.pivotal.example.xd;

import java.io.IOException;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.cloud.Cloud;
import org.springframework.cloud.CloudException;
import org.springframework.cloud.CloudFactory;
import org.springframework.cloud.service.ServiceInfo;
import org.springframework.cloud.service.common.RabbitServiceInfo;

import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.pivotal.example.xd.controller.OrderController;
import com.rabbitmq.client.ConnectionFactory;

public class RabbitClient {

	static Logger logger = Logger.getLogger(RabbitClient.class);
	private static RabbitClient instance;
	private CachingConnectionFactory ccf;
	private Queue orderQueue;
	private Queue orderProcQueue;
	private RabbitTemplate rabbitTemplate;

	private static final String EXCHANGE_NAME = "ORDERS_EXCHANGE";
	private static final String ORDER_PROCESSING_QUEUE = "ORDERS_QUEUE";
	private static final String ORDER_RECEIVING_QUEUE = "ORDERS_RECEIVING_QUEUE";

	Connection connection;
	private String rabbitURI;

	private RabbitClient() {

		try {
			logger.info("Getting cloud instance...");
			Cloud cloud = new CloudFactory().getCloud();
			Iterator<ServiceInfo> services = cloud.getServiceInfos().iterator();
			while (services.hasNext()) {
				ServiceInfo svc = services.next();
				logger.info("**** Looking for rabbit service..." + svc.getId() + 
						" " + svc.toString());
				
				if (svc instanceof RabbitServiceInfo) {
					logger.info("Found rabbit instance...");
					RabbitServiceInfo rabbitSvc = ((RabbitServiceInfo) svc);
					rabbitURI = rabbitSvc.getUri();
					/*
					ApplicationEnvironment ae= new ApplicationEnvironment();
					System.out.println("Application Env: " + ae.toString());
					*/
					System.out.println("RabbitURI: " + rabbitURI);
					try {
						logger.info("Getting connection factory...");
						ConnectionFactory factory = new ConnectionFactory();
						logger.info("Setting rabbitURI " + rabbitURI);
						factory.setUri(rabbitURI);
						logger.info("Creating caching connection factory");
						ccf = new CachingConnectionFactory(factory);
						logger.info("Creating connection");
						connection = ccf.createConnection();

						logger.info("Creating fanout exchange..." + EXCHANGE_NAME);
						FanoutExchange fanoutExchange = new FanoutExchange(
								EXCHANGE_NAME, false, true);
						logger.info("Creating rabbit admin...");
						RabbitAdmin rabbitAdmin = new RabbitAdmin(ccf);
						logger.info("Declaring fanoutExchange...");
						rabbitAdmin.declareExchange(fanoutExchange);

						// orderQueue = new AnonymousQueue();
						logger.info("Creating queue..." + ORDER_RECEIVING_QUEUE);
						orderQueue = new Queue(ORDER_RECEIVING_QUEUE);
						logger.info("Declaring order queue...");
						rabbitAdmin.declareQueue(orderQueue);
						logger.info("Binding order queue to fanout exchange...");
						rabbitAdmin.declareBinding(BindingBuilder.bind(
								orderQueue).to(fanoutExchange));

						logger.info("Creating new queue..." + ORDER_PROCESSING_QUEUE);
						orderProcQueue = new Queue(ORDER_PROCESSING_QUEUE);
						logger.info("Declaring order processing queue...");
						rabbitAdmin.declareQueue(orderProcQueue);
						logger.info("Binding order processing queue to fanout exchange...");
						rabbitAdmin.declareBinding(BindingBuilder.bind(
								orderProcQueue).to(fanoutExchange));

						logger.info("Getting rabbit admin template...");
						rabbitTemplate = rabbitAdmin.getRabbitTemplate();
						logger.info("Setting exchange on rabbit template..." + EXCHANGE_NAME);
						rabbitTemplate.setExchange(EXCHANGE_NAME);
						logger.info("Setting connection factory on rabbit template...");
						rabbitTemplate.setConnectionFactory(ccf);

						rabbitTemplate.afterPropertiesSet();

					} catch (Exception e) {
						throw new RuntimeException(
								"Exception connecting to RabbitMQ", e);
					}

				}
			}
		} catch (CloudException ce) {
			// means its not being deployed on Cloud
			logger.warn(ce.getMessage());
		}

	}

	public static synchronized RabbitClient getInstance() {
		if (instance == null) {
			instance = new RabbitClient();
		}
		return instance;
	}

	public synchronized void post(Order order) throws IOException {
		logger.info("Sending order in byte form..." + order);
		rabbitTemplate.send(new Message(order.toBytes(),
				new MessageProperties()));
	}

	public void startMessageListener() {
		logger.info("In startMessageListener...");
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(
				ccf);
		container.setQueues(orderQueue);
		container.setMessageListener(new MessageListener() {

			@Override
			public void onMessage(Message message) {
				// System.out.println(message.getBody());
				// Order order = Order.fromBytes(message.getBody()); // Error
				// here
				logger.info("In onMessage (startMessageListener)...");
				Order order = new Order();
				String orderStr = new String(message.getBody());
				logger.info("*** Listening Order: " + orderStr);
				orderStr = orderStr.replaceAll("\\r", "");
				logger.info("*** Order string after replacing special chars: " + orderStr);
				
				logger.info("Creating new gson object...");
				Gson gson = new Gson();
				try {
					logger.info("fromJson... " + orderStr);
					order = gson.fromJson(orderStr, Order.class);
					if(order != null) {
						logger.info("registerOrder..." + order);
						OrderController.registerOrder(order);
						logger.info("Printing order... " + order);
					}	
					else {
						logger.error("Object Order is null");
					}
				} catch (JsonIOException e) {
					logger.error("Listening JsonIOException..." + orderStr);
					e.printStackTrace();
				} catch (JsonSyntaxException e) {
					logger.error("Listening JsonSyntaxException..." + orderStr);
					e.printStackTrace();
				} catch (Exception e) {
					logger.error("Listening Exception..." + orderStr);
					logger.error("Error Message: " + e.getMessage());
					e.printStackTrace();
				}

				/*
				 * try {
				 * 
				 * order = new ObjectMapper().readValue(orderStr, Order.class);
				 * } catch (JsonParseException e) { // TODO Auto-generated catch
				 * block e.printStackTrace(); } catch (JsonMappingException e) {
				 * // TODO Auto-generated catch block e.printStackTrace(); }
				 * catch (IOException e) { // TODO Auto-generated catch block
				 * e.printStackTrace(); }
				 */

			}
		});
		logger.info("Setting acknowledge mode to AUTO...");
		container.setAcknowledgeMode(AcknowledgeMode.AUTO);
		logger.info("Starting container...");
		container.start();

	}

	public void startOrderProcessing() {
		logger.info("In startOrderProcessing...");
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(
				ccf);
		container.setQueues(orderProcQueue);
		container.setMessageListener(new MessageListener() {

			@Override
			public void onMessage(Message message) {
				// for now simply log the order
				// Order order = Order.fromBytes(message.getBody()); //Error
				// here
				// Order order = new Order();
				// System.out.println("Processing Order: " + order);
				// logger.info("Process Order: " +
				// order.getState()+":"+order.getAmount());
				logger.info("In onMessage (startOrderProcessing)...");
				Order order = new Order();
				String orderStr = new String(message.getBody());
				logger.info("*** Processing Order String: " + orderStr);
				orderStr = orderStr.replaceAll("\\r", "");
				logger.info("*** Order string after replacing special chars: " + orderStr);
				try {
					logger.info("Creating new gson object...");
					Gson gson = new Gson();
					logger.info("Created new Gson object...");
					order = gson.fromJson(orderStr, Order.class);
					logger.info("*** Processing Order Object: " + order);
				} catch (JsonIOException e) {
					logger.error("Processing JsonIOException..." + orderStr);
					e.printStackTrace();
				} catch (JsonSyntaxException e) {
					logger.error("Processing JsonSyntaxException..." + orderStr);
					e.printStackTrace();
				} catch (Exception e) {
					logger.error("Processing Exception..." + orderStr);
					e.printStackTrace();
				}
				/*
				 * try { order = new ObjectMapper().readValue(orderStr,
				 * Order.class); } catch (JsonParseException e) { // TODO
				 * Auto-generated catch block e.printStackTrace(); } catch
				 * (JsonMappingException e) { // TODO Auto-generated catch block
				 * e.printStackTrace(); } catch (IOException e) { // TODO
				 * Auto-generated catch block e.printStackTrace(); }
				 */
				// String orderStr = new String(message.getBody());
				logger.info("Processing Order: " + orderStr);
			}
		});
		logger.info("Setting container acknowledge mode to AUTO");
		container.setAcknowledgeMode(AcknowledgeMode.AUTO);
		logger.info("Starting container...");
		container.start();

	}

	public boolean isBound() {
		return (rabbitURI != null);
	}

	public String getRabbitURI() {
		return rabbitURI;
	}
}
